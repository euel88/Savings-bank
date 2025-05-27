# bank_scraper_headless.py
"""
저축은행 중앙회 통일경영공시 데이터 자동 스크래핑 도구 (GitHub Actions 최적화 버전)
목적: GitHub Actions에서 자동 실행, 병렬 처리를 통한 속도 개선
작성일: 2025-03-31 (최종 수정일: 2025-05-28)
특징:
- GUI 없음, CLI 기반 실행
- asyncio 및 ThreadPoolExecutor를 사용한 병렬 처리 (Semaphore로 동시 작업 제어)
- GitHub Actions 환경에 최적화된 WebDriver 설정
- 환경 변수를 통한 주요 설정 관리
- 자동 재시도 및 강화된 에러 핸들링
- 이메일 알림 기능 (은행별 공시 날짜 포함)
- 실행 시간 단축을 위한 대기 시간 최적화
- 공시 날짜 확인 및 경고 기능 추가
"""

import os
import sys
import time
import random
import json
import re
import asyncio
import concurrent.futures # 명시적으로 사용하진 않지만 run_in_executor의 기본값으로 활용됨
import zipfile
from datetime import datetime
from io import StringIO
import argparse # 현재는 직접 사용하지 않으나, 향후 CLI 옵션 확장 가능성 있음
import logging
from pathlib import Path
import queue # 드라이버 풀 관리를 위해 추가

# 이메일 전송 관련 임포트
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

# Selenium 관련 임포트
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException
from selenium.webdriver.chrome.options import Options

# 데이터 처리 관련 임포트
from bs4 import BeautifulSoup # pd.read_html(flavor='bs4') 에서 사용됨
import pandas as pd
from tqdm import tqdm # CLI 진행률 표시
import warnings
warnings.filterwarnings("ignore") # 경고 메시지 무시

# --- 로깅 설정 ---
def setup_logging(log_file_path, log_level="INFO"):
    """로깅 시스템을 설정합니다."""
    # 루트 로거의 기존 핸들러 제거 (중복 로깅 방지)
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
        
    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S', # 날짜 형식 추가
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_file_path, encoding='utf-8')
        ]
    )
    # 서드파티 라이브러리 로거 레벨 조정
    logging.getLogger('selenium.webdriver.remote.remote_connection').setLevel(logging.WARNING)
    logging.getLogger('urllib3.connectionpool').setLevel(logging.WARNING)
    return logging.getLogger(__name__) # 현재 모듈의 로거 반환

logger = None # Config 초기화 후 설정됨

# --- 이메일 전송 클래스 ---
class EmailSender:
    def __init__(self):
        self.smtp_server = "smtp.gmail.com"
        self.smtp_port = 587
        self.sender_email = os.getenv('GMAIL_ADDRESS')
        self.sender_password = os.getenv('GMAIL_APP_PASSWORD')
        self.recipient_emails = [email.strip() for email in os.getenv('RECIPIENT_EMAILS', '').split(',') if email.strip()]
        
        self.enabled = bool(self.sender_email and self.sender_password and self.recipient_emails)
        if not self.enabled:
            if logger: logger.warning("이메일 설정(GMAIL_ADDRESS, GMAIL_APP_PASSWORD, RECIPIENT_EMAILS)이 유효하지 않아 이메일 전송을 건너뜁니다.")
        else:
            if logger: logger.info(f"이메일 전송 설정 완료. 수신자: {', '.join(self.recipient_emails)}")

    def send_email_with_attachment(self, subject, body, attachment_path=None):
        if not self.enabled:
            if logger: logger.info("이메일 전송이 비활성화되어 있습니다.")
            return False
        
        try:
            msg = MIMEMultipart()
            msg['From'] = self.sender_email
            msg['To'] = ', '.join(self.recipient_emails)
            msg['Subject'] = subject
            msg.attach(MIMEText(body, 'html', 'utf-8')) # 인코딩 명시
            
            if attachment_path and Path(attachment_path).exists(): # Path 객체 사용
                with open(attachment_path, 'rb') as attachment_file:
                    part = MIMEBase('application', 'octet-stream')
                    part.set_payload(attachment_file.read())
                    encoders.encode_base64(part)
                    filename_encoded = f"\"{os.path.basename(attachment_path)}\""
                    try: 
                        filename_encoded = encoders.encode_rfc2231(os.path.basename(attachment_path))
                        part.add_header('Content-Disposition', 'attachment', filename=filename_encoded)
                    except:
                        part.add_header('Content-Disposition', f'attachment; filename="{os.path.basename(attachment_path)}"')
                    msg.attach(part)
                if logger: logger.info(f"첨부 파일 추가: {attachment_path}")
            
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.ehlo() 
                server.starttls() 
                server.ehlo() 
                server.login(self.sender_email, self.sender_password)
                server.send_message(msg)
            if logger: logger.info(f"이메일 전송 성공: {', '.join(self.recipient_emails)}")
            return True
        except Exception as e:
            if logger: logger.error(f"이메일 전송 실패: {str(e)}", exc_info=True)
            return False

# --- 설정 클래스 ---
class Config:
    def __init__(self):
        self.VERSION = "2.8-opt-emaildate" 
        self.BASE_URL = "https://www.fsb.or.kr/busmagequar_0100.act"
        self.MAX_RETRIES = int(os.getenv('MAX_RETRIES', '2'))
        self.PAGE_LOAD_TIMEOUT = int(os.getenv('PAGE_LOAD_TIMEOUT', '20')) 
        self.WAIT_TIMEOUT = int(os.getenv('WAIT_TIMEOUT', '10')) 
        self.MAX_WORKERS = int(os.getenv('MAX_WORKERS', '3')) 

        self.today = datetime.now().strftime("%Y%m%d")
        self.output_dir_base = Path(os.getenv('OUTPUT_DIR', "./output"))
        self.output_dir = self.output_dir_base / f"저축은행_데이터_{self.today}"
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.progress_file = self.output_dir / 'progress.json'
        self.log_file_path = self.output_dir / f'scraping_log_{self.today}.log'

        global logger
        if logger is None: 
            logger = setup_logging(self.log_file_path, os.getenv('LOG_LEVEL', 'INFO'))

        self.BANKS = [
            "다올", "대신", "더케이", "민국", "바로", "스카이", "신한", "애큐온", "예가람", "웰컴",
            "유안타", "조은", "키움YES", "푸른", "하나", "DB", "HB", "JT", "친애", "KB",
            "NH", "OK", "OSB", "SBI", "금화", "남양", "모아", "부림", "삼정", "상상인",
            "세람", "안국", "안양", "영진", "융창", "인성", "인천", "키움", "페퍼", "평택",
            "한국투자", "한화", "고려", "국제", "동원제일", "솔브레인", "에스앤티", "우리", "조흥", "진주",
            "흥국", "BNK", "DH", "IBK", "대백", "대아", "대원", "드림", "라온", "머스트", "삼일",
            "엠에스", "오성", "유니온", "참", "CK", "대한", "더블", "동양", "삼호",
            "센트럴", "스마트", "스타", "대명", "상상인플러스", "아산", "오투", "우리금융", "청주", "한성"
        ]
        self.CATEGORIES = ["영업개황", "재무현황", "손익현황", "기타"]
        
        logger.info(f"--- 설정 초기화 완료 (v{self.VERSION}) ---")
        logger.info(f"출력 디렉토리: {self.output_dir.resolve()}")
        logger.info(f"로그 파일: {self.log_file_path.resolve()}")
        logger.info(f"워커 수: {self.MAX_WORKERS}, 재시도: {self.MAX_RETRIES}")
        logger.info(f"페이지 타임아웃: {self.PAGE_LOAD_TIMEOUT}s, 요소 대기 타임아웃: {self.WAIT_TIMEOUT}s")

# --- 웹드라이버 관리 클래스 ---
class DriverManager:
    def __init__(self, config):
        self.config = config
        self.driver_pool = queue.Queue(maxsize=self.config.MAX_WORKERS)
        self._initialize_pool()

    def _create_new_driver(self):
        logger.debug("새 WebDriver 인스턴스 생성 시도...")
        options = Options()
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        options.add_argument('--window-size=1920,1080')
        options.add_argument('user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
        options.add_argument('--disable-extensions')
        options.add_argument('--disable-browser-side-navigation') # 일부 환경에서 안정성 향상
        options.add_argument('--disable-infobars')
        options.add_argument('--disable-notifications')
        options.add_argument('--disable-popup-blocking')
        options.add_argument('--blink-settings=imagesEnabled=false') # 이미지 로딩 비활성화
        options.add_experimental_option('excludeSwitches', ['enable-logging', 'enable-automation'])
        options.add_experimental_option('useAutomationExtension', False)
        
        driver = webdriver.Chrome(options=options)
        driver.set_page_load_timeout(self.config.PAGE_LOAD_TIMEOUT)
        logger.debug("새 WebDriver 인스턴스 생성 완료.")
        return driver

    def _initialize_pool(self):
        logger.info(f"드라이버 풀 초기화 시작 (최대 {self.config.MAX_WORKERS}개)...")
        for i in range(self.config.MAX_WORKERS):
            try:
                driver = self._create_new_driver()
                self.driver_pool.put_nowait(driver) 
                logger.debug(f"드라이버 {i+1} 생성하여 풀에 추가. 현재 풀 크기: {self.driver_pool.qsize()}")
            except queue.Full:
                logger.warning(f"드라이버 {i+1} 추가 시도 중 풀이 꽉 참.")
                break 
            except Exception as e:
                logger.error(f"초기 드라이버 {i+1} 생성 실패: {e}", exc_info=True)
        logger.info(f"드라이버 풀 초기화 완료. 사용 가능 드라이버: {self.driver_pool.qsize()}개.")

    def get_driver(self):
        try:
            driver = self.driver_pool.get(block=True, timeout=60) 
            logger.debug(f"풀에서 드라이버 가져옴. 남은 드라이버: {self.driver_pool.qsize()}")
            return driver
        except queue.Empty:
            logger.error(f"60초 대기 후에도 풀에서 드라이버를 가져오지 못함 (MAX_WORKERS: {self.config.MAX_WORKERS}).")
            raise TimeoutError("드라이버 풀에서 드라이버를 가져오는 데 실패했습니다.")

    def return_driver(self, driver):
        if driver:
            returned_successfully = False
            try:
                _ = driver.title 
                if self.driver_pool.qsize() < self.config.MAX_WORKERS:
                    self.driver_pool.put_nowait(driver) 
                    returned_successfully = True
                    logger.debug(f"사용된 드라이버 풀에 반환. 현재 풀 크기: {self.driver_pool.qsize()}")
                else:
                    logger.warning(f"드라이버 풀이 이미 꽉 차있어({self.driver_pool.qsize()}), 반환 시도한 드라이버를 종료합니다.")
                    driver.quit()
            except queue.Full: 
                logger.warning(f"드라이버 반납 시 풀이 꽉 참(Full). 드라이버를 종료합니다. 현재 풀 크기: {self.driver_pool.qsize()}")
                driver.quit()
            except Exception as e:
                logger.warning(f"손상된 드라이버 반환 시도 ({type(e).__name__}: {e}). 드라이버를 종료합니다.")
                try:
                    driver.quit()
                except: pass 
                if not returned_successfully: 
                    self._add_new_driver_to_pool_if_needed()
            
    def _add_new_driver_to_pool_if_needed(self):
        if self.driver_pool.qsize() < self.config.MAX_WORKERS:
            try:
                logger.info("손상된 드라이버 대체 위해 새 드라이버 생성 시도...")
                new_driver = self._create_new_driver()
                self.driver_pool.put_nowait(new_driver)
                logger.info(f"대체 드라이버 풀에 추가 완료. 현재 풀 크기: {self.driver_pool.qsize()}")
            except queue.Full:
                logger.warning("대체 드라이버 추가 시도 중 풀이 꽉 참.")
            except Exception as e_new:
                logger.error(f"대체 드라이버 생성 실패: {e_new}", exc_info=True)
        else:
            logger.debug("풀이 이미 최대 용량이므로 대체 드라이버를 추가하지 않음.")

    def quit_all(self):
        logger.info("모든 드라이버 종료 시작...")
        drained_drivers = []
        while not self.driver_pool.empty():
            try:
                driver = self.driver_pool.get_nowait()
                drained_drivers.append(driver)
            except queue.Empty:
                break
        
        for i, driver in enumerate(drained_drivers):
            try:
                driver.quit()
                logger.debug(f"드라이버 {i+1}/{len(drained_drivers)} 종료됨.")
            except Exception as e:
                logger.error(f"드라이버 종료 중 오류: {e}")
        logger.info(f"총 {len(drained_drivers)}개의 드라이버 종료 시도 완료.")

# --- 진행 상황 관리 클래스 ---
class ProgressManager:
    def __init__(self, config):
        self.config = config
        self.progress_file_path = config.progress_file
        self.progress = self._load()

    def _load(self):
        default_progress = {'banks': {}, 'stats': {'last_run': None, 'success_count': 0, 'failure_count': 0}}
        if self.progress_file_path.exists():
            try:
                with open(self.progress_file_path, 'r', encoding='utf-8') as f:
                    loaded_progress = json.load(f)
                    if 'banks' not in loaded_progress: # 이전 버전 호환성
                        logger.warning("progress.json에 'banks' 키가 없습니다. 새 구조로 초기화합니다.")
                        loaded_progress['banks'] = {}
                    if 'stats' not in loaded_progress: # 이전 버전 호환성
                        loaded_progress['stats'] = default_progress['stats']
                    return loaded_progress
            except json.JSONDecodeError:
                logger.warning(f"진행 상황 파일({self.progress_file_path}) 손상. 새로 시작합니다.")
            except Exception as e:
                logger.warning(f"진행 상황 파일 로드 중 오류({type(e).__name__}: {e}). 새로 시작합니다.")
        return default_progress

    def save(self):
        self.progress['stats']['last_run'] = datetime.now().isoformat()
        
        completed_count = 0
        failure_count = 0
        if 'banks' in self.progress:
            for bank_info in self.progress['banks'].values():
                if bank_info.get('status') == 'completed':
                    completed_count += 1
                elif bank_info.get('status') == 'failed':
                    failure_count += 1
        
        self.progress['stats']['success_count'] = completed_count
        self.progress['stats']['failure_count'] = failure_count
        
        try:
            with open(self.progress_file_path, 'w', encoding='utf-8') as f:
                json.dump(self.progress, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"진행 상황 파일 저장 실패: {e}", exc_info=True)

    def mark_completed(self, bank_name, date_info):
        self.progress.setdefault('banks', {})[bank_name] = {
            'status': 'completed',
            'date_info': date_info
        }
        self.save()

    def mark_failed(self, bank_name):
        # 실패 시 기존에 성공한 은행의 date_info를 유지할지, 아니면 초기화할지 결정.
        # 현재는 실패 시 date_info는 이전 값을 유지하거나 없으면 None.
        existing_date_info = self.progress.get('banks', {}).get(bank_name, {}).get('date_info')
        self.progress.setdefault('banks', {})[bank_name] = {
            'status': 'failed',
            'date_info': existing_date_info 
        }
        self.save()

    def get_pending_banks(self):
        processed_banks = self.progress.get('banks', {})
        pending_banks = [
            bank for bank in self.config.BANKS
            if bank not in processed_banks or processed_banks[bank].get('status') != 'completed'
        ]
        # 실패한 은행을 재시도하지 않으려면:
        # pending_banks = [bank for bank in self.config.BANKS if bank not in processed_banks.keys()]
        logger.info(f"보류 중인 은행 수 (재시도 포함 가능): {len(pending_banks)}")
        return pending_banks
        
    def get_bank_data(self, bank_name): # 특정 은행의 저장된 정보 가져오기 (필요시 사용)
        return self.progress.get('banks', {}).get(bank_name)

# --- 스크래퍼 클래스 ---
class BankScraper:
    def __init__(self, config, driver_manager, progress_manager):
        self.config = config
        self.driver_manager = driver_manager
        self.progress_manager = progress_manager
        self.email_sender = EmailSender()

    def _wait_for_element(self, driver, by, value, timeout=None):
        timeout = timeout or self.config.WAIT_TIMEOUT
        try:
            return WebDriverWait(driver, timeout).until(EC.presence_of_element_located((by, value)))
        except TimeoutException:
            logger.debug(f"요소 대기 시간 초과: ({by}, {value})")
            return None

    def _robust_click(self, driver, element):
        try:
            driver.execute_script("arguments[0].scrollIntoView({block: 'center', inline: 'nearest'});", element)
            time.sleep(0.2) # 스크롤 후 JS 실행 위한 짧은 대기
            driver.execute_script("arguments[0].click();", element)
            logger.debug("JavaScript 클릭 성공")
            return True
        except Exception as e_js:
            logger.debug(f"JavaScript 클릭 실패 ({e_js}). 일반 Selenium 클릭 시도.")
            try:
                element.click()
                logger.debug("Selenium 일반 클릭 성공")
                return True
            except Exception as e_sel:
                logger.warning(f"일반 Selenium 클릭도 실패: {e_sel}")
                return False

    def extract_date_information(self, driver):
        try:
            js_script = """
            var bodyText = document.body.innerText;
            var datePattern = /(\d{4}년\s*\d{1,2}월말)/g; 
            var matches = bodyText.match(datePattern);
            if (matches && matches.length > 0) {
                return matches[0].replace(/\s+/g, ''); 
            }
            var tags = ['h1', 'h2', 'h3', 'th', 'p', 'span', 'div'];
            for (var i = 0; i < tags.length; i++) {
                var elements = document.getElementsByTagName(tags[i]);
                for (var j = 0; j < elements.length; j++) {
                    if (elements[j].innerText) {
                        var tagMatch = elements[j].innerText.match(datePattern);
                        if (tagMatch && tagMatch.length > 0) {
                            return tagMatch[0].replace(/\s+/g, '');
                        }
                    }
                }
            }
            return '날짜 정보 없음';
            """
            date_info = driver.execute_script(js_script)
            return date_info
        except Exception as e:
            logger.error(f"날짜 정보 추출 중 오류: {e}", exc_info=True)
            return "날짜 추출 실패"

    def select_bank(self, driver, bank_name):
        logger.debug(f"[{bank_name}] 은행 선택 시도...")
        driver.get(self.config.BASE_URL)
        WebDriverWait(driver, self.config.PAGE_LOAD_TIMEOUT).until(
            lambda d: d.execute_script('return document.readyState') == 'complete'
        )
        time.sleep(random.uniform(0.4, 0.8)) # 시간 단축

        exact_xpaths = [
            f"//td[normalize-space(text())='{bank_name}']",
            f"//a[normalize-space(text())='{bank_name}']"
        ]
        for xpath in exact_xpaths:
            try:
                elements = driver.find_elements(By.XPATH, xpath)
                for element in elements:
                    if element.is_displayed():
                        if self._robust_click(driver, element):
                            time.sleep(random.uniform(0.5, 1.0)) # 시간 단축
                            return True
            except Exception as e: logger.debug(f"XPath (정확) '{xpath}' 오류: {e}")
        
        js_script = f"""
        var elements = Array.from(document.querySelectorAll('a, td'));
        var targetElement = elements.find(el => el.textContent && el.textContent.trim().includes('{bank_name}'));
        if (targetElement) {{
            targetElement.scrollIntoView({{block: 'center', inline: 'nearest'}});
            if (targetElement.tagName === 'TD' && targetElement.querySelector('a')) {{
                targetElement.querySelector('a').click();
            }} else {{ targetElement.click(); }}
            return true;
        }} return false;
        """
        try:
            if driver.execute_script(js_script):
                logger.debug(f"[{bank_name}] JavaScript로 은행 선택 성공.")
                time.sleep(random.uniform(0.5, 1.0)) # 시간 단축
                return True
        except Exception as e: logger.debug(f"[{bank_name}] JavaScript 선택 오류: {e}")

        partial_xpaths = [
            f"//td[contains(normalize-space(.), '{bank_name}')]",
            f"//a[contains(normalize-space(.), '{bank_name}')]"
        ]
        for xpath in partial_xpaths:
            try:
                elements = driver.find_elements(By.XPATH, xpath)
                elements.sort(key=lambda x: len(x.text) if x.text else float('inf'))
                for element in elements:
                    if element.is_displayed():
                        if self._robust_click(driver, element):
                            time.sleep(random.uniform(0.5, 1.0)) # 시간 단축
                            return True
            except Exception as e: logger.debug(f"XPath (부분) '{xpath}' 오류: {e}")

        logger.warning(f"[{bank_name}] 은행 선택 최종 실패.")
        return False

    def select_category(self, driver, category_name):
        logger.debug(f"카테고리 선택 시도: '{category_name}'")
        time.sleep(random.uniform(0.2, 0.5)) # 시간 단축

        selectors = [
            (By.XPATH, f"//a[normalize-space(translate(text(), ' \t\n\r', ''))='{category_name.replace(' ', '')}']"),
            (By.XPATH, f"//button[normalize-space(translate(text(), ' \t\n\r', ''))='{category_name.replace(' ', '')}']"),
            (By.LINK_TEXT, category_name),
            (By.PARTIAL_LINK_TEXT, category_name)
        ]
        for by_type, selector_val in selectors:
            try:
                elements = driver.find_elements(by_type, selector_val)
                for element in elements:
                    if element.is_displayed() and element.is_enabled():
                        if self._robust_click(driver, element):
                            time.sleep(random.uniform(0.4, 0.8)) # 시간 단축
                            return True
            except Exception as e: logger.debug(f"카테고리 선택 중 {by_type} '{selector_val}' 오류: {e}")
        
        js_script = f"""
        var elements = Array.from(document.querySelectorAll('a, li, button, span, div[role="tab"]'));
        var targetElement = elements.find(el => el.textContent && el.textContent.trim().includes('{category_name}'));
        if (targetElement) {{
            targetElement.scrollIntoView({{block: 'center', inline: 'nearest'}});
            targetElement.click(); return true;
        }} return false;
        """
        try:
            if driver.execute_script(js_script):
                logger.debug(f"'{category_name}' 카테고리: JavaScript로 선택 성공.")
                time.sleep(random.uniform(0.4, 0.8)) # 시간 단축
                return True
        except Exception as e: logger.debug(f"'{category_name}' 카테고리: JavaScript 선택 오류: {e}")

        logger.warning(f"'{category_name}' 카테고리 탭 선택 최종 실패.")
        return False
        
    def extract_tables_from_page(self, driver):
        logger.debug("페이지에서 테이블 추출 시도.")
        WebDriverWait(driver, self.config.PAGE_LOAD_TIMEOUT).until(
            lambda d: d.execute_script('return document.readyState') == 'complete'
        )
        time.sleep(random.uniform(0.3, 0.6)) # 시간 단축

        try:
            html_source = driver.page_source
            if not html_source or len(html_source) < 300 or "<table" not in html_source.lower():
                logger.debug("페이지 소스가 매우 짧거나 table 태그 없음.")
                return []

            dfs = pd.read_html(StringIO(html_source), flavor='bs4', encoding='utf-8')
            valid_dfs = []
            if dfs:
                logger.debug(f"pandas.read_html이 {len(dfs)}개의 DataFrame 반환.")
                for idx, df in enumerate(dfs):
                    if not isinstance(df, pd.DataFrame) or df.empty:
                        continue
                    df.dropna(axis=0, how='all', inplace=True)
                    df.dropna(axis=1, how='all', inplace=True)
                    if df.empty:
                        continue
                    if isinstance(df.columns, pd.MultiIndex):
                        df.columns = ['_'.join(map(str, col)).strip('_ ') for col in df.columns.values]
                    else:
                        df.columns = [str(col).strip() for col in df.columns]
                    valid_dfs.append(df.reset_index(drop=True))
                logger.debug(f"{len(valid_dfs)}개의 유효한 테이블 추출 완료.")
            else:
                logger.debug("pandas.read_html이 테이블을 찾지 못했거나 빈 리스트 반환.")
            return valid_dfs
        except ValueError as ve:
            logger.warning(f"pandas.read_html 실행 중 ValueError (테이블 없음 가능성): {ve}")
            return []
        except Exception as e:
            logger.error(f"테이블 추출 중 심각한 오류: {e}", exc_info=True)
            return []

    def _scrape_single_bank_attempt(self, bank_name, driver):
        logger.info(f"[{bank_name}] 스크래핑 시도 시작...")
        
        if not self.select_bank(driver, bank_name):
            logger.error(f"[{bank_name}] 은행 선택에 최종 실패했습니다.")
            return None

        date_info_scraped = self.extract_date_information(driver)
        logger.info(f"[{bank_name}] 공시 날짜 정보: {date_info_scraped}")

        if date_info_scraped not in ["날짜 정보 없음", "날짜 추출 실패"]:
            match = re.search(r'(\d{4})년\s*(\d{1,2})월말', date_info_scraped)
            if match:
                month_extracted = int(match.group(2))
                expected_months = [3, 6, 9, 12] 
                if month_extracted not in expected_months:
                    logger.warning(f"[{bank_name}] 추출된 공시월({month_extracted}월)이 일반적인 분기 공시월({expected_months})과 다릅니다. (날짜: {date_info_scraped})")
            else:
                logger.warning(f"[{bank_name}] 날짜 형식이 'YYYY년 MM월말' 패턴과 일치하지 않습니다: {date_info_scraped}")
        
        bank_data_for_excel = {'_INFO_': pd.DataFrame({
            '은행명': [bank_name], '공시날짜': [date_info_scraped],
            '추출일시': [datetime.now().strftime("%Y-%m-%d %H:%M:%S")],
            '스크래퍼버전': [self.config.VERSION]
        })}
        
        scraped_something_meaningful = False
        original_url_after_bank_selection = driver.current_url 

        for category_name in self.config.CATEGORIES:
            logger.info(f"[{bank_name}] '{category_name}' 카테고리 처리 시작.")
            category_selected = False
            for attempt in range(2): 
                if attempt > 0: 
                    logger.debug(f"[{bank_name}] '{category_name}' 탭 선택 재시도...")
                    driver.get(original_url_after_bank_selection) 
                    WebDriverWait(driver, self.config.PAGE_LOAD_TIMEOUT).until(
                        lambda d: d.execute_script('return document.readyState') == 'complete'
                    )
                    time.sleep(0.5) 
                if self.select_category(driver, category_name):
                    category_selected = True
                    break
                else:
                    logger.warning(f"[{bank_name}] '{category_name}' 카테고리 선택 실패 (시도 {attempt + 1}).")
            
            if not category_selected:
                logger.error(f"[{bank_name}] '{category_name}' 카테고리 최종 선택 실패. 다음 카테고리로.")
                continue

            tables = self.extract_tables_from_page(driver)
            if tables:
                logger.info(f"[{bank_name}] '{category_name}'에서 {len(tables)}개 테이블 발견.")
                for i, df_table in enumerate(tables):
                    sheet_name = f"{category_name}_{i+1}"
                    sheet_name = re.sub(r'[\\/*?:\[\]]', '', sheet_name)[:31] 
                    bank_data_for_excel[sheet_name] = df_table
                scraped_something_meaningful = True
            else:
                logger.warning(f"[{bank_name}] '{category_name}'에서 테이블을 찾지 못함.")
        
        if scraped_something_meaningful:
            logger.info(f"[{bank_name}] 스크래핑 데이터 수집 완료.")
            return bank_data_for_excel
        else:
            logger.warning(f"[{bank_name}] 의미있는 데이터를 스크랩하지 못했습니다.")
            return None

    def save_bank_data(self, bank_name, excel_data_dict):
        date_info_df = excel_data_dict.get('_INFO_')
        date_str_for_filename = "날짜정보없음"
        if date_info_df is not None and not date_info_df.empty:
            raw_date_str = date_info_df['공시날짜'].iloc[0]
            match = re.search(r'(\d{4})년(\d{1,2})월', raw_date_str)
            if match:
                date_str_for_filename = f"{match.group(1)}-{int(match.group(2)):02d}"
            elif raw_date_str and raw_date_str not in ['날짜 정보 없음', '날짜 추출 실패']:
                date_str_for_filename = re.sub(r'[^\w\-_.]', '', raw_date_str)

        excel_file_name = f"{bank_name}_{date_str_for_filename}.xlsx"
        excel_path = self.config.output_dir / excel_file_name
        
        try:
            with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
                for sheet_name_key, df_to_save in excel_data_dict.items():
                    actual_sheet_name = '정보' if sheet_name_key == '_INFO_' else sheet_name_key
                    df_to_save.to_excel(writer, sheet_name=actual_sheet_name, index=False)
            logger.info(f"[{bank_name}] 데이터 저장 완료: {excel_path.name}")
            return True
        except Exception as e:
            logger.error(f"[{bank_name}] 데이터 저장 실패 ({excel_path.name}): {e}", exc_info=True)
            return False

    async def worker_process_bank(self, bank_name, pbar_instance, semaphore):
        async with semaphore:
            logger.debug(f"[{bank_name}] Semaphore 획득, 작업 시작.")
            driver = None
            success_status = False
            bank_date_info_for_progress = self.progress_manager.get_bank_data(bank_name).get('date_info') if self.progress_manager.get_bank_data(bank_name) else None


            try:
                driver = await asyncio.get_event_loop().run_in_executor(None, self.driver_manager.get_driver)
                if not driver:
                    logger.error(f"[{bank_name}] WebDriver를 가져올 수 없습니다. 건너뜁니다.")
                    self.progress_manager.mark_failed(bank_name)
                    return bank_name, False, bank_date_info_for_progress 

                scraped_data = None
                for attempt in range(self.config.MAX_RETRIES):
                    logger.info(f"[{bank_name}] 스크래핑 시도 {attempt + 1}/{self.config.MAX_RETRIES}")
                    try:
                        scraped_data = self._scrape_single_bank_attempt(bank_name, driver)
                        if scraped_data:
                            bank_date_info_for_progress = scraped_data.get('_INFO_', pd.DataFrame({'공시날짜': ["날짜 기록 안됨"]}))['공시날짜'].iloc[0]
                            logger.info(f"[{bank_name}] 데이터 스크랩 성공 (시도 {attempt + 1}). 날짜: {bank_date_info_for_progress}")
                            break
                    except Exception as e_attempt:
                        logger.warning(f"[{bank_name}] 스크래핑 시도 {attempt + 1} 중 예외: {type(e_attempt).__name__} - {e_attempt}")
                        if attempt < self.config.MAX_RETRIES - 1:
                            logger.info(f"[{bank_name}] 드라이버 재설정 및 재시도 준비.")
                            await asyncio.get_event_loop().run_in_executor(None, self.driver_manager.return_driver, driver)
                            driver = await asyncio.get_event_loop().run_in_executor(None, self.driver_manager.get_driver)
                            if not driver:
                                logger.error(f"[{bank_name}] 재시도를 위한 WebDriver 획득 실패. 중단.")
                                break 
                        else:
                            logger.error(f"[{bank_name}] 모든 재시도({self.config.MAX_RETRIES}회) 후에도 스크래핑 실패.")
                            scraped_data = None 
                
                if scraped_data:
                    if self.save_bank_data(bank_name, scraped_data):
                        self.progress_manager.mark_completed(bank_name, bank_date_info_for_progress)
                        success_status = True
                    else:
                        self.progress_manager.mark_failed(bank_name) 
                else:
                    self.progress_manager.mark_failed(bank_name) 
                
                return bank_name, success_status, bank_date_info_for_progress

            except TimeoutError as te:
                logger.error(f"[{bank_name}] 드라이버 획득 타임아웃: {te}")
                self.progress_manager.mark_failed(bank_name)
                return bank_name, False, bank_date_info_for_progress
            except Exception as e_worker:
                logger.error(f"[{bank_name}] 작업자 내부에서 치명적 예외: {e_worker}", exc_info=True)
                self.progress_manager.mark_failed(bank_name)
                return bank_name, False, bank_date_info_for_progress
            finally:
                if driver:
                    logger.debug(f"[{bank_name}] 작업 완료, 드라이버 반납 시도.")
                    await asyncio.get_event_loop().run_in_executor(None, self.driver_manager.return_driver, driver)
                if pbar_instance: pbar_instance.update(1)
                final_log_date = bank_date_info_for_progress if bank_date_info_for_progress else "날짜 정보 미확정"
                logger.info(f"[{bank_name}] 처리 결과: {'성공' if success_status else '실패'}{f' (공시일: {final_log_date})' if success_status else ''}")
                logger.debug(f"[{bank_name}] Semaphore 반납.")
    
    async def run(self):
        start_time_total = time.monotonic()
        logger.info(f"==== 스크래핑 프로세스 시작 (v{self.config.VERSION}) ====")

        pending_banks = self.progress_manager.get_pending_banks()
        if not pending_banks:
            logger.info("처리할 은행이 없습니다. (이미 완료되었거나 목록이 비어있음)")
            self.generate_summary_and_send_email() 
            return

        logger.info(f"총 {len(pending_banks)}개 은행 처리 예정. (샘플: {pending_banks[:3]}{'...' if len(pending_banks)>3 else ''})")
        semaphore = asyncio.Semaphore(self.config.MAX_WORKERS)
        tasks = []
        with tqdm(total=len(pending_banks), desc="은행 데이터 스크래핑", unit="은행", dynamic_ncols=True, smoothing=0.1) as pbar:
            for bank_name in pending_banks:
                tasks.append(self.worker_process_bank(bank_name, pbar, semaphore))
            
            results_or_exceptions = await asyncio.gather(*tasks, return_exceptions=True)

        # 결과 로깅 (주로 디버깅 및 확인용, 최종 집계는 progress_manager에서)
        temp_successful_runs = 0
        temp_failed_runs = 0
        for i, res_or_exc in enumerate(results_or_exceptions):
            bank_name_processed = pending_banks[i] 
            if isinstance(res_or_exc, Exception):
                logger.error(f"[{bank_name_processed}] 작업 실행 중 최상위 예외 포착: {res_or_exc}", exc_info=True)
                temp_failed_runs +=1
            elif isinstance(res_or_exc, tuple) and len(res_or_exc) == 3:
                _, success_status, _ = res_or_exc # date_info는 이미 worker에서 로깅 및 저장
                if success_status: temp_successful_runs +=1
                else: temp_failed_runs +=1
            else: 
                logger.error(f"[{bank_name_processed}] 작업 결과가 예상치 않음: {res_or_exc}")
                temp_failed_runs +=1
        logger.info(f"asyncio.gather 결과 임시 집계: 성공 {temp_successful_runs}건, 실패 {temp_failed_runs}건 (예외 포함)")


        end_time_total = time.monotonic()
        total_duration_sec = end_time_total - start_time_total
        logger.info(f"==== 전체 스크래핑 작업 완료. 총 소요 시간: {total_duration_sec:.2f}초 ({total_duration_sec/60:.2f}분) ====")
        
        self.generate_summary_and_send_email()

    def generate_summary_and_send_email(self):
        logger.info("요약 보고서 생성 및 이메일 전송 시작...")
        summary_data = []
        all_banks_in_config = self.config.BANKS
        
        processed_banks_data = self.progress_manager.progress.get('banks', {})
        completed_count = 0
        failed_count = 0
        failed_banks_names = []

        for bank_name_iter in all_banks_in_config:
            bank_detail = processed_banks_data.get(bank_name_iter)
            status = '미처리'
            disclosure_date_val = '' 

            if bank_detail:
                current_status = bank_detail.get('status')
                date_from_progress = bank_detail.get('date_info', '날짜 없음')

                if current_status == 'completed':
                    status = '완료'
                    disclosure_date_val = date_from_progress
                    completed_count +=1
                elif current_status == 'failed':
                    status = '실패'
                    disclosure_date_val = date_from_progress if date_from_progress != '날짜 없음' else '' # 실패했어도 이전 날짜 정보가 있다면 표시
                    failed_count +=1
                    failed_banks_names.append(bank_name_iter)
            
            summary_data.append({
                '은행명': bank_name_iter, 
                '공시 날짜': disclosure_date_val, 
                '처리 상태': status, 
                '확인 시간': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            })
        
        summary_df = pd.DataFrame(summary_data)
        summary_filename = f"스크래핑_요약_{self.config.today}.xlsx"
        summary_file_path = self.config.output_dir / summary_filename
        try:
            summary_df.to_excel(summary_file_path, index=False)
            logger.info(f"요약 보고서 생성 완료: {summary_file_path}")
        except Exception as e:
            logger.error(f"요약 보고서 저장 실패: {e}", exc_info=True)

        zip_filename = f"저축은행_데이터_{self.config.today}.zip"
        zip_file_path = self.config.output_dir.parent / zip_filename 
        try:
            logger.info(f"결과 압축 시작: {self.config.output_dir} -> {zip_file_path}")
            with zipfile.ZipFile(zip_file_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                for file_path in self.config.output_dir.rglob('*'):
                    if file_path.is_file():
                        arcname = file_path.relative_to(self.config.output_dir.parent)
                        zipf.write(file_path, arcname)
            logger.info(f"결과 압축 완료: {zip_file_path}")
        except Exception as e:
            logger.error(f"결과 압축 실패: {e}", exc_info=True)
            zip_file_path = None 

        total_banks_in_list = len(all_banks_in_config)
        processed_attempt_count = completed_count + failed_count # 미처리는 제외
        success_rate = (completed_count / processed_attempt_count * 100) if processed_attempt_count > 0 else 0
        
        email_subject = f"[저축은행 데이터] {self.config.today} 스크래핑 결과 ({completed_count}/{total_banks_in_list} 완료, {failed_count} 실패)"
        
        failed_banks_display_list = failed_banks_names[:10]
        failed_banks_html = "<ul>" + "".join(f"<li>{b}</li>" for b in failed_banks_display_list) + "</ul>"
        if len(failed_banks_names) > 10:
            failed_banks_html += f"<p>...외 {len(failed_banks_names) - 10}개 은행 실패.</p>"
        if not failed_banks_names:
            failed_banks_html = "<p>없음</p>"

        body_html = f"""
        <html><head><style>
            body {{ font-family: Arial, sans-serif; margin: 20px; }}
            h2 {{ color: #2c3e50; }}
            .summary-box {{ border: 1px solid #ddd; padding: 15px; margin-bottom: 20px; background-color: #f9f9f9; border-radius: 5px; }}
            .summary-box p {{ margin: 5px 0; }}
            .status-completed {{ color: green; font-weight: bold; }}
            .status-failed {{ color: red; font-weight: bold; }}
            table {{ border-collapse: collapse; width: 90%; margin-top:15px; font-size: 0.9em; }}
            th, td {{ border: 1px solid #ddd; padding: 6px; text-align: left; }}
            th {{ background-color: #f0f0f0; }}
        </style></head><body>
        <h2>저축은행 데이터 스크래핑 결과 ({self.config.today})</h2>
        <div class="summary-box">
            <p><strong>총 대상 은행 수:</strong> {total_banks_in_list}개</p>
            <p><strong>처리 시도된 은행 수 (성공+실패):</strong> {processed_attempt_count}개</p>
            <p><span class="status-completed">✅ 성공:</span> {completed_count}개</p>
            <p><span class="status-failed">❌ 실패:</span> {failed_count}개</p>
            <p><strong>📈 성공률 (처리 시도된 은행 기준):</strong> {success_rate:.1f}%</p>
            <p><strong>📂 생성된 데이터 폴더:</strong> {self.config.output_dir.name}</p>
        </div>
        <h3>실패한 은행 목록 (최대 10개):</h3>
        {failed_banks_html}
        <p>세부 결과는 첨부된 요약 보고서(엑셀) 및 전체 데이터(ZIP)를 확인하세요.</p>
        <h3>전체 은행 처리 현황 (공시 날짜 포함):</h3>
        {summary_df.to_html(index=False, border=1, na_rep='') if not summary_df.empty else "<p>요약 테이블 데이터가 없습니다.</p>"}
        <br><p><small>이 메일은 자동 발송되었습니다. (스크래퍼 버전: {self.config.VERSION})</small></p>
        </body></html>
        """
        
        attachment_to_send = None
        if zip_file_path and zip_file_path.exists():
            attachment_to_send = str(zip_file_path)
        elif summary_file_path.exists(): 
            logger.warning("압축 파일 생성 실패. 요약 보고서만 첨부합니다.")
            attachment_to_send = str(summary_file_path)
        else:
            logger.warning("압축 파일 및 요약 보고서 생성 실패. 첨부 파일 없이 이메일 발송.")

        self.email_sender.send_email_with_attachment(email_subject, body_html, attachment_to_send)

# --- 메인 실행 로직 ---
def main():
    config = Config() 
    driver_manager = None 
    try:
        logger.info(f"스크립트 실행 시작: {sys.argv[0]}")
        driver_manager = DriverManager(config)
        progress_manager = ProgressManager(config)
        scraper = BankScraper(config, driver_manager, progress_manager)
        asyncio.run(scraper.run()) 
        logger.info("모든 스크래핑 프로세스가 정상적으로 완료되었습니다.")
    except Exception as e:
        print(f"스크립트 실행 중 최상위 레벨에서 오류 발생: {e}")
        if logger:
            logger.critical(f"스크립트 실행 중 최상위 레벨에서 오류 발생: {e}", exc_info=True)
        sys.exit(1) 
    finally:
        if driver_manager:
            logger.info("애플리케이션 종료 전 드라이버 풀 정리 시도...")
            driver_manager.quit_all() 
        if logger:
            logger.info("스크립트 실행이 종료되었습니다.")
        else:
            print("스크립트 실행이 종료되었습니다 (로거 미설정).")

if __name__ == "__main__":
    # Windows에서 Python 3.8+ asyncio 관련 ProactorEventLoop 오류 방지 (필요시 주석 해제)
    # if sys.platform == "win32" and sys.version_info >= (3, 8):
    # asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    main()
