"""
저축은행 중앙회 통일경영공시 데이터 자동 스크래핑 도구 (GitHub Actions 최적화 버전)
목적: GitHub Actions에서 자동 실행, 병렬 처리를 통한 속도 개선
작성일: 2025-03-31 (최종 수정일 반영)
특징:
- GUI 없음, CLI 기반 실행
- asyncio 및 ThreadPoolExecutor를 사용한 병렬 처리
- GitHub Actions 환경에 최적화된 WebDriver 설정
- 환경 변수를 통한 주요 설정 관리
- 자동 재시도 및 강화된 에러 핸들링
- 이메일 알림 기능
- 다중 HTML 파싱 전략으로 안정성 향상
"""

import os
import sys
import time
import random
import json
import re
import asyncio
import concurrent.futures
import zipfile
from datetime import datetime
from io import StringIO
import argparse
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
from bs4 import BeautifulSoup
import pandas as pd
from tqdm import tqdm # CLI 진행률 표시
import warnings
warnings.filterwarnings("ignore") # 경고 메시지 무시

# --- 로깅 설정 ---
def setup_logging(log_file_path, log_level="INFO"):
    """로깅 시스템을 설정합니다."""
    # 기존 핸들러 제거 (중복 로깅 방지)
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
        
    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_file_path, encoding='utf-8')
        ]
    )
    # Selenium 로거 레벨 조정 (지나치게 상세한 로그 줄이기)
    logging.getLogger('selenium.webdriver.remote.remote_connection').setLevel(logging.WARNING)
    logging.getLogger('urllib3.connectionpool').setLevel(logging.WARNING)
    return logging.getLogger(__name__)

# 전역 로거는 Config 초기화 후 설정 (로그 파일 경로 때문에)
logger = None

# --- 이메일 전송 클래스 ---
class EmailSender:
    """Gmail을 통해 이메일을 전송하는 클래스"""
    
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
        """첨부 파일과 함께 이메일을 전송합니다."""
        if not self.enabled:
            if logger: logger.info("이메일 전송이 비활성화되어 있습니다.")
            return False
        
        try:
            msg = MIMEMultipart()
            msg['From'] = self.sender_email
            msg['To'] = ', '.join(self.recipient_emails)
            msg['Subject'] = subject
            msg.attach(MIMEText(body, 'html'))
            
            if attachment_path and os.path.exists(attachment_path):
                with open(attachment_path, 'rb') as attachment_file:
                    part = MIMEBase('application', 'octet-stream')
                    part.set_payload(attachment_file.read())
                    encoders.encode_base64(part)
                    part.add_header('Content-Disposition', f'attachment; filename="{os.path.basename(attachment_path)}"')
                    msg.attach(part)
                if logger: logger.info(f"첨부 파일 추가: {attachment_path}")
            
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls()
                server.login(self.sender_email, self.sender_password)
                server.send_message(msg)
            if logger: logger.info(f"이메일 전송 성공: {', '.join(self.recipient_emails)}")
            return True
        except Exception as e:
            if logger: logger.error(f"이메일 전송 실패: {str(e)}")
            return False

# --- 설정 클래스 ---
class Config:
    """환경 변수와 기본값을 관리하는 설정 클래스"""
    
    def __init__(self):
        self.VERSION = "2.6-fixed" # 버전 업데이트 (html5lib 문제 수정)
        self.BASE_URL = "https://www.fsb.or.kr/busmagequar_0100.act"
        self.MAX_RETRIES = int(os.getenv('MAX_RETRIES', '2'))
        self.PAGE_LOAD_TIMEOUT = int(os.getenv('PAGE_LOAD_TIMEOUT', '15'))
        self.WAIT_TIMEOUT = int(os.getenv('WAIT_TIMEOUT', '7'))
        self.MAX_WORKERS = int(os.getenv('MAX_WORKERS', '3'))

        self.today = datetime.now().strftime("%Y%m%d")
        self.output_dir_base = Path(os.getenv('OUTPUT_DIR', "./output"))
        self.output_dir = self.output_dir_base / f"저축은행_데이터_{self.today}"
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.progress_file = self.output_dir / 'progress.json'
        self.log_file_path = self.output_dir / f'scraping_log_{self.today}.log'

        global logger
        logger = setup_logging(self.log_file_path, os.getenv('LOG_LEVEL', 'INFO'))

        # 전체 80개 저축은행 목록 (머스트, 삼일 분리)
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
        
        logger.info(f"설정 초기화 완료 (v{self.VERSION}): 출력 디렉토리={self.output_dir}, 워커 수={self.MAX_WORKERS}")
        logger.info(f"페이지 로드 타임아웃: {self.PAGE_LOAD_TIMEOUT}s, 요소 대기 타임아웃: {self.WAIT_TIMEOUT}s")

# --- 웹드라이버 관리 클래스 (풀링 방식) ---
class DriverManager:
    """Chrome 웹드라이버를 풀링 방식으로 관리하는 클래스"""
    
    def __init__(self, config):
        self.config = config
        self.driver_pool = queue.Queue(maxsize=self.config.MAX_WORKERS)
        self._initialize_pool()

    def _create_new_driver(self):
        """새로운 Chrome 드라이버 인스턴스를 생성합니다."""
        options = Options()
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        options.add_argument('--window-size=1920,1080')
        options.add_argument('user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
        
        options.add_argument('--disable-extensions')
        options.add_argument('--disable-browser-side-navigation')
        options.add_argument('--disable-infobars')
        options.add_argument('--disable-notifications')
        options.add_argument('--disable-popup-blocking')
        options.add_argument('--blink-settings=imagesEnabled=false') # 이미지 로딩 비활성화 (속도 향상)

        # 불필요한 로그 줄이기
        options.add_experimental_option('excludeSwitches', ['enable-logging'])
        options.set_capability('goog:loggingPrefs', {'performance': 'ALL', 'browser': 'ALL'})
        
        driver = webdriver.Chrome(options=options)
        driver.set_page_load_timeout(self.config.PAGE_LOAD_TIMEOUT)
        return driver

    def _initialize_pool(self):
        """드라이버 풀을 초기화합니다."""
        logger.info(f"드라이버 풀 초기화 중 (최대 {self.config.MAX_WORKERS}개)...")
        for i in range(self.config.MAX_WORKERS):
            try:
                driver = self._create_new_driver()
                self.driver_pool.put(driver, block=False)
                logger.debug(f"드라이버 {i+1} 생성하여 풀에 추가.")
            except Exception as e:
                logger.error(f"드라이버 {i+1} 생성 실패: {e}")
        logger.info(f"드라이버 풀 초기화 완료. 사용 가능 드라이버: {self.driver_pool.qsize()}개")

    def get_driver(self):
        """풀에서 사용 가능한 드라이버를 가져옵니다."""
        try:
            driver = self.driver_pool.get(timeout=60)
            logger.debug("풀에서 드라이버 가져옴.")
            return driver
        except queue.Empty:
            logger.warning("드라이버 풀이 비어있고 타임아웃 발생. 새 드라이버를 임시로 생성합니다.")
            return self._create_new_driver()

    def return_driver(self, driver):
        """사용이 끝난 드라이버를 풀에 반환합니다."""
        if driver:
            try:
                _ = driver.current_url
                if self.driver_pool.qsize() < self.config.MAX_WORKERS:
                    self.driver_pool.put(driver, block=False)
                    logger.debug("사용된 드라이버 풀에 반환.")
                else:
                    logger.warning("드라이버 풀이 꽉 차 있어 반환되는 드라이버를 종료합니다.")
                    driver.quit()
            except StaleElementReferenceException:
                logger.warning("오래된 참조(Stale) 드라이버 반환 시도. 드라이버를 종료합니다.")
                try: driver.quit()
                except: pass
                self._add_new_driver_to_pool_if_needed()
            except Exception as e:
                logger.warning(f"손상된 드라이버 반환 시도 ({type(e).__name__}). 드라이버를 종료하고 새 드라이버를 풀에 추가합니다.")
                try: driver.quit()
                except: pass
                self._add_new_driver_to_pool_if_needed()
    
    def _add_new_driver_to_pool_if_needed(self):
        """필요시 새 드라이버를 풀에 추가합니다."""
        if self.driver_pool.qsize() < self.config.MAX_WORKERS:
            try:
                new_driver = self._create_new_driver()
                self.driver_pool.put(new_driver, block=False)
                logger.info("손상된 드라이버 대체용 새 드라이버 풀에 추가.")
            except Exception as e_new:
                logger.error(f"대체 드라이버 생성 실패: {e_new}")

    def quit_all(self):
        """모든 드라이버를 종료합니다."""
        logger.info("모든 드라이버 종료 중...")
        while not self.driver_pool.empty():
            try:
                driver = self.driver_pool.get_nowait()
                driver.quit()
            except queue.Empty:
                break
            except Exception as e:
                logger.error(f"드라이버 종료 중 오류: {e}")
        logger.info("모든 드라이버 종료 완료.")

# --- 진행 상황 관리 클래스 ---
class ProgressManager:
    """스크래핑 진행 상황을 추적하고 저장하는 클래스"""
    
    def __init__(self, config):
        self.config = config
        self.progress_file_path = config.progress_file
        self.progress = self._load()

    def _load(self):
        """저장된 진행 상황을 로드합니다."""
        if self.progress_file_path.exists():
            try:
                with open(self.progress_file_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except json.JSONDecodeError:
                logger.warning(f"진행 상황 파일({self.progress_file_path}) 손상. 새로 시작합니다.")
            except Exception as e:
                logger.warning(f"진행 상황 파일 로드 중 오류({e}). 새로 시작합니다.")
        return {'completed': [], 'failed': [], 'stats': {'last_run': None, 'success_count': 0, 'failure_count': 0}}

    def save(self):
        """진행 상황을 파일에 저장합니다."""
        self.progress['stats']['last_run'] = datetime.now().isoformat()
        self.progress['stats']['success_count'] = len(self.progress.get('completed', []))
        self.progress['stats']['failure_count'] = len(self.progress.get('failed', []))
        try:
            with open(self.progress_file_path, 'w', encoding='utf-8') as f:
                json.dump(self.progress, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"진행 상황 파일 저장 실패: {e}")

    def mark_completed(self, bank_name):
        """은행을 완료 목록에 추가합니다."""
        if bank_name not in self.progress.get('completed', []):
            self.progress.setdefault('completed', []).append(bank_name)
        if bank_name in self.progress.get('failed', []):
            self.progress['failed'].remove(bank_name)
        self.save()

    def mark_failed(self, bank_name):
        """은행을 실패 목록에 추가합니다."""
        if bank_name not in self.progress.get('failed', []) and bank_name not in self.progress.get('completed', []):
            self.progress.setdefault('failed', []).append(bank_name)
        self.save()

    def get_pending_banks(self):
        """아직 처리하지 않은 은행 목록을 반환합니다."""
        completed_set = set(self.progress.get('completed', []))
        return [bank for bank in self.config.BANKS if bank not in completed_set]

# --- 스크래퍼 클래스 ---
class BankScraper:
    """실제 스크래핑을 수행하는 클래스"""
    
    def __init__(self, config, driver_manager, progress_manager):
        self.config = config
        self.driver_manager = driver_manager
        self.progress_manager = progress_manager
        self.email_sender = EmailSender()

    def _wait_for_element(self, driver, by, value, timeout=None):
        """요소가 나타날 때까지 대기합니다."""
        timeout = timeout or self.config.WAIT_TIMEOUT
        try:
            return WebDriverWait(driver, timeout).until(EC.presence_of_element_located((by, value)))
        except TimeoutException:
            logger.debug(f"요소 대기 시간 초과: ({by}, {value})")
            return None

    def _wait_for_clickable(self, driver, by, value, timeout=None):
        """요소가 클릭 가능해질 때까지 대기합니다."""
        timeout = timeout or self.config.WAIT_TIMEOUT
        try:
            return WebDriverWait(driver, timeout).until(EC.element_to_be_clickable((by, value)))
        except TimeoutException:
            logger.debug(f"클릭 가능 요소 대기 시간 초과: ({by}, {value})")
            return None
            
    def _wait_for_page_load(self, driver, timeout=None):
        """페이지가 완전히 로드될 때까지 대기합니다."""
        timeout = timeout or self.config.PAGE_LOAD_TIMEOUT
        try:
            WebDriverWait(driver, timeout).until(lambda d: d.execute_script('return document.readyState') == 'complete')
            return True
        except TimeoutException:
            logger.warning("페이지 로드 시간 초과.")
            return False

    def _robust_click(self, driver, element):
        """다양한 방법으로 요소를 클릭합니다."""
        try:
            # JavaScript 클릭이 더 안정적일 수 있음
            driver.execute_script("arguments[0].scrollIntoView({block: 'center', inline: 'center'});", element)
            time.sleep(0.3)
            driver.execute_script("arguments[0].click();", element)
            return True
        except Exception as e:
            logger.debug(f"JavaScript 클릭 실패: {e}. 일반 클릭 시도.")
            try:
                element.click()
                return True
            except Exception as e2:
                logger.warning(f"일반 클릭도 실패: {e2}")
                return False

    def extract_date_information(self, driver):
        """웹페이지에서 공시 날짜 정보를 추출합니다."""
        try:
            js_script = """
            var allText = document.body.innerText;
            var match = allText.match(/\\d{4}년\\d{1,2}월말/);
            return match ? match[0] : '날짜 정보 없음';
            """
            date_info = driver.execute_script(js_script)
            if date_info == '날짜 정보 없음':
                elements = driver.find_elements(By.XPATH, "//*[contains(text(), '기말') and contains(text(), '년')]")
                for el in elements:
                    match = re.search(r'(\d{4}년\d{1,2}월말)', el.text)
                    if match:
                        return match.group(1)
            return date_info
        except Exception as e:
            logger.error(f"날짜 정보 추출 중 오류: {str(e)}")
            return "날짜 추출 실패"

    def select_bank(self, driver, bank_name):
        """은행을 선택합니다."""
        logger.debug(f"{bank_name}: 은행 선택 시도.")
        driver.get(self.config.BASE_URL)
        if not self._wait_for_page_load(driver): return False
        time.sleep(random.uniform(0.5, 1.5))

        # 전략 1: XPath
        xpaths_to_try = [
            f"//td[normalize-space()='{bank_name}']//a|//td[normalize-space()='{bank_name}']",
            f"//a[normalize-space()='{bank_name}']",
            f"//td[contains(., '{bank_name}')]//a|//td[contains(., '{bank_name}')]",
            f"//a[contains(., '{bank_name}')]"
        ]
        for xpath in xpaths_to_try:
            try:
                elements = driver.find_elements(By.XPATH, xpath)
                for element in elements:
                    if element.is_displayed():
                        logger.debug(f"{bank_name}: XPath '{xpath}'로 요소 찾음. 클릭 시도.")
                        if self._robust_click(driver, element):
                            if self._wait_for_page_load(driver, 5): return True
                            return True
            except Exception as e:
                logger.debug(f"{bank_name}: XPath '{xpath}' 시도 중 오류: {e}")
        
        # 전략 2: JavaScript
        logger.debug(f"{bank_name}: JavaScript로 은행 선택 시도.")
        js_script = f"""
        var elements = Array.from(document.querySelectorAll('a, td'));
        var targetElement = elements.find(el => el.textContent.trim().includes('{bank_name}'));
        if (targetElement) {{
            targetElement.scrollIntoView({{block: 'center'}});
            if (targetElement.tagName === 'TD' && targetElement.querySelector('a')) {{
                targetElement.querySelector('a').click();
            }} else {{
                targetElement.click();
            }}
            return true;
        }}
        return false;
        """
        try:
            if driver.execute_script(js_script):
                logger.debug(f"{bank_name}: JavaScript로 은행 선택 성공.")
                if self._wait_for_page_load(driver, 5): return True
                return True
        except Exception as e:
            logger.debug(f"{bank_name}: JavaScript 은행 선택 중 오류: {e}")

        logger.warning(f"{bank_name}: 은행 선택 실패 (모든 전략 사용).")
        return False

    def select_category(self, driver, category_name):
        """카테고리 탭을 선택합니다."""
        logger.debug(f"카테고리 선택 시도: {category_name}")
        time.sleep(random.uniform(0.3, 0.8))

        # 전략 1: XPath
        xpaths_to_try = [
            f"//a[normalize-space()='{category_name}']",
            f"//li[normalize-space()='{category_name}']//a|//li[normalize-space()='{category_name}']",
            f"//button[normalize-space()='{category_name}']",
            f"//*[contains(@class,'tab') and normalize-space()='{category_name}']",
            f"//a[contains(text(),'{category_name}')]",
            f"//li[contains(text(),'{category_name}')]//a|//li[contains(text(),'{category_name}')]",
        ]
        for xpath in xpaths_to_try:
            try:
                elements = driver.find_elements(By.XPATH, xpath)
                for element in elements:
                    if element.is_displayed() and element.is_enabled():
                        logger.debug(f"'{category_name}' 카테고리: XPath '{xpath}'로 요소 찾음. 클릭 시도.")
                        if self._robust_click(driver, element):
                             time.sleep(random.uniform(0.5, 1.0))
                             return True
            except StaleElementReferenceException:
                logger.debug(f"'{category_name}' 카테고리: XPath '{xpath}' stale element. 재시도 필요할 수 있음.")
                return False
            except Exception as e:
                logger.debug(f"'{category_name}' 카테고리: XPath '{xpath}' 시도 중 오류: {e}")
        
        # 전략 2: JavaScript
        logger.debug(f"'{category_name}' 카테고리: JavaScript로 선택 시도.")
        js_script = f"""
        var elements = Array.from(document.querySelectorAll('a, li, button, span, div[role="tab"]'));
        var targetElement = elements.find(el => el.textContent.trim().includes('{category_name}'));
        if (targetElement) {{
            targetElement.scrollIntoView({{block: 'center'}});
            targetElement.click();
            return true;
        }}
        return false;
        """
        try:
            if driver.execute_script(js_script):
                logger.debug(f"'{category_name}' 카테고리: JavaScript로 선택 성공.")
                time.sleep(random.uniform(0.5, 1.0))
                return True
        except Exception as e:
             logger.debug(f"'{category_name}' 카테고리: JavaScript 선택 중 오류: {e}")
        
        logger.warning(f"'{category_name}' 카테고리 탭 선택 실패 (모든 전략 사용).")
        return False
        
    def extract_tables_from_page(self, driver):
        """현재 페이지에서 테이블을 추출합니다. 여러 파싱 방법을 시도하여 안정성을 높입니다."""
        logger.debug("페이지에서 테이블 추출 시도.")
        
        # 페이지 로드 완료 대기
        if not self._wait_for_page_load(driver): 
            return []
        
        # AJAX 컨텐츠 로드를 위한 대기
        time.sleep(random.uniform(0.5, 1.0))

        try:
            html_source = driver.page_source
            
            # HTML이 너무 짧거나 테이블이 없으면 빠르게 반환
            if not html_source or len(html_source) < 500 or "table" not in html_source.lower():
                logger.debug("페이지 소스가 너무 짧거나 'table' 태그 없음. 빈 리스트 반환.")
                return []

            dfs = []
            
            # 방법 1: 기본 파서 시도 (pandas가 자동으로 선택)
            try:
                dfs = pd.read_html(StringIO(html_source))
                logger.debug(f"기본 파서로 {len(dfs)}개 테이블 추출 성공.")
            except Exception as e1:
                logger.debug(f"기본 파서 실패: {e1}")
                
                # 방법 2: lxml 파서 명시적으로 시도
                try:
                    dfs = pd.read_html(StringIO(html_source), flavor='lxml')
                    logger.debug(f"lxml 파서로 {len(dfs)}개 테이블 추출 성공.")
                except Exception as e2:
                    logger.debug(f"lxml 파서 실패: {e2}")
                    
                    # 방법 3: html5lib 파서 시도 (설치되어 있다면)
                    try:
                        dfs = pd.read_html(StringIO(html_source), flavor='html5lib')
                        logger.debug(f"html5lib 파서로 {len(dfs)}개 테이블 추출 성공.")
                    except Exception as e3:
                        logger.debug(f"html5lib 파서 실패: {e3}")
                        
                        # 방법 4: BeautifulSoup으로 수동 파싱
                        logger.info("모든 pandas 파서 실패. BeautifulSoup으로 수동 파싱 시도.")
                        dfs = self._manual_table_extraction(html_source)
            
            # 추출된 테이블 검증 및 정리
            valid_dfs = []
            if dfs:
                for df in dfs:
                    if not df.empty and df.shape[0] > 0 and df.shape[1] > 0:
                        # 빈 행/열 제거
                        df.dropna(axis=0, how='all', inplace=True)
                        df.dropna(axis=1, how='all', inplace=True)
                        
                        if df.empty: 
                            continue

                        # MultiIndex 컬럼 처리
                        if isinstance(df.columns, pd.MultiIndex):
                            df.columns = ['_'.join(map(str, col)).strip('_') for col in df.columns.values]
                        else:
                            df.columns = [str(col) for col in df.columns]
                        
                        valid_dfs.append(df.reset_index(drop=True))
                        
                logger.debug(f"{len(valid_dfs)}개의 유효한 테이블 추출 성공.")
            else:
                logger.debug("추출된 테이블이 없습니다.")
                
            return valid_dfs
            
        except ValueError as ve:
            # pandas.read_html이 "No tables found" 등의 오류를 발생시킬 때
            logger.debug(f"pandas.read_html 실행 중 ValueError: {ve}. 테이블 없음으로 간주.")
            return []
        except Exception as e:
            logger.error(f"테이블 추출 중 예상치 못한 오류: {e}", exc_info=True)
            return []

    def _manual_table_extraction(self, html_source):
        """BeautifulSoup을 사용하여 수동으로 테이블을 추출합니다."""
        try:
            soup = BeautifulSoup(html_source, 'html.parser')
            tables = soup.find_all('table')
            
            extracted_dfs = []
            
            for table_idx, table in enumerate(tables):
                try:
                    # 테이블의 모든 행 가져오기
                    rows = table.find_all('tr')
                    if not rows:
                        continue
                    
                    # 헤더 추출 시도
                    headers = []
                    data_rows = []
                    
                    # 첫 번째 행에서 헤더 찾기 (th 태그 우선, 없으면 td 사용)
                    first_row = rows[0]
                    header_cells = first_row.find_all('th')
                    if not header_cells:
                        header_cells = first_row.find_all('td')
                    
                    if header_cells:
                        headers = [cell.get_text(strip=True) for cell in header_cells]
                        # 헤더가 있으면 두 번째 행부터 데이터로 처리
                        data_start_idx = 1
                    else:
                        # 헤더가 없으면 첫 번째 행부터 데이터로 처리
                        data_start_idx = 0
                    
                    # 데이터 행 추출
                    for row in rows[data_start_idx:]:
                        cells = row.find_all(['td', 'th'])
                        if cells:
                            row_data = [cell.get_text(strip=True) for cell in cells]
                            # 빈 행 제외
                            if any(row_data):
                                data_rows.append(row_data)
                    
                    # DataFrame 생성
                    if data_rows:
                        if headers and len(headers) > 0:
                            # 헤더와 데이터 열 개수 맞추기
                            max_cols = max(len(headers), max(len(row) for row in data_rows))
                            
                            # 헤더 패딩
                            if len(headers) < max_cols:
                                headers.extend([f'Column_{i+1}' for i in range(len(headers), max_cols)])
                            
                            # 데이터 행 패딩
                            for i, row in enumerate(data_rows):
                                if len(row) < max_cols:
                                    data_rows[i] = row + [''] * (max_cols - len(row))
                                elif len(row) > max_cols:
                                    data_rows[i] = row[:max_cols]
                            
                            df = pd.DataFrame(data_rows, columns=headers)
                        else:
                            # 헤더가 없으면 기본 컬럼명 사용
                            df = pd.DataFrame(data_rows)
                            df.columns = [f'Column_{i+1}' for i in range(len(df.columns))]
                        
                        extracted_dfs.append(df)
                        logger.debug(f"수동 파싱으로 테이블 {table_idx+1} 추출 성공: {df.shape}")
                        
                except Exception as e:
                    logger.warning(f"테이블 {table_idx+1} 수동 파싱 실패: {e}")
                    continue
            
            logger.info(f"BeautifulSoup 수동 파싱으로 {len(extracted_dfs)}개 테이블 추출")
            return extracted_dfs
            
        except Exception as e:
            logger.error(f"수동 테이블 추출 중 오류: {e}")
            return []

    def _scrape_single_bank_attempt(self, bank_name, driver):
        """단일 은행 데이터 스크래핑 시도 (1회)"""
        logger.info(f"[{bank_name}] 스크래핑 시도 시작.")
        
        original_window_handle = None
        if driver.window_handles:
             original_window_handle = driver.current_window_handle

        if not self.select_bank(driver, bank_name):
            logger.error(f"[{bank_name}] 은행 선택 실패.")
            return None

        # 날짜 정보 추출
        date_info = self.extract_date_information(driver)
        logger.info(f"[{bank_name}] 공시 날짜 정보: {date_info}")
        
        bank_data_for_excel = {'_INFO_': pd.DataFrame({
            '은행명': [bank_name], 
            '공시날짜': [date_info], 
            '추출일시': [datetime.now().strftime("%Y-%m-%d %H:%M:%S")]
        })}
        
        scraped_something_meaningful = False

        for category_idx, category_name in enumerate(self.config.CATEGORIES):
            logger.info(f"[{bank_name}] '{category_name}' 카테고리 처리 시작.")
            
            # 카테고리 탭 선택 시도
            category_selected = False
            for cat_attempt in range(2):
                if self.select_category(driver, category_name):
                    category_selected = True
                    break
                else:
                    logger.warning(f"[{bank_name}] '{category_name}' 카테고리 선택 실패 (시도 {cat_attempt+1}). 페이지 새로고침 후 재시도.")
                    driver.refresh()
                    self._wait_for_page_load(driver)
                    time.sleep(1)
            
            if not category_selected:
                logger.error(f"[{bank_name}] '{category_name}' 카테고리 최종 선택 실패.")
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
                logger.warning(f"[{bank_name}] '{category_name}'에서 테이블을 찾을 수 없음.")
        
        # 원래 창으로 돌아오기
        if original_window_handle and original_window_handle != driver.current_window_handle:
            for handle in driver.window_handles:
                if handle != original_window_handle:
                    driver.switch_to.window(handle)
                    driver.close()
            driver.switch_to.window(original_window_handle)

        if scraped_something_meaningful:
            return bank_data_for_excel
        else:
            logger.warning(f"[{bank_name}] 의미있는 데이터를 스크랩하지 못했습니다.")
            return None

    def save_bank_data(self, bank_name, excel_data_dict):
        """스크래핑한 데이터를 엑셀 파일로 저장합니다."""
        date_info_df = excel_data_dict.get('_INFO_')
        date_str_for_filename = "날짜미상"
        if date_info_df is not None and not date_info_df.empty:
            raw_date_str = date_info_df['공시날짜'].iloc[0]
            match = re.search(r'(\d{4})년(\d{1,2})월', raw_date_str)
            if match:
                date_str_for_filename = f"{match.group(1)}-{int(match.group(2)):02d}"
            else:
                 date_str_for_filename = re.sub(r'[^\w\-_.]', '', raw_date_str)

        excel_file_name = f"{bank_name}_{date_str_for_filename}.xlsx"
        excel_path = self.config.output_dir / excel_file_name
        
        try:
            with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
                for sheet_name, df_to_save in excel_data_dict.items():
                    actual_sheet_name = '정보' if sheet_name == '_INFO_' else sheet_name
                    df_to_save.to_excel(writer, sheet_name=actual_sheet_name, index=False)
            logger.info(f"[{bank_name}] 데이터 저장 완료: {excel_path.name}")
            return True
        except Exception as e:
            logger.error(f"[{bank_name}] 데이터 저장 실패 ({excel_path.name}): {e}")
            return False

    async def worker_process_bank(self, bank_name, pbar):
        """단일 은행 스크래핑 작업자 (비동기)"""
        driver = None
        try:
            driver = await asyncio.get_event_loop().run_in_executor(None, self.driver_manager.get_driver)
            if not driver:
                logger.error(f"[{bank_name}] WebDriver를 가져올 수 없습니다. 이 은행 처리를 건너뜁니다.")
                self.progress_manager.mark_failed(bank_name)
                pbar.update(1)
                return bank_name, False

            scraped_data = None
            for attempt in range(self.config.MAX_RETRIES):
                try:
                    scraped_data = self._scrape_single_bank_attempt(bank_name, driver)
                    if scraped_data:
                        break
                except Exception as e_attempt:
                    logger.warning(f"[{bank_name}] 스크래핑 시도 {attempt + 1} 중 오류: {e_attempt}. 드라이버 상태 확인 및 재시도.")
                    self.driver_manager.return_driver(driver)
                    if attempt < self.config.MAX_RETRIES - 1:
                         driver = await asyncio.get_event_loop().run_in_executor(None, self.driver_manager.get_driver)
                         if not driver:
                             logger.error(f"[{bank_name}] 재시도를 위한 WebDriver를 가져올 수 없습니다.")
                             break
                    else:
                        logger.error(f"[{bank_name}] 모든 재시도({self.config.MAX_RETRIES}회) 실패.")
                        scraped_data = None
            
            success = False
            if scraped_data:
                if self.save_bank_data(bank_name, scraped_data):
                    self.progress_manager.mark_completed(bank_name)
                    success = True
                else:
                    self.progress_manager.mark_failed(bank_name)
            else:
                self.progress_manager.mark_failed(bank_name)

            pbar.update(1)
            pbar.set_postfix_str(f"{'성공' if success else '실패'}: {bank_name}")
            return bank_name, success

        except Exception as e_worker:
            logger.error(f"[{bank_name}] 작업자 실행 중 예외 발생: {e_worker}", exc_info=True)
            self.progress_manager.mark_failed(bank_name)
            pbar.update(1)
            pbar.set_postfix_str(f"오류: {bank_name}")
            return bank_name, False
        finally:
            if driver:
                await asyncio.get_event_loop().run_in_executor(None, self.driver_manager.return_driver, driver)
    
    async def run(self):
        """메인 스크래핑 실행 (비동기)"""
        start_time_total = time.monotonic()
        logger.info(f"스크래핑 시작 (v{self.config.VERSION})")

        pending_banks = self.progress_manager.get_pending_banks()
        if not pending_banks:
            logger.info("처리할 은행이 없습니다. 모든 은행이 이미 처리되었습니다.")
            self.generate_summary_and_send_email()
            return

        logger.info(f"총 {len(pending_banks)}개 은행 처리 예정: {pending_banks[:5]}... 등")

        tasks = []
        with tqdm(total=len(pending_banks), desc="은행 데이터 스크래핑", unit="은행", dynamic_ncols=True) as pbar:
            for bank_name in pending_banks:
                tasks.append(self.worker_process_bank(bank_name, pbar))
            
            await asyncio.gather(*tasks)

        end_time_total = time.monotonic()
        total_duration_sec = end_time_total - start_time_total
        logger.info(f"전체 스크래핑 작업 완료. 총 소요 시간: {total_duration_sec:.2f}초 ({total_duration_sec/60:.2f}분)")
        
        self.generate_summary_and_send_email()

    def generate_summary_and_send_email(self):
        """스크래핑 결과 요약 보고서를 생성하고 이메일로 전송합니다."""
        # 요약 보고서 생성
        summary_data = []
        for bank in self.config.BANKS:
            status = '미처리'
            date_info_str = ''
            file_found = False
            
            bank_files = list(self.config.output_dir.glob(f"{bank}_*.xlsx"))
            if bank_files:
                file_found = True
            
            if bank in self.progress_manager.progress.get('completed', []) and file_found:
                status = '완료'
            elif bank in self.progress_manager.progress.get('failed', []):
                status = '실패'
            elif file_found:
                status = '파일있음 (상태확인필요)'
            
            summary_data.append({
                '은행명': bank, 
                '상태': status, 
                '확인시간': datetime.now().strftime("%H:%M:%S")
            })
        
        summary_df = pd.DataFrame(summary_data)
        summary_filename = f"스크래핑_요약_{self.config.today}.xlsx"
        summary_file_path = self.config.output_dir / summary_filename
        try:
            summary_df.to_excel(summary_file_path, index=False)
            logger.info(f"요약 보고서 생성: {summary_file_path}")
        except Exception as e:
            logger.error(f"요약 보고서 저장 실패: {e}")
            return

        # 결과 압축
        zip_filename = f"저축은행_데이터_{self.config.today}.zip"
        zip_file_path = self.config.output_dir.parent / zip_filename
        try:
            with zipfile.ZipFile(zip_file_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                for item in self.config.output_dir.rglob('*'):
                    if item.is_file():
                        zipf.write(item, item.relative_to(self.config.output_dir.parent))
            logger.info(f"결과 압축 완료: {zip_file_path}")
        except Exception as e:
            logger.error(f"결과 압축 실패: {e}")
            zip_file_path = None

        # 이메일 본문 생성
        completed_count = len(self.progress_manager.progress.get('completed', []))
        failed_count = len(self.progress_manager.progress.get('failed', []))
        total_processed = completed_count + failed_count
        success_rate = (completed_count / total_processed * 100) if total_processed > 0 else 0
        
        email_subject = f"[저축은행 데이터] {self.config.today} 스크래핑 결과 - 성공률 {success_rate:.1f}%"
        body_html = f"""
        <html><body>
        <h2>저축은행 데이터 스크래핑 결과 ({self.config.today})</h2>
        <p><strong>총 처리 시도 은행 수:</strong> {total_processed} / {len(self.config.BANKS)}</p>
        <p><strong style="color:green;">성공:</strong> {completed_count}개</p>
        <p><strong style="color:red;">실패:</strong> {failed_count}개</p>
        <p><strong>성공률:</strong> {success_rate:.1f}%</p>
        <p><strong>출력 폴더:</strong> {self.config.output_dir.name}</p>
        <p>세부 결과는 첨부된 요약 보고서 및 압축 파일을 확인하세요.</p>
        <br>
        <h3>은행별 처리 결과</h3>
        {summary_df.to_html(index=False, border=1) if not summary_df.empty else "<p>요약 데이터가 없습니다.</p>"}
        <br>
        <p><small>이 메일은 자동 발송되었습니다. (v{self.config.VERSION})</small></p>
        </body></html>
        """
        
        # 이메일 전송
        self.email_sender.send_email_with_attachment(
            email_subject, 
            body_html, 
            zip_file_path if zip_file_path and zip_file_path.exists() else None
        )

# --- 메인 실행 로직 ---
def main():
    """메인 함수: 스크래핑 프로세스를 시작합니다."""
    parser = argparse.ArgumentParser(description='저축은행 중앙회 데이터 스크래퍼 (GitHub Actions 최적화)')
    args = parser.parse_args()

    # 설정 초기화
    config = Config()
    
    driver_manager = None
    try:
        # 드라이버 관리자 초기화
        driver_manager = DriverManager(config)
        
        # 진행 상황 관리자 초기화
        progress_manager = ProgressManager(config)
        
        # 스크래퍼 초기화 및 실행
        scraper = BankScraper(config, driver_manager, progress_manager)
        
        # 비동기 실행
        asyncio.run(scraper.run())
        
        logger.info("모든 스크래핑 프로세스 정상 완료.")

    except Exception as e:
        if logger:
            logger.error(f"스크립트 실행 중 최상위 레벨 오류 발생: {e}", exc_info=True)
        else:
            print(f"FATAL ERROR (logger not initialized): {e}")
        sys.exit(1)
    finally:
        if driver_manager:
            driver_manager.quit_all()
        if logger:
            logger.info("스크립트 실행 종료.")

if __name__ == "__main__":
    main()
