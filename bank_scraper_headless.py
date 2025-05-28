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
- 이메일 알림 기능 (은행별 공시 날짜 및 예상 날짜 일치 여부 포함)
- 실행 시간 단축을 위한 대기 시간 최적화
- 강화된 공시 날짜 확인 및 경고 기능 (분기말 + 2개월 후 마지막 평일 업로드 규칙 기반)
- 더 견고해진 웹페이지 내 날짜 정보 추출 로직 적용
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
from datetime import datetime, date, timedelta # date, timedelta 추가
import calendar # calendar 추가
from io import StringIO
import argparse
import logging
from pathlib import Path
import queue

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
from selenium.common.exceptions import WebDriverException, TimeoutException, NoSuchElementException, StaleElementReferenceException
from selenium.webdriver.chrome.options import Options

# 데이터 처리 관련 임포트
from bs4 import BeautifulSoup
import pandas as pd
from tqdm import tqdm
import warnings
warnings.filterwarnings("ignore")

# --- 로깅 설정 ---
def setup_logging(log_file_path, log_level="INFO"):
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_file_path, encoding='utf-8')
        ]
    )
    logging.getLogger('selenium.webdriver.remote.remote_connection').setLevel(logging.WARNING)
    logging.getLogger('urllib3.connectionpool').setLevel(logging.WARNING)
    return logging.getLogger(__name__)

logger = None

# --- 날짜 검증 클래스 ---
class DateVerifier:
    def get_last_weekday(self, year: int, month: int) -> date:
        """특정 연도와 월의 마지막 평일(월-금)을 반환합니다."""
        last_day_num = calendar.monthrange(year, month)[1]
        last_date_of_month = date(year, month, last_day_num)
        current_date = last_date_of_month
        while current_date.weekday() >= 5:  # 토요일(5), 일요일(6)
            current_date -= timedelta(days=1)
        return current_date

    def get_expected_disclosure_period_info(self, current_processing_date: date) -> tuple[str, str]:
        """
        스크래퍼 실행 현재 날짜를 기준으로 예상되는 최신 공시 자료의 기준 기간과 판단 근거를 반환합니다.
        공시 규칙: 매 분기말(3,6,9월) 종료 후 2개월이 지난 시점의 마지막 평일 업로드.
        반환 예: ("2024년9월말", "현재 처리일자 2025-01-15 기준, 2024년 11월 마지막 평일(2024-11-29) 이후이므로 2024년9월말 데이터 예상.")
        """
        year = current_processing_date.year
        
        lw_may_curr_year = self.get_last_weekday(year, 5)
        lw_aug_curr_year = self.get_last_weekday(year, 8)
        lw_nov_curr_year = self.get_last_weekday(year, 11)

        expected_period_str = "결정 불가"
        reason_details = [] 

        if current_processing_date >= lw_nov_curr_year:
            expected_period_str = f"{year}년{9}월말"
            reason_details.append(f"{year}년 11월 마지막 평일({lw_nov_curr_year}) 이후")
            reason_details.append(f"따라서 {year}년 9월말 데이터 예상.")
        elif current_processing_date >= lw_aug_curr_year:
            expected_period_str = f"{year}년{6}월말"
            reason_details.append(f"{year}년 8월 마지막 평일({lw_aug_curr_year}) 이후")
            reason_details.append(f"{year}년 11월 마지막 평일({lw_nov_curr_year}) 이전")
            reason_details.append(f"따라서 {year}년 6월말 데이터 예상.")
        elif current_processing_date >= lw_may_curr_year:
            expected_period_str = f"{year}년{3}월말"
            reason_details.append(f"{year}년 5월 마지막 평일({lw_may_curr_year}) 이후")
            reason_details.append(f"{year}년 8월 마지막 평일({lw_aug_curr_year}) 이전")
            reason_details.append(f"따라서 {year}년 3월말 데이터 예상.")
        else:
            prev_year = year - 1
            expected_period_str = f"{prev_year}년{9}월말"
            reason_details.append(f"{year}년 5월 마지막 평일({lw_may_curr_year}) 이전")
            reason_details.append(f"따라서 전년도 기준 적용, {prev_year}년 9월말 데이터 예상.")
        
        full_reason = f"현재 처리일자 {current_processing_date} 기준: " + ", ".join(reason_details)
        return expected_period_str, full_reason

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
            msg.attach(MIMEText(body, 'html', 'utf-8'))
            
            if attachment_path and Path(attachment_path).exists():
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
        self.VERSION = "2.9.1-robust-date-extraction" 
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
        
        try:
            self.processing_date_kst = datetime.now().date() 
        except Exception as e:
            logger.error(f"KST 기준 현재 날짜 가져오기 실패 (TZ 환경변수 확인 필요): {e}. UTC 기준으로 대체합니다.")
            self.processing_date_kst = datetime.utcnow().date()

        self.date_verifier = DateVerifier()
        self.expected_latest_disclosure_period, self.expected_period_reason = \
            self.date_verifier.get_expected_disclosure_period_info(self.processing_date_kst)

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
        logger.info(f"현재 처리일자(KST 가정): {self.processing_date_kst}")
        logger.info(f"예상되는 최신 공시 기준일: '{self.expected_latest_disclosure_period}'.")
        logger.info(f"판단 근거: {self.expected_period_reason}")
        logger.info(f"출력 기본 디렉토리: {self.output_dir_base.resolve()}")
        logger.info(f"출력 상세 디렉토리: {self.output_dir.resolve()}")
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
        options.add_argument('--no-sandbox'); options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu'); options.add_argument('--window-size=1920,1080')
        options.add_argument('user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
        options.add_argument('--disable-extensions'); options.add_argument('--disable-browser-side-navigation')
        options.add_argument('--disable-infobars'); options.add_argument('--disable-notifications')
        options.add_argument('--disable-popup-blocking'); options.add_argument('--blink-settings=imagesEnabled=false')
        options.add_experimental_option('excludeSwitches', ['enable-logging', 'enable-automation'])
        options.add_experimental_option('useAutomationExtension', False)
        try:
            driver = webdriver.Chrome(options=options)
            driver.set_page_load_timeout(self.config.PAGE_LOAD_TIMEOUT)
            logger.debug("새 WebDriver 인스턴스 생성 완료.")
            return driver
        except WebDriverException as e:
            logger.error(f"WebDriver 인스턴스 생성 실패: {e}", exc_info=True)
            if "executable needs to be in PATH" in str(e).lower() or \
               "unable to find driver" in str(e).lower() or \
               "cannot find chrome binary" in str(e).lower():
                logger.error("WebDriverException 관련 오류: ChromeDriver 또는 Chrome 브라우저를 찾을 수 없는 것 같습니다.")
                logger.error(f"현재 시스템 PATH: {os.getenv('PATH')}")
            raise
        except Exception as e:
            logger.error(f"WebDriver 인스턴스 생성 중 예상치 못한 오류: {e}", exc_info=True)
            raise

    def _initialize_pool(self):
        logger.info(f"드라이버 풀 초기화 시작 (최대 {self.config.MAX_WORKERS}개)...")
        for i in range(self.config.MAX_WORKERS):
            try:
                driver = self._create_new_driver()
                self.driver_pool.put_nowait(driver) 
            except queue.Full: logger.warning(f"드라이버 {i+1} 추가 시도 중 풀 꽉 참."); break 
            except Exception: logger.error(f"초기 드라이버 {i+1} 생성 실패. 풀 초기화 영향 가능성.")
        logger.info(f"드라이버 풀 초기화 완료. 사용 가능: {self.driver_pool.qsize()}개.")

    def get_driver(self):
        try: return self.driver_pool.get(block=True, timeout=60)
        except queue.Empty: raise TimeoutError(f"60초 대기 후에도 풀에서 드라이버 가져오지 못함 (MAX_WORKERS: {self.config.MAX_WORKERS}).")

    def return_driver(self, driver):
        if not driver: return
        returned = False
        try:
            _ = driver.title 
            if self.driver_pool.qsize() < self.config.MAX_WORKERS: self.driver_pool.put_nowait(driver); returned = True
            else: logger.warning(f"풀({self.driver_pool.qsize()}) 꽉 참, 반환 드라이버 종료."); driver.quit()
        except queue.Full: logger.warning(f"반납 시 풀 꽉 참. 드라이버 종료."); driver.quit()
        except Exception as e:
            logger.warning(f"손상된 드라이버 반환 시도 ({type(e).__name__}). 드라이버 종료.")
            try: driver.quit()
            except: pass 
            if not returned: self._add_new_driver_to_pool_if_needed()
            
    def _add_new_driver_to_pool_if_needed(self):
        if self.driver_pool.qsize() < self.config.MAX_WORKERS:
            try:
                logger.info("손상 드라이버 대체용 새 드라이버 생성 시도...")
                self.driver_pool.put_nowait(self._create_new_driver())
            except Exception as e: logger.error(f"대체 드라이버 생성/추가 실패: {e}", exc_info=True)

    def quit_all(self):
        logger.info("모든 드라이버 종료 시작...")
        drained = 0
        while not self.driver_pool.empty():
            try:
                driver = self.driver_pool.get_nowait(); driver.quit(); drained += 1
            except: break # Or log error
        logger.info(f"총 {drained}개 드라이버 종료 시도 완료.")

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
                    loaded = json.load(f)
                    if 'banks' not in loaded: loaded['banks'] = {}
                    if 'stats' not in loaded: loaded['stats'] = default_progress['stats']
                    return loaded
            except Exception as e: logger.warning(f"진행 파일 로드 오류({self.progress_file_path}): {e}. 새로 시작.")
        return default_progress

    def save(self):
        self.progress['stats']['last_run'] = datetime.now().isoformat()
        s, f = 0, 0
        for bank_info in self.progress.get('banks', {}).values():
            if bank_info.get('status') == 'completed': s += 1
            elif bank_info.get('status') == 'failed': f += 1
        self.progress['stats']['success_count'] = s; self.progress['stats']['failure_count'] = f
        try:
            with open(self.progress_file_path, 'w', encoding='utf-8') as f_out:
                json.dump(self.progress, f_out, ensure_ascii=False, indent=2)
        except Exception as e: logger.error(f"진행 파일 저장 실패: {e}", exc_info=True)

    def mark_completed(self, bank_name, date_info):
        self.progress.setdefault('banks', {})[bank_name] = {'status': 'completed', 'date_info': date_info}
        self.save()

    def mark_failed(self, bank_name):
        bank_data = self.progress.setdefault('banks', {}).get(bank_name, {})
        bank_data['status'] = 'failed' # 기존 date_info는 유지될 수 있도록 status만 변경
        self.progress['banks'][bank_name] = bank_data
        self.save()

    def get_pending_banks(self):
        processed = self.progress.get('banks', {})
        pending = [b for b in self.config.BANKS if b not in processed or processed[b].get('status') != 'completed']
        logger.info(f"보류 은행(재시도 포함 가능): {len(pending)}개")
        return pending
        
    def get_bank_data(self, bank_name): return self.progress.get('banks', {}).get(bank_name)

# --- 스크래퍼 클래스 ---
class BankScraper:
    def __init__(self, config, driver_manager, progress_manager):
        self.config = config
        self.driver_manager = driver_manager
        self.progress_manager = progress_manager
        self.email_sender = EmailSender()

    def _wait_for_element(self, driver, by, value, timeout=None):
        try: return WebDriverWait(driver, timeout or self.config.WAIT_TIMEOUT).until(EC.presence_of_element_located((by, value)))
        except TimeoutException: logger.debug(f"요소 대기 시간 초과: ({by}, {value})"); return None

    def _robust_click(self, driver, element):
        try:
            driver.execute_script("arguments[0].scrollIntoView({block: 'center', inline: 'nearest'});", element)
            time.sleep(0.2)
            driver.execute_script("arguments[0].click();", element)
            return True
        except Exception: # Fallback to selenium click
            try: element.click(); return True
            except Exception as e_sel: logger.warning(f"Robust 클릭 실패: {e_sel}"); return False

    def extract_date_information(self, driver):
        logger.debug("날짜 정보 추출 시도 (개선된 로직 v2.9.1)...")
        try:
            js_script = """
            var allMatches = [];
            var datePattern = /(\d{4})년\s*(\d{1,2})월말/g; // 연도와 월을 캡처

            var bodyText = document.body.innerText || "";
            var match;
            datePattern.lastIndex = 0; 
            while ((match = datePattern.exec(bodyText)) !== null) {
                allMatches.push({
                    fullText: match[0], 
                    year: parseInt(match[1]),
                    month: parseInt(match[2])
                });
            }

            var tagsToSearch = ['h1', 'h2', 'h3', 'th', 'td', 'p', 'span', 'div.title', 'caption', '.date', '#publishDate', '.disclosure-date'];
            for (var i = 0; i < tagsToSearch.length; i++) {
                var elements;
                try { elements = document.querySelectorAll(tagsToSearch[i]); } 
                catch (e) { elements = []; }
                
                for (var j = 0; j < elements.length; j++) {
                    var elementText = elements[j].innerText || "";
                    if (elementText.length > 3000) elementText = elementText.substring(0, 3000);
                    
                    datePattern.lastIndex = 0; 
                    while ((match = datePattern.exec(elementText)) !== null) {
                        var currentYear = parseInt(match[1]);
                        var currentMonth = parseInt(match[2]);
                        var exists = allMatches.some(function(m) {
                            return m.year === currentYear && m.month === currentMonth &&
                                   m.fullText.replace(/\s+/g, '') === match[0].replace(/\s+/g, '');
                        });
                        if (!exists) {
                            allMatches.push({ fullText: match[0], year: currentYear, month: currentMonth });
                        }
                    }
                }
            }

            if (allMatches.length === 0) return '날짜 정보 없음';

            allMatches.sort(function(a, b) {
                if (b.year !== a.year) return b.year - a.year;
                return b.month - a.month;
            });

            var systemYear = new Date().getFullYear();
            var reasonableDates = [];
            var latestFoundYear = allMatches.length > 0 ? allMatches[0].year : 0;

            if (latestFoundYear > 0) {
                reasonableDates = allMatches.filter(function(m) {
                    if (m.year < latestFoundYear - 5 && m.year < systemYear - 3) return false;
                    return m.year >= systemYear - 10; 
                });
            }

            if (reasonableDates.length > 0) return reasonableDates[0].fullText.replace(/\s+/g, '');
            else if (allMatches.length > 0) return allMatches[0].fullText.replace(/\s+/g, '');
            return '날짜 정보 없음';
            """
            date_info = driver.execute_script(js_script)
            logger.debug(f"추출된 최종 날짜 정보: '{date_info}'")
            return date_info
        except Exception as e:
            logger.error(f"날짜 정보 추출 중 오류 (개선된 로직): {e}", exc_info=True)
            return "날짜 추출 실패"

    def select_bank(self, driver, bank_name):
        logger.debug(f"[{bank_name}] 은행 선택 시도...")
        driver.get(self.config.BASE_URL)
        WebDriverWait(driver, self.config.PAGE_LOAD_TIMEOUT).until(lambda d: d.execute_script('return document.readyState') == 'complete')
        time.sleep(random.uniform(0.4, 0.8))
        for xpath_fmt in [f"//td[normalize-space(text())='{bank_name}']", f"//a[normalize-space(text())='{bank_name}']"]:
            try:
                for el in driver.find_elements(By.XPATH, xpath_fmt):
                    if el.is_displayed() and self._robust_click(driver, el): time.sleep(random.uniform(0.5,1.0)); return True
            except Exception: pass
        js_select = f"var els=Array.from(document.querySelectorAll('a,td')); var t=els.find(e=>e.textContent&&e.textContent.trim().includes('{bank_name}')); if(t){{t.scrollIntoView({{block:'center'}}); (t.tagName==='TD'&&t.querySelector('a')?t.querySelector('a'):t).click();return true;}} return false;"
        try:
            if driver.execute_script(js_select): time.sleep(random.uniform(0.5,1.0)); return True
        except Exception: pass
        for xpath_fmt in [f"//td[contains(normalize-space(.),'{bank_name}')]",f"//a[contains(normalize-space(.),'{bank_name}')]"]:
            try:
                els = driver.find_elements(By.XPATH, xpath_fmt)
                els.sort(key=lambda x: len(x.text) if x.text else float('inf'))
                for el in els:
                    if el.is_displayed() and self._robust_click(driver, el): time.sleep(random.uniform(0.5,1.0)); return True
            except Exception: pass
        logger.warning(f"[{bank_name}] 은행 선택 최종 실패."); return False

    def select_category(self, driver, category_name):
        logger.debug(f"카테고리 선택: '{category_name}'")
        time.sleep(random.uniform(0.2, 0.5))
        cat_norm = category_name.replace(' ','')
        selectors = [
            (By.XPATH, f"//a[normalize-space(translate(text(),' \t\n\r',''))='{cat_norm}']"), (By.XPATH, f"//button[normalize-space(translate(text(),' \t\n\r',''))='{cat_norm}']"),
            (By.LINK_TEXT, category_name), (By.PARTIAL_LINK_TEXT, category_name)
        ]
        for by_type, val in selectors:
            try:
                for el in driver.find_elements(by_type, val):
                    if el.is_displayed() and el.is_enabled() and self._robust_click(driver, el): time.sleep(random.uniform(0.4,0.8)); return True
            except Exception: pass
        js_select = f"var els=Array.from(document.querySelectorAll('a,li,button,span,div[role=\"tab\"]'));var t=els.find(e=>e.textContent&&e.textContent.trim().includes('{category_name}'));if(t){{t.scrollIntoView({{block:'center'}});t.click();return true;}}return false;"
        try:
            if driver.execute_script(js_select): time.sleep(random.uniform(0.4,0.8)); return True
        except Exception: pass
        logger.warning(f"'{category_name}' 카테고리 선택 최종 실패."); return False
        
    def extract_tables_from_page(self, driver):
        WebDriverWait(driver, self.config.PAGE_LOAD_TIMEOUT).until(lambda d: d.execute_script('return document.readyState') == 'complete')
        time.sleep(random.uniform(0.3, 0.6))
        try:
            src = driver.page_source
            if not src or len(src) < 300 or "<table" not in src.lower(): return []
            dfs = pd.read_html(StringIO(src), flavor='bs4', encoding='utf-8')
            valid = []
            for df in dfs:
                if not isinstance(df,pd.DataFrame) or df.empty: continue
                df.dropna(axis=0,how='all',inplace=True); df.dropna(axis=1,how='all',inplace=True)
                if df.empty: continue
                df.columns = ['_'.join(map(str,c)).strip('_ ') if isinstance(df.columns,pd.MultiIndex) else str(c).strip() for c in df.columns]
                valid.append(df.reset_index(drop=True))
            return valid
        except ValueError: return []
        except Exception as e: logger.error(f"테이블 추출 오류: {e}", exc_info=True); return []

    def _scrape_single_bank_attempt(self, bank_name, driver):
        logger.info(f"[{bank_name}] 스크래핑 시도...")
        if not self.select_bank(driver, bank_name): return None

        date_info_scraped = self.extract_date_information(driver)
        logger.info(f"[{bank_name}] 추출 공시일: '{date_info_scraped}'")
        
        expected_period = self.config.expected_latest_disclosure_period
        if date_info_scraped in ["날짜 정보 없음", "날짜 추출 실패"]:
            logger.error(f"[{bank_name}] 날짜 추출 실패. 예상일('{expected_period}')과 비교 불가.")
        elif date_info_scraped != expected_period:
            logger.critical(f"[{bank_name}] !!날짜 불일치!! 웹사이트: '{date_info_scraped}', 예상: '{expected_period}'. (근거: {self.config.expected_period_reason})")
        else:
            logger.info(f"[{bank_name}] 공시일('{date_info_scraped}')이 예상과 일치.")
        
        data = {'_INFO_': pd.DataFrame([{'은행명':bank_name, '공시날짜':date_info_scraped, '추출일시':datetime.now().strftime("%Y-%m-%d %H:%M:%S"), '스크래퍼버전':self.config.VERSION}])}
        
        has_data = False
        orig_url = driver.current_url 
        for cat_name in self.config.CATEGORIES:
            cat_selected = False
            for attempt in range(2):
                if attempt > 0: 
                    driver.get(orig_url); WebDriverWait(driver,self.config.PAGE_LOAD_TIMEOUT).until(lambda d:d.execute_script('return document.readyState')=='complete'); time.sleep(0.5)
                if self.select_category(driver, cat_name): cat_selected=True; break
            if not cat_selected: logger.error(f"[{bank_name}] '{cat_name}' 최종 선택 실패."); continue
            
            tables = self.extract_tables_from_page(driver)
            if tables:
                for i, df_tbl in enumerate(tables):
                    data[re.sub(r'[\\/*?:\[\]]', '', f"{cat_name}_{i+1}")[:31]] = df_tbl
                has_data = True
        return data if has_data else None

    def save_bank_data(self, bank_name, excel_data_dict):
        raw_date = excel_data_dict['_INFO_']['공시날짜'].iloc[0]
        match = re.search(r'(\d{4})년(\d{1,2})월', raw_date)
        date_fn = f"{match.group(1)}-{int(match.group(2)):02d}" if match else re.sub(r'[^\w\-_.]', '', raw_date or "날짜정보없음")
        
        excel_path = self.config.output_dir / f"{bank_name}_{date_fn}.xlsx"
        try:
            with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
                for sheet, df in excel_data_dict.items(): df.to_excel(writer, sheet_name=('정보' if sheet=='_INFO_' else sheet), index=False)
            logger.info(f"[{bank_name}] 저장 완료: {excel_path.name}")
            return True
        except Exception as e: logger.error(f"[{bank_name}] 저장 실패 ({excel_path.name}): {e}", exc_info=True); return False

    async def worker_process_bank(self, bank_name, pbar, semaphore): # pbar 인자명 수정
        async with semaphore:
            driver, success, date_info = None, False, self.progress_manager.get_bank_data(bank_name).get('date_info') if self.progress_manager.get_bank_data(bank_name) else None
            try:
                driver = await asyncio.get_event_loop().run_in_executor(None, self.driver_manager.get_driver)
                if not driver: self.progress_manager.mark_failed(bank_name); return bank_name, False, date_info
                
                data = None
                for attempt in range(self.config.MAX_RETRIES):
                    try:
                        data = self._scrape_single_bank_attempt(bank_name, driver)
                        if data: date_info = data['_INFO_']['공시날짜'].iloc[0]; break
                    except Exception as e: 
                        logger.warning(f"[{bank_name}] 시도 {attempt+1} 중 예외: {e}")
                        if attempt < self.config.MAX_RETRIES-1:
                            await asyncio.get_event_loop().run_in_executor(None, self.driver_manager.return_driver, driver)
                            driver = await asyncio.get_event_loop().run_in_executor(None, self.driver_manager.get_driver)
                            if not driver: break
                        else: data = None
                
                if data and self.save_bank_data(bank_name, data):
                    self.progress_manager.mark_completed(bank_name, date_info); success = True
                else: self.progress_manager.mark_failed(bank_name)
                return bank_name, success, date_info
            except Exception as e: logger.error(f"[{bank_name}] 작업자 예외: {e}", exc_info=True); self.progress_manager.mark_failed(bank_name); return bank_name, False, date_info
            finally:
                if driver: await asyncio.get_event_loop().run_in_executor(None, self.driver_manager.return_driver, driver)
                if pbar: pbar.update(1)
                logger.info(f"[{bank_name}] 처리: {'성공' if success else '실패'}, 공시일: {date_info or '미확정'}")
    
    async def run(self):
        logger.info(f"==== 스크래핑 시작 (v{self.config.VERSION}) ====")
        start_time = time.monotonic()
        pending = self.progress_manager.get_pending_banks()
        if not pending: logger.info("처리할 은행 없음."); self.generate_summary_and_send_email(); return
        
        with tqdm(total=len(pending), desc="은행 스크래핑", unit="은행", dynamic_ncols=True) as pbar:
            tasks = [self.worker_process_bank(b, pbar, asyncio.Semaphore(self.config.MAX_WORKERS)) for b in pending]
            await asyncio.gather(*tasks, return_exceptions=True) # 결과 로깅은 worker 내부에서 처리
            
        logger.info(f"==== 전체 스크래핑 완료. 소요시간: {time.monotonic() - start_time:.2f}초 ====")
        self.generate_summary_and_send_email()

    def generate_summary_and_send_email(self):
        logger.info("요약 보고서 및 이메일 생성 시작...")
        summary, banks_cfg = [], self.config.BANKS
        processed = self.progress_manager.progress.get('banks', {})
        expected_date = self.config.expected_latest_disclosure_period
        comp, fail = 0,0; failed_names = []

        for bn in banks_cfg:
            detail = processed.get(bn); status, disc_date, match_status = '미처리', '', ''
            if detail:
                actual_disc_date = detail.get('date_info', '날짜 없음')
                if detail.get('status') == 'completed':
                    status, disc_date, comp = '완료', actual_disc_date, comp + 1
                    if disc_date == expected_date: match_status = "✅ 일치"
                    elif disc_date in ["날짜 정보 없음", "날짜 추출 실패"]: match_status = "⚠️ 추출실패"
                    else: match_status = f"❌ 불일치! (예상: {expected_date})"
                elif detail.get('status') == 'failed':
                    status, disc_date, fail = '실패', (actual_disc_date if actual_disc_date not in ["날짜 정보 없음", "날짜 추출 실패", None, ''] else ''), fail + 1
                    failed_names.append(bn); match_status = "Н/Д (실패)"
            summary.append({'은행명':bn, '공시 날짜':disc_date, '날짜 확인':match_status, '처리 상태':status, '확인 시간':datetime.now().strftime("%Y-%m-%d %H:%M:%S")})
        
        summary_df = pd.DataFrame(summary)
        summary_fp = self.config.output_dir / f"스크래핑_요약_{self.config.today}.xlsx"
        try: summary_df.to_excel(summary_fp, index=False); logger.info(f"요약 보고서: {summary_fp}")
        except Exception as e: logger.error(f"요약 보고서 저장 실패: {e}", exc_info=True)

        zip_fn = f"저축은행_데이터_{self.config.today}.zip"
        zip_fp = self.config.output_dir_base / zip_fn # ./output/저축은행_데이터_YYYYMMDD.zip
        try:
            with zipfile.ZipFile(zip_fp, 'w', zipfile.ZIP_DEFLATED) as zf:
                # ./output/저축은행_데이터_YYYYMMDD/ 폴더 내용을 압축 시 상위 폴더명(저축은행_데이터_YYYYMMDD)으로 시작하도록 arcname 설정
                for f_path in self.config.output_dir.rglob('*'):
                    if f_path.is_file(): 
                        arc = Path(self.config.output_dir.name) / f_path.relative_to(self.config.output_dir)
                        zf.write(f_path, arc)
            logger.info(f"결과 압축 완료: {zip_fp}")
        except Exception as e: logger.error(f"결과 압축 실패: {e}", exc_info=True); zip_fp = None

        proc_attempt = comp + fail
        success_rate = (comp / proc_attempt * 100) if proc_attempt > 0 else 0
        email_subject = f"[저축은행 데이터] {self.config.today} ({comp}/{len(banks_cfg)} 완료, 날짜확인 필요)"
        
        failed_disp = "".join(f"<li>{b}</li>" for b in failed_names[:10])
        if len(failed_names)>10: failed_disp += f"<p>...외 {len(failed_names)-10}개.</p>"
        
        body_html = f"""
        <html><head><style>
            body{{font-family:Arial,sans-serif;margin:20px}} h2{{color:#2c3e50}}
            .summary-box{{border:1px solid #ddd;padding:15px;margin-bottom:20px;background-color:#f9f9f9;border-radius:5px}}
            .summary-box p{{margin:5px 0}} .status-completed{{color:green}} .status-failed{{color:red}}
            table{{border-collapse:collapse;width:95%;margin-top:15px;font-size:0.85em}}
            th,td{{border:1px solid #ddd;padding:5px;text-align:left;white-space:nowrap}} th{{background-color:#f0f0f0}}
        </style></head><body>
        <h2>저축은행 스크래핑 결과 ({self.config.today})</h2>
        <p><strong>예상 최신 공시 기준일:</strong> {expected_date} (근거: {self.config.expected_period_reason})</p>
        <div class="summary-box">
            <p>총 대상: {len(banks_cfg)}개</p> <p>처리 시도: {proc_attempt}개</p>
            <p><span class="status-completed">✅ 성공: {comp}개</span></p>
            <p><span class="status-failed">❌ 실패: {fail}개</span> (성공률: {success_rate:.1f}%)</p>
            <p>📂 데이터: {self.config.output_dir.name} (압축: {zip_fn if zip_fp else '생성실패'})</p>
        </div>
        <h3>실패 은행 (최대 10개):</h3><ul>{failed_disp if failed_names else "없음"}</ul>
        <p>세부 결과는 첨부파일 확인.</p>
        <h3>은행별 처리 현황:</h3>
        {summary_df.to_html(index=False,border=1,na_rep='').replace('<td>','<td style="white-space:normal;">') if not summary_df.empty else "<p>요약 데이터 없음.</p>"}
        <br><p><small>자동 발송 (v{self.config.VERSION})</small></p>
        </body></html>"""
        
        attach_path = str(zip_fp) if zip_fp and zip_fp.exists() else (str(summary_fp) if summary_fp.exists() else None)
        self.email_sender.send_email_with_attachment(email_subject, body_html, attach_path)

# --- 메인 실행 로직 ---
def main():
    config = Config() 
    driver_mgr = None 
    try:
        logger.info(f"스크립트 실행: {sys.argv[0]}")
        driver_mgr = DriverManager(config)
        progress_mgr = ProgressManager(config)
        scraper = BankScraper(config, driver_mgr, progress_mgr)
        asyncio.run(scraper.run()) 
        logger.info("모든 스크래핑 정상 완료.")
    except Exception as e:
        if logger: logger.critical(f"최상위 오류: {e}", exc_info=True)
        else: print(f"최상위 오류 (로거 미설정): {e}\n{traceback.format_exc()}") # traceback 추가
        sys.exit(1) 
    finally:
        if driver_mgr: driver_mgr.quit_all() 
        if logger: logger.info("스크립트 실행 종료.")
        else: print("스크립트 실행 종료 (로거 미설정).")

if __name__ == "__main__":
    # Windows에서 Python 3.8+ asyncio 관련 ProactorEventLoop 오류 방지 (필요시 주석 해제)
    # if sys.platform == "win32" and sys.version_info >= (3, 8):
    # import asyncio
    # asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    import traceback # main의 예외 처리를 위해 임포트
    main()
