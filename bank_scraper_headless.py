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
        
        # 기준 업로드 날짜 (해당 분기 자료가 업로드되는 달의 마지막 평일)
        # 3월말 자료 -> 5월 마지막 평일 업로드
        # 6월말 자료 -> 8월 마지막 평일 업로드
        # 9월말 자료 -> 11월 마지막 평일 업로드
        
        lw_may_curr_year = self.get_last_weekday(year, 5)
        lw_aug_curr_year = self.get_last_weekday(year, 8)
        lw_nov_curr_year = self.get_last_weekday(year, 11)

        expected_period_str = "결정 불가"
        reason_details = [] # 상세 사유를 담을 리스트

        if current_processing_date >= lw_nov_curr_year:
            # 현 년도 11월 마지막 평일 이후 -> 현 년도 9월말 자료가 최신
            expected_period_str = f"{year}년{9}월말"
            reason_details.append(f"{year}년 11월 마지막 평일({lw_nov_curr_year}) 이후")
            reason_details.append(f"따라서 {year}년 9월말 데이터 예상.")
        elif current_processing_date >= lw_aug_curr_year:
            # 현 년도 8월 마지막 평일 이후 (11월 마지막 평일 이전) -> 현 년도 6월말 자료가 최신
            expected_period_str = f"{year}년{6}월말"
            reason_details.append(f"{year}년 8월 마지막 평일({lw_aug_curr_year}) 이후")
            reason_details.append(f"{year}년 11월 마지막 평일({lw_nov_curr_year}) 이전")
            reason_details.append(f"따라서 {year}년 6월말 데이터 예상.")
        elif current_processing_date >= lw_may_curr_year:
            # 현 년도 5월 마지막 평일 이후 (8월 마지막 평일 이전) -> 현 년도 3월말 자료가 최신
            expected_period_str = f"{year}년{3}월말"
            reason_details.append(f"{year}년 5월 마지막 평일({lw_may_curr_year}) 이후")
            reason_details.append(f"{year}년 8월 마지막 평일({lw_aug_curr_year}) 이전")
            reason_details.append(f"따라서 {year}년 3월말 데이터 예상.")
        else:
            # 현 년도 5월 마지막 평일 이전 -> 전년도 9월말 자료가 최신
            prev_year = year - 1
            # lw_nov_prev_year = self.get_last_weekday(prev_year, 11) # 참고용
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
        self.VERSION = "2.9-strict-date-check" 
        self.BASE_URL = "https://www.fsb.or.kr/busmagequar_0100.act"
        self.MAX_RETRIES = int(os.getenv('MAX_RETRIES', '2'))
        self.PAGE_LOAD_TIMEOUT = int(os.getenv('PAGE_LOAD_TIMEOUT', '20')) 
        self.WAIT_TIMEOUT = int(os.getenv('WAIT_TIMEOUT', '10')) 
        self.MAX_WORKERS = int(os.getenv('MAX_WORKERS', '3')) 

        self.today = datetime.now().strftime("%Y%m%d") # 파일명 등에 사용될 YYYYMMDD 형식
        self.output_dir_base = Path(os.getenv('OUTPUT_DIR', "./output"))
        self.output_dir = self.output_dir_base / f"저축은행_데이터_{self.today}" # 예: ./output/저축은행_데이터_20250528
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.progress_file = self.output_dir / 'progress.json'
        self.log_file_path = self.output_dir / f'scraping_log_{self.today}.log'

        global logger
        if logger is None: 
            logger = setup_logging(self.log_file_path, os.getenv('LOG_LEVEL', 'INFO'))
        
        # 날짜 검증 로직 관련 초기화
        try:
            # TZ 환경변수(예: 'Asia/Seoul')가 시스템 레벨에서 적용되었다고 가정
            self.processing_date_kst = datetime.now().date() 
        except Exception as e:
            logger.error(f"KST 기준 현재 날짜 가져오기 실패 (TZ 환경변수 확인 필요): {e}. UTC 기준으로 대체합니다.")
            self.processing_date_kst = datetime.utcnow().date() # Fallback

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
        options.add_argument('--blink-settings=imagesEnabled=false')
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
                logger.debug(f"드라이버 {i+1} 생성하여 풀에 추가. 현재 풀 크기: {self.driver_pool.qsize()}")
            except queue.Full:
                logger.warning(f"드라이버 {i+1} 추가 시도 중 풀이 꽉 참.")
                break 
            except Exception as e: # _create_new_driver에서 발생한 예외 포함
                logger.error(f"초기 드라이버 {i+1} 생성 실패. 풀 초기화 중단 가능성 있음.")
                # 필요에 따라 여기서 풀 초기화를 중단하거나, 계속 시도할 수 있습니다.
                # 여기서는 로깅 후 다음 드라이버 생성을 시도합니다.
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
                    if 'banks' not in loaded_progress:
                        logger.warning("progress.json에 'banks' 키가 없습니다. 새 구조로 초기화합니다.")
                        loaded_progress['banks'] = {}
                    if 'stats' not in loaded_progress:
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
        existing_bank_data = self.progress.get('banks', {}).get(bank_name, {})
        self.progress.setdefault('banks', {})[bank_name] = {
            'status': 'failed',
            'date_info': existing_bank_data.get('date_info') # 실패해도 기존 날짜 정보 유지
        }
        self.save()

    def get_pending_banks(self):
        processed_banks = self.progress.get('banks', {})
        pending_banks = [
            bank for bank in self.config.BANKS
            if bank not in processed_banks or processed_banks[bank].get('status') != 'completed'
        ]
        logger.info(f"보류 중인 은행 수 (재시도 대상 포함 가능): {len(pending_banks)}")
        return pending_banks
        
    def get_bank_data(self, bank_name):
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
            time.sleep(0.2)
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
        time.sleep(random.uniform(0.4, 0.8))

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
                            time.sleep(random.uniform(0.5, 1.0))
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
                time.sleep(random.uniform(0.5, 1.0))
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
                            time.sleep(random.uniform(0.5, 1.0))
                            return True
            except Exception as e: logger.debug(f"XPath (부분) '{xpath}' 오류: {e}")

        logger.warning(f"[{bank_name}] 은행 선택 최종 실패.")
        return False

    def select_category(self, driver, category_name):
        logger.debug(f"카테고리 선택 시도: '{category_name}'")
        time.sleep(random.uniform(0.2, 0.5))

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
                            time.sleep(random.uniform(0.4, 0.8))
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
                time.sleep(random.uniform(0.4, 0.8))
                return True
        except Exception as e: logger.debug(f"'{category_name}' 카테고리: JavaScript 선택 오류: {e}")

        logger.warning(f"'{category_name}' 카테고리 탭 선택 최종 실패.")
        return False
        
    def extract_tables_from_page(self, driver):
        logger.debug("페이지에서 테이블 추출 시도.")
        WebDriverWait(driver, self.config.PAGE_LOAD_TIMEOUT).until(
            lambda d: d.execute_script('return document.readyState') == 'complete'
        )
        time.sleep(random.uniform(0.3, 0.6))

        try:
            html_source = driver.page_source
            if not html_source or len(html_source) < 300 or "<table" not in html_source.lower():
                return []
            dfs = pd.read_html(StringIO(html_source), flavor='bs4', encoding='utf-8')
            valid_dfs = []
            if dfs:
                for df in dfs:
                    if not isinstance(df, pd.DataFrame) or df.empty: continue
                    df.dropna(axis=0, how='all', inplace=True)
                    df.dropna(axis=1, how='all', inplace=True)
                    if df.empty: continue
                    if isinstance(df.columns, pd.MultiIndex):
                        df.columns = ['_'.join(map(str, col)).strip('_ ') for col in df.columns.values]
                    else:
                        df.columns = [str(col).strip() for col in df.columns]
                    valid_dfs.append(df.reset_index(drop=True))
            return valid_dfs
        except ValueError: return [] # pandas.read_html이 테이블을 못 찾으면 ValueError 발생
        except Exception as e:
            logger.error(f"테이블 추출 중 오류: {e}", exc_info=True)
            return []

    def _scrape_single_bank_attempt(self, bank_name, driver):
        logger.info(f"[{bank_name}] 스크래핑 시도 시작...")
        if not self.select_bank(driver, bank_name):
            logger.error(f"[{bank_name}] 은행 선택 최종 실패.")
            return None

        date_info_scraped = self.extract_date_information(driver)
        logger.info(f"[{bank_name}] 추출된 공시 날짜 정보: '{date_info_scraped}'")
        
        expected_period_to_compare = self.config.expected_latest_disclosure_period
        if date_info_scraped in ["날짜 정보 없음", "날짜 추출 실패"]:
            logger.error(f"[{bank_name}] 웹사이트 날짜 추출 실패. 예상 기준일('{expected_period_to_compare}')과 비교 불가.")
        elif date_info_scraped != expected_period_to_compare:
            logger.critical(
                f"[{bank_name}] 심각한 날짜 불일치! "
                f"웹사이트 공시일: '{date_info_scraped}', "
                f"예상 최신 공시일: '{expected_period_to_compare}'. "
                f"({self.config.expected_period_reason})"
            )
        else:
            logger.info(f"[{bank_name}] 웹사이트 공시일('{date_info_scraped}')이 예상 최신 공시일과 일치합니다.")
        
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
                    driver.get(original_url_after_bank_selection) 
                    WebDriverWait(driver, self.config.PAGE_LOAD_TIMEOUT).until(lambda d: d.execute_script('return document.readyState') == 'complete')
                    time.sleep(0.5) 
                if self.select_category(driver, category_name):
                    category_selected = True; break
                else: logger.warning(f"[{bank_name}] '{category_name}' 선택 실패 (시도 {attempt + 1}).")
            
            if not category_selected:
                logger.error(f"[{bank_name}] '{category_name}' 최종 선택 실패. 다음 카테고리로."); continue
            tables = self.extract_tables_from_page(driver)
            if tables:
                for i, df_table in enumerate(tables):
                    sheet_name = re.sub(r'[\\/*?:\[\]]', '', f"{category_name}_{i+1}")[:31]
                    bank_data_for_excel[sheet_name] = df_table
                scraped_something_meaningful = True
            else: logger.warning(f"[{bank_name}] '{category_name}'에서 테이블 찾지 못함.")
        
        return bank_data_for_excel if scraped_something_meaningful else None

    def save_bank_data(self, bank_name, excel_data_dict):
        date_info_df = excel_data_dict.get('_INFO_')
        date_str_for_filename = "날짜정보없음"
        if date_info_df is not None and not date_info_df.empty:
            raw_date_str = date_info_df['공시날짜'].iloc[0]
            match = re.search(r'(\d{4})년(\d{1,2})월', raw_date_str) # 월말 제거하고 년월만 사용
            if match:
                date_str_for_filename = f"{match.group(1)}-{int(match.group(2)):02d}"
            elif raw_date_str and raw_date_str not in ['날짜 정보 없음', '날짜 추출 실패']:
                date_str_for_filename = re.sub(r'[^\w\-_.]', '', raw_date_str)

        # 파일명은 은행명_날짜.xlsx (예: 다올_2024-09.xlsx)
        # 저장 경로는 output/저축은행_데이터_YYYYMMDD/다올_2024-09.xlsx
        excel_file_name = f"{bank_name}_{date_str_for_filename}.xlsx"
        excel_path = self.config.output_dir / excel_file_name # 상세 디렉토리 내에 저장
        
        try:
            with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
                for sheet_name_key, df_to_save in excel_data_dict.items():
                    actual_sheet_name = '정보' if sheet_name_key == '_INFO_' else sheet_name_key
                    df_to_save.to_excel(writer, sheet_name=actual_sheet_name, index=False)
            logger.info(f"[{bank_name}] 데이터 저장 완료: {excel_path.name} (경로: {excel_path})")
            return True
        except Exception as e:
            logger.error(f"[{bank_name}] 데이터 저장 실패 ({excel_path.name}): {e}", exc_info=True)
            return False

    async def worker_process_bank(self, bank_name, pbar_instance, semaphore):
        async with semaphore:
            logger.debug(f"[{bank_name}] Semaphore 획득, 작업 시작.")
            driver = None
            success_status = False
            # 실패 시에도 기존 날짜 정보가 있다면 유지하기 위해 progress_manager에서 먼저 조회
            bank_progress_data = self.progress_manager.get_bank_data(bank_name)
            bank_date_info_for_progress = bank_progress_data.get('date_info') if bank_progress_data else None

            try:
                driver = await asyncio.get_event_loop().run_in_executor(None, self.driver_manager.get_driver)
                if not driver:
                    self.progress_manager.mark_failed(bank_name); return bank_name, False, bank_date_info_for_progress
                scraped_data = None
                for attempt in range(self.config.MAX_RETRIES):
                    logger.info(f"[{bank_name}] 스크래핑 시도 {attempt + 1}/{self.config.MAX_RETRIES}")
                    try:
                        scraped_data = self._scrape_single_bank_attempt(bank_name, driver)
                        if scraped_data:
                            bank_date_info_for_progress = scraped_data.get('_INFO_').iloc[0]['공시날짜']
                            break
                    except Exception as e_attempt:
                        logger.warning(f"[{bank_name}] 시도 {attempt + 1} 중 예외: {e_attempt}")
                        if attempt < self.config.MAX_RETRIES - 1:
                            await asyncio.get_event_loop().run_in_executor(None, self.driver_manager.return_driver, driver)
                            driver = await asyncio.get_event_loop().run_in_executor(None, self.driver_manager.get_driver)
                            if not driver: break 
                        else: scraped_data = None 
                
                if scraped_data:
                    if self.save_bank_data(bank_name, scraped_data):
                        self.progress_manager.mark_completed(bank_name, bank_date_info_for_progress)
                        success_status = True
                    else: self.progress_manager.mark_failed(bank_name) 
                else: self.progress_manager.mark_failed(bank_name) 
                return bank_name, success_status, bank_date_info_for_progress
            except TimeoutError: self.progress_manager.mark_failed(bank_name); return bank_name, False, bank_date_info_for_progress
            except Exception as e: logger.error(f"[{bank_name}] 작업자 예외: {e}", exc_info=True); self.progress_manager.mark_failed(bank_name); return bank_name, False, bank_date_info_for_progress
            finally:
                if driver: await asyncio.get_event_loop().run_in_executor(None, self.driver_manager.return_driver, driver)
                if pbar_instance: pbar_instance.update(1)
                log_date = bank_date_info_for_progress or "날짜 미확정"
                logger.info(f"[{bank_name}] 처리: {'성공' if success_status else '실패'}, 공시일: {log_date}")

    async def run(self):
        start_time_total = time.monotonic()
        logger.info(f"==== 스크래핑 프로세스 시작 (v{self.config.VERSION}) ====")
        pending_banks = self.progress_manager.get_pending_banks()
        if not pending_banks:
            logger.info("처리할 은행 없음."); self.generate_summary_and_send_email(); return
        logger.info(f"총 {len(pending_banks)}개 은행 처리 예정: {pending_banks[:3]}{'...' if len(pending_banks)>3 else ''}")
        semaphore = asyncio.Semaphore(self.config.MAX_WORKERS)
        tasks = [self.worker_process_bank(b, tqdm(total=len(pending_banks), desc="은행 스크래핑", unit="은행", dynamic_ncols=True), semaphore) for b in pending_banks]
        # tqdm을 tasks 생성 시점에 한 번만 만들고, 각 worker에 pbar_instance로 전달하는 방식이 더 일반적.
        # 현재 코드는 각 worker 호출 시마다 tqdm 인스턴스를 (재)생성하므로, tqdm 진행바가 여러 줄 생기거나 이상하게 보일 수 있음.
        # 여기서는 코드 구조 변경 최소화를 위해 그대로 두지만, 개선점으로 인지. (pbar_instance 인자를 수정)
        # 수정: with tqdm(...) as pbar: tasks = [self.worker_process_bank(b, pbar, semaphore) for b in pending_banks]
        with tqdm(total=len(pending_banks), desc="은행 데이터 스크래핑", unit="은행", dynamic_ncols=True, smoothing=0.1) as pbar:
            tasks = [self.worker_process_bank(bank_name, pbar, semaphore) for bank_name in pending_banks]
            results_or_exceptions = await asyncio.gather(*tasks, return_exceptions=True)

        # gather 결과 로깅 (주로 디버깅용)
        processed_count = 0
        for res_or_exc in results_or_exceptions:
            if not isinstance(res_or_exc, Exception): processed_count +=1
        logger.info(f"asyncio.gather로 {processed_count}/{len(pending_banks)}개 작업 반환됨 (예외 포함).")
        
        end_time_total = time.monotonic()
        logger.info(f"==== 전체 스크래핑 완료. 소요시간: {end_time_total - start_time_total:.2f}초 ====")
        self.generate_summary_and_send_email()

    def generate_summary_and_send_email(self):
        logger.info("요약 보고서 생성 및 이메일 전송 시작...")
        summary_data = []
        all_banks_in_config = self.config.BANKS
        processed_banks_data = self.progress_manager.progress.get('banks', {})
        expected_period_for_summary = self.config.expected_latest_disclosure_period
        
        completed_count = 0
        failed_count = 0
        failed_banks_names = []

        for bank_name_iter in all_banks_in_config:
            bank_detail = processed_banks_data.get(bank_name_iter)
            status = '미처리'
            disclosure_date_val = '' 
            date_match_status = '' 

            if bank_detail:
                current_status = bank_detail.get('status')
                actual_disclosure_date_from_progress = bank_detail.get('date_info', '날짜 없음')

                if current_status == 'completed':
                    status = '완료'
                    disclosure_date_val = actual_disclosure_date_from_progress
                    completed_count +=1
                    if disclosure_date_val == expected_period_for_summary:
                        date_match_status = "✅ 일치"
                    elif disclosure_date_val in ["날짜 정보 없음", "날짜 추출 실패"]:
                        date_match_status = "⚠️ 추출실패"
                    else:
                        date_match_status = f"❌ 불일치! (예상: {expected_period_for_summary})"
                elif current_status == 'failed':
                    status = '실패'
                    disclosure_date_val = actual_disclosure_date_from_progress if actual_disclosure_date_from_progress not in ["날짜 정보 없음", "날짜 추출 실패", None, ''] else ''
                    failed_count +=1
                    failed_banks_names.append(bank_name_iter)
                    date_match_status = "Н/Д (실패)" # Not Applicable
            
            summary_data.append({
                '은행명': bank_name_iter, 
                '공시 날짜': disclosure_date_val,
                '날짜 확인': date_match_status, 
                '처리 상태': status, 
                '확인 시간': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            })
        
        summary_df = pd.DataFrame(summary_data)
        summary_filename = f"스크래핑_요약_{self.config.today}.xlsx"
        summary_file_path = self.config.output_dir / summary_filename # 상세 디렉토리에 저장
        try:
            summary_df.to_excel(summary_file_path, index=False)
            logger.info(f"요약 보고서 생성 완료: {summary_file_path}")
        except Exception as e:
            logger.error(f"요약 보고서 저장 실패: {e}", exc_info=True)

        # 압축 파일명 및 경로 (Python 스크립트의 Config 및 저장 로직 기준)
        # output_dir_base (예: ./output) 내부에 zip 파일 생성
        zip_filename = f"저축은행_데이터_{self.config.today}.zip"
        zip_file_path = self.config.output_dir_base / zip_filename # ./output/저축은행_데이터_YYYYMMDD.zip
        try:
            logger.info(f"결과 압축 시작: {self.config.output_dir}의 내용물과 기타 파일을 {zip_file_path}에 압축")
            with zipfile.ZipFile(zip_file_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                # output_dir (./output/저축은행_데이터_YYYYMMDD) 내부의 모든 파일 압축
                # arcname을 지정하여 zip 파일 내 경로를 output_dir_base 기준으로 맞춤
                # 예: 저축은행_데이터_YYYYMMDD/스크래핑_요약_...xlsx
                for file_path in self.config.output_dir.rglob('*'):
                    if file_path.is_file():
                        # zip 파일 내 경로: 저축은행_데이터_YYYYMMDD/실제파일명
                        arcname_in_zip = Path(self.config.output_dir.name) / file_path.relative_to(self.config.output_dir)
                        zipf.write(file_path, arcname_in_zip)
                # 로그 파일이 output_dir 밖에 있을 경우 추가 (현재는 output_dir 안에 있음)
            logger.info(f"결과 압축 완료: {zip_file_path}")
        except Exception as e:
            logger.error(f"결과 압축 실패: {e}", exc_info=True)
            zip_file_path = None 

        total_banks_in_list = len(all_banks_in_config)
        processed_attempt_count = completed_count + failed_count
        success_rate = (completed_count / processed_attempt_count * 100) if processed_attempt_count > 0 else 0
        email_subject = f"[저축은행 데이터] {self.config.today} 스크래핑 ({completed_count}/{total_banks_in_list} 완료, 날짜확인 요망)"
        
        failed_banks_display_list = failed_banks_names[:10]
        failed_banks_html = "<ul>" + "".join(f"<li>{b}</li>" for b in failed_banks_display_list) + "</ul>"
        if len(failed_banks_names) > 10: failed_banks_html += f"<p>...외 {len(failed_banks_names) - 10}개.</p>"
        if not failed_banks_names: failed_banks_html = "<p>없음</p>"

        body_html = f"""
        <html><head><style>
            body {{ font-family: Arial, sans-serif; margin: 20px; }} h2 {{ color: #2c3e50; }}
            .summary-box {{ border: 1px solid #ddd; padding: 15px; margin-bottom: 20px; background-color: #f9f9f9; border-radius: 5px; }}
            .summary-box p {{ margin: 5px 0; }} .status-completed {{ color: green; }} .status-failed {{ color: red; }}
            table {{ border-collapse: collapse; width: 95%; margin-top:15px; font-size: 0.85em; }}
            th, td {{ border: 1px solid #ddd; padding: 5px; text-align: left; }} th {{ background-color: #f0f0f0; }}
        </style></head><body>
        <h2>저축은행 데이터 스크래핑 결과 ({self.config.today})</h2>
        <p><strong>예상 최신 공시 기준일:</strong> {expected_period_for_summary} (근거: {self.config.expected_period_reason})</p>
        <div class="summary-box">
            <p>총 대상 은행: {total_banks_in_list}개</p>
            <p>처리 시도: {processed_attempt_count}개</p>
            <p><span class="status-completed">✅ 성공: {completed_count}개</span></p>
            <p><span class="status-failed">❌ 실패: {failed_count}개</span> (성공률: {success_rate:.1f}%)</p>
            <p>📂 데이터 폴더: {self.config.output_dir.name} (압축파일: {zip_filename if zip_file_path else '생성실패'})</p>
        </div>
        <h3>실패 은행 (최대 10개):</h3>{failed_banks_html}
        <p>세부 결과는 첨부된 요약 보고서(엑셀) 및 전체 데이터(ZIP)를 확인하세요.</p>
        <h3>은행별 처리 현황 (공시일 및 날짜 확인 상태 포함):</h3>
        {summary_df.to_html(index=False, border=1, na_rep='') if not summary_df.empty else "<p>요약 테이블 데이터 없음.</p>"}
        <br><p><small>자동 발송 메일 (v{self.config.VERSION})</small></p>
        </body></html>"""
        
        attachment_to_send = str(zip_file_path) if zip_file_path and zip_file_path.exists() else \
                             (str(summary_file_path) if summary_file_path.exists() else None)
        if attachment_to_send and Path(attachment_to_send).name == summary_filename and zip_file_path:
            logger.warning("압축 파일 생성 실패 또는 누락. 요약 보고서만 첨부합니다.")
        elif not attachment_to_send:
             logger.warning("압축 파일 및 요약 보고서 모두 누락. 첨부 파일 없이 발송.")

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
        logger.info("모든 스크래핑 프로세스 정상 완료.")
    except Exception as e:
        if logger: logger.critical(f"최상위 레벨 오류: {e}", exc_info=True)
        else: print(f"최상위 레벨 오류 (로거 미설정): {e}")
        sys.exit(1) 
    finally:
        if driver_manager: driver_manager.quit_all() 
        if logger: logger.info("스크립트 실행 종료.")
        else: print("스크립트 실행 종료 (로거 미설정).")

if __name__ == "__main__":
    main()
