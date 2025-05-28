# bank_scraper_headless.py
"""
저축은행 중앙회 통일경영공시 데이터 자동 스크래핑 도구 (GitHub Actions 최적화 버전)
목적: GitHub Actions에서 자동 실행, 병렬 처리를 통한 속도 개선
작성일: 2025-03-31 (최종 수정일: 2025-05-28)
특징:
- (이전 기능 모두 포함)
- 실행 속도 개선을 위한 타임아웃 및 대기 시간 최적화 시도
- 엑셀 시트 취합 시 카테고리별 데이터 중복 누적 오류 수정
- MAX_WORKERS 기본값 조정 (GitHub Actions 환경 고려)
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
from datetime import datetime, date, timedelta
import calendar
from io import StringIO
import argparse
import logging
from pathlib import Path
import queue
import traceback
from typing import Union, Dict, Tuple

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
    for handler in logging.root.handlers[:]: logging.root.removeHandler(handler)
    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler(log_file_path, encoding='utf-8')]
    )
    logging.getLogger('selenium.webdriver.remote.remote_connection').setLevel(logging.WARNING)
    logging.getLogger('urllib3.connectionpool').setLevel(logging.WARNING)
    return logging.getLogger(__name__)

logger = None

# --- 유틸리티 함수 ---
def normalize_datestr_for_comparison(datestr: str) -> Union[str, None]:
    if not datestr or datestr in ["날짜 정보 없음", "날짜 추출 실패"]: return None
    match = re.search(r'(\d{4})년\s*(\d{1,2})월말', datestr)
    if match: return f"{int(match.group(1))}년{int(match.group(2))}월말"
    if logger: logger.warning(f"날짜 문자열 정규화 실패(패턴 불일치): '{datestr}'")
    return "알 수 없는 형식"

def get_quarter_string_from_period(period_str: str) -> str:
    if not period_str or not isinstance(period_str, str): return "분기정보 불명확"
    match = re.search(r'(\d{4})년(\d{1,2})월말', period_str)
    if match:
        year, month = match.group(1), int(match.group(2))
        q_map = {3: "1분기", 6: "2분기", 9: "3분기", 12: "4분기"}
        return f"{year}년 {q_map.get(month, f'{month}월')}"
    return period_str

# --- 날짜 검증 클래스 ---
class DateVerifier:
    def get_last_weekday(self, year: int, month: int) -> date:
        _, last_day_num = calendar.monthrange(year, month)
        d = date(year, month, last_day_num)
        while d.weekday() >= 5: d -= timedelta(days=1)
        return d
    def get_relevant_disclosure_periods(self, current_processing_date: date) -> Dict[str, str]:
        year = current_processing_date.year
        lw_may_curr = self.get_last_weekday(year, 5)
        lw_aug_curr = self.get_last_weekday(year, 8)
        lw_nov_curr = self.get_last_weekday(year, 11)
        res = {"latest_due_period": "결정 불가", "latest_due_reason": "", "next_imminent_period": "결정 불가", "next_imminent_reason": ""}
        base_reason = f"현재 처리일자 {current_processing_date} 기준"
        if current_processing_date >= lw_nov_curr:
            res["latest_due_period"] = f"{year}년9월말"
            res["latest_due_reason"] = f"{base_reason}: {year}년 11월 마지막 평일({lw_nov_curr}) 이후이므로 {res['latest_due_period']} 데이터가 공식적으로 최신이어야 함."
        elif current_processing_date >= lw_aug_curr:
            res["latest_due_period"] = f"{year}년6월말"
            res["latest_due_reason"] = f"{base_reason}: {year}년 8월 마지막 평일({lw_aug_curr}) 이후이므로 {res['latest_due_period']} 데이터가 공식적으로 최신이어야 함."
        elif current_processing_date >= lw_may_curr:
            res["latest_due_period"] = f"{year}년3월말"
            res["latest_due_reason"] = f"{base_reason}: {year}년 5월 마지막 평일({lw_may_curr}) 이후이므로 {res['latest_due_period']} 데이터가 공식적으로 최신이어야 함."
        else:
            res["latest_due_period"] = f"{year-1}년9월말"
            res["latest_due_reason"] = f"{base_reason}: {year}년 5월 마지막 평일({lw_may_curr}) 이전이므로 전년도 기준 적용, {res['latest_due_period']} 데이터가 공식적으로 최신이어야 함."
        
        if current_processing_date <= lw_may_curr:
            res["next_imminent_period"] = f"{year}년3월말"
            res["next_imminent_reason"] = f"{base_reason}: {year}년 5월 마지막 평일({lw_may_curr})까지 {res['next_imminent_period']} 자료 업로드 기간/임박."
        elif current_processing_date <= lw_aug_curr:
            res["next_imminent_period"] = f"{year}년6월말"
            res["next_imminent_reason"] = f"{base_reason}: {year}년 8월 마지막 평일({lw_aug_curr})까지 {res['next_imminent_period']} 자료 업로드 기간/임박."
        elif current_processing_date <= lw_nov_curr:
            res["next_imminent_period"] = f"{year}년9월말"
            res["next_imminent_reason"] = f"{base_reason}: {year}년 11월 마지막 평일({lw_nov_curr})까지 {res['next_imminent_period']} 자료 업로드 기간/임박."
        else: 
            res["next_imminent_period"] = f"{year+1}년3월말"
            res["next_imminent_reason"] = f"{base_reason}: {year}년 11월 마지막 평일({lw_nov_curr}) 이후이므로, 다음 대상은 {res['next_imminent_period']} (업로드: {year+1}년 5월 마지막 평일)."
        return res

# --- 이메일 전송 클래스 ---
class EmailSender:
    def __init__(self):
        self.smtp_server = "smtp.gmail.com"; self.smtp_port = 587
        self.sender_email = os.getenv('GMAIL_ADDRESS'); self.sender_password = os.getenv('GMAIL_APP_PASSWORD')
        self.recipient_emails = [e.strip() for e in os.getenv('RECIPIENT_EMAILS', '').split(',') if e.strip()]
        self.enabled = bool(self.sender_email and self.sender_password and self.recipient_emails)
        log_msg = "이메일 설정 유효X. 전송X." if not self.enabled else f"이메일 설정OK. 수신자: {self.recipient_emails}"
        if logger: (logger.warning if not self.enabled else logger.info)(log_msg)

    def send_email_with_attachment(self, subject, body, attachment_path=None):
        if not self.enabled:
            if logger: logger.info("이메일 전송 비활성화됨."); return False
        try:
            msg = MIMEMultipart(); msg['From'] = self.sender_email; msg['To'] = ', '.join(self.recipient_emails); msg['Subject'] = subject
            msg.attach(MIMEText(body, 'html', 'utf-8'))
            if attachment_path and Path(attachment_path).exists():
                p_attach = Path(attachment_path)
                if p_attach.suffix.lower() == '.zip': part = MIMEBase('application', 'zip')
                elif p_attach.suffix.lower() == '.xlsx': part = MIMEBase('application', 'vnd.openxmlformats-officedocument.spreadsheetml.sheet')
                else: part = MIMEBase('application', 'octet-stream')
                with open(p_attach, 'rb') as af: part.set_payload(af.read())
                encoders.encode_base64(part)
                base_filename = p_attach.name
                try: part.add_header('Content-Disposition', 'attachment', filename=encoders.encode_rfc2231(base_filename))
                except: part.add_header('Content-Disposition', f'attachment; filename="{base_filename}"')
                msg.attach(part)
                if logger: logger.info(f"첨부파일 추가: {p_attach.name} (Type: {part.get_content_type()})")
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.ehlo(); server.starttls(); server.ehlo()
                server.login(self.sender_email, self.sender_password); server.send_message(msg)
            if logger: logger.info(f"이메일 전송 성공: {self.recipient_emails}"); return True
        except Exception as e:
            if logger: logger.error(f"이메일 전송 실패: {e}", exc_info=True); return False
        return False

# --- 설정 클래스 ---
class Config:
    def __init__(self):
        self.VERSION = "2.10.2-hotfix-kr"  # 버전 업데이트 (국문 설명 반영 표시)
        self.BASE_URL = "https://www.fsb.or.kr/busmagequar_0100.act"
        self.MAX_RETRIES = int(os.getenv('MAX_RETRIES', '2'))
        self.PAGE_LOAD_TIMEOUT = int(os.getenv('PAGE_LOAD_TIMEOUT', '10')) 
        self.WAIT_TIMEOUT = int(os.getenv('WAIT_TIMEOUT', '5'))   
        # 수정됨: MAX_WORKERS 기본값을 4에서 2로 변경 (GitHub Actions 2-core CPU 고려)
        self.MAX_WORKERS = int(os.getenv('MAX_WORKERS', '2')) 

        self.today = datetime.now().strftime("%Y%m%d")
        self.output_dir_base = Path(os.getenv('OUTPUT_DIR', "./output"))
        self.output_dir = self.output_dir_base / f"저축은행_데이터_{self.today}"
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.progress_file = self.output_dir / 'progress.json'
        self.log_file_path = self.output_dir / f'scraping_log_{self.today}.log'

        global logger
        if logger is None: 
            logger = setup_logging(self.log_file_path, os.getenv('LOG_LEVEL', 'INFO'))
        
        try: self.processing_date_kst = datetime.now().date() 
        except Exception as e: logger.error(f"KST 날짜 얻기 실패: {e}. UTC로 대체."); self.processing_date_kst = datetime.utcnow().date()

        self.date_verifier = DateVerifier()
        self.date_expectations = self.date_verifier.get_relevant_disclosure_periods(self.processing_date_kst)
        self.latest_due_period = self.date_expectations["latest_due_period"]
        self.next_imminent_period = self.date_expectations["next_imminent_period"]

        self.BANKS = [ 
            "다올", "대신", "더케이", "민국", "바로", "스카이", "신한", "애큐온", "예가람", "웰컴", "유안타", "조은", "키움YES", "푸른", "하나", "DB", "HB", "JT", "친애", "KB", "NH", "OK", "OSB", "SBI", "금화", "남양", "모아", "부림", "삼정", "상상인", "세람", "안국", "안양", "영진", "융창", "인성", "인천", "키움", "페퍼", "평택", "한국투자", "한화", "고려", "국제", "동원제일", "솔브레인", "에스앤티", "우리", "조흥", "진주", "흥국", "BNK", "DH", "IBK", "대백", "대아", "대원", "드림", "라온", "머스트삼일", "엠에스", "오성", "유니온", "참", "CK", "대한", "더블", "동양", "삼호", "센트럴", "스마트", "스타", "대명", "상상인플러스", "아산", "오투", "우리금융", "청주", "한성"
        ]
        self.CATEGORIES = ["영업개황", "재무현황", "손익현황", "기타"]
        
        logger.info(f"--- 설정 초기화 (v{self.VERSION}) ---")
        logger.info(f"MAX_WORKERS: {self.MAX_WORKERS} (환경변수 MAX_WORKERS로 조정 가능)")
        logger.info(f"처리일자(KST 가정): {self.processing_date_kst}")
        logger.info(f"예상 (기한 지난) 최신 공시일: '{self.latest_due_period}' (근거: {self.date_expectations['latest_due_reason']})")
        logger.info(f"예상 (다음/현재) 공시일: '{self.next_imminent_period}' (근거: {self.date_expectations['next_imminent_reason']})")
        logger.info(f"출력 디렉토리: {self.output_dir.resolve()}")
        logger.info(f"로그 파일: {self.log_file_path.resolve()}")


# --- DriverManager ---
class DriverManager:
    def __init__(self, config):
        self.config = config
        self.driver_pool = queue.Queue(maxsize=self.config.MAX_WORKERS)
        self._initialize_pool()

    def _create_new_driver(self):
        logger.debug("새 WebDriver 생성 시도...")
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
        options.add_argument('--blink-settings=imagesEnabled=false') # 이미지 로딩 비활성화
        options.add_experimental_option('excludeSwitches', ['enable-logging', 'enable-automation'])
        options.add_experimental_option('useAutomationExtension', False)
        
        # GitHub Actions 환경에서 ChromeDriver 경로 명시적 지정 시도 (필요한 경우)
        # if "CI" in os.environ and not os.getenv('CHROMEWEBDRIVER'):
        #     # 예시: /usr/local/bin/chromedriver
        #     # 실제 경로는 GitHub Actions Runner 환경에 따라 다를 수 있음
        #     # self.chrome_driver_path = shutil.which('chromedriver') or "/usr/local/bin/chromedriver"
        #     # driver = webdriver.Chrome(service=Service(executable_path=self.chrome_driver_path), options=options)
        #     pass # 현재는 자동 경로 탐색 사용

        try:
            driver = webdriver.Chrome(options=options)
            driver.set_page_load_timeout(self.config.PAGE_LOAD_TIMEOUT)
            logger.debug("새 WebDriver 생성 완료.")
            return driver
        except WebDriverException as e:
            logger.error(f"WebDriver 생성 실패: {e}", exc_info=True)
            if any(s in str(e).lower() for s in ["executable needs to be in path", "unable to find driver", "cannot find chrome binary"]):
                logger.error(f"WebDriverException: ChromeDriver 또는 Chrome 브라우저 경로 문제로 추정됨. PATH: {os.getenv('PATH')}")
            raise
        except Exception as e:
            logger.error(f"WebDriver 생성 중 예상치 못한 오류: {e}", exc_info=True)
            raise

    def _initialize_pool(self):
        logger.info(f"드라이버 풀 초기화 시작 (최대 {self.config.MAX_WORKERS}개)...")
        for i in range(self.config.MAX_WORKERS):
            try:
                self.driver_pool.put_nowait(self._create_new_driver())
            except queue.Full:
                logger.warning(f"드라이버 {i+1} 추가 중 풀이 가득 찼습니다.")
                break 
            except Exception as e:
                logger.error(f"초기 드라이버 {i+1} 생성 실패 ({type(e).__name__}). 풀 초기화 중단 가능성 있음.")
                # Optionally, continue trying to create other drivers or break
        logger.info(f"드라이버 풀 초기화 완료. 사용 가능 드라이버: {self.driver_pool.qsize()}개.")

    def get_driver(self):
        try:
            return self.driver_pool.get(block=True, timeout=30) # 드라이버 획득 대기 시간 30초
        except queue.Empty:
            raise TimeoutError(f"30초 대기 후에도 풀에서 드라이버를 가져오지 못했습니다 (MAX_WORKERS: {self.config.MAX_WORKERS}).")

    def return_driver(self, driver): 
        if not driver:
            return
        
        can_be_reused = True
        try:
            _ = driver.title # 간단한 드라이버 상태 확인
        except Exception:
            can_be_reused = False

        if can_be_reused:
            if self.driver_pool.qsize() < self.config.MAX_WORKERS:
                try:
                    self.driver_pool.put_nowait(driver)
                except queue.Full: # 이 경우는 거의 없어야 하지만, 안전장치
                    logger.warning("드라이버 반납 시 풀이 가득 참 (예상치 못한 상황). 드라이버 종료.")
                    try: driver.quit()
                    except: pass
            else:
                # 풀이 가득 차면 (MAX_WORKERS 만큼 이미 있음), 현재 드라이버는 종료
                logger.debug(f"드라이버 풀({self.driver_pool.qsize()})이 이미 최대치입니다. 반환된 드라이버를 종료합니다.")
                try: driver.quit()
                except: pass
        else: # 드라이버 손상 시
            logger.warning(f"손상된 드라이버 반환 시도 감지. 드라이버를 종료하고 필요한 경우 새 드라이버를 풀에 추가합니다.")
            try: driver.quit()
            except: pass
            self._add_new_driver_to_pool_if_needed(True) # 손상 드라이버 대체

    def _add_new_driver_to_pool_if_needed(self, replace_broken=False): 
        if self.driver_pool.qsize() < self.config.MAX_WORKERS:
            log_message_prefix = "손상된 드라이버 대체용 " if replace_broken else ""
            logger.info(f"{log_message_prefix}새 드라이버 생성 시도...")
            try:
                self.driver_pool.put_nowait(self._create_new_driver())
                logger.info(f"{log_message_prefix}새 드라이버 풀에 추가 완료. 현재 풀 크기: {self.driver_pool.qsize()}")
            except Exception as e:
                logger.error(f"{log_message_prefix}새 드라이버 생성 또는 풀 추가 실패: {e}", exc_info=True)

    def quit_all(self): 
        logger.info("모든 드라이버 종료 시작...")
        drained_count = 0
        while not self.driver_pool.empty():
            try:
                driver = self.driver_pool.get_nowait()
                driver.quit()
                drained_count += 1
            except queue.Empty:
                break 
            except Exception as e:
                logger.warning(f"드라이버 종료 중 오류: {e}")
        logger.info(f"총 {drained_count}개의 드라이버 종료 시도 완료.")

# --- ProgressManager ---
class ProgressManager:
    def __init__(self, config):
        self.config=config
        self.progress_file_path=config.progress_file
        self.progress=self._load()

    def _load(self):
        default = {'banks': {}, 'stats': {'last_run':None,'success_count':0,'failure_count':0}}
        if self.progress_file_path.exists():
            try:
                with open(self.progress_file_path,'r',encoding='utf-8') as f:
                    loaded_progress =json.load(f)
                    # 기본 구조 유효성 검사 및 보강
                    if 'banks' not in loaded_progress or not isinstance(loaded_progress['banks'], dict):
                        loaded_progress['banks'] = {}
                    if 'stats' not in loaded_progress or not isinstance(loaded_progress['stats'], dict):
                        loaded_progress['stats'] = default['stats'].copy()
                    else: # stats 내부 필드도 검사
                        for key, val_type in [('last_run', (str, type(None))), ('success_count', int), ('failure_count', int)]:
                            if key not in loaded_progress['stats'] or not isinstance(loaded_progress['stats'][key], val_type):
                                loaded_progress['stats'][key] = default['stats'][key]
                    return loaded_progress
            except json.JSONDecodeError as e:
                logger.warning(f"진행 파일 ({self.progress_file_path}) JSON 디코딩 오류: {e}. 새 진행 파일로 시작합니다.")
            except Exception as e:
                logger.warning(f"진행 파일 ({self.progress_file_path}) 로드 중 예외 발생: {e}. 새로 시작합니다.")
        return default

    def save(self):
        # 통계 업데이트
        success_count, failure_count = 0, 0
        for bank_info in self.progress.get('banks', {}).values():
            if bank_info.get('status') == 'completed':
                success_count += 1
            elif bank_info.get('status') == 'failed':
                failure_count += 1
        
        self.progress['stats']['last_run'] = datetime.now().isoformat()
        self.progress['stats']['success_count'] = success_count
        self.progress['stats']['failure_count'] = failure_count
        
        try:
            with open(self.progress_file_path,'w',encoding='utf-8') as fo:
                json.dump(self.progress, fo, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"진행 파일 저장 실패 ({self.progress_file_path}): {e}", exc_info=True)

    def mark_completed(self, bank_name, date_info):
        self.progress.setdefault('banks',{}).setdefault(bank_name, {}) # 은행 키가 없으면 생성
        self.progress['banks'][bank_name]['status'] = 'completed'
        self.progress['banks'][bank_name]['date_info'] = date_info
        self.progress['banks'][bank_name]['last_updated'] = datetime.now().isoformat()
        self.save()

    def mark_failed(self, bank_name):
        self.progress.setdefault('banks',{}).setdefault(bank_name, {}) # 은행 키가 없으면 생성
        # 실패 시 기존 date_info는 유지 (이전에 성공적으로 추출했을 수 있으므로)
        # 만약 date_info가 없다면, "N/A" 또는 빈 문자열 등으로 채울 수 있음
        if 'date_info' not in self.progress['banks'][bank_name]:
             self.progress['banks'][bank_name]['date_info'] = "추출 실패 또는 정보 없음"
        self.progress['banks'][bank_name]['status'] = 'failed'
        self.progress['banks'][bank_name]['last_updated'] = datetime.now().isoformat()
        self.save()

    def get_pending_banks(self):
        processed_banks_info = self.progress.get('banks',{})
        pending_banks_list = [
            b_name for b_name in self.config.BANKS 
            if b_name not in processed_banks_info or processed_banks_info[b_name].get('status') != 'completed'
        ]
        if not pending_banks_list and not processed_banks_info: # 아무것도 처리된 적 없고, 보류 은행도 없는 경우 (첫 실행)
             logger.info("처리된 은행 기록이 없습니다. 모든 은행을 대상으로 합니다.")
             return self.config.BANKS
        logger.info(f"처리 대상 은행 (보류/재시도 포함): {len(pending_banks_list)}개")
        return pending_banks_list

    def get_bank_data(self, bank_name):
        return self.progress.get('banks',{}).get(bank_name)

# --- 스크래퍼 클래스 ---
class BankScraper:
    def __init__(self, config, driver_manager, progress_manager):
        self.config = config
        self.driver_manager = driver_manager
        self.progress_manager = progress_manager
        self.email_sender = EmailSender()

    def _wait_for_element(self, driver, by, value, timeout=None):
        try:
            return WebDriverWait(driver, timeout or self.config.WAIT_TIMEOUT).until(EC.presence_of_element_located((by, value)))
        except TimeoutException:
            logger.debug(f"요소 대기 시간 초과: ({by}, {value})")
            return None

    def _robust_click(self, driver, element):
        try:
            # 요소가 화면 중앙에 오도록 스크롤 후 JavaScript 클릭 시도
            driver.execute_script("arguments[0].scrollIntoView({block: 'center', inline: 'nearest'});", element)
            time.sleep(random.uniform(0.1, 0.25)) # 스크롤 후 DOM 안정화 대기
            driver.execute_script("arguments[0].click();", element)
            return True
        except Exception as e1:
            logger.debug(f"JavaScript 클릭 실패 ({type(e1).__name__}), Selenium click() 시도...")
            try:
                element.click() # 일반 Selenium 클릭 시도
                return True
            except Exception as e2:
                logger.warning(f"Robust 클릭 최종 실패: JS_Error='{str(e1)[:50]}...', Selenium_Click_Error='{type(e2).__name__}: {str(e2)[:50]}...'")
                return False

    def extract_date_information(self, driver):
        logger.debug(f"날짜 정보 추출 시도 (v{self.config.VERSION})...")
        try:
            # JavaScript를 사용하여 페이지 내에서 날짜 패턴을 직접 검색
            js_script = """
            var allMatches = [];
            // 정규식: YYYY년MM월말 또는 YYYY년 M월말 (공백 유무 처리)
            var datePattern = /(\d{4})년\s*(\d{1,2})월말/g;
            var bodyText = document.body.innerText || "";
            var match;
            datePattern.lastIndex = 0; // 정규식 상태 초기화

            // 1. 전체 텍스트에서 검색
            while ((match = datePattern.exec(bodyText)) !== null) {
                allMatches.push({fullText: match[0], year: parseInt(match[1]), month: parseInt(match[2])});
            }

            // 2. 특정 태그들 내에서 더 정밀하게 검색 (중복 방지하며 추가)
            var tagsToSearch = ['h1','h2','h3','th','td','p','span','div.title','caption','.date','#publishDate','.disclosure-date', 'article', 'header'];
            for (var i=0; i < tagsToSearch.length; i++) {
                var elements;
                try {
                    elements = document.querySelectorAll(tagsToSearch[i]);
                } catch(e) { elements = []; } // querySelectorAll 실패 대비

                for (var j=0; j < elements.length; j++) {
                    var elText = elements[j].innerText || "";
                    // 너무 긴 텍스트는 성능을 위해 자르기 (3000자)
                    if (elText.length > 3000) elText = elText.substring(0, 3000);
                    
                    datePattern.lastIndex = 0; // 각 요소 텍스트 검색 전 정규식 초기화
                    while((match = datePattern.exec(elText)) !== null) {
                        var currentYear = parseInt(match[1]);
                        var currentMonth = parseInt(match[2]);
                        // 이미 allMatches에 동일한 연/월이 있는지 확인
                        if (!allMatches.some(m => m.year === currentYear && m.month === currentMonth)) {
                            allMatches.push({fullText: match[0], year: currentYear, month: currentMonth});
                        }
                    }
                }
            }

            if (allMatches.length === 0) return '날짜 정보 없음';

            // 가장 최신 날짜를 찾기 위해 정렬 (연도 내림차순, 월 내림차순)
            allMatches.sort((a, b) => {
                if (b.year !== a.year) return b.year - a.year;
                return b.month - a.month;
            });
            
            // 비상식적인 과거/미래 날짜 필터링 (예: 현재년도 기준 -10년 ~ +1년 범위)
            var systemYear = new Date().getFullYear();
            var latestFoundYear = allMatches[0].year; // 정렬 후 첫 번째가 가장 최신 후보

            // 합리적인 날짜 필터링: 너무 과거(시스템 연도 -3년 미만이면서 발견된 최신 연도 -5년 미만)이거나,
            // 시스템 연도 대비 10년 이상 과거인 데이터는 제외. 단, 발견된 최신 연도가 시스템 연도보다 클 경우(미래 데이터)는 허용.
            var reasonableDates = allMatches.filter(m => 
                !(m.year < latestFoundYear - 5 && m.year < systemYear - 3) && // 극단적 과거 방지
                m.year >= systemYear - 10 && // 일반적인 과거 데이터 범위
                m.year <= systemYear + 1    // 약간의 미래 데이터 허용 (다음해 초)
            );

            if (reasonableDates.length > 0) {
                // 합리적인 날짜 중 가장 최신 것 반환 (공백 제거)
                return reasonableDates[0].fullText.replace(/\s+/g, '');
            } else if (allMatches.length > 0) {
                // 합리적인 날짜가 없으면, 필터링 전 가장 최신 것 반환
                return allMatches[0].fullText.replace(/\s+/g, '');
            }
            
            return '날짜 정보 없음'; // 모든 조건 불충족 시
            """
            date_info = driver.execute_script(js_script)
            logger.debug(f"추출된 최종 날짜 정보 (JS): '{date_info}'")
            return date_info
        except Exception as e:
            logger.error(f"날짜 정보 추출 중 JavaScript 오류: {e}", exc_info=True)
            return "날짜 추출 실패"

    def select_bank(self, driver, bank_name):
        logger.debug(f"[{bank_name}] 은행 선택 시도...")
        try:
            driver.get(self.config.BASE_URL)
            WebDriverWait(driver, self.config.PAGE_LOAD_TIMEOUT).until(
                lambda d: d.execute_script('return document.readyState') == 'complete'
            )
            time.sleep(random.uniform(0.2, 0.4)) # 페이지 안정화

            # 1. 정확한 텍스트 매칭 (TD 또는 A 태그)
            for xpath_expr in [f"//td[normalize-space(.)='{bank_name}']", f"//a[normalize-space(.)='{bank_name}']"]:
                try:
                    elements = driver.find_elements(By.XPATH, xpath_expr)
                    for el in elements:
                        if el.is_displayed() and self._robust_click(driver, el):
                            logger.info(f"[{bank_name}] 선택 성공 (XPath 정확한 텍스트).")
                            time.sleep(random.uniform(0.2, 0.5)) # 클릭 후 대기
                            return True
                except NoSuchElementException:
                    continue # 다음 XPath 시도
                except Exception as e_click:
                     logger.debug(f"[{bank_name}] 은행 선택 중 클릭 오류 (XPath 정확): {e_click}")


            # 2. JavaScript를 이용한 탐색 및 클릭 (포함 문자열)
            js_script = f"""
            var bankName = '{bank_name}';
            var elements = Array.from(document.querySelectorAll('a, td'));
            var targetElement = elements.find(e => e.textContent && e.textContent.trim().includes(bankName));
            if (targetElement) {{
                targetElement.scrollIntoView({{block: 'center'}});
                // TD 안에 A가 있으면 A를 클릭, 없으면 TD 자체를 클릭
                var clickable = (targetElement.tagName === 'TD' && targetElement.querySelector('a')) ? targetElement.querySelector('a') : targetElement;
                clickable.click();
                return true;
            }}
            return false;
            """
            try:
                if driver.execute_script(js_script):
                    logger.info(f"[{bank_name}] 선택 성공 (JS 포함 문자열).")
                    time.sleep(random.uniform(0.2, 0.5))
                    return True
            except Exception as e_js:
                logger.debug(f"[{bank_name}] 은행 선택 중 JS 오류: {e_js}")

            # 3. 포함 문자열 매칭 (TD 또는 A 태그), 텍스트 길이 짧은 순 우선
            for xpath_expr_partial in [f"//td[contains(normalize-space(.),'{bank_name}')]", f"//a[contains(normalize-space(.),'{bank_name}')]"]:
                try:
                    elements = driver.find_elements(By.XPATH, xpath_expr_partial)
                    # 텍스트 길이가 은행명과 가장 유사한 것을 우선 시도 (불필요한 부분 문자열 매칭 방지)
                    elements.sort(key=lambda x: len(x.text.strip()) if x.text else float('inf'))
                    for el in elements:
                        if el.is_displayed() and bank_name in el.text.strip() and self._robust_click(driver, el) : # 더 정확한 확인
                            logger.info(f"[{bank_name}] 선택 성공 (XPath 포함 문자열 - 길이 정렬).")
                            time.sleep(random.uniform(0.2, 0.5))
                            return True
                except NoSuchElementException:
                    continue
                except Exception as e_click_partial:
                    logger.debug(f"[{bank_name}] 은행 선택 중 클릭 오류 (XPath 포함): {e_click_partial}")

            logger.warning(f"[{bank_name}] 은행 선택 최종 실패.")
            return False
        except TimeoutException:
            logger.error(f"[{bank_name}] 은행 목록 페이지 로드 시간 초과 ({self.config.BASE_URL})")
            return False
        except Exception as e:
            logger.error(f"[{bank_name}] 은행 선택 중 예외 발생: {e}", exc_info=True)
            return False


    def select_category(self, driver, category_name):
        logger.debug(f"카테고리 선택 시도: '{category_name}'")
        time.sleep(random.uniform(0.1, 0.3)) # 카테고리 선택 전 짧은 대기

        # 카테고리 이름에서 공백 제거 (웹사이트 HTML 구조에 따라 필요할 수 있음)
        normalized_category_name = category_name.replace(' ', '')

        # 다양한 XPath 및 링크 텍스트 시도
        selectors = [
            (By.XPATH, f"//a[normalize-space(translate(text(),' \t\n\r',''))='{normalized_category_name}']"), # 공백/줄바꿈 무시 정확 일치
            (By.XPATH, f"//button[normalize-space(translate(text(),' \t\n\r',''))='{normalized_category_name}']"),
            (By.LINK_TEXT, category_name), # 정확한 링크 텍스트
            (By.PARTIAL_LINK_TEXT, category_name), # 부분 링크 텍스트
             # 탭 역할을 하는 li 요소 내부의 a 태그 등 더 구체적인 XPath
            (By.XPATH, f"//li[contains(@role,'tab') or contains(@class,'tab')][.//text()[normalize-space()='{category_name}']]//a"),
            (By.XPATH, f"//li[contains(@role,'tab') or contains(@class,'tab')][normalize-space()='{category_name}']"),
            (By.XPATH, f"//div[contains(@role,'tab') or contains(@class,'tab')][normalize-space()='{category_name}']"),

        ]

        for by_type, selector_value in selectors:
            try:
                elements = driver.find_elements(by_type, selector_value)
                for el in elements:
                    # 화면에 보이고 활성화된 요소만 클릭
                    if el.is_displayed() and el.is_enabled():
                        if self._robust_click(driver, el):
                            logger.info(f"'{category_name}' 카테고리 선택 성공 ({by_type}, '{selector_value}').")
                            time.sleep(random.uniform(0.2, 0.4)) # 클릭 후 DOM 변경 대기
                            return True
            except NoSuchElementException:
                continue # 다음 선택자 시도
            except Exception as e_click_cat:
                logger.debug(f"카테고리 '{category_name}' 선택 중 클릭 오류 ({by_type}, '{selector_value}'): {e_click_cat}")


        # JavaScript를 이용한 최종 시도 (포함 문자열)
        js_script = f"""
        var category = '{category_name}';
        // role='tab', class에 'tab' 포함, 또는 a, li, button, span 태그 중 텍스트 포함 요소
        var elements = Array.from(document.querySelectorAll('a, li, button, span, div[role="tab"], div[class*="tab"]'));
        var targetElement = elements.find(e => e.textContent && e.textContent.trim().includes(category));
        if (targetElement) {{
            targetElement.scrollIntoView({{block: 'center'}});
            targetElement.click();
            return true;
        }}
        return false;
        """
        try:
            if driver.execute_script(js_script):
                logger.info(f"'{category_name}' 카테고리 선택 성공 (JS 포함 문자열).")
                time.sleep(random.uniform(0.2, 0.4))
                return True
        except Exception as e_js_cat:
            logger.debug(f"카테고리 '{category_name}' 선택 중 JS 오류: {e_js_cat}")

        logger.warning(f"'{category_name}' 카테고리 선택 최종 실패.")
        return False
        
    def extract_tables_from_page(self, driver):
        # 페이지 로드 완료 및 약간의 추가 대기 (동적 컨텐츠 로딩 고려)
        try:
            WebDriverWait(driver, self.config.PAGE_LOAD_TIMEOUT).until(
                lambda d: d.execute_script('return document.readyState') == 'complete'
            )
        except TimeoutException:
            logger.warning("테이블 추출 전 페이지 로드 대기 시간 초과. 현재 상태로 진행.")

        time.sleep(random.uniform(0.2, 0.5)) # DOM 안정화 및 비동기 컨텐츠 로드 대기

        extracted_dfs = []
        try:
            page_source = driver.page_source
            # 페이지 소스가 너무 짧거나 table 태그가 없으면 바로 반환
            if not page_source or len(page_source) < 300 or "<table" not in page_source.lower():
                logger.debug("페이지 소스가 없거나 짧거나 table 태그가 없어 테이블 추출 건너뜀.")
                return []

            # pandas.read_html 사용
            # 'bs4'와 'lxml' 파서 모두 시도해볼 수 있으나, bs4가 기본적으로 유연함
            # flavor='html5lib' 도 고려 가능
            dfs_from_html = pd.read_html(StringIO(page_source), flavor='bs4', encoding='utf-8')
            
            if not dfs_from_html:
                logger.debug("pd.read_html이 반환한 테이블 없음.")
                return []

            for i, df in enumerate(dfs_from_html):
                if not isinstance(df, pd.DataFrame) or df.empty:
                    logger.debug(f"테이블 {i} 비어있거나 DataFrame 아님. 건너뜀.")
                    continue
                
                # 모든 값이 NaN인 행/열 제거
                df.dropna(axis=0, how='all', inplace=True)
                df.dropna(axis=1, how='all', inplace=True)

                if df.empty:
                    logger.debug(f"테이블 {i} 모든 NaN 제거 후 비어있음. 건너뜀.")
                    continue
                
                # 컬럼 이름 정리 (MultiIndex 처리 포함)
                if isinstance(df.columns, pd.MultiIndex):
                    df.columns = ['_'.join(map(str, col_part)).strip('_ ') for col_part in df.columns.values]
                else:
                    df.columns = [str(col).strip() for col in df.columns]
                
                # Unnamed 컬럼 이름 변경 (선택적)
                df.columns = [f"Col{idx}" if "Unnamed:" in str(col_name) else col_name for idx, col_name in enumerate(df.columns)]

                extracted_dfs.append(df.reset_index(drop=True))
            
            logger.info(f"{len(extracted_dfs)}개의 유효한 테이블 추출 완료.")
            return extracted_dfs

        except ValueError as ve:
            # "No tables found"는 ValueError로 발생
            logger.debug(f"pd.read_html에서 테이블 찾지 못함 (ValueError): {ve}. 페이지에 테이블이 없거나 인식 가능한 형식이 아닐 수 있음.")
            return []
        except Exception as e:
            logger.error(f"테이블 추출 중 예외 발생: {e}", exc_info=True)
            return []

    # 수정됨: _scrape_single_bank_attempt (테이블 중복 제거 로직 강화)
    def _scrape_single_bank_attempt(self, bank_name, driver):
        logger.info(f"[{bank_name}] 스크래핑 시도...")
        if not self.select_bank(driver, bank_name): 
            return None # 은행 선택 실패 시 None 반환
        
        # 날짜 정보 추출
        date_info_scraped = self.extract_date_information(driver)
        logger.info(f"[{bank_name}] 추출 공시일(원본): '{date_info_scraped}'")
        normalized_scraped_date = normalize_datestr_for_comparison(date_info_scraped)
        
        # 예상 공시일과 비교 로깅 (기존 로직 유지)
        expected_officially_due = self.config.latest_due_period
        expected_next_imminent = self.config.next_imminent_period
        logger.info(f"[{bank_name}] 정규화 공시일: '{normalized_scraped_date}', 공식적 최신 예상: '{expected_officially_due}', 다음 업로드 예상: '{expected_next_imminent}'")
        if normalized_scraped_date is None: 
            logger.error(f"[{bank_name}] 날짜 추출 실패 또는 정규화 불가. 비교 중단.")
        elif normalized_scraped_date == "알 수 없는 형식": 
            logger.warning(f"[{bank_name}] 날짜 형식이 알 수 없음 ('{date_info_scraped}'). 비교 주의.")
        elif normalized_scraped_date == expected_officially_due: 
            logger.info(f"[{bank_name}] 공시일('{normalized_scraped_date}')이 공식적으로 최신이어야 할 기간과 일치.")
        elif normalized_scraped_date == expected_next_imminent: 
            logger.info(f"[{bank_name}] 공시일('{normalized_scraped_date}')이 다음 업로드 예정/진행 기간과 일치 (선제적 업데이트 가능성).")
        else: 
            logger.critical(f"[{bank_name}] !!날짜 불일치!! 웹사이트(정규화): '{normalized_scraped_date}', 공식 최신 예상: '{expected_officially_due}', 다음 업로드 예상: '{expected_next_imminent}'. (사이트 원본: '{date_info_scraped}')")

        # 결과 데이터 초기화
        data = {'_INFO_': pd.DataFrame([{'은행명':bank_name, '공시날짜':date_info_scraped, '추출일시':datetime.now().strftime("%Y-%m-%d %H:%M:%S"), '스크래퍼버전':self.config.VERSION}])}
        has_data_collected_for_bank = False # 은행 전체적으로 데이터 수집 여부
        
        current_bank_page_url = ""
        try:
            current_bank_page_url = driver.current_url # 카테고리 변경 후 돌아올 URL
        except WebDriverException as e_url:
            logger.warning(f"[{bank_name}] 은행 기본 페이지 URL 가져오기 실패: {e_url}. 카테고리 재시도 시 문제 발생 가능.")

        processed_table_hashes_for_this_bank = set() # 이 은행에서 이미 처리(저장)된 테이블의 해시를 저장

        for cat_name in self.config.CATEGORIES:
            logger.info(f"[{bank_name}] '{cat_name}' 카테고리 처리 시작.")
            category_selected_successfully = False
            # 카테고리 선택 (2회 시도)
            for attempt in range(1, 3): 
                if attempt > 1: 
                    logger.info(f"[{bank_name}] '{cat_name}' 카테고리 선택 재시도 ({attempt}차)...")
                    if current_bank_page_url: # 이전 은행 페이지 URL이 있다면 거기로 이동 후 시도
                        try:
                            driver.get(current_bank_page_url) 
                            WebDriverWait(driver,self.config.PAGE_LOAD_TIMEOUT).until(lambda d:d.execute_script('return document.readyState')=='complete')
                            time.sleep(random.uniform(0.3, 0.6)) 
                        except Exception as e_nav_cat_retry:
                            logger.warning(f"[{bank_name}] '{cat_name}' 카테고리 재선택 위한 페이지 재접속 실패({current_bank_page_url}): {e_nav_cat_retry}")
                    else: # URL 없으면 현재 페이지에서 잠시 대기 후 재시도
                         time.sleep(random.uniform(0.5, 1.0)) 

                if self.select_category(driver, cat_name): 
                    category_selected_successfully = True
                    break # 카테고리 선택 성공
            
            if not category_selected_successfully: 
                logger.error(f"[{bank_name}] '{cat_name}' 카테고리 최종 선택 실패. 해당 카테고리 건너뜀."); 
                continue # 다음 카테고리로
            
            # 현재 카테고리에서 테이블 추출
            tables_in_current_view = self.extract_tables_from_page(driver)
            
            if tables_in_current_view:
                logger.info(f"[{bank_name}] '{cat_name}' 카테고리에서 {len(tables_in_current_view)}개 테이블 발견.")
                tables_added_for_this_category = 0 # 이 카테고리에서 실제로 '새롭게' 추가된 테이블 수
                
                for df_table_from_view in tables_in_current_view: 
                    try:
                        # 테이블 해시 생성 (내용 기반으로 중복 판단)
                        # shape, columns, 그리고 첫 1~2행의 데이터로 해시 생성
                        table_shape_str = str(df_table_from_view.shape)
                        table_columns_str = str(list(df_table_from_view.columns))
                        table_head_content_str = ""
                        if not df_table_from_view.empty:
                            try:
                                # to_string()으로 첫 행(또는 몇 행)의 내용을 일관된 문자열로 변환
                                table_head_content_str = df_table_from_view.head(2).to_string(index=False, header=False).strip()
                            except Exception: # 예외 발생 시 대체 문자열 사용 (DataFrame 내용에 따라 to_string 실패 가능성)
                                table_head_content_str = str(list(df_table_from_view.head(2).astype(str).values.flatten()))[:200] # 내용 일부만
                        
                        current_table_unique_hash = (table_shape_str, table_columns_str, table_head_content_str)

                        # 이 은행에서 이 해시값의 테이블이 처음 처리되는 경우
                        if current_table_unique_hash not in processed_table_hashes_for_this_bank:
                            processed_table_hashes_for_this_bank.add(current_table_unique_hash) # 해시 세트에 추가
                            
                            tables_added_for_this_category += 1
                            
                            # 시트 이름 생성: "카테고리명_순번" (예: 재무현황_1, 재무현황_2)
                            clean_category_name_for_sheet = re.sub(r'[\\/*?:\[\]]','', cat_name)
                            sheet_name_candidate = f"{clean_category_name_for_sheet}_{tables_added_for_this_category}"
                            
                            # Excel 시트 이름 길이 제한(31자) 및 최종 유일성 확보
                            final_sheet_name = sheet_name_candidate[:31]
                            collision_counter = 1
                            while final_sheet_name in data: # 거의 발생하지 않겠지만, 만약의 시트 이름 충돌 시
                                final_sheet_name = f"{sheet_name_candidate[:28]}_{collision_counter}"[:31] # 이름 줄이고 카운터 추가
                                collision_counter += 1
                            
                            data[final_sheet_name] = df_table_from_view # 결과 데이터에 추가
                            has_data_collected_for_bank = True # 이 은행에서 데이터가 하나라도 수집되었음을 표시
                            logger.info(f"[{bank_name}] '{cat_name}' 카테고리에 새 테이블 추가 -> 시트명 '{final_sheet_name}'.")
                        else:
                            # 이미 처리된 해시값의 테이블 (중복 테이블)
                            logger.debug(f"[{bank_name}] '{cat_name}' 카테고리에서 중복 테이블 발견 (이전 카테고리에서 이미 처리됨). 건너뜀.")
                    
                    except Exception as e_table_processing_loop:
                        logger.error(f"[{bank_name}] '{cat_name}' 카테고리 내 테이블 처리 중 루프 오류: {e_table_processing_loop}", exc_info=True)
            else:
                logger.info(f"[{bank_name}] '{cat_name}' 카테고리에서 추출된 테이블 없음.")
        
        # 데이터 수집 결과 로깅
        if not has_data_collected_for_bank:
            # 날짜 정보가 유효한데 데이터가 없는 경우
            if date_info_scraped and date_info_scraped not in ["날짜 정보 없음", "날짜 추출 실패"]:
                 logger.warning(f"[{bank_name}] 스크래핑 시도했으나 유효한 테이블 데이터 수집 못함 (공시일 정보: '{date_info_scraped}').")
            else: # 날짜 정보도 없는 경우
                 logger.warning(f"[{bank_name}] 스크래핑 시도했으나 유효한 테이블 데이터 및 날짜 정보 모두 수집 못함.")
            return None # 유효 데이터 없으면 None 반환 (실패 처리 위함)

        return data # 수집된 데이터 반환

    def save_bank_data(self, bank_name, excel_data_dict):
        if not excel_data_dict or '_INFO_' not in excel_data_dict:
            logger.error(f"[{bank_name}] 저장할 데이터가 없거나 '_INFO_' 시트가 누락되었습니다.")
            return False

        # 파일명에 사용될 날짜 정보 추출 및 정제
        raw_date_from_info = excel_data_dict['_INFO_']['공시날짜'].iloc[0]
        date_match_for_filename = re.search(r'(\d{4})년(\d{1,2})월', raw_date_from_info)
        date_part_for_filename = f"{date_match_for_filename.group(1)}-{int(date_match_for_filename.group(2)):02d}" \
                                 if date_match_for_filename else re.sub(r'[^\w\-_.]', '', raw_date_from_info or "날짜정보없음")[:20]
        
        # 은행명에서 파일 시스템에 부적합한 문자 제거
        safe_bank_name_for_filename = re.sub(r'[\\/*?:"<>|]', '_', bank_name) # Windows/Linux 호환
        excel_file_name = f"{safe_bank_name_for_filename}_{date_part_for_filename}.xlsx"
        excel_file_path = self.config.output_dir / excel_file_name

        try:
            with pd.ExcelWriter(excel_file_path, engine='openpyxl') as writer:
                for sheet_key_original, df_to_write in excel_data_dict.items():
                    if not isinstance(df_to_write, pd.DataFrame):
                        logger.warning(f"[{bank_name}] 시트 '{sheet_key_original}'의 내용이 DataFrame이 아닙니다 (타입: {type(df_to_write)}). 이 시트는 건너뜁니다.")
                        continue
                    
                    # 시트 이름 정리 (Excel은 31자 제한 및 일부 특수 문자 사용 불가)
                    clean_sheet_name_for_excel = re.sub(r'[\\/*?:\[\]]', '', sheet_key_original) # 기본적인 특수문자 제거
                    clean_sheet_name_for_excel = clean_sheet_name_for_excel[:31] # 길이 제한
                    
                    if sheet_key_original == '_INFO_': # 특별 정보 시트 이름 변경
                        clean_sheet_name_for_excel = '정보'

                    df_to_write.to_excel(writer, sheet_name=clean_sheet_name_for_excel, index=False)
            logger.info(f"[{bank_name}] 데이터 저장 완료: {excel_file_path.name} (전체 경로: {excel_file_path})")
            return True
        except Exception as e_save: 
            logger.error(f"[{bank_name}] 데이터 저장 실패 ({excel_file_path.name}): {e_save}", exc_info=True)
            return False

    async def worker_process_bank(self, bank_name, pbar, semaphore):
        async with semaphore: # 동시에 실행되는 작업 수 제어
            driver_instance = None
            operation_success = False
            final_date_info_for_bank = None # 이 은행 작업의 최종 날짜 정보

            # 이전 실행에서 실패했더라도 날짜 정보는 가져올 수 있도록 시도
            previous_bank_progress = self.progress_manager.get_bank_data(bank_name)
            if previous_bank_progress and 'date_info' in previous_bank_progress:
                final_date_info_for_bank = previous_bank_progress['date_info']

            try:
                # 드라이버 풀에서 드라이버 가져오기
                driver_instance = await asyncio.get_event_loop().run_in_executor(None, self.driver_manager.get_driver)
                if not driver_instance: 
                    logger.error(f"[{bank_name}] 작업자용 드라이버를 가져오지 못했습니다. 해당 은행 처리 실패로 간주.")
                    self.progress_manager.mark_failed(bank_name) # 실패로 기록
                    return bank_name, False, final_date_info_for_bank # (은행명, 성공여부, 날짜정보)
                
                collected_bank_data = None
                # 최대 재시도 횟수만큼 스크래핑 시도
                for attempt_num in range(1, self.config.MAX_RETRIES + 1):
                    try:
                        logger.info(f"[{bank_name}] 스크래핑 시도 {attempt_num}/{self.config.MAX_RETRIES}...")
                        collected_bank_data = self._scrape_single_bank_attempt(bank_name, driver_instance)
                        
                        # 데이터 수집 성공 여부 판단
                        if collected_bank_data and '_INFO_' in collected_bank_data:
                            final_date_info_for_bank = collected_bank_data['_INFO_']['공시날짜'].iloc[0] # 최신 시도의 날짜 정보로 업데이트
                            # '_INFO_' 시트 외에 실제 데이터 시트가 하나라도 있는지 확인
                            if any(sheet_name != '_INFO_' for sheet_name in collected_bank_data):
                                logger.info(f"[{bank_name}] 시도 {attempt_num}에서 데이터 수집 성공 (날짜: {final_date_info_for_bank}).")
                                break # 성공했으므로 재시도 루프 탈출
                            else:
                                logger.warning(f"[{bank_name}] 시도 {attempt_num}에서 '_INFO_' 시트 외 실제 데이터 없음. 다음 시도 진행.")
                                collected_bank_data = None # 실제 데이터 없는 것으로 간주
                        elif collected_bank_data : # _INFO_가 없는 비정상적 상황
                             logger.warning(f"[{bank_name}] 시도 {attempt_num}에서 데이터는 있으나 '_INFO_' 시트 누락. 데이터 무효 처리.")
                             collected_bank_data = None
                        else: # collected_bank_data is None (스크래핑 실패)
                             logger.info(f"[{bank_name}] 시도 {attempt_num}에서 데이터 수집 못함.")

                        # 마지막 시도가 아니면 재시도 전 대기
                        if attempt_num < self.config.MAX_RETRIES:
                            logger.info(f"[{bank_name}] 다음 스크래핑 시도 전 잠시 대기...")
                            await asyncio.sleep(random.uniform(1.5, 3.5)) # 재시도 간 간격

                    except WebDriverException as wde_retry: # 웹 드라이버 관련 예외 (연결 끊김 등)
                        logger.warning(f"[{bank_name}] 시도 {attempt_num} 중 WebDriver 예외 발생: {type(wde_retry).__name__} - {str(wde_retry)[:120]}...")
                        # 연결 관련 문제 시 드라이버 교체 시도
                        if "ERR_CONNECTION" in str(wde_retry).upper() or "TIMED OUT" in str(wde_retry).upper():
                            logger.error(f"[{bank_name}] 연결 문제 감지. 드라이버를 교체하고 재시도합니다.")
                            if driver_instance: # 기존 드라이버 반환 (폐기될 수 있음)
                                await asyncio.get_event_loop().run_in_executor(None, self.driver_manager.return_driver, driver_instance)
                                driver_instance = None 
                            driver_instance = await asyncio.get_event_loop().run_in_executor(None, self.driver_manager.get_driver) # 새 드라이버 요청
                            if not driver_instance: 
                                logger.error(f"[{bank_name}] 재시도용 새 드라이버 획득 실패. 해당 은행 처리 중단.")
                                break # 드라이버 없으면 재시도 불가
                        elif attempt_num < self.config.MAX_RETRIES :
                            await asyncio.sleep(random.uniform(2,5)) # 일반 WebDriver 오류 시 좀 더 긴 대기
                        else: # 마지막 시도에서도 WebDriver 오류
                            logger.error(f"[{bank_name}] 시도 {attempt_num} (마지막) WebDriver 예외로 최종 실패.")
                    
                    except Exception as e_retry: # 기타 일반 예외
                        logger.warning(f"[{bank_name}] 스크래핑 시도 {attempt_num} 중 일반 예외 발생: {type(e_retry).__name__} - {e_retry}")
                        if attempt_num < self.config.MAX_RETRIES:
                             await asyncio.sleep(random.uniform(1,3)) # 일반 예외 시 짧은 대기
                        else: 
                            logger.error(f"[{bank_name}] 모든 재시도({attempt_num}) 실패 후 최종적으로 데이터 수집 못함.")
                            collected_bank_data = None # 모든 재시도 실패 시 데이터 없음을 명확히 함
                
                # 최종적으로 수집된 데이터가 있고, 저장에 성공하면 완료 처리
                if collected_bank_data and self.save_bank_data(bank_name, collected_bank_data):
                    self.progress_manager.mark_completed(bank_name, final_date_info_for_bank)
                    operation_success = True
                else: # 데이터 수집 실패 또는 저장 실패 시
                    self.progress_manager.mark_failed(bank_name)
                    # 실패했더라도, 날짜 정보라도 찾았다면 진행 상황에 기록 시도
                    if final_date_info_for_bank:
                         current_progress = self.progress_manager.progress.setdefault('banks',{}).setdefault(bank_name,{})
                         current_progress['date_info'] = final_date_info_for_bank # 실패해도 날짜정보 업데이트
                         self.progress_manager.save() # 저장

                return bank_name, operation_success, final_date_info_for_bank
            
            except TimeoutError as te_get_driver: 
                logger.error(f"[{bank_name}] 드라이버 풀에서 드라이버 획득 시간 초과: {te_get_driver}")
                self.progress_manager.mark_failed(bank_name)
                return bank_name, False, final_date_info_for_bank
            except Exception as e_worker_main: 
                logger.error(f"[{bank_name}] 작업자 메인 로직에서 예외 발생: {type(e_worker_main).__name__} - {e_worker_main}", exc_info=True)
                self.progress_manager.mark_failed(bank_name)
                return bank_name, False, final_date_info_for_bank
            finally:
                # 사용한 드라이버 반납
                if driver_instance:
                    await asyncio.get_event_loop().run_in_executor(None, self.driver_manager.return_driver, driver_instance)
                if pbar: pbar.update(1) # 진행 바 업데이트
                logger.info(f"[{bank_name}] 처리 완료. 결과: {'성공' if operation_success else '실패'}, 공시일(최종 기록): {final_date_info_for_bank or '미확정/없음'}")
    
    async def run(self):
        logger.info(f"==== 스크래핑 작업 시작 (v{self.config.VERSION}) ====")
        overall_start_time = time.monotonic()
        
        banks_to_process = self.progress_manager.get_pending_banks()
        
        if not banks_to_process:
            logger.info("처리할 은행이 없습니다. (이전 실행에서 모두 완료되었거나, 진행 파일에 처리 대상 은행 없음)")
            # 진행 파일 자체가 비어있는 첫 실행인지, 아니면 모든 은행이 'completed' 상태인지 확인
            if not self.progress_manager.progress['banks']: 
                logger.info("진행 파일에 어떠한 은행 처리 기록도 없습니다. 전체 은행 목록을 대상으로 실행합니다.")
                banks_to_process = self.config.BANKS # 이 경우 모든 은행을 대상으로 설정
            else: # 진행 파일은 있으나, 모든 은행이 completed 상태
                self.generate_summary_and_send_email() # 요약 및 이메일 전송 후 종료
                return

        if not banks_to_process: # 재확인 후에도 처리할 은행 없으면 종료
            logger.info("재확인 결과, 처리할 은행이 없습니다. 요약 생성 후 종료합니다.")
            self.generate_summary_and_send_email()
            return

        logger.info(f"총 {len(banks_to_process)}개 은행 처리 예정. (예시: {banks_to_process[:3]}{'...' if len(banks_to_process)>3 else ''})")
        
        # 동시 실행 제어를 위한 세마포어
        # MAX_WORKERS가 0이하일 경우 대비, 최소 1로 설정
        actual_max_workers = max(1, self.config.MAX_WORKERS)
        semaphore = asyncio.Semaphore(actual_max_workers) 
        
        # tqdm 진행 바 설정
        # bar_format : 진행 바 모양 커스터마이징 (진행률, 시간 등 표시)
        progress_bar_format = '{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}{postfix}]'
        with tqdm(total=len(banks_to_process), desc="은행 스크래핑", unit="은행", dynamic_ncols=True, smoothing=0.05, bar_format=progress_bar_format) as pbar:
            # 각 은행에 대한 비동기 작업 생성
            tasks = [self.worker_process_bank(bank_name, pbar, semaphore) for bank_name in banks_to_process]
            # asyncio.gather를 사용하여 모든 작업 병렬 실행 및 결과 수집
            # return_exceptions=True : 개별 작업에서 예외 발생 시 gather 전체가 중단되지 않고, 예외 객체가 결과 리스트에 포함됨
            all_task_results = await asyncio.gather(*tasks, return_exceptions=True) 
        
        # 결과 처리
        num_tasks_attempted = len(all_task_results)
        num_exceptions_in_gather = 0
        num_successfully_returned_tasks = 0
        num_actually_completed_banks = 0

        for i, task_result_or_exception in enumerate(all_task_results):
            if isinstance(task_result_or_exception, Exception):
                # asyncio.gather에서 반환된 예외 처리
                failed_bank_name = banks_to_process[i] # 예외가 발생한 은행 이름 특정
                logger.error(f"은행 '{failed_bank_name}' 처리 중 asyncio.gather 레벨에서 예외 반환됨: {task_result_or_exception}", 
                             exc_info=task_result_or_exception if isinstance(task_result_or_exception, BaseException) else None)
                self.progress_manager.mark_failed(failed_bank_name) # 예외 발생 시 해당 은행 실패로 기록
                num_exceptions_in_gather +=1
            else:
                # 정상적으로 반환된 결과 (bank_name, success_flag, date_info)
                num_successfully_returned_tasks +=1
                if task_result_or_exception and task_result_or_exception[1]: # success_flag (두 번째 요소)
                    num_actually_completed_banks +=1
        
        logger.info(f"asyncio.gather로 {num_tasks_attempted}개 작업 시도 완료.")
        logger.info(f" - 정상 반환된 작업 수: {num_successfully_returned_tasks}개")
        logger.info(f" - gather 레벨 예외 발생 수: {num_exceptions_in_gather}개")
        logger.info(f" - 최종 '성공' 처리된 은행 수: {num_actually_completed_banks}개")
        
        total_elapsed_time = time.monotonic() - overall_start_time
        logger.info(f"==== 전체 스크래핑 작업 완료. 총 소요 시간: {total_elapsed_time:.2f}초 ====")
        
        self.generate_summary_and_send_email() # 최종 요약 및 이메일 발송

    def generate_summary_and_send_email(self):
        logger.info("요약 보고서 및 이메일 생성 시작...")
        summary_data_list = []
        
        all_configured_banks = self.config.BANKS
        current_progress_data = self.progress_manager.progress.get('banks', {})
        
        # 예상 공시일 정보
        expected_due_period = self.config.latest_due_period
        expected_imminent_period = self.config.next_imminent_period
        
        # 통계용 카운터
        count_completed = 0
        count_failed = 0
        count_unprocessed = 0 # 명시적으로 처리되지 않은 은행 수
        list_of_failed_banks = []

        for bank_name_from_config in all_configured_banks:
            bank_progress_detail = current_progress_data.get(bank_name_from_config)
            
            # 기본값 설정
            status_display = '⚪️ 미처리' # 미처리 상태를 기본으로
            scraped_original_date = ''
            date_match_analysis_result = 'N/A (미처리)'

            if bank_progress_detail: # 진행 파일에 해당 은행 정보가 있는 경우
                status_from_file = bank_progress_detail.get('status')
                scraped_original_date = bank_progress_detail.get('date_info', '') # 원본 날짜 정보
                normalized_scraped_date_for_match = normalize_datestr_for_comparison(scraped_original_date)

                if status_from_file == 'completed':
                    status_display = '✅ 완료'
                    count_completed += 1
                    # 날짜 일치 여부 분석
                    if not scraped_original_date or scraped_original_date == "날짜 정보 없음": 
                        date_match_analysis_result = "⚠️ 날짜정보 없음"
                    elif scraped_original_date == "날짜 추출 실패": 
                        date_match_analysis_result = "❗️ 날짜추출 실패"
                    elif normalized_scraped_date_for_match is None : # 정규화 함수가 None 반환 (패턴 불일치 등)
                        date_match_analysis_result = f"❓ 형식불일치 ({scraped_original_date})"
                    elif normalized_scraped_date_for_match == "알 수 없는 형식": # 정규화 함수가 명시적 반환
                        date_match_analysis_result = f"❓ 형식모름 ({scraped_original_date})"
                    elif normalized_scraped_date_for_match == expected_due_period: 
                        date_match_analysis_result = "🟢 일치 (기한내 최신)"
                    elif normalized_scraped_date_for_match == expected_imminent_period: 
                        date_match_analysis_result = "🔵 일치 (예정분 선반영)"
                    else: # 불일치
                        date_match_analysis_result = f"❌ 불일치! (웹: {normalized_scraped_date_for_match} vs 예상: {expected_due_period} 또는 {expected_imminent_period})"
                
                elif status_from_file == 'failed':
                    status_display = '❌ 실패'
                    count_failed += 1
                    list_of_failed_banks.append(bank_name_from_config)
                    date_match_analysis_result = "N/A (실패)"
                    # 실패했더라도 날짜 정보가 있다면 부가 정보로 표시
                    if scraped_original_date and scraped_original_date not in ["날짜 정보 없음", "날짜 추출 실패"]:
                         date_match_analysis_result = f"N/A (실패, 날짜: {scraped_original_date})"
                # else: 'pending' 또는 기타 상태는 미처리로 간주 (위 기본값 유지)
                
            else: # 진행 파일에 은행 정보가 없는 경우 (미처리)
                count_unprocessed += 1
                # status_display, scraped_original_date, date_match_analysis_result는 기본값 사용

            summary_data_list.append({
                '은행명': bank_name_from_config,
                '공시 날짜(원본)': scraped_original_date,
                '날짜 확인 상태': date_match_analysis_result,
                '처리 상태': status_display,
                '리포트 생성시간': datetime.now().strftime("%H:%M:%S") # 간단히 시간만
            })
        
        # 요약 DataFrame 생성 및 Excel 파일 저장
        summary_df_final = pd.DataFrame(summary_data_list)
        summary_excel_filename = f"스크래핑_요약_{self.config.today}.xlsx"
        summary_excel_filepath = self.config.output_dir / summary_excel_filename
        try: 
            # 상태별 정렬 (완료 > 실패 > 미처리 순)
            status_sort_order = {'✅ 완료': 0, '❌ 실패': 1, '⚪️ 미처리': 2}
            summary_df_final['temp_sort_col'] = summary_df_final['처리 상태'].map(status_sort_order).fillna(3) # 혹시 모를 다른 상태값 대비
            summary_df_final.sort_values(by=['temp_sort_col', '은행명'], inplace=True)
            summary_df_final.drop(columns=['temp_sort_col'], inplace=True) # 정렬 후 임시 열 제거
            
            summary_df_final.to_excel(summary_excel_filepath, index=False, engine='openpyxl')
            logger.info(f"요약 보고서 Excel 파일 생성 완료: {summary_excel_filepath}")
        except Exception as e_summary_excel:
            logger.error(f"요약 보고서 Excel 파일 저장 실패: {e_summary_excel}", exc_info=True)

        # 결과 파일 압축 (xlsx 파일들과 로그 파일 포함)
        zip_file_basename = f"저축은행_데이터_{self.config.today}.zip"
        # 압축 파일은 output_dir_base (./output) 에 저장
        zip_file_full_path = self.config.output_dir_base / zip_file_basename 
        
        # 압축 대상 파일 목록: 생성된 모든 xlsx 파일 + 로그 파일
        files_to_zip = list(self.config.output_dir.glob('*.xlsx')) 
        if self.config.log_file_path.exists(): # 로그 파일도 추가
            files_to_zip.append(self.config.log_file_path)
        
        zip_creation_success = False
        if files_to_zip: # 압축할 파일이 하나라도 있으면
            try:
                with zipfile.ZipFile(zip_file_full_path, 'w', zipfile.ZIP_DEFLATED) as zf:
                    for file_path_obj in files_to_zip:
                        if file_path_obj.exists() and file_path_obj.is_file():
                             # zip 파일 내부에 "저축은행_데이터_YYYYMMDD" 폴더명으로 파일 저장
                            archive_name = Path(self.config.output_dir.name) / file_path_obj.name
                            zf.write(file_path_obj, arcname=archive_name)
                logger.info(f"결과 파일 압축 완료: {zip_file_full_path} ({len(files_to_zip)}개 파일 포함)")
                zip_creation_success = True
            except Exception as e_zip: 
                logger.error(f"결과 파일 압축 실패: {e_zip}", exc_info=True)
                # zip_file_full_path는 그대로 유지하되, 성공 플래그는 False
        else:
            logger.warning("압축할 대상 파일이 없습니다 (xlsx 또는 로그 파일). Zip 파일 생성을 건너뜁니다.")
            # zip_file_full_path는 경로 객체로 남아있지만, 실제 파일은 생성되지 않음

        # 이메일 내용 구성
        num_banks_attempted_processing = count_completed + count_failed # 실제로 처리가 시도된 은행 수
        actual_success_rate = (count_completed / num_banks_attempted_processing * 100) if num_banks_attempted_processing > 0 else 0
        
        email_date_subject_part = self.config.processing_date_kst.strftime("%y%m%d") # 예: 250528
        email_quarter_subject_part = get_quarter_string_from_period(expected_due_period) # 예: 2025년1분기
        email_subject_line = f"[저축은행 공시] {email_quarter_subject_part} ({email_date_subject_part}) 결과: 완료 {count_completed}, 실패 {count_failed}"
        
        # 실패 은행 목록 HTML (최대 15개 표시)
        failed_banks_list_html = "".join(f"<li>{bank_name}</li>" for bank_name in list_of_failed_banks[:15])
        if len(list_of_failed_banks) > 15:
            failed_banks_list_html += f"<li>... 그 외 {len(list_of_failed_banks)-15}개 은행.</li>"
        if not list_of_failed_banks: # 실패 은행 없으면
            failed_banks_list_html = "<li>없음</li>"

        email_body_html_content = f"""
        <html><head><style>
            body {{ font-family: 'Malgun Gothic', Arial, sans-serif; margin: 15px; background-color: #f8f9fa; color: #212529; line-height: 1.6; }}
            h2, h3 {{ color: #0056b3; border-bottom: 2px solid #0056b3; padding-bottom: 5px;}}
            .container {{ background-color: #ffffff; border: 1px solid #dee2e6; padding: 20px; margin-bottom: 25px; border-radius: 8px; box-shadow: 0 4px 8px rgba(0,0,0,0.05); }}
            .container p {{ margin: 10px 0; font-size: 1rem; }}
            .status-completed {{ color: #28a745; font-weight: bold; }} /* Bootstrap success green */
            .status-failed {{ color: #dc3545; font-weight: bold; }} /* Bootstrap danger red */
            table {{ border-collapse: collapse; width: 100%; margin-top: 20px; font-size: 0.9rem; box-shadow: 0 2px 4px rgba(0,0,0,0.05); }}
            th, td {{ border: 1px solid #ced4da; padding: 10px; text-align: left; word-break: keep-all; }} /* 한글 줄바꿈 고려 */
            th {{ background-color: #e9ecef; color: #495057; font-weight: bold; }}
            tr:nth-child(even) {{ background-color: #f8f9fa; }}
            tr:hover {{ background-color: #e2e6ea; }}
            ul {{ padding-left: 25px; list-style-type: square; }}
            small, .text-muted {{ color: #6c757d; font-size: 0.85em; }}
        </style></head><body>
        <h2>저축은행 경영공시 스크래핑 결과 ({self.config.today})</h2>
        <div class="container">
            <p><strong>스크립트 실행일:</strong> {self.config.processing_date_kst.strftime('%Y-%m-%d')}</p>
            <p><strong>공식적 최신 예상 공시분기:</strong> {expected_due_period} <small class="text-muted">({self.config.date_expectations['latest_due_reason']})</small></p>
            <p><strong>다음 업로드 예상 공시분기:</strong> {expected_imminent_period} <small class="text-muted">({self.config.date_expectations['next_imminent_reason']})</small></p>
        </div>
        
        <div class="container">
            <h3>작업 요약</h3>
            <p>총 대상 은행: {len(all_configured_banks)}개</p>
            <p>처리 시도 은행: {num_banks_attempted_processing}개 (미처리 대상: {count_unprocessed}개)</p>
            <p><span class="status-completed">✅ 완료: {count_completed}개</span></p>
            <p><span class="status-failed">❌ 실패: {count_failed}개</span> (실제 시도 대비 성공률: {actual_success_rate:.1f}%)</p>
            <p>📂 데이터 저장 폴더명: {self.config.output_dir.name}</p>
            <p>🗜️ 압축파일: {zip_file_full_path.name if zip_creation_success and zip_file_full_path.exists() else ('생성 실패 또는 파일 없음' if not zip_creation_success else '압축 파일 경로 확인 필요')}</p>
        </div>
        
        <div class="container">
            <h3>실패 은행 목록 (최대 15개 표시):</h3>
            <ul>{failed_banks_list_html}</ul>
        </div>
        
        <p class="text-muted">세부 결과는 첨부된 압축파일 내의 <strong>{summary_excel_filename}</strong> 또는 개별 은행 엑셀 파일을 확인하세요.</p>
        
        <h3>은행별 처리 현황 상세:</h3>
        {summary_df_final.to_html(index=False, border=0, na_rep='-', classes='table table-striped table-hover').replace('<td>','<td style="min-width:75px; vertical-align:top;">') if not summary_df_final.empty else "<p>요약 테이블 데이터가 없습니다.</p>"}
        <br><p><small class="text-muted">이 메시지는 자동으로 발송되었습니다. (스크립트 버전: v{self.config.VERSION})</small></p>
        </body></html>"""
        
        # 이메일 첨부파일 결정
        final_attachment_path_for_email = None
        if zip_creation_success and zip_file_full_path.exists(): # 압축 성공 시 압축파일 첨부
            final_attachment_path_for_email = str(zip_file_full_path)
        elif summary_excel_filepath.exists(): # 압축 실패 시 요약 엑셀 파일이라도 첨부
            final_attachment_path_for_email = str(summary_excel_filepath)
            logger.warning(f"압축 파일({zip_file_full_path.name if zip_file_full_path else 'N/A'}) 생성 실패 또는 누락. 요약 보고서({summary_excel_filename})만 첨부합니다.")
        else: # 둘 다 없으면 첨부파일 없음
            logger.warning(f"압축 파일 및 요약 보고서 모두 찾을 수 없습니다. 첨부 파일 없이 이메일이 발송됩니다.")

        self.email_sender.send_email_with_attachment(email_subject_line, email_body_html_content, final_attachment_path_for_email)

# --- 메인 실행 로직 ---
def main():
    config_instance = None # 로거 초기화를 위해 Config 인스턴스 먼저 생성
    driver_manager_instance = None 
    try:
        config_instance = Config() # 이 시점에서 로거가 설정됨
        logger.info(f"스크립트 실행 시작: {Path(sys.argv[0]).name} (Version: {config_instance.VERSION})")
        
        # 명령줄 인자 파서 설정 (선택적 기능: --force-all)
        arg_parser = argparse.ArgumentParser(description="저축은행 중앙회 경영공시 스크래퍼")
        arg_parser.add_argument(
            "--force-all", 
            action="store_true", 
            help="기존 진행 상황(progress.json)을 무시하고 모든 은행을 처음부터 다시 처리합니다."
        )
        parsed_args = arg_parser.parse_args()

        # DriverManager 및 ProgressManager 초기화
        driver_manager_instance = DriverManager(config_instance)
        progress_manager_instance = ProgressManager(config_instance)

        if parsed_args.force_all:
            logger.info("--force-all 옵션이 감지되었습니다. 이전 진행 상황을 초기화하고 모든 은행을 재처리합니다.")
            # 진행 상황 초기화
            progress_manager_instance.progress = {'banks': {}, 'stats': {'last_run':None,'success_count':0,'failure_count':0}}
            progress_manager_instance.save() # 초기화된 상태 저장

        # BankScraper 인스턴스 생성
        bank_scraper_instance = BankScraper(config_instance, driver_manager_instance, progress_manager_instance)
        
        # 비동기 이벤트 루프 실행
        # Python 3.7+ 에서는 asyncio.run() 사용이 간편
        asyncio.run(bank_scraper_instance.run()) 
        
        logger.info("모든 스크래핑 작업 관련 루틴이 정상적으로 완료되었습니다.")

    except KeyboardInterrupt: # 사용자가 Ctrl+C 등으로 중단 시
        if logger: logger.warning("사용자에 의해 스크립트 실행이 중단되었습니다 (KeyboardInterrupt).")
        else: print("사용자에 의해 스크립트 실행이 중단되었습니다 (KeyboardInterrupt).")
        sys.exit(130) # Ctrl+C에 대한 표준 종료 코드
    except Exception as top_level_exception: # 예상치 못한 최상위 예외 처리
        if logger: logger.critical(f"스크립트 실행 중 최상위 오류 발생: {top_level_exception}", exc_info=True)
        else: print(f"스크립트 실행 중 최상위 오류 발생 (로거 미설정): {top_level_exception}\n{traceback.format_exc()}")
        sys.exit(1) # 오류 발생 시 비정상 종료 코드
    finally:
        # 스크립트 종료 전 정리 작업
        if driver_manager_instance: 
            logger.info("드라이버 리소스 정리 (quit_all) 시작...")
            driver_manager_instance.quit_all() 
        
        if logger: # 로거가 설정된 경우
            logger.info("스크립트 실행을 종료합니다.")
            logging.shutdown() # 모든 로깅 핸들러를 안전하게 닫음
        else: # 로거 설정 전 오류 발생 등 예외적 상황
            print("스크립트 실행을 종료합니다 (로거 미설정 상태).")

if __name__ == "__main__":
    # CI/CD 환경 (예: GitHub Actions)에서의 표준 출력 버퍼링 문제 완화
    if "CI" in os.environ:
        # 표준 출력/에러 스트림을 라인 버퍼링으로 설정 (즉시 출력되도록)
        sys.stdout = open(sys.stdout.fileno(), mode='w', buffering=1, encoding='utf-8', errors='replace')
        sys.stderr = open(sys.stderr.fileno(), mode='w', buffering=1, encoding='utf-8', errors='replace')
        if logger: logger.info("CI 환경 감지됨. 표준 출력/에러 버퍼링 조정.")
            
    main()
