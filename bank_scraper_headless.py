# bank_scraper_headless.py
"""
저축은행 중앙회 통일경영공시 데이터 자동 스크래핑 도구 (GitHub Actions 최적화 버전)
목적: GitHub Actions에서 자동 실행, 병렬 처리를 통한 속도 개선
작성일: 2025-03-31 (최종 수정일: 2025-05-28)
특징:
- (이전 기능 모두 포함)
- 이메일 첨부파일 MIME 타입 명시 및 제목 형식 변경
- 공시 날짜 확인 로직 고도화 (예정된 미래 공시 조기 발견 시 일치로 처리)
- 각 은행별 Excel 파일 내 카테고리별 시트 통합 및 데이터 누적 방지
- 이메일 첨부 파일명 형식 변경 ('저축은행 분기 공시자료_yyyy.mm.dd.zip')
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
from typing import Union, Dict, Tuple, List # List 추가

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
    if match: return f"{int(match.group(1))}년{int(match.group(2))}월말" # YYYY년M월말 형식
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

        res = {
            "latest_due_period": "결정 불가", "latest_due_reason": "",
            "next_imminent_period": "결정 불가", "next_imminent_reason": ""
        }
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
                part = MIMEBase('application', 'octet-stream') # Default, will be overridden if specific type found
                if p_attach.suffix.lower() == '.zip':
                    part = MIMEBase('application', 'zip')
                elif p_attach.suffix.lower() == '.xlsx':
                    part = MIMEBase('application', 'vnd.openxmlformats-officedocument.spreadsheetml.sheet')
                
                with open(p_attach, 'rb') as af: part.set_payload(af.read())
                encoders.encode_base64(part)
                
                base_filename = p_attach.name
                try: part.add_header('Content-Disposition', 'attachment', filename=encoders.encode_rfc2231(base_filename))
                except: part.add_header('Content-Disposition', f'attachment; filename="{base_filename}"') # Fallback
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
        self.VERSION = "2.11.0-sheet-consolidation-filename-update" 
        self.BASE_URL = "https://www.fsb.or.kr/busmagequar_0100.act"
        self.MAX_RETRIES = int(os.getenv('MAX_RETRIES', '2'))
        self.PAGE_LOAD_TIMEOUT = int(os.getenv('PAGE_LOAD_TIMEOUT', '30')) # 페이지 로드 타임아웃 약간 증가
        self.WAIT_TIMEOUT = int(os.getenv('WAIT_TIMEOUT', '20'))      # 요소 대기 타임아웃 약간 증가
        self.MAX_WORKERS = int(os.getenv('MAX_WORKERS', '2')) 

        self.today_yyyymmdd = datetime.now().strftime("%Y%m%d") # For internal folder naming
        self.output_dir_base = Path(os.getenv('OUTPUT_DIR', "./output"))
        self.output_dir = self.output_dir_base / f"저축은행_데이터_{self.today_yyyymmdd}"
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.progress_file = self.output_dir / 'progress.json'
        self.log_file_path = self.output_dir / f'scraping_log_{self.today_yyyymmdd}.log'

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
            "다올", "대신", "더케이", "민국", "바로", "스카이", "신한", "애큐온", "예가람", "웰컴", "유안타", "조은", "키움YES", "푸른", "하나", "DB", "HB", "JT", "친애", "KB", "NH", "OK", "OSB", "SBI", "금화", "남양", "모아", "부림", "삼정", "상상인", "세람", "안국", "안양", "영진", "융창", "인성", "인천", "키움", "페퍼", "평택", "한국투자", "한화", "고려", "국제", "동원제일", "솔브레인", "에스앤티", "우리", "조흥", "진주", "흥국", "BNK", "DH", "IBK", "대백", "대아", "대원", "드림", "라온", "머스트", "삼일", "엠에스", "오성", "유니온", "참", "CK", "대한", "더블", "동양", "삼호", "센트럴", "스마트", "스타", "대명", "상상인플러스", "아산", "오투", "우리금융", "청주", "한성"
        ]
        self.CATEGORIES = ["영업개황", "재무현황", "손익현황", "기타"]
        
        logger.info(f"--- 설정 초기화 (v{self.VERSION}) ---")
        logger.info(f"처리일자(KST 가정): {self.processing_date_kst}")
        logger.info(f"예상 (기한 지난) 최신 공시일: '{self.latest_due_period}' (근거: {self.date_expectations['latest_due_reason']})")
        logger.info(f"예상 (다음/현재) 공시일: '{self.next_imminent_period}' (근거: {self.date_expectations['next_imminent_reason']})")
        logger.info(f"출력 기본 디렉토리: {self.output_dir_base.resolve()}")
        logger.info(f"개별 은행 데이터 저장 폴더: {self.output_dir.resolve()}")


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
            try: self.driver_pool.put_nowait(self._create_new_driver())
            except queue.Full: logger.warning(f"드라이버 {i+1} 추가 중 풀 꽉 참."); break 
            except Exception as e: logger.error(f"초기 드라이버 {i+1} 생성 실패 ({type(e).__name__}). 풀 초기화 영향 가능성.")
        logger.info(f"드라이버 풀 초기화 완료. 사용 가능: {self.driver_pool.qsize()}개.")
    def get_driver(self):
        try: return self.driver_pool.get(block=True, timeout=60)
        except queue.Empty: raise TimeoutError(f"60초 대기 후에도 풀에서 드라이버를 가져오지 못함 (MAX_WORKERS: {self.config.MAX_WORKERS}).")
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
            try: driver = self.driver_pool.get_nowait(); driver.quit(); drained += 1
            except: break 
        logger.info(f"총 {drained}개 드라이버 종료 시도 완료.")

class ProgressManager:
    def __init__(self, config):
        self.config = config; self.progress_file_path = config.progress_file; self.progress = self._load()
    def _load(self):
        default = {'banks': {}, 'stats': {'last_run':None,'success_count':0,'failure_count':0}}
        if self.progress_file_path.exists():
            try:
                with open(self.progress_file_path, 'r', encoding='utf-8') as f:
                    loaded = json.load(f)
                    if 'banks' not in loaded: loaded['banks'] = {}
                    if 'stats' not in loaded: loaded['stats'] = default['stats']
                    return loaded
            except Exception as e: logger.warning(f"진행 파일 로드 오류({self.progress_file_path}): {e}. 새로 시작.")
        return default
    def save(self):
        self.progress['stats']['last_run'] = datetime.now().isoformat()
        s, f = 0, 0
        for info in self.progress.get('banks', {}).values():
            if info.get('status') == 'completed': s += 1
            elif info.get('status') == 'failed': f += 1
        self.progress['stats']['success_count'] = s; self.progress['stats']['failure_count'] = f
        try:
            with open(self.progress_file_path, 'w', encoding='utf-8') as fo: json.dump(self.progress, fo, ensure_ascii=False, indent=2)
        except Exception as e: logger.error(f"진행 파일 저장 실패: {e}", exc_info=True)
    def mark_completed(self, bank_name, date_info):
        self.progress.setdefault('banks', {})[bank_name] = {'status': 'completed', 'date_info': date_info}; self.save()
    def mark_failed(self, bank_name):
        data = self.progress.setdefault('banks', {}).get(bank_name, {})
        data['status'] = 'failed'; self.progress['banks'][bank_name] = data; self.save()
    def get_pending_banks(self):
        processed = self.progress.get('banks', {}); pending = [b for b in self.config.BANKS if b not in processed or processed[b].get('status') != 'completed']
        logger.info(f"보류 은행(재시도 포함 가능): {len(pending)}개"); return pending
    def get_bank_data(self, bank_name): return self.progress.get('banks', {}).get(bank_name)

# --- 스크래퍼 클래스 ---
class BankScraper:
    def __init__(self, config, driver_manager, progress_manager):
        self.config = config; self.driver_manager = driver_manager; self.progress_manager = progress_manager
        self.email_sender = EmailSender()

    def _wait_for_element(self, driver, by, value, timeout=None):
        try: return WebDriverWait(driver, timeout or self.config.WAIT_TIMEOUT).until(EC.presence_of_element_located((by, value)))
        except TimeoutException: logger.debug(f"요소 대기 시간 초과: ({by}, {value})"); return None

    def _robust_click(self, driver, element):
        try:
            driver.execute_script("arguments[0].scrollIntoView({block: 'center', inline: 'nearest'});", element); time.sleep(0.2)
            driver.execute_script("arguments[0].click();", element); return True
        except:
            try: element.click(); return True
            except Exception as e: logger.warning(f"Robust 클릭 실패: {e}"); return False

    def extract_date_information(self, driver):
        logger.debug(f"날짜 정보 추출 시도 (v{self.config.VERSION})...")
        try:
            js_script = """
            var allMatches = [];
            var datePattern = /(\d{4})년\s*(\d{1,2})월말/g;
            var bodyText = document.body.innerText || "";
            var match; datePattern.lastIndex = 0;
            while ((match = datePattern.exec(bodyText)) !== null) {
                allMatches.push({fullText: match[0], year: parseInt(match[1]), month: parseInt(match[2])});
            }
            var tagsToSearch = ['h1','h2','h3','th','td','p','span','div.title','caption','.date','#publishDate','.disclosure-date'];
            for (var i = 0; i < tagsToSearch.length; i++) {
                var elements;
                try { elements = document.querySelectorAll(tagsToSearch[i]); } catch (e) { elements = []; }
                for (var j = 0; j < elements.length; j++) {
                    var elementText = elements[j].innerText || "";
                    if (elementText.length > 3000) elementText = elementText.substring(0, 3000); // 성능을 위해 텍스트 길이 제한
                    datePattern.lastIndex = 0;
                    while ((match = datePattern.exec(elementText)) !== null) {
                        var cY = parseInt(match[1]), cM = parseInt(match[2]);
                        if (!allMatches.some(m => m.year === cY && m.month === cM)) { 
                            allMatches.push({fullText: match[0], year: cY, month: cM});
                        }
                    }
                }
            }
            if (allMatches.length === 0) return '날짜 정보 없음';
            // 연도와 월을 기준으로 내림차순 정렬
            allMatches.sort((a,b) => (b.year !== a.year) ? (b.year - a.year) : (b.month - a.month));
            
            var sysYear = new Date().getFullYear();
            // 현재 연도 기준 +- 범위 내의 합리적인 날짜 필터링 (너무 과거/미래 제외)
            var reasonableDates = allMatches.filter(m => m.year >= sysYear - 3 && m.year <= sysYear + 1);
            
            if (reasonableDates.length > 0) return reasonableDates[0].fullText.replace(/\s+/g, '');
            if (allMatches.length > 0) return allMatches[0].fullText.replace(/\s+/g, ''); // 합리적 날짜 없으면 최신순 첫번째
            return '날짜 정보 없음';
            """
            date_info = driver.execute_script(js_script)
            logger.debug(f"추출된 최종 날짜 정보: '{date_info}'")
            return date_info
        except Exception as e:
            logger.error(f"날짜 정보 추출 중 오류 (JS 로직): {e}", exc_info=True)
            return "날짜 추출 실패"

    def select_bank(self, driver, bank_name):
        logger.debug(f"[{bank_name}] 은행 선택 시도..."); 
        driver.get(self.config.BASE_URL)
        try:
            WebDriverWait(driver,self.config.PAGE_LOAD_TIMEOUT).until(lambda d:d.execute_script('return document.readyState')=='complete')
            time.sleep(random.uniform(0.5, 1.0)) # 페이지 안정화
        except TimeoutException:
            logger.error(f"[{bank_name}] 은행 선택 페이지 로드 타임아웃: {self.config.BASE_URL}")
            return False

        # 우선적으로 정확한 이름 매칭 시도
        for xp_template in [f"//td[normalize-space(.)='{bank_name}']//ancestor::tr//a", f"//a[normalize-space(.)='{bank_name}']"]:
            try:
                elements = driver.find_elements(By.XPATH, xp_template)
                for el in elements:
                    if el.is_displayed() and el.is_enabled():
                        logger.debug(f"[{bank_name}] XPath로 요소 찾음: {xp_template}, Text: {el.text}")
                        if self._robust_click(driver, el):
                            time.sleep(random.uniform(0.8, 1.5)) # 클릭 후 페이지 전환 대기
                            # 현재 URL이 BASE_URL과 다른지 확인하여 페이지 이동 여부 판단
                            if driver.current_url != self.config.BASE_URL:
                                logger.info(f"[{bank_name}] 은행 선택 성공 (XPath). URL 변경됨: {driver.current_url}")
                                return True
                            else:
                                logger.warning(f"[{bank_name}] 은행 클릭은 했으나 URL 변경 안됨 (XPath).")
            except NoSuchElementException:
                pass # 다음 XPath 시도
            except Exception as e:
                logger.warning(f"[{bank_name}] 은행 선택 중 예외 (XPath: {xp_template}): {e}")
        
        # JavaScript를 이용한 클릭 시도 (이름 포함)
        js_click_script = f"""
        var bankName = '{bank_name}';
        var links = Array.from(document.querySelectorAll('a'));
        var targetLink = links.find(a => a.innerText && a.innerText.trim().includes(bankName));
        if (targetLink) {{
            targetLink.scrollIntoView({{block: 'center'}});
            targetLink.click();
            return true;
        }}
        var tds = Array.from(document.querySelectorAll('td'));
        var targetTd = tds.find(td => td.innerText && td.innerText.trim().includes(bankName));
        if(targetTd) {{
            var linkInTd = targetTd.querySelector('a') || (targetTd.parentNode.querySelector('a'));
            if (linkInTd) {{
                 linkInTd.scrollIntoView({{block: 'center'}});
                 linkInTd.click();
                 return true;
            }}
        }}
        return false;
        """
        try:
            if driver.execute_script(js_click_script):
                time.sleep(random.uniform(0.8, 1.5))
                if driver.current_url != self.config.BASE_URL:
                    logger.info(f"[{bank_name}] 은행 선택 성공 (JS). URL 변경됨: {driver.current_url}")
                    return True
                else:
                    logger.warning(f"[{bank_name}] 은행 클릭은 했으나 URL 변경 안됨 (JS).")
        except Exception as e:
            logger.warning(f"[{bank_name}] 은행 선택 중 예외 (JS 클릭): {e}")

        logger.warning(f"[{bank_name}] 은행 선택 최종 실패."); return False


    def select_category(self, driver, category_name):
        logger.debug(f"[{category_name}] 카테고리 선택 시도..."); 
        time.sleep(random.uniform(0.3, 0.6)) # 이전 동작과의 간격
        
        cat_norm = category_name.replace(' ', '')
        selectors = [
            (By.XPATH, f"//a[normalize-space(translate(text(),' \t\n\r',''))='{cat_norm}']"),
            (By.XPATH, f"//button[normalize-space(translate(text(),' \t\n\r',''))='{cat_norm}']"),
            (By.LINK_TEXT, category_name),
            (By.PARTIAL_LINK_TEXT, category_name)
        ]
        
        for by_type, val in selectors:
            try:
                elements = driver.find_elements(by_type, val)
                for el in elements:
                    # 정확히 일치하는 텍스트를 가진 요소 우선
                    el_text_norm = (el.text or "").replace(' ','').strip()
                    if el.is_displayed() and el.is_enabled() and (el_text_norm == cat_norm or val == category_name):
                        logger.debug(f"[{category_name}] 선택자 {by_type}, '{val}' 로 요소 찾음. 텍스트: '{el.text}'")
                        if self._robust_click(driver, el):
                            time.sleep(random.uniform(0.7, 1.2)) # 클릭 후 AJAX 로딩 대기
                            logger.info(f"[{category_name}] 카테고리 선택 성공 ({by_type}).")
                            return True
            except NoSuchElementException:
                continue
            except StaleElementReferenceException:
                logger.warning(f"[{category_name}] 카테고리 선택 중 StaleElementReferenceException 발생. 재시도 가능성.")
                return False # 실패로 간주하고 호출부에서 재시도 유도
            except Exception as e:
                logger.warning(f"[{category_name}] 카테고리 선택 중 예외 ({by_type}, {val}): {e}")
        
        # JavaScript 클릭 시도
        js_cat_script = f"""
        var catName = '{category_name}';
        var elements = Array.from(document.querySelectorAll('a, li > span, button, div[role="tab"]'));
        var targetElement = elements.find(e => e.innerText && e.innerText.trim().includes(catName));
        if (targetElement) {{
            targetElement.scrollIntoView({{block: 'center'}});
            targetElement.click();
            return true;
        }}
        return false;
        """
        try:
            if driver.execute_script(js_cat_script):
                time.sleep(random.uniform(0.7, 1.2)) # JS 클릭 후 AJAX 로딩 대기
                logger.info(f"[{category_name}] 카테고리 선택 성공 (JS).")
                return True
        except Exception as e:
            logger.warning(f"[{category_name}] 카테고리 선택 중 예외 (JS 클릭): {e}")
            
        logger.warning(f"[{category_name}] 카테고리 선택 최종 실패."); return False
        
    def extract_tables_from_page(self, driver) -> List[pd.DataFrame]:
        # 페이지가 완전히 로드되고 JS가 실행될 시간을 충분히 줌
        WebDriverWait(driver,self.config.PAGE_LOAD_TIMEOUT).until(lambda d:d.execute_script('return document.readyState')=='complete')
        time.sleep(random.uniform(1.0, 2.0)) # AJAX 컨텐츠 로드를 위한 추가 대기 시간
        
        valid_dfs = []
        try:
            src = driver.page_source
            if not src or len(src) < 200 or "<table" not in src.lower(): # 간단한 유효성 검사
                logger.info("페이지 소스에 테이블 태그가 없거나 내용이 너무 짧습니다.")
                return []

            # BeautifulSoup으로 특정 영역만 파싱하도록 개선 가능 (웹사이트 구조에 따라)
            # 예: soup = BeautifulSoup(src, 'html.parser')
            #     target_div = soup.find('div', id='현재_카테고리_데이터_영역_ID')
            #     if target_div:
            #         dfs = pd.read_html(StringIO(str(target_div)), flavor='bs4', encoding='utf-8')
            #     else: ...
            # 현재는 페이지 전체에서 테이블을 읽음
            dfs = pd.read_html(StringIO(src), flavor='bs4', encoding='utf-8')
            
            if not dfs:
                logger.info("pd.read_html이 테이블을 찾지 못했습니다.")
                return []

            for i, df in enumerate(dfs):
                if not isinstance(df, pd.DataFrame) or df.empty:
                    logger.debug(f"테이블 {i}는 비어있거나 DataFrame이 아님. 건너뜀.")
                    continue
                
                df.dropna(axis=0, how='all', inplace=True)
                df.dropna(axis=1, how='all', inplace=True)
                
                if df.empty:
                    logger.debug(f"테이블 {i}는 NA 제거 후 비어있음. 건너뜀.")
                    continue
                
                # 멀티인덱스 컬럼 처리 및 일반 컬럼 문자열 변환
                if isinstance(df.columns, pd.MultiIndex):
                    df.columns = ['_'.join(map(str,c)).strip('_ ') for c in df.columns]
                else:
                    df.columns = [str(c).strip() for c in df.columns]
                
                valid_dfs.append(df.reset_index(drop=True))
            
            logger.info(f"총 {len(dfs)}개 테이블 발견, 유효한 테이블 {len(valid_dfs)}개 추출 완료.")
            return valid_dfs
            
        except ValueError as ve:
            logger.warning(f"테이블 추출 중 ValueError (일치하는 테이블 없음 가능성): {ve}")
            return []
        except Exception as e:
            logger.error(f"테이블 추출 중 심각한 오류: {e}", exc_info=True)
            return []


    def _scrape_single_bank_attempt(self, bank_name, driver) -> Union[Dict[str, pd.DataFrame], None]:
        logger.info(f"[{bank_name}] 스크래핑 시도...")
        if not self.select_bank(driver, bank_name):
            logger.error(f"[{bank_name}] 은행 선택 실패. 이 은행 스크래핑 중단.")
            return None

        date_info_scraped = self.extract_date_information(driver)
        logger.info(f"[{bank_name}] 추출 공시일(원본): '{date_info_scraped}'")
        
        normalized_scraped_date = normalize_datestr_for_comparison(date_info_scraped)
        expected_officially_due = self.config.latest_due_period
        expected_next_imminent = self.config.next_imminent_period

        logger.info(f"[{bank_name}] 정규화 공시일: '{normalized_scraped_date}', 공식적 최신 예상: '{expected_officially_due}', 다음 업로드 예상: '{expected_next_imminent}'")

        if normalized_scraped_date is None:
            logger.error(f"[{bank_name}] 날짜 추출 실패 또는 정규화 불가. 공시일 비교 불가.")
        elif normalized_scraped_date == "알 수 없는 형식":
            logger.warning(f"[{bank_name}] 날짜 형식 알 수 없음: '{date_info_scraped}'. 공시일 비교 불가.")
        elif normalized_scraped_date == expected_officially_due:
            logger.info(f"[{bank_name}] 공시일('{normalized_scraped_date}')이 공식적으로 최신이어야 할 기간과 일치.")
        elif normalized_scraped_date == expected_next_imminent:
            logger.info(f"[{bank_name}] 공시일('{normalized_scraped_date}')이 다음 업로드 예정/진행 기간과 일치 (선제적 업데이트).")
        else:
            logger.critical(f"[{bank_name}] !!날짜 불일치!! 웹사이트(정규화): '{normalized_scraped_date}', 공식적 최신 예상: '{expected_officially_due}', 다음 업로드 예상: '{expected_next_imminent}'. (사이트 원본: '{date_info_scraped}')")
        
        # 최종 Excel 파일에 저장될 데이터를 담는 딕셔너리
        excel_data_for_bank = {
            '_INFO_': pd.DataFrame([{
                '은행명': bank_name, 
                '공시날짜': date_info_scraped, 
                '추출일시': datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 
                '스크래퍼버전': self.config.VERSION
            }])
        }
        has_any_data_in_bank = False # 이 은행에서 유효한 카테고리 데이터를 하나라도 찾았는지 여부
        initial_bank_page_url = driver.current_url # 은행 선택 후의 URL (카테고리 선택 실패 시 돌아올 기준점)

        for cat_name in self.config.CATEGORIES:
            logger.info(f"[{bank_name}] '{cat_name}' 카테고리 처리 시작.")
            cat_selected = False
            # 카테고리 선택 시도 (최대 2번: 현재 페이지에서, 실패 시 은행 페이지 재방문 후)
            for attempt in range(1, 3): # 1, 2
                if attempt > 1: # 두 번째 시도라면
                    logger.info(f"[{bank_name}] '{cat_name}' 선택 재시도를 위해 은행 초기 페이지로 돌아감: {initial_bank_page_url}")
                    driver.get(initial_bank_page_url)
                    try:
                        WebDriverWait(driver, self.config.PAGE_LOAD_TIMEOUT).until(
                            lambda d: d.execute_script('return document.readyState') == 'complete'
                        )
                        time.sleep(random.uniform(0.5,1.0)) # 페이지 안정화
                    except TimeoutException:
                        logger.error(f"[{bank_name}] '{cat_name}' 카테고리 선택 재시도 중 은행 페이지({initial_bank_page_url}) 로드 타임아웃.")
                        break # 이 카테고리 선택 포기 (for attempt loop)
                
                if self.select_category(driver, cat_name):
                    cat_selected = True
                    # 카테고리 선택 후 DOM이 완전히 업데이트 될 시간을 줌
                    time.sleep(random.uniform(1.5, 2.5)) # 충분한 대기 시간 부여
                    break 
                else:
                    logger.warning(f"[{bank_name}] '{cat_name}' 카테고리 선택 시도 {attempt} 실패.")
            
            if not cat_selected:
                logger.error(f"[{bank_name}] '{cat_name}' 카테고리 최종 선택 실패. 이 카테고리 건너뜀.")
                continue # 다음 카테고리로

            # 현재 선택된 카테고리의 테이블들만 추출
            logger.info(f"[{bank_name}] '{cat_name}' 카테고리에서 테이블 추출 시도...")
            tables_for_current_category = self.extract_tables_from_page(driver)

            if tables_for_current_category:
                logger.info(f"[{bank_name}] '{cat_name}' 카테고리에서 {len(tables_for_current_category)}개 테이블 원본 발견.")
                
                # 실제 DataFrame으로 변환된 테이블만 필터링
                valid_dataframes_in_category = [df for df in tables_for_current_category if isinstance(df, pd.DataFrame) and not df.empty]

                if valid_dataframes_in_category:
                    logger.info(f"[{bank_name}] '{cat_name}' 카테고리에서 {len(valid_dataframes_in_category)}개 유효 DataFrame 확보.")
                    # 모든 유효 DataFrame을 하나로 병합
                    try:
                        combined_df_for_category = pd.concat(valid_dataframes_in_category, ignore_index=True)
                        logger.info(f"[{bank_name}] '{cat_name}' 카테고리 테이블 병합 완료. 총 {len(combined_df_for_category)} 행.")
                    except Exception as e:
                        logger.error(f"[{bank_name}] '{cat_name}' 카테고리 테이블 병합 중 오류: {e}. 이 카테고리 데이터 저장 안 함.")
                        continue # 다음 카테고리로
                    
                    sheet_name = re.sub(r'[\\/*?:\[\]]', '', cat_name)[:31] # 시트 이름 정제 및 길이 제한
                    excel_data_for_bank[sheet_name] = combined_df_for_category
                    has_any_data_in_bank = True
                else:
                    logger.info(f"[{bank_name}] '{cat_name}' 카테고리에서 테이블은 발견했으나 유효한 DataFrame이 없음.")
            else:
                logger.info(f"[{bank_name}] '{cat_name}' 카테고리에서 테이블을 찾지 못함.")
        
        if has_any_data_in_bank:
            logger.info(f"[{bank_name}] 스크래핑으로부터 데이터 수집 완료 ({len(excel_data_for_bank) - 1}개 카테고리).")
            return excel_data_for_bank
        else:
            # _INFO_ 시트만 있고 실제 데이터가 없는 경우
            logger.warning(f"[{bank_name}] 스크래핑 결과, '_INFO_' 외 유효한 카테고리 데이터를 찾지 못함.")
            # 실패로 처리하려면 None 반환. 날짜 정보만이라도 남기려면 excel_data_for_bank 반환.
            # 여기서는 유효한 카테고리 데이터가 없으면 실패로 간주.
            return None

    def save_bank_data(self, bank_name, excel_data_dict: Dict[str, pd.DataFrame]):
        if '_INFO_' not in excel_data_dict or excel_data_dict['_INFO_'].empty:
            logger.error(f"[{bank_name}] 저장할 데이터에 _INFO_ 시트가 없거나 비어있습니다.")
            return False
            
        raw_date = excel_data_dict['_INFO_']['공시날짜'].iloc[0]
        match = re.search(r'(\d{4})년(\d{1,2})월', raw_date) # YYYY년M월 형식
        if match:
            date_fn_suffix = f"{match.group(1)}-{int(match.group(2)):02d}" # YYYY-MM
        else:
            # 날짜 형식이 예상과 다를 경우, 원본 문자열에서 파일명에 부적합한 문자 제거
            date_fn_suffix = re.sub(r'[^\w\-_.]', '', raw_date or "날짜정보없음")
            if not date_fn_suffix: date_fn_suffix = "날짜정보없음" # 최종 폴백

        excel_filename = f"{bank_name}_{date_fn_suffix}.xlsx"
        excel_path = self.config.output_dir / excel_filename
        
        try:
            with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
                for sheet_name_original, df_sheet_data in excel_data_dict.items():
                    # 시트 이름 정제 (중복이지만 안전하게 한 번 더)
                    # _INFO_는 '정보' 시트로, 나머지는 원본 카테고리명(정제된)으로
                    sheet_name_final = '정보' if sheet_name_original == '_INFO_' else re.sub(r'[\\/*?:\[\]]', '', sheet_name_original)[:31]
                    
                    if df_sheet_data.empty and sheet_name_final != '정보': # 정보 시트는 비어있을 수 없음 (위에서 체크)
                        logger.info(f"[{bank_name}] '{sheet_name_final}' 시트 데이터가 비어있어 저장하지 않습니다.")
                        continue
                    df_sheet_data.to_excel(writer, sheet_name=sheet_name_final, index=False)
            logger.info(f"[{bank_name}] 데이터 저장 완료: {excel_path.name} (경로: {excel_path})")
            return True
        except Exception as e:
            logger.error(f"[{bank_name}] 데이터 저장 실패 ({excel_path.name}): {e}", exc_info=True)
            return False

    async def worker_process_bank(self, bank_name, pbar, semaphore):
        async with semaphore:
            driver, success, date_info_final = None, False, None
            # progress_manager에서 이전 실행 시 date_info 가져오기 (선택적)
            previous_run_data = self.progress_manager.get_bank_data(bank_name)
            if previous_run_data and 'date_info' in previous_run_data:
                date_info_final = previous_run_data['date_info']

            try:
                driver = await asyncio.get_event_loop().run_in_executor(None, self.driver_manager.get_driver)
                if not driver:
                    logger.error(f"[{bank_name}] WebDriver 인스턴스를 가져오지 못했습니다.")
                    self.progress_manager.mark_failed(bank_name)
                    return bank_name, False, date_info_final
                
                bank_excel_data = None
                for attempt in range(1, self.config.MAX_RETRIES + 1):
                    try:
                        logger.info(f"[{bank_name}] 스크래핑 시도 {attempt}/{self.config.MAX_RETRIES}...")
                        bank_excel_data = self._scrape_single_bank_attempt(bank_name, driver)
                        
                        if bank_excel_data and '_INFO_' in bank_excel_data: # 성공 조건: 데이터가 있고, INFO 시트가 있음
                             # INFO 시트에서 최종 공시일자 업데이트
                            date_info_final = bank_excel_data['_INFO_']['공시날짜'].iloc[0]
                            logger.info(f"[{bank_name}] 시도 {attempt} 성공. 공시일: {date_info_final}")
                            break # 성공했으므로 재시도 루프 탈출
                        else:
                            logger.warning(f"[{bank_name}] 시도 {attempt}에서 유효한 데이터 못 얻음. bank_excel_data: {bank_excel_data is not None}")
                            if attempt < self.config.MAX_RETRIES:
                                logger.info(f"[{bank_name}] 잠시 후 재시도합니다...")
                                await asyncio.sleep(random.uniform(2,4)) # 재시도 전 잠시 대기
                            else:
                                logger.error(f"[{bank_name}] 모든 ({self.config.MAX_RETRIES}회) 재시도 실패.")
                                bank_excel_data = None # 최종적으로 데이터 없음 명시
                    
                    except WebDriverException as wde: # WebDriver 관련 심각한 예외
                        logger.error(f"[{bank_name}] 시도 {attempt} 중 WebDriverException: {wde}", exc_info=True)
                        # 드라이버가 손상되었을 가능성이 있으므로, 현재 드라이버를 반환(폐기)하고 새 드라이버 요청
                        await asyncio.get_event_loop().run_in_executor(None, self.driver_manager.return_driver, driver)
                        driver = None # 기존 드라이버 참조 제거
                        if attempt < self.config.MAX_RETRIES:
                            logger.info(f"[{bank_name}] 새 드라이버로 재시도합니다.")
                            driver = await asyncio.get_event_loop().run_in_executor(None, self.driver_manager.get_driver)
                            if not driver:
                                logger.error(f"[{bank_name}] 재시도를 위한 새 드라이버 획득 실패. 이 은행 처리 중단.")
                                break # 재시도 루프 중단
                        else:
                            logger.error(f"[{bank_name}] WebDriverException 후 모든 재시도 실패.")
                            bank_excel_data = None
                            break # 재시도 루프 중단
                    except Exception as e:
                        logger.warning(f"[{bank_name}] 시도 {attempt} 중 일반 예외: {type(e).__name__} - {e}", exc_info=True)
                        if attempt == self.config.MAX_RETRIES:
                            logger.error(f"[{bank_name}] 일반 예외로 모든 재시도 실패.")
                            bank_excel_data = None # 최종적으로 데이터 없음 명시
                        else: # 일반 예외시에는 현재 드라이버로 재시도 가능
                             await asyncio.sleep(random.uniform(2,4))

                if bank_excel_data and self.save_bank_data(bank_name, bank_excel_data):
                    self.progress_manager.mark_completed(bank_name, date_info_final)
                    success = True
                else:
                    self.progress_manager.mark_failed(bank_name)
                
                return bank_name, success, date_info_final

            except TimeoutError as te: 
                logger.error(f"[{bank_name}] 드라이버 풀에서 드라이버 획득 타임아웃: {te}"); self.progress_manager.mark_failed(bank_name); return bank_name, False, date_info_final
            except Exception as e: 
                logger.error(f"[{bank_name}] 작업자 프로세스에서 예상치 못한 최상위 예외: {type(e).__name__} - {e}", exc_info=True); self.progress_manager.mark_failed(bank_name); return bank_name, False, date_info_final
            finally:
                if driver: await asyncio.get_event_loop().run_in_executor(None, self.driver_manager.return_driver, driver)
                if pbar: pbar.update(1)
                logger.log(logging.INFO if success else logging.ERROR, f"[{bank_name}] 처리 {'성공' if success else '실패'}. 최종 공시일(추정): {date_info_final or '미확정'}")
    
    async def run(self):
        logger.info(f"==== 스크래핑 시작 (v{self.config.VERSION}) ====")
        start_time = time.monotonic()
        pending_banks = self.progress_manager.get_pending_banks()
        if not pending_banks: 
            logger.info("처리할 은행 없음 (모두 완료 또는 대상 은행 없음).")
            self.generate_summary_and_send_email() # 처리할 은행 없어도 요약 및 메일 발송
            return
        
        logger.info(f"총 {len(pending_banks)}개 은행 처리 예정: {pending_banks[:3]}{'...' if len(pending_banks)>3 else ''}")
        semaphore = asyncio.Semaphore(self.config.MAX_WORKERS)
        
        with tqdm(total=len(pending_banks), desc="은행 스크래핑", unit="은행", dynamic_ncols=True, smoothing=0.1) as pbar:
            tasks = [self.worker_process_bank(bank_name, pbar, semaphore) for bank_name in pending_banks]
            # 예외 발생 시에도 계속 진행하고 결과에 포함
            results = await asyncio.gather(*tasks, return_exceptions=True) 
        
        processed_count = 0
        for i, res in enumerate(results):
            if isinstance(res, Exception):
                bank_name_failed = pending_banks[i] # gather는 순서 보장
                logger.error(f"은행 '{bank_name_failed}' 처리 중 gather에서 예외 반환: {res}", exc_info=isinstance(res, BaseException)) # exc_info=True로 트레이스백
                # 이미 worker_process_bank 내부에서 mark_failed를 호출했을 수 있지만, 여기서 한 번 더 보장
                self.progress_manager.mark_failed(bank_name_failed)
            else:
                # res는 (bank_name, success, date_info_final) 튜플
                if res and len(res) == 3: # 정상 반환 확인
                     processed_count +=1
                else: # 예상치 못한 반환값 (오류로 간주)
                    bank_name_unknown_state = pending_banks[i]
                    logger.error(f"은행 '{bank_name_unknown_state}' 처리 결과가 비정상적: {res}. 실패로 간주.")
                    self.progress_manager.mark_failed(bank_name_unknown_state)

        logger.info(f"asyncio.gather로 {len(results)}개 작업 시도, 이 중 {processed_count}개 정상적 반환 완료 (개별 성공/실패는 로그 및 요약 참조).")
        logger.info(f"==== 전체 스크래핑 완료. 소요시간: {time.monotonic() - start_time:.2f}초 ====")
        self.generate_summary_and_send_email()

    def generate_summary_and_send_email(self):
        logger.info("요약 보고서 및 이메일 생성 시작...")
        summary_data = []
        all_banks_in_config = self.config.BANKS
        processed_banks_data = self.progress_manager.progress.get('banks', {})
        
        expected_officially_due = self.config.latest_due_period
        expected_next_imminent = self.config.next_imminent_period
        
        completed_count, failed_count = 0,0; failed_banks_names = []

        for bank_name_iter in all_banks_in_config:
            bank_detail = processed_banks_data.get(bank_name_iter)
            status, original_disc_date, date_match_status_str = '미처리', '', ''
            if bank_detail:
                current_status = bank_detail.get('status')
                original_disc_date = bank_detail.get('date_info', '') 
                normalized_disc_date = normalize_datestr_for_comparison(original_disc_date)

                if current_status == 'completed':
                    status, completed_count = '완료', completed_count + 1
                    if normalized_disc_date is None: date_match_status_str = "⚠️ 정규화실패"
                    elif normalized_disc_date == "알 수 없는 형식": date_match_status_str = f"❓ 형식모름 ({original_disc_date})"
                    elif normalized_disc_date == expected_officially_due: date_match_status_str = "✅ 일치 (기한내 최신)"
                    elif normalized_disc_date == expected_next_imminent: date_match_status_str = "🟢 일치 (예정분 선반영)"
                    else: date_match_status_str = f"❌ 불일치! (웹: {normalized_disc_date}, 예상: {expected_officially_due} 또는 {expected_next_imminent})"
                elif current_status == 'failed':
                    status, failed_count = '실패', failed_count + 1
                    failed_banks_names.append(bank_name_iter); date_match_status_str = "N/A (실패)"
                else: # '미처리' 또는 다른 상태 (예: '진행중' 등은 현재 로직에 없음)
                    status = current_status if current_status else '미처리'
                    if status != '미처리' : failed_banks_names.append(bank_name_iter) # 미처리가 아닌 다른 상태도 일단 실패로 카운트 (로직상 나와선 안됨)

            summary_data.append({
                '은행명':bank_name_iter, '공시 날짜(웹사이트)':original_disc_date, 
                '날짜 확인 상태':date_match_status_str, '처리 상태':status, 
                '확인 시간':datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            })
        
        summary_df = pd.DataFrame(summary_data)
        # 파일명에 사용할 날짜 (YYYY.MM.DD 형식)
        date_for_output_filename = self.config.processing_date_kst.strftime("%Y.%m.%d")
        
        summary_filename = f"스크래핑_요약_{date_for_output_filename}.xlsx"
        summary_file_path = self.config.output_dir / summary_filename # 개별 은행 데이터와 같은 폴더에 요약 저장
        try: 
            summary_df.to_excel(summary_file_path, index=False)
            logger.info(f"요약 보고서 저장: {summary_file_path}")
        except Exception as e: 
            logger.error(f"요약 보고서 저장 실패: {e}", exc_info=True)

        # 압축 파일명 (요청된 형식 사용)
        zip_filename_base = f"저축은행 분기 공시자료_{date_for_output_filename}"
        zip_filename_str = f"{zip_filename_base}.zip"
        zip_file_path_obj = self.config.output_dir_base / zip_filename_str # output_dir_base (./output) 에 저장
        
        try:
            with zipfile.ZipFile(zip_file_path_obj, 'w', zipfile.ZIP_DEFLATED) as zf:
                # output_dir (./output/저축은행_데이터_YYYYMMDD) 내의 모든 파일과 폴더를 압축
                # zip 파일 내에서는 '저축은행_데이터_YYYYMMDD/파일들' 구조를 가지도록 함
                for f_path in self.config.output_dir.rglob('*'):
                    if f_path.is_file():
                        # 압축 파일 내 경로: output_dir의 이름(저축은행_데이터_YYYYMMDD)을 최상위로 하고 그 아래 파일 상대 경로
                        arcname = Path(self.config.output_dir.name) / f_path.relative_to(self.config.output_dir)
                        zf.write(f_path, arcname=arcname)
            logger.info(f"결과 압축 완료: {zip_file_path_obj}")
        except Exception as e: 
            logger.error(f"결과 압축 실패: {e}", exc_info=True)
            zip_file_path_obj = None # 압축 실패 시 None으로 설정

        processed_attempt_count = completed_count + failed_count
        # 미처리 은행도 실패로 간주할지 여부에 따라 아래 계산이 달라질 수 있음. 현재는 completed와 failed만으로 계산.
        unprocessed_count = len(all_banks_in_config) - processed_attempt_count
        if unprocessed_count > 0:
             logger.warning(f"{unprocessed_count}개의 은행이 '미처리' 상태입니다. (실패 집계에 미포함)")


        success_rate = (completed_count / processed_attempt_count * 100) if processed_attempt_count > 0 else 0
        
        # 이메일 제목
        date_for_email_subject = self.config.processing_date_kst.strftime("%Y.%m.%d")
        quarter_info_for_email_subject = get_quarter_string_from_period(expected_officially_due) # 예: "2023년 3분기"
        email_subject = f"저축은행 분기 공시 취합_{quarter_info_for_email_subject} ({date_for_email_subject})"
        
        failed_banks_list_html = "".join(f"<li>{b}</li>" for b in failed_banks_names[:15]) # 실패 은행 최대 15개 표시
        if len(failed_banks_names) > 15:
            failed_banks_list_html += f"<li>...외 {len(failed_banks_names)-15}개 은행 실패.</li>"
        if not failed_banks_names:
            failed_banks_list_html = "<li>없음</li>"
            
        body_html = f"""
        <html><head><style>
            body{{font-family: 'Malgun Gothic', Arial, sans-serif; margin:20px; font-size:10pt;}} h2{{color:#2c3e50; font-size:14pt;}}
            .summary-box{{border:1px solid #ddd; padding:15px; margin-bottom:20px; background-color:#f9f9f9; border-radius:5px;}}
            .summary-box p{{margin:5px 0;}} .status-completed{{color:green;}} .status-failed{{color:red;}}
            table{{border-collapse:collapse; width:98%; margin-top:15px; font-size:9pt; table-layout: fixed;}} /* table-layout fixed 추가 */
            th,td{{border:1px solid #ddd; padding:6px; text-align:left; word-break:break-all;}} 
            th{{background-color:#e9eff7; white-space:nowrap; text-align:center;}}
            td:nth-child(1){{width:100px; white-space:nowrap;}} /* 은행명 */
            td:nth-child(2){{width:120px;}} /* 공시날짜 */
            td:nth-child(3){{width:180px;}} /* 날짜 확인 상태 */
            td:nth-child(4){{width:80px; text-align:center;}} /* 처리 상태 */
        </style></head><body>
        <h2>저축은행 스크래핑 결과 ({self.config.processing_date_kst.strftime("%Y-%m-%d")})</h2>
        <p><strong>스크립트 실행일:</strong> {self.config.processing_date_kst.strftime('%Y-%m-%d %H:%M:%S')}</p>
        <p><strong>공식적 최신 예상 공시 기준일:</strong> {expected_officially_due} (근거: {self.config.date_expectations['latest_due_reason']})</p>
        <p><strong>다음 업로드 예상 공시 기준일:</strong> {expected_next_imminent} (근거: {self.config.date_expectations['next_imminent_reason']})</p>
        <div class="summary-box">
            <p>총 대상 은행: {len(all_banks_in_config)}개</p> 
            <p>처리 시도된 은행: {processed_attempt_count}개 (미처리: {unprocessed_count}개)</p>
            <p><span class="status-completed">✅ 성공: {completed_count}개</span></p> 
            <p><span class="status-failed">❌ 실패: {failed_count}개</span> (성공률: {success_rate:.1f}%)</p>
            <p>📂 데이터 저장 폴더: {self.config.output_dir.name}</p>
            <p>📨 첨부 파일: {zip_filename_str if zip_file_path_obj else ('압축 파일 생성 실패. ' + (summary_filename if summary_file_path.exists() else '요약 파일도 없음.'))} </p>
        </div>
        <h3>실패 은행 목록 (최대 15개):</h3><ul>{failed_banks_list_html}</ul>
        <p>세부 결과는 첨부된 요약 Excel 파일(압축파일 내 포함)을 확인해주시기 바랍니다.</p> 
        <h3>은행별 처리 현황 간략 보기:</h3>
        {summary_df.to_html(index=False, border=1, na_rep='-').replace('<table border="1" class="dataframe">','<table>') if not summary_df.empty else "<p>요약 데이터 없음.</p>"}
        <br><p><small><i>이 메일은 자동으로 발송되었습니다. (스크래퍼 버전: {self.config.VERSION})</i></small></p>
        </body></html>"""
        
        attachment_to_send = None
        if zip_file_path_obj and zip_file_path_obj.exists():
            attachment_to_send = str(zip_file_path_obj)
        elif summary_file_path and summary_file_path.exists(): # 압축 실패 시 요약 파일이라도 첨부
            logger.warning("압축 파일 생성 실패 또는 누락. 요약 보고서만 첨부합니다.")
            attachment_to_send = str(summary_file_path)
        else:
             logger.warning("압축 파일 및 요약 보고서 모두 누락. 첨부 파일 없이 발송될 수 있습니다.")
        
        self.email_sender.send_email_with_attachment(email_subject, body_html, attachment_to_send)

# --- 메인 실행 로직 ---
def main():
    config = Config() 
    driver_mgr = None 
    try:
        logger.info(f"스크립트 실행: {sys.argv[0]} (PID: {os.getpid()})")
        driver_mgr = DriverManager(config)
        if driver_mgr.driver_pool.qsize() == 0 and config.MAX_WORKERS > 0 :
             logger.critical(f"WebDriver 풀 초기화 실패. 사용 가능한 드라이버가 없습니다. (MAX_WORKERS: {config.MAX_WORKERS})")
             logger.critical("ChromeDriver 또는 Chrome 브라우저 경로/버전 문제를 확인하세요.")
             # GitHub Actions 환경에서는 여기서 종료해도 다음 단계에서 에러로 잡힐 것임
             # 로컬에서는 sys.exit(1) 고려
             # return # 또는 sys.exit(1)
        progress_mgr = ProgressManager(config)
        scraper = BankScraper(config, driver_mgr, progress_mgr)
        asyncio.run(scraper.run()) 
        logger.info("모든 스크래핑 작업 정상적으로 완료 루틴 진입.")
    except WebDriverException as wd_exc: # WebDriver 관련 예외를 최상위에서 잡아서 좀 더 명확한 메시지
         if logger: logger.critical(f"최상위 WebDriver 관련 오류 발생: {wd_exc}", exc_info=True)
         else: print(f"최상위 WebDriver 관련 오류 (로거 미설정): {wd_exc}\n{traceback.format_exc()}")
         # GitHub Actions에서는 이 오류로 인해 workflow가 실패로 표시될 것임
         sys.exit(2) # WebDriver 문제에 대한 특정 종료 코드
    except Exception as e:
        if logger: logger.critical(f"스크립트 실행 중 최상위 오류 발생: {e}", exc_info=True)
        else: print(f"최상위 오류 (로거 미설정): {e}\n{traceback.format_exc()}")
        sys.exit(1) 
    finally:
        if driver_mgr: driver_mgr.quit_all() 
        if logger: logger.info("스크립트 실행 종료 루틴.")
        else: print("스크립트 실행 종료 루틴 (로거 미설정).")

if __name__ == "__main__":
    # GitHub Actions 환경변수 확인 (선택적 로깅)
    if os.getenv('GITHUB_ACTIONS') == 'true':
        if logger: logger.info("GitHub Actions 환경에서 실행 중입니다.")
        else: print("GitHub Actions 환경에서 실행 중입니다. (로거 미설정)")
    main()
