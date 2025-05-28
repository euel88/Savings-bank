# bank_scraper_headless.py
"""
저축은행 중앙회 통일경영공시 데이터 자동 스크래핑 도구 (GitHub Actions 최적화 버전)
목적: GitHub Actions에서 자동 실행, 병렬 처리를 통한 속도 개선
작성일: 2025-03-31 (최종 수정일: 2025-05-28)
특징:
- (이전 기능 모두 포함)
- 이메일 첨부파일 MIME 타입 명시 및 제목 형식 변경
- 공시 날짜 확인 로직 고도화 (예정된 미래 공시 조기 발견 시 일치로 처리)
- 카테고리별 시트 중복 문제 해결
- 압축 파일명 형식 변경
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
from typing import Union, Dict, Tuple # Dict, Tuple 추가

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

        # 1. 기한이 지난 최신 공시
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

        # 2. 다음 업로드 예정/진행 공시
        if current_processing_date <= lw_may_curr: # 5월 말까지는 3월말 자료가 다음 또는 현재 업로드 대상
            res["next_imminent_period"] = f"{year}년3월말"
            res["next_imminent_reason"] = f"{base_reason}: {year}년 5월 마지막 평일({lw_may_curr})까지 {res['next_imminent_period']} 자료 업로드 기간/임박."
        elif current_processing_date <= lw_aug_curr: # 8월 말까지는 6월말 자료
            res["next_imminent_period"] = f"{year}년6월말"
            res["next_imminent_reason"] = f"{base_reason}: {year}년 8월 마지막 평일({lw_aug_curr})까지 {res['next_imminent_period']} 자료 업로드 기간/임박."
        elif current_processing_date <= lw_nov_curr: # 11월 말까지는 9월말 자료
            res["next_imminent_period"] = f"{year}년9월말"
            res["next_imminent_reason"] = f"{base_reason}: {year}년 11월 마지막 평일({lw_nov_curr})까지 {res['next_imminent_period']} 자료 업로드 기간/임박."
        else: # 11월 말 이후 (12월) -> 내년 3월말 자료가 다음 대상
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
                ctype, encoding = None, None
                if p_attach.suffix.lower() == '.zip':
                    part = MIMEBase('application', 'zip')
                elif p_attach.suffix.lower() == '.xlsx':
                    part = MIMEBase('application', 'vnd.openxmlformats-officedocument.spreadsheetml.sheet')
                else:
                    part = MIMEBase('application', 'octet-stream')
                
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
        self.VERSION = "2.10.1-sheet-fix" 
        self.BASE_URL = "https://www.fsb.or.kr/busmagequar_0100.act"
        self.MAX_RETRIES = int(os.getenv('MAX_RETRIES', '2'))
        self.PAGE_LOAD_TIMEOUT = int(os.getenv('PAGE_LOAD_TIMEOUT', '25'))
        self.WAIT_TIMEOUT = int(os.getenv('WAIT_TIMEOUT', '15'))
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
            "다올", "대신", "더케이", "민국", "바로", "스카이", "신한", "애큐온", "예가람", "웰컴", "유안타", "조은", "키움YES", "푸른", "하나", "DB", "HB", "JT", "친애", "KB", "NH", "OK", "OSB", "SBI", "금화", "남양", "모아", "부림", "삼정", "상상인", "세람", "안국", "안양", "영진", "융창", "인성", "인천", "키움", "페퍼", "평택", "한국투자", "한화", "고려", "국제", "동원제일", "솔브레인", "에스앤티", "우리", "조흥", "진주", "흥국", "BNK", "DH", "IBK", "대백", "대아", "대원", "드림", "라온", "머스트", "삼일", "엠에스", "오성", "유니온", "참", "CK", "대한", "더블", "동양", "삼호", "센트럴", "스마트", "스타", "대명", "상상인플러스", "아산", "오투", "우리금융", "청주", "한성"
        ]
        self.CATEGORIES = ["영업개황", "재무현황", "손익현황", "기타"]
        
        logger.info(f"--- 설정 초기화 (v{self.VERSION}) ---")
        logger.info(f"처리일자(KST 가정): {self.processing_date_kst}")
        logger.info(f"예상 (기한 지난) 최신 공시일: '{self.latest_due_period}' (근거: {self.date_expectations['latest_due_reason']})")
        logger.info(f"예상 (다음/현재) 공시일: '{self.next_imminent_period}' (근거: {self.date_expectations['next_imminent_reason']})")
        logger.info(f"출력 기본 디렉토리: {self.output_dir_base.resolve()}")

# --- DriverManager, ProgressManager ---
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
                    if (elementText.length > 3000) elementText = elementText.substring(0, 3000);
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
            allMatches.sort((a,b) => (b.year !== a.year) ? (b.year - a.year) : (b.month - a.month));
            var sysYear = new Date().getFullYear();
            var latestFYear = allMatches[0].year; 
            var reasonableDates = allMatches.filter(m => !(m.year < latestFYear - 5 && m.year < sysYear - 3) && m.year >= sysYear - 10);
            if (reasonableDates.length > 0) return reasonableDates[0].fullText.replace(/\s+/g, '');
            if (allMatches.length > 0) return allMatches[0].fullText.replace(/\s+/g, '');
            return '날짜 정보 없음';
            """
            date_info = driver.execute_script(js_script)
            logger.debug(f"추출된 최종 날짜 정보: '{date_info}'")
            return date_info
        except Exception as e:
            logger.error(f"날짜 정보 추출 중 오류 (개선된 로직): {e}", exc_info=True)
            return "날짜 추출 실패"

    def select_bank(self, driver, bank_name):
        logger.debug(f"[{bank_name}] 은행 선택..."); driver.get(self.config.BASE_URL)
        WebDriverWait(driver,self.config.PAGE_LOAD_TIMEOUT).until(lambda d:d.execute_script('return document.readyState')=='complete'); time.sleep(random.uniform(0.4,0.8))
        for xp in [f"//td[normalize-space(.)='{bank_name}']", f"//a[normalize-space(.)='{bank_name}']"]:
            try:
                for el in driver.find_elements(By.XPATH,xp):
                    if el.is_displayed() and self._robust_click(driver,el): time.sleep(random.uniform(0.5,1.0)); return True
            except: pass
        js = f"var els=Array.from(document.querySelectorAll('a,td'));var t=els.find(e=>e.textContent&&e.textContent.trim().includes('{bank_name}'));if(t){{t.scrollIntoView({{block:'center'}});(t.tagName==='TD'&&t.querySelector('a')?t.querySelector('a'):t).click();return true}}return false"
        try:
            if driver.execute_script(js): time.sleep(random.uniform(0.5,1.0)); return True
        except: pass
        for xp in [f"//td[contains(normalize-space(.),'{bank_name}')]",f"//a[contains(normalize-space(.),'{bank_name}')]"]:
            try:
                els=driver.find_elements(By.XPATH,xp); els.sort(key=lambda x:len(x.text) if x.text else float('inf'))
                for el in els:
                    if el.is_displayed() and self._robust_click(driver,el): time.sleep(random.uniform(0.5,1.0)); return True
            except: pass
        logger.warning(f"[{bank_name}] 은행 선택 최종 실패."); return False

    def select_category(self, driver, category_name):
        logger.debug(f"카테고리 선택: '{category_name}'"); time.sleep(random.uniform(0.2,0.5))
        cat_norm=category_name.replace(' ','')
        sels=[(By.XPATH,f"//a[normalize-space(translate(text(),' \t\n\r',''))='{cat_norm}']"),(By.XPATH,f"//button[normalize-space(translate(text(),' \t\n\r',''))='{cat_norm}']"),(By.LINK_TEXT,category_name),(By.PARTIAL_LINK_TEXT,category_name)]
        for by_type,val in sels:
            try:
                for el in driver.find_elements(by_type,val):
                    if el.is_displayed() and el.is_enabled() and self._robust_click(driver,el): time.sleep(random.uniform(0.4,0.8)); return True
            except: pass
        js=f"var els=Array.from(document.querySelectorAll('a,li,button,span,div[role=\"tab\"]'));var t=els.find(e=>e.textContent&&e.textContent.trim().includes('{category_name}'));if(t){{t.scrollIntoView({{block:'center'}});t.click();return true}}return false"
        try:
            if driver.execute_script(js): time.sleep(random.uniform(0.4,0.8)); return True
        except: pass
        logger.warning(f"'{category_name}' 카테고리 선택 최종 실패."); return False
        
    def extract_tables_from_page(self, driver):
        WebDriverWait(driver,self.config.PAGE_LOAD_TIMEOUT).until(lambda d:d.execute_script('return document.readyState')=='complete'); time.sleep(random.uniform(0.3,0.6))
        try:
            src=driver.page_source;
            if not src or len(src)<300 or "<table" not in src.lower(): return []
            dfs=pd.read_html(StringIO(src),flavor='bs4',encoding='utf-8'); valid=[]
            for df in dfs:
                if not isinstance(df,pd.DataFrame) or df.empty: continue
                df.dropna(axis=0,how='all',inplace=True); df.dropna(axis=1,how='all',inplace=True)
                if df.empty: continue
                df.columns=['_'.join(map(str,c)).strip('_ ') if isinstance(df.columns,pd.MultiIndex) else str(c).strip() for c in df.columns]
                valid.append(df.reset_index(drop=True))
            return valid
        except ValueError: return []
        except Exception as e: logger.error(f"테이블 추출 오류: {e}", exc_info=True); return []

    def _scrape_single_bank_attempt(self, bank_name, driver):
        logger.info(f"[{bank_name}] 스크래핑 시도...")
        if not self.select_bank(driver, bank_name): return None

        date_info_scraped = self.extract_date_information(driver)
        logger.info(f"[{bank_name}] 추출 공시일(원본): '{date_info_scraped}'")
        
        normalized_scraped_date = normalize_datestr_for_comparison(date_info_scraped)
        # Config에서 두 가지 예상 날짜 가져오기
        expected_officially_due = self.config.latest_due_period
        expected_next_imminent = self.config.next_imminent_period

        logger.info(f"[{bank_name}] 정규화 공시일: '{normalized_scraped_date}', 공식적 최신 예상: '{expected_officially_due}', 다음 업로드 예상: '{expected_next_imminent}'")

        if normalized_scraped_date is None:
            logger.error(f"[{bank_name}] 날짜 추출 실패. 비교 불가.")
        elif normalized_scraped_date == "알 수 없는 형식":
             logger.warning(f"[{bank_name}] 날짜 형식 알 수 없음: '{date_info_scraped}'. 비교 불가.")
        elif normalized_scraped_date == expected_officially_due:
            logger.info(f"[{bank_name}] 공시일('{normalized_scraped_date}')이 공식적으로 최신이어야 할 기간과 일치.")
        elif normalized_scraped_date == expected_next_imminent:
            logger.info(f"[{bank_name}] 공시일('{normalized_scraped_date}')이 다음 업로드 예정/진행 기간과 일치 (선제적 업데이트).")
        else:
            logger.critical(f"[{bank_name}] !!날짜 불일치!! 웹사이트(정규화): '{normalized_scraped_date}', 공식적 최신 예상: '{expected_officially_due}', 다음 업로드 예상: '{expected_next_imminent}'. (사이트 원본: '{date_info_scraped}')")
        
        data = {'_INFO_': pd.DataFrame([{'은행명':bank_name, '공시날짜':date_info_scraped, '추출일시':datetime.now().strftime("%Y-%m-%d %H:%M:%S"), '스크래퍼버전':self.config.VERSION}])}
        has_data = False; orig_url = driver.current_url

        for cat_name in self.config.CATEGORIES:
            cat_selected=False
            for attempt in range(2):
                if attempt > 0: driver.get(orig_url); WebDriverWait(driver,self.config.PAGE_LOAD_TIMEOUT).until(lambda d:d.execute_script('return document.readyState')=='complete'); time.sleep(0.5)
                if self.select_category(driver,cat_name): cat_selected=True; break
            if not cat_selected: logger.error(f"[{bank_name}] '{cat_name}' 최종 선택 실패."); continue
            tables = self.extract_tables_from_page(driver)
            if tables:
                # 수정된 부분: 각 카테고리별로 하나의 시트만 생성
                sheet_name = re.sub(r'[\\/*?:\[\]]','',cat_name)[:31]
                if len(tables) == 1:
                    data[sheet_name] = tables[0]
                else:
                    # 여러 테이블이 있을 경우 세로로 병합
                    logger.info(f"[{bank_name}] {cat_name}에서 {len(tables)}개 테이블 발견. 병합 중...")
                    # 빈 행 추가하여 테이블 구분
                    merged_tables = []
                    for i, table in enumerate(tables):
                        if i > 0:
                            # 테이블 사이에 빈 행 추가
                            empty_row = pd.DataFrame([[''] * len(table.columns)], columns=table.columns)
                            merged_tables.append(empty_row)
                        merged_tables.append(table)
                    merged_df = pd.concat(merged_tables, axis=0, ignore_index=True)
                    data[sheet_name] = merged_df
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
            logger.info(f"[{bank_name}] 저장 완료: {excel_path.name} (경로: {excel_path})")
            return True
        except Exception as e: logger.error(f"[{bank_name}] 저장 실패 ({excel_path.name}): {e}", exc_info=True); return False

    async def worker_process_bank(self, bank_name, pbar, semaphore):
        async with semaphore:
            driver, success, date_info = None, False, (d.get('date_info') if (d:=self.progress_manager.get_bank_data(bank_name)) else None)
            try:
                driver = await asyncio.get_event_loop().run_in_executor(None, self.driver_manager.get_driver)
                if not driver: self.progress_manager.mark_failed(bank_name); return bank_name, False, date_info
                data = None
                for attempt in range(self.config.MAX_RETRIES):
                    try:
                        data = self._scrape_single_bank_attempt(bank_name, driver)
                        if data: date_info = data['_INFO_']['공시날짜'].iloc[0]; break
                    except Exception as e: 
                        logger.warning(f"[{bank_name}] 시도 {attempt+1} 중 예외: {type(e).__name__} - {e}")
                        if attempt < self.config.MAX_RETRIES-1:
                            await asyncio.get_event_loop().run_in_executor(None, self.driver_manager.return_driver, driver); driver = None
                            driver = await asyncio.get_event_loop().run_in_executor(None, self.driver_manager.get_driver)
                            if not driver: logger.error(f"[{bank_name}] 재시도용 드라이버 획득 실패."); break
                        else: data = None; logger.error(f"[{bank_name}] 모든 재시도 실패.")
                if data and self.save_bank_data(bank_name, data):
                    self.progress_manager.mark_completed(bank_name, date_info); success = True
                else: self.progress_manager.mark_failed(bank_name)
                return bank_name, success, date_info
            except TimeoutError as te: 
                logger.error(f"[{bank_name}] 드라이버 획득 타임아웃: {te}"); self.progress_manager.mark_failed(bank_name); return bank_name, False, date_info
            except Exception as e: 
                logger.error(f"[{bank_name}] 작업자 예외: {type(e).__name__} - {e}", exc_info=True); self.progress_manager.mark_failed(bank_name); return bank_name, False, date_info
            finally:
                if driver: await asyncio.get_event_loop().run_in_executor(None, self.driver_manager.return_driver, driver)
                if pbar: pbar.update(1)
                logger.info(f"[{bank_name}] 처리: {'성공' if success else '실패'}, 공시일(원본): {date_info or '미확정'}")
    
    async def run(self):
        logger.info(f"==== 스크래핑 시작 (v{self.config.VERSION}) ====")
        start_time = time.monotonic()
        pending_banks = self.progress_manager.get_pending_banks()
        if not pending_banks: logger.info("처리할 은행 없음."); self.generate_summary_and_send_email(); return
        
        logger.info(f"총 {len(pending_banks)}개 은행 처리 예정: {pending_banks[:3]}{'...' if len(pending_banks)>3 else ''}")
        semaphore = asyncio.Semaphore(self.config.MAX_WORKERS)
        
        with tqdm(total=len(pending_banks), desc="은행 스크래핑", unit="은행", dynamic_ncols=True, smoothing=0.1) as pbar:
            tasks = [self.worker_process_bank(bank_name, pbar, semaphore) for bank_name in pending_banks]
            results = await asyncio.gather(*tasks, return_exceptions=True)
        
        processed_count = sum(1 for r in results if not isinstance(r, Exception))
        logger.info(f"asyncio.gather로 {processed_count}/{len(pending_banks)}개 작업 반환 완료.")
        logger.info(f"==== 전체 스크래핑 완료. 소요시간: {time.monotonic() - start_time:.2f}초 ====")
        self.generate_summary_and_send_email()

    def generate_summary_and_send_email(self):
        logger.info("요약 보고서 및 이메일 생성 시작...")
        summary_data = []
        all_banks_in_config = self.config.BANKS
        processed_banks_data = self.progress_manager.progress.get('banks', {})
        
        # Config에서 예상 날짜들 가져오기
        expected_officially_due = self.config.latest_due_period
        expected_next_imminent = self.config.next_imminent_period
        
        completed_count, failed_count = 0,0; failed_banks_names = []

        for bank_name_iter in all_banks_in_config:
            bank_detail = processed_banks_data.get(bank_name_iter)
            status, original_disc_date, date_match_status = '미처리', '', ''
            if bank_detail:
                current_status = bank_detail.get('status')
                original_disc_date = bank_detail.get('date_info', '') 
                normalized_disc_date = normalize_datestr_for_comparison(original_disc_date)

                if current_status == 'completed':
                    status, completed_count = '완료', completed_count + 1
                    if normalized_disc_date is None: date_match_status = "⚠️ 추출실패"
                    elif normalized_disc_date == "알 수 없는 형식": date_match_status = f"❓ 형식모름 ({original_disc_date})"
                    elif normalized_disc_date == expected_officially_due: date_match_status = "✅ 일치 (기한내 최신)"
                    elif normalized_disc_date == expected_next_imminent: date_match_status = "🟢 일치 (예정분 선반영)"
                    else: date_match_status = f"❌ 불일치! (예상: {expected_officially_due} 또는 {expected_next_imminent})"
                elif current_status == 'failed':
                    status, failed_count = '실패', failed_count + 1
                    failed_banks_names.append(bank_name_iter); date_match_status = "Н/Д (실패)"
            summary_data.append({
                '은행명':bank_name_iter, '공시 날짜(원본)':original_disc_date, 
                '날짜 확인':date_match_status, '처리 상태':status, 
                '확인 시간':datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            })
        
        summary_df = pd.DataFrame(summary_data)
        summary_filename = f"스크래핑_요약_{self.config.today}.xlsx"
        summary_file_path = self.config.output_dir / summary_filename
        try: summary_df.to_excel(summary_file_path, index=False); logger.info(f"요약 보고서: {summary_file_path}")
        except Exception as e: logger.error(f"요약 보고서 저장 실패: {e}", exc_info=True)

        # 수정된 부분: 압축 파일명 형식 변경
        date_for_filename = self.config.processing_date_kst.strftime("%Y.%m.%d")
        zip_filename_str = f"저축은행 분기 공시자료_{date_for_filename}.zip"
        zip_file_path_obj = self.config.output_dir_base / zip_filename_str

        try:
            with zipfile.ZipFile(zip_file_path_obj, 'w', zipfile.ZIP_DEFLATED) as zf:
                for f_path in self.config.output_dir.rglob('*'):
                    if f_path.is_file(): zf.write(f_path, Path(self.config.output_dir.name) / f_path.relative_to(self.config.output_dir))
            logger.info(f"결과 압축 완료: {zip_file_path_obj}")
        except Exception as e: logger.error(f"결과 압축 실패: {e}", exc_info=True); zip_file_path_obj = None

        processed_attempt_count = completed_count + failed_count
        success_rate = (completed_count / processed_attempt_count * 100) if processed_attempt_count > 0 else 0
        
        # 이메일 제목 생성
        date_for_subject = self.config.processing_date_kst.strftime("%Y.%m.%d")
        # 제목에 사용할 분기 정보 (공식적으로 최신이어야 할 기간 기준)
        quarter_info_for_subject = get_quarter_string_from_period(expected_officially_due)
        email_subject = f"저축은행 분기 공시 취합_{quarter_info_for_subject} ({date_for_subject})"
        
        failed_banks_display_html = "".join(f"<li>{b}</li>" for b in failed_banks_names[:10]) + \
                                (f"<p>...외 {len(failed_banks_names)-10}개.</p>" if len(failed_banks_names)>10 else \
                                 ("없음" if not failed_banks_names else ""))
        
        body_html = f"""
        <html><head><style>
            body{{font-family:Arial,sans-serif;margin:20px}} h2{{color:#2c3e50}}
            .summary-box{{border:1px solid #ddd;padding:15px;margin-bottom:20px;background-color:#f9f9f9;border-radius:5px}}
            .summary-box p{{margin:5px 0}} .status-completed{{color:green}} .status-failed{{color:red}}
            table{{border-collapse:collapse;width:95%;margin-top:15px;font-size:0.85em}}
            th,td{{border:1px solid #ddd;padding:5px;text-align:left;word-break:break-all;}} th{{background-color:#f0f0f0;white-space:nowrap;}}
        </style></head><body>
        <h2>저축은행 스크래핑 결과 ({self.config.today})</h2>
        <p><strong>스크립트 실행일:</strong> {self.config.processing_date_kst.strftime('%Y-%m-%d')}</p>
        <p><strong>공식적 최신 예상일:</strong> {expected_officially_due} (근거: {self.config.date_expectations['latest_due_reason']})</p>
        <p><strong>다음 업로드 예상일:</strong> {expected_next_imminent} (근거: {self.config.date_expectations['next_imminent_reason']})</p>
        <div class="summary-box">
            <p>총 대상: {len(all_banks_in_config)}개</p> <p>처리 시도: {processed_attempt_count}개</p>
            <p><span class="status-completed">✅ 성공: {completed_count}개</span></p> <p><span class="status-failed">❌ 실패: {failed_count}개</span> (성공률: {success_rate:.1f}%)</p>
            <p>📂 데이터: {self.config.output_dir.name} (압축: {zip_filename_str if zip_file_path_obj else '생성실패'})</p>
        </div>
        <h3>실패 은행 (최대 10개):</h3><ul>{failed_banks_display_html}</ul>
        <p>세부 결과는 첨부파일 확인.</p> <h3>은행별 처리 현황:</h3>
        {summary_df.to_html(index=False,border=1,na_rep='').replace('<td>','<td style="word-break:normal;">') if not summary_df.empty else "<p>요약 데이터 없음.</p>"}
        <br><p><small>자동 발송 (v{self.config.VERSION})</small></p>
        </body></html>"""
        
        attachment_to_send = None
        if zip_file_path_obj and zip_file_path_obj.exists():
            attachment_to_send = str(zip_file_path_obj)
        elif summary_file_path and summary_file_path.exists():
            attachment_to_send = str(summary_file_path)
        
        if attachment_to_send and Path(attachment_to_send).name == summary_filename and zip_file_path_obj is None : 
            logger.warning("압축 파일 생성 실패 또는 누락. 요약 보고서만 첨부합니다.")
        elif not attachment_to_send: 
             logger.warning("압축 파일 및 요약 보고서 모두 누락. 첨부 파일 없이 발송.")
        
        self.email_sender.send_email_with_attachment(email_subject, body_html, attachment_to_send)

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
        else: print(f"최상위 오류 (로거 미설정): {e}\n{traceback.format_exc()}")
        sys.exit(1) 
    finally:
        if driver_mgr: driver_mgr.quit_all() 
        if logger: logger.info("스크립트 실행 종료.")
        else: print("스크립트 실행 종료 (로거 미설정).")

if __name__ == "__main__":
    main()
