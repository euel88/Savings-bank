# bank_scraper_headless_improved.py
"""
저축은행 중앙회 통일경영공시 데이터 자동 스크래핑 도구 (개선 버전)
목적: GitHub Actions에서 자동 실행, 카테고리별 데이터 중복 방지, 성능 최적화
작성일: 2025-03-31 (최종 수정일: 2025-05-28)
개선사항:
- 카테고리별 데이터 중복 누적 문제 해결
- 페이지 상태 초기화 로직 추가
- 날짜 추출 로직 강화
- 성능 최적화 (타임아웃 조정, 불필요한 대기 제거)
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
        self.VERSION = "2.11.0-improved" 
        self.BASE_URL = "https://www.fsb.or.kr/busmagequar_0100.act"
        self.MAX_RETRIES = int(os.getenv('MAX_RETRIES', '2')) 
        # 구글 코랩 수준의 성능 설정 유지
        self.PAGE_LOAD_TIMEOUT = int(os.getenv('PAGE_LOAD_TIMEOUT', '8'))
        self.WAIT_TIMEOUT = int(os.getenv('WAIT_TIMEOUT', '4'))
        self.MAX_WORKERS = int(os.getenv('MAX_WORKERS', '5'))

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
        logger.info(f"처리일자(KST 가정): {self.processing_date_kst}")
        logger.info(f"예상 (기한 지난) 최신 공시일: '{self.latest_due_period}' (근거: {self.date_expectations['latest_due_reason']})")
        logger.info(f"예상 (다음/현재) 공시일: '{self.next_imminent_period}' (근거: {self.date_expectations['next_imminent_reason']})")

# --- DriverManager ---
class DriverManager:
    def __init__(self, config):
        self.config = config; self.driver_pool = queue.Queue(maxsize=self.config.MAX_WORKERS); self._initialize_pool()
    
    def _create_new_driver(self):
        logger.debug("새 WebDriver 생성..."); options = Options()
        options.add_argument('--headless'); options.add_argument('--no-sandbox'); options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu'); options.add_argument('--window-size=1920,1080')
        options.add_argument('user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
        options.add_argument('--disable-extensions'); options.add_argument('--disable-browser-side-navigation')
        options.add_argument('--disable-infobars'); options.add_argument('--disable-notifications')
        options.add_argument('--disable-popup-blocking'); options.add_argument('--blink-settings=imagesEnabled=false')
        options.add_experimental_option('excludeSwitches', ['enable-logging', 'enable-automation']); options.add_experimental_option('useAutomationExtension', False)
        try:
            driver = webdriver.Chrome(options=options); driver.set_page_load_timeout(self.config.PAGE_LOAD_TIMEOUT)
            logger.debug("새 WebDriver 생성 완료."); return driver
        except WebDriverException as e:
            logger.error(f"WebDriver 생성 실패: {e}", exc_info=True)
            if any(s in str(e).lower() for s in ["executable needs to be in path", "unable to find driver", "cannot find chrome binary"]):
                logger.error(f"WebDriverException: ChromeDriver/Chrome 경로 문제 추정. PATH: {os.getenv('PATH')}")
            raise
        except Exception as e: logger.error(f"WebDriver 생성 중 예상치 못한 오류: {e}", exc_info=True); raise
    
    def _initialize_pool(self):
        logger.info(f"드라이버 풀 초기화 시작 (최대 {self.config.MAX_WORKERS}개)...")
        for i in range(self.config.MAX_WORKERS):
            try: self.driver_pool.put_nowait(self._create_new_driver())
            except queue.Full: logger.warning(f"드라이버 {i+1} 추가 중 풀 꽉 참."); break 
            except Exception as e: logger.error(f"초기 드라이버 {i+1} 생성 실패 ({type(e).__name__}).")
        logger.info(f"드라이버 풀 초기화 완료. 사용 가능: {self.driver_pool.qsize()}개.")
    
    def get_driver(self):
        try: return self.driver_pool.get(block=True, timeout=30)
        except queue.Empty: raise TimeoutError(f"30초 대기 후에도 풀에서 드라이버를 가져오지 못함 (MAX_WORKERS: {self.config.MAX_WORKERS}).")
    
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
            try: logger.info("손상 드라이버 대체용 새 드라이버 생성..."); self.driver_pool.put_nowait(self._create_new_driver())
            except Exception as e: logger.error(f"대체 드라이버 생성/추가 실패: {e}", exc_info=True)
    
    def quit_all(self): 
        logger.info("모든 드라이버 종료 시작..."); drained = 0
        while not self.driver_pool.empty():
            try: driver = self.driver_pool.get_nowait(); driver.quit(); drained += 1
            except: break 
        logger.info(f"총 {drained}개 드라이버 종료 시도 완료.")

class ProgressManager:
    def __init__(self, config):
        self.config=config; self.progress_file_path=config.progress_file; self.progress=self._load()
    
    def _load(self):
        default = {'banks': {}, 'stats': {'last_run':None,'success_count':0,'failure_count':0}}
        if self.progress_file_path.exists():
            try:
                with open(self.progress_file_path,'r',encoding='utf-8') as f:
                    loaded=json.load(f)
                    if 'banks' not in loaded: loaded['banks']={}
                    if 'stats' not in loaded: loaded['stats']=default['stats']
                    return loaded
            except Exception as e: logger.warning(f"진행 파일 로드 오류({self.progress_file_path}): {e}. 새로 시작.")
        return default
    
    def save(self):
        self.progress['stats']['last_run']=datetime.now().isoformat(); s,f=0,0
        for info in self.progress.get('banks',{}).values():
            if info.get('status')=='completed': s+=1
            elif info.get('status')=='failed': f+=1
        self.progress['stats']['success_count']=s; self.progress['stats']['failure_count']=f
        try:
            with open(self.progress_file_path,'w',encoding='utf-8') as fo: json.dump(self.progress,fo,ensure_ascii=False,indent=2)
        except Exception as e: logger.error(f"진행 파일 저장 실패: {e}",exc_info=True)
    
    def mark_completed(self,bank_name,date_info):
        self.progress.setdefault('banks',{})[bank_name]={'status':'completed','date_info':date_info}; self.save()
    
    def mark_failed(self,bank_name):
        data=self.progress.setdefault('banks',{}).get(bank_name,{}); data['status']='failed'
        self.progress['banks'][bank_name]=data; self.save()
    
    def get_pending_banks(self):
        processed=self.progress.get('banks',{}); pending=[b for b in self.config.BANKS if b not in processed or processed[b].get('status')!='completed']
        logger.info(f"보류 은행(재시도 포함 가능): {len(pending)}개"); return pending
    
    def get_bank_data(self,bank_name): return self.progress.get('banks',{}).get(bank_name)

# --- 스크래퍼 클래스 (개선됨) ---
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
        """강화된 날짜 정보 추출 로직"""
        logger.debug(f"날짜 정보 추출 시도 (v{self.config.VERSION})...")
        try:
            # 다중 방법으로 날짜 추출 시도
            js_script = """
            var allMatches = []; 
            var datePattern = /(\d{4})년\s*(\d{1,2})월말/g;
            var bodyText = document.body.innerText || ""; 
            var match; datePattern.lastIndex = 0;
            
            // 전체 텍스트에서 날짜 패턴 추출
            while ((match = datePattern.exec(bodyText)) !== null) { 
                allMatches.push({
                    fullText: match[0], 
                    year: parseInt(match[1]), 
                    month: parseInt(match[2]),
                    source: 'body'
                });
            }
            
            // 특정 태그에서 날짜 패턴 검색 (우선순위가 높은 태그들)
            var priorityTags = [
                'h1', 'h2', 'h3', 'caption', 'th[colspan]', 'td[colspan]',
                '.title', '.date', '#publishDate', '.disclosure-date',
                'span[class*="date"]', 'div[class*="date"]'
            ];
            
            for (var i = 0; i < priorityTags.length; i++) {
                var elements;
                try { elements = document.querySelectorAll(priorityTags[i]); }
                catch(e) { elements = []; }
                
                for (var j = 0; j < elements.length; j++) {
                    var elText = elements[j].innerText || "";
                    if (elText.length > 1000) elText = elText.substring(0, 1000);
                    datePattern.lastIndex = 0;
                    
                    while ((match = datePattern.exec(elText)) !== null) {
                        var cY = parseInt(match[1]), cM = parseInt(match[2]);
                        if (!allMatches.some(m => m.year === cY && m.month === cM)) {
                            allMatches.push({
                                fullText: match[0],
                                year: cY,
                                month: cM,
                                source: priorityTags[i]
                            });
                        }
                    }
                }
            }
            
            if (allMatches.length === 0) return '날짜 정보 없음';
            
            // 날짜 필터링 및 정렬 로직 개선
            var currentYear = new Date().getFullYear();
            var validDates = allMatches.filter(function(m) {
                // 합리적인 날짜 범위 (현재년도 기준 -10년 ~ +1년)
                return m.year >= (currentYear - 10) && m.year <= (currentYear + 1) && 
                       m.month >= 1 && m.month <= 12;
            });
            
            if (validDates.length > 0) {
                // 우선순위별 정렬: 최신년도 > 분기별 월(12,9,6,3) > 기타
                validDates.sort(function(a, b) {
                    if (a.year !== b.year) return b.year - a.year; // 최신 년도 우선
                    
                    var quarterMonths = [12, 9, 6, 3];
                    var aIsQuarter = quarterMonths.indexOf(a.month) !== -1;
                    var bIsQuarter = quarterMonths.indexOf(b.month) !== -1;
                    
                    if (aIsQuarter && !bIsQuarter) return -1;
                    if (!aIsQuarter && bIsQuarter) return 1;
                    if (aIsQuarter && bIsQuarter) {
                        return quarterMonths.indexOf(a.month) - quarterMonths.indexOf(b.month);
                    }
                    
                    return b.month - a.month; // 최신 월 우선
                });
                
                return validDates[0].fullText.replace(/\s+/g, '');
            }
            
            // 모든 매치에서 최신 선택
            if (allMatches.length > 0) {
                allMatches.sort((a, b) => (b.year !== a.year) ? (b.year - a.year) : (b.month - a.month));
                return allMatches[0].fullText.replace(/\s+/g, '');
            }
            
            return '날짜 정보 없음';
            """
            date_info = driver.execute_script(js_script)
            logger.debug(f"추출된 최종 날짜 정보: '{date_info}'")
            return date_info
        except Exception as e: 
            logger.error(f"날짜 정보 추출 중 오류: {e}", exc_info=True)
            return "날짜 추출 실패"

    def select_bank(self, driver, bank_name):
        logger.debug(f"[{bank_name}] 은행 선택...")
        driver.get(self.config.BASE_URL)
        WebDriverWait(driver, self.config.PAGE_LOAD_TIMEOUT).until(lambda d: d.execute_script('return document.readyState') == 'complete')
        time.sleep(random.uniform(0.3, 0.5))
        
        for xp in [f"//td[normalize-space(.)='{bank_name}']", f"//a[normalize-space(.)='{bank_name}']"]:
            try:
                for el in driver.find_elements(By.XPATH, xp):
                    if el.is_displayed() and self._robust_click(driver, el): 
                        time.sleep(random.uniform(0.3, 0.6))
                        return True
            except: pass
        
        js = f"""
        var els = Array.from(document.querySelectorAll('a,td'));
        var t = els.find(e => e.textContent && e.textContent.trim().includes('{bank_name}'));
        if (t) {{
            t.scrollIntoView({{block:'center'}});
            (t.tagName === 'TD' && t.querySelector('a') ? t.querySelector('a') : t).click();
            return true;
        }}
        return false;
        """
        try:
            if driver.execute_script(js): 
                time.sleep(random.uniform(0.3, 0.6))
                return True
        except: pass
        
        logger.warning(f"[{bank_name}] 은행 선택 최종 실패.")
        return False

    def select_category(self, driver, category_name):
        """성능 최적화된 카테고리 선택 로직"""
        logger.debug(f"카테고리 선택: '{category_name}'")
        
        # 코랩 수준의 빠른 처리를 위한 최소 대기
        time.sleep(random.uniform(0.1, 0.2))
        
        cat_norm = category_name.replace(' ', '')
        
        # 방법 1: 정확한 텍스트 매칭 (빠른 처리)
        selectors = [
            (By.XPATH, f"//a[normalize-space(translate(text(),' \t\n\r',''))='{cat_norm}']"),
            (By.LINK_TEXT, category_name),
            (By.PARTIAL_LINK_TEXT, category_name)
        ]
        
        for by_type, val in selectors:
            try:
                for el in driver.find_elements(by_type, val):
                    if el.is_displayed() and el.is_enabled() and self._robust_click(driver, el): 
                        time.sleep(random.uniform(0.2, 0.3))
                        return True
            except: pass
        
        # 방법 2: JavaScript 기반 선택 (단일 시도)
        js = f"""
        var els = Array.from(document.querySelectorAll('a,li,button,span,div[role="tab"]'));
        var t = els.find(e => e.textContent && e.textContent.trim().includes('{category_name}'));
        if (t) {{
            t.scrollIntoView({{block:'center'}});
            t.click();
            return true;
        }}
        return false;
        """
        try:
            if driver.execute_script(js): 
                time.sleep(random.uniform(0.2, 0.3))
                return True
        except: pass
        
        logger.warning(f"'{category_name}' 카테고리 선택 실패.")
        return False
        
    def extract_tables_from_page(self, driver):
        """개선된 테이블 추출 - 중복 방지 강화"""
        WebDriverWait(driver, self.config.PAGE_LOAD_TIMEOUT).until(lambda d: d.execute_script('return document.readyState') == 'complete')
        time.sleep(random.uniform(0.2, 0.4))
        
        try:
            src = driver.page_source
            if not src or len(src) < 300 or "<table" not in src.lower(): 
                return []
            
            dfs = pd.read_html(StringIO(src), flavor='bs4', encoding='utf-8')
            valid = []
            seen_hashes = set()
            
            for df in dfs:
                if not isinstance(df, pd.DataFrame) or df.empty: 
                    continue
                
                # 빈 행/열 제거
                df.dropna(axis=0, how='all', inplace=True)
                df.dropna(axis=1, how='all', inplace=True)
                
                if df.empty: 
                    continue
                
                # MultiIndex 컬럼 처리
                if isinstance(df.columns, pd.MultiIndex):
                    new_cols = []
                    for col in df.columns:
                        if isinstance(col, tuple):
                            clean_col = [str(c).strip() for c in col if str(c).strip() and str(c).lower() != 'nan']
                            new_cols.append('_'.join(clean_col) if clean_col else f"Column_{len(new_cols)+1}")
                        else:
                            new_cols.append(str(col))
                    df.columns = new_cols
                else:
                    df.columns = ['_'.join(map(str, c)).strip('_ ') if isinstance(c, tuple) else str(c).strip() for c in df.columns]
                
                # 중복 체크를 위한 해시 생성 (더 정확한 중복 검사)
                try:
                    # 테이블 구조와 데이터를 종합한 해시
                    shape_str = f"{df.shape[0]}x{df.shape[1]}"
                    cols_str = "|".join(str(col) for col in df.columns)
                    
                    if len(df) > 0:
                        # 첫 번째와 마지막 행의 데이터를 해시에 포함
                        first_row = "|".join(str(val) for val in df.iloc[0].values)
                        last_row = "|".join(str(val) for val in df.iloc[-1].values) if len(df) > 1 else first_row
                        data_str = f"{first_row}||{last_row}"
                    else:
                        data_str = "empty"
                    
                    table_hash = f"{shape_str}#{cols_str}#{data_str}"
                    
                    if table_hash not in seen_hashes:
                        valid.append(df.reset_index(drop=True))
                        seen_hashes.add(table_hash)
                    
                except Exception as hash_error:
                    logger.debug(f"테이블 해시 생성 실패, 그대로 추가: {hash_error}")
                    valid.append(df.reset_index(drop=True))
            
            return valid
            
        except ValueError as ve:
            logger.debug(f"pandas 테이블 추출 실패 (ValueError): {ve}")
            return []
        except Exception as e: 
            logger.error(f"테이블 추출 오류: {e}", exc_info=True)
            return []

    def _scrape_single_bank_attempt(self, bank_name, driver):
        """성능 최적화된 스크래핑 로직 - 카테고리 중복 방지"""
        logger.info(f"[{bank_name}] 스크래핑 시도...")
        
        if not self.select_bank(driver, bank_name): 
            return None
        
        # 은행 페이지 최소 안정화
        time.sleep(0.3)
        
        date_info_scraped = self.extract_date_information(driver)
        logger.info(f"[{bank_name}] 추출 공시일(원본): '{date_info_scraped}'")
        
        normalized_scraped_date = normalize_datestr_for_comparison(date_info_scraped)
        expected_officially_due = self.config.latest_due_period
        expected_next_imminent = self.config.next_imminent_period
        
        # 날짜 검증 로직은 유지하되 로깅 간소화
        if normalized_scraped_date == expected_officially_due:
            logger.info(f"[{bank_name}] 날짜 일치: 공식적 최신")
        elif normalized_scraped_date == expected_next_imminent:
            logger.info(f"[{bank_name}] 날짜 일치: 선제적 업데이트")
        elif normalized_scraped_date not in [None, "알 수 없는 형식"]:
            logger.warning(f"[{bank_name}] 날짜 불일치: {normalized_scraped_date}")
        
        data = {
            '_INFO_': pd.DataFrame([{
                '은행명': bank_name, 
                '공시날짜': date_info_scraped, 
                '추출일시': datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 
                '스크래퍼버전': self.config.VERSION
            }])
        }
        
        has_data = False
        base_url = driver.current_url
        
        # 카테고리별 데이터 수집 (각 카테고리당 하나의 시트)
        for cat_name in self.config.CATEGORIES:
            # 카테고리 변경을 위한 페이지 새로고침 (중복 방지용)
            if has_data:  # 첫 번째 카테고리가 아닌 경우만 새로고침
                driver.refresh()
                WebDriverWait(driver, self.config.PAGE_LOAD_TIMEOUT).until(
                    lambda d: d.execute_script('return document.readyState') == 'complete'
                )
                time.sleep(0.2)
            
            if self.select_category(driver, cat_name):
                tables = self.extract_tables_from_page(driver)
                
                if tables:
                    # 카테고리별로 모든 테이블을 하나의 시트에 통합
                    data[cat_name] = tables  # 테이블 리스트를 저장
                    has_data = True
                    logger.debug(f"[{bank_name}] '{cat_name}': {len(tables)}개 테이블")
                else:
                    logger.debug(f"[{bank_name}] '{cat_name}': 테이블 없음")
            else:
                logger.warning(f"[{bank_name}] '{cat_name}' 선택 실패")
        
    def classify_table_by_content(self, df, source_category=None):
        """테이블 내용을 분석하여 적절한 카테고리 분류 (안정화된 버전)"""
        try:
            if df is None or df.empty:
                return source_category
            
            # 테이블의 텍스트 내용을 안전하게 추출
            table_text = ""
            
            # 컬럼명 텍스트 추가
            try:
                if hasattr(df, 'columns') and df.columns is not None:
                    col_text = " ".join(str(col) for col in df.columns if col is not None)
                    table_text += col_text + " "
            except Exception as e:
                logger.debug(f"컬럼명 텍스트 추출 실패: {e}")
            
            # 데이터 내용 텍스트 추가 (처음 3행만 분석하여 성능 최적화)
            try:
                max_rows = min(3, len(df))
                for i in range(max_rows):
                    row_values = []
                    for val in df.iloc[i].values:
                        try:
                            if pd.notna(val) and val is not None:
                                row_values.append(str(val))
                        except:
                            continue
                    if row_values:
                        table_text += " ".join(row_values) + " "
            except Exception as e:
                logger.debug(f"데이터 내용 텍스트 추출 실패: {e}")
            
            if not table_text.strip():
                return source_category
            
            table_text = table_text.lower()
            
            # 카테고리별 키워드 정의 (간소화)
            category_keywords = {
                "영업개황": ["영업점", "직원", "점포", "지점", "임직원", "조직", "영업망", "본점", "점포수", "직원수"],
                "재무현황": ["자산", "부채", "자본", "자기자본", "총자산", "총부채", "대차대조표", "재무상태표", "현금", "예금", "대출"],
                "손익현황": ["수익", "비용", "손익", "이익", "손실", "매출", "영업이익", "순이익", "손익계산서", "이자수익", "이자비용"],
                "기타": ["기타", "부가정보", "주요사항", "특기사항", "공시사항", "참고사항", "비고", "주석"]
            }
            
            # 각 카테고리별 점수 계산
            category_scores = {}
            for category, keywords in category_keywords.items():
                score = 0
                for keyword in keywords:
                    try:
                        score += table_text.count(keyword)
                    except:
                        continue
                category_scores[category] = score
            
            # 최고 점수를 받은 카테고리 선택
            if category_scores:
                best_category = max(category_scores, key=category_scores.get)
                max_score = category_scores[best_category]
                
                # 점수가 1 이상이면 분류 결과 반영, 그렇지 않으면 원래 카테고리 유지
                if max_score >= 1:
                    return best_category
            
            return source_category
            
        except Exception as e:
            logger.warning(f"테이블 분류 중 오류 발생: {e}, 원래 카테고리 유지")
            return source_category

    def save_bank_data(self, bank_name, excel_data_dict):
        """카테고리별 단일 시트로 데이터 저장"""
        raw_date = excel_data_dict['_INFO_']['공시날짜'].iloc[0]
        match = re.search(r'(\d{4})년(\d{1,2})월', raw_date)
        date_fn = f"{match.group(1)}-{int(match.group(2)):02d}" if match else re.sub(r'[^\w\-_.]', '', raw_date or "날짜정보없음")
        excel_path = self.config.output_dir / f"{bank_name}_{date_fn}.xlsx"
        
        try:
            with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
                # 정보 시트 저장
                if '_INFO_' in excel_data_dict:
                    excel_data_dict['_INFO_'].to_excel(writer, sheet_name='정보', index=False)
                
                # 각 카테고리별 시트 생성 (테이블 통합)
                for category_name in self.config.CATEGORIES:
                    if category_name in excel_data_dict and excel_data_dict[category_name]:
                        tables = excel_data_dict[category_name]
                        
                        if len(tables) == 1:
                            # 테이블이 하나인 경우 그대로 저장
                            tables[0].to_excel(writer, sheet_name=category_name, index=False)
                        else:
                            # 여러 테이블을 하나의 시트에 통합
                            combined_df_list = []
                            
                            for i, table in enumerate(tables):
                                # 테이블 제목 행 추가 (구분용)
                                if i > 0:  # 첫 번째 테이블이 아닌 경우
                                    separator_row = pd.DataFrame([[''] * len(table.columns)], columns=table.columns)
                                    title_row = pd.DataFrame([[f'=== 테이블 {i+1} ==='] + [''] * (len(table.columns)-1)], columns=table.columns)
                                    combined_df_list.extend([separator_row, title_row])
                                
                                combined_df_list.append(table)
                            
                            # 모든 테이블을 세로로 연결
                            if combined_df_list:
                                combined_df = pd.concat(combined_df_list, ignore_index=True)
                                combined_df.to_excel(writer, sheet_name=category_name, index=False)
                
            logger.info(f"[{bank_name}] 저장 완료: {excel_path.name} (경로: {excel_path})")
            return True
        except Exception as e: 
            logger.error(f"[{bank_name}] 저장 실패 ({excel_path.name}): {e}", exc_info=True)
            return False

    async def worker_process_bank(self, bank_name, pbar, semaphore):
        async with semaphore:
            driver, success, date_info = None, False, (d.get('date_info') if (d:=self.progress_manager.get_bank_data(bank_name)) else None)
            try:
                driver = await asyncio.get_event_loop().run_in_executor(None, self.driver_manager.get_driver)
                if not driver: 
                    self.progress_manager.mark_failed(bank_name)
                    return bank_name, False, date_info
                
                data = None
                for attempt in range(self.config.MAX_RETRIES):
                    try:
                        data = self._scrape_single_bank_attempt(bank_name, driver)
                        if data: 
                            date_info = data['_INFO_']['공시날짜'].iloc[0]
                            break
                    except Exception as e: 
                        logger.warning(f"[{bank_name}] 시도 {attempt+1} 중 예외: {type(e).__name__} - {e}")
                        if attempt < self.config.MAX_RETRIES-1:
                            await asyncio.get_event_loop().run_in_executor(None, self.driver_manager.return_driver, driver)
                            driver = None
                            driver = await asyncio.get_event_loop().run_in_executor(None, self.driver_manager.get_driver)
                            if not driver: 
                                logger.error(f"[{bank_name}] 재시도용 드라이버 획득 실패.")
                                break
                        else: 
                            data = None
                            logger.error(f"[{bank_name}] 모든 재시도 실패.")
                
                if data and self.save_bank_data(bank_name, data):
                    self.progress_manager.mark_completed(bank_name, date_info)
                    success = True
                else: 
                    self.progress_manager.mark_failed(bank_name)
                
                return bank_name, success, date_info
                
            except TimeoutError as te: 
                logger.error(f"[{bank_name}] 드라이버 획득 타임아웃: {te}")
                self.progress_manager.mark_failed(bank_name)
                return bank_name, False, date_info
            except Exception as e: 
                logger.error(f"[{bank_name}] 작업자 예외: {type(e).__name__} - {e}", exc_info=True)
                self.progress_manager.mark_failed(bank_name)
                return bank_name, False, date_info
            finally:
                if driver: 
                    await asyncio.get_event_loop().run_in_executor(None, self.driver_manager.return_driver, driver)
                if pbar: 
                    pbar.update(1)
                logger.info(f"[{bank_name}] 처리: {'성공' if success else '실패'}, 공시일(원본): {date_info or '미확정'}")
    
    async def run(self):
        logger.info(f"==== 스크래핑 시작 (v{self.config.VERSION}) ====")
        start_time = time.monotonic()
        pending_banks = self.progress_manager.get_pending_banks()
        
        if not pending_banks: 
            logger.info("처리할 은행 없음.")
            self.generate_summary_and_send_email()
            return
        
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
        expected_officially_due = self.config.latest_due_period
        expected_next_imminent = self.config.next_imminent_period
        completed_count, failed_count = 0, 0
        failed_banks_names = []

        for bank_name_iter in all_banks_in_config:
            bank_detail = processed_banks_data.get(bank_name_iter)
            status, original_disc_date, date_match_status = '미처리', '', ''
            
            if bank_detail:
                current_status = bank_detail.get('status')
                original_disc_date = bank_detail.get('date_info', '') 
                normalized_disc_date = normalize_datestr_for_comparison(original_disc_date)
                
                if current_status == 'completed':
                    status, completed_count = '완료', completed_count + 1
                    if normalized_disc_date is None: 
                        date_match_status = "⚠️ 추출실패"
                    elif normalized_disc_date == "알 수 없는 형식": 
                        date_match_status = f"❓ 형식모름 ({original_disc_date})"
                    elif normalized_disc_date == expected_officially_due: 
                        date_match_status = "✅ 일치 (기한내 최신)"
                    elif normalized_disc_date == expected_next_imminent: 
                        date_match_status = "🟢 일치 (예정분 선반영)"
                    else: 
                        date_match_status = f"❌ 불일치! (예상: {expected_officially_due} 또는 {expected_next_imminent})"
                elif current_status == 'failed':
                    status, failed_count = '실패', failed_count + 1
                    failed_banks_names.append(bank_name_iter)
                    date_match_status = "N/A (실패)"
            
            summary_data.append({
                '은행명': bank_name_iter, 
                '공시 날짜(원본)': original_disc_date, 
                '날짜 확인': date_match_status, 
                '처리 상태': status, 
                '확인 시간': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            })
        
        summary_df = pd.DataFrame(summary_data)
        summary_filename = f"스크래핑_요약_{self.config.today}.xlsx"
        summary_file_path = self.config.output_dir / summary_filename
        
        try: 
            summary_df.to_excel(summary_file_path, index=False)
            logger.info(f"요약 보고서: {summary_file_path}")
        except Exception as e: 
            logger.error(f"요약 보고서 저장 실패: {e}", exc_info=True)

        zip_filename_str = f"저축은행_데이터_{self.config.today}.zip"
        zip_file_path_obj = self.config.output_dir_base / zip_filename_str
        
        try:
            with zipfile.ZipFile(zip_file_path_obj, 'w', zipfile.ZIP_DEFLATED) as zf:
                for f_path in self.config.output_dir.rglob('*'):
                    if f_path.is_file(): 
                        zf.write(f_path, Path(self.config.output_dir.name) / f_path.relative_to(self.config.output_dir))
            logger.info(f"결과 압축 완료: {zip_file_path_obj}")
        except Exception as e: 
            logger.error(f"결과 압축 실패: {e}", exc_info=True)
            zip_file_path_obj = None

        processed_attempt_count = completed_count + failed_count
        success_rate = (completed_count / processed_attempt_count * 100) if processed_attempt_count > 0 else 0
        date_for_subject = self.config.processing_date_kst.strftime("%Y.%m.%d")
        quarter_info_for_subject = get_quarter_string_from_period(expected_officially_due)
        email_subject = f"저축은행 분기 공시 취합_{quarter_info_for_subject} ({date_for_subject})"
        
        failed_banks_display_html = "".join(f"<li>{b}</li>" for b in failed_banks_names[:10])
        if len(failed_banks_names) > 10:
            failed_banks_display_html += f"<p>...외 {len(failed_banks_names)-10}개.</p>"
        elif not failed_banks_names:
            failed_banks_display_html = "없음"
        
        body_html = f"""
        <html><head><style>
        body {{ font-family: Arial, sans-serif; margin: 20px; }}
        .summary-box {{ background-color: #f0f8ff; padding: 15px; border-radius: 5px; margin: 10px 0; }}
        .status-completed {{ color: #28a745; font-weight: bold; }}
        .status-failed {{ color: #dc3545; font-weight: bold; }}
        table {{ border-collapse: collapse; width: 100%; margin-top: 10px; }}
        th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
        th {{ background-color: #f2f2f2; }}
        </style></head><body>
        <h2>저축은행 스크래핑 결과 ({self.config.today})</h2>
        <p><strong>스크립트 실행일:</strong> {self.config.processing_date_kst.strftime('%Y-%m-%d')}</p>
        <p><strong>공식적 최신 예상일:</strong> {expected_officially_due} (근거: {self.config.date_expectations['latest_due_reason']})</p>
        <p><strong>다음 업로드 예상일:</strong> {expected_next_imminent} (근거: {self.config.date_expectations['next_imminent_reason']})</p>
        <div class="summary-box">
            <p>총 대상: {len(all_banks_in_config)}개</p> 
            <p>처리 시도: {processed_attempt_count}개</p>
            <p><span class="status-completed">✅ 성공: {completed_count}개</span></p> 
            <p><span class="status-failed">❌ 실패: {failed_count}개</span> (성공률: {success_rate:.1f}%)</p>
            <p>📂 데이터: {self.config.output_dir.name} (압축: {zip_filename_str if zip_file_path_obj else '생성실패'})</p>
        </div>
        <h3>실패 은행 (최대 10개):</h3><ul>{failed_banks_display_html}</ul>
        <p>세부 결과는 첨부파일 확인.</p> 
        <h3>은행별 처리 현황:</h3>
        {summary_df.to_html(index=False, border=1, na_rep='').replace('<td>','<td style="word-break:normal;">') if not summary_df.empty else "<p>요약 데이터 없음.</p>"}
        <br><p><small>자동 발송 (v{self.config.VERSION})</small></p>
        </body></html>"""
        
        attachment_to_send = str(zip_file_path_obj) if zip_file_path_obj and zip_file_path_obj.exists() else (str(summary_file_path) if summary_file_path.exists() else None)
        
        if attachment_to_send and Path(attachment_to_send).name == summary_filename and zip_file_path_obj is None: 
            logger.warning("압축 파일 생성 실패. 요약 보고서만 첨부.")
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
        if logger: 
            logger.critical(f"최상위 오류: {e}", exc_info=True)
        else: 
            print(f"최상위 오류 (로거 미설정): {e}\n{traceback.format_exc()}")
        sys.exit(1) 
    finally:
        if driver_mgr: 
            driver_mgr.quit_all() 
        if logger: 
            logger.info("스크립트 실행 종료.")
        else: 
            print("스크립트 실행 종료 (로거 미설정).")

if __name__ == "__main__":
    main()
