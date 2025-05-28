# bank_scraper_headless_simplified.py
"""
저축은행 중앙회 통일경영공시 데이터 자동 스크래핑 도구 (단순화 버전)
목적: GitHub Actions에서 안정적 실행, Colab 성공 구조 기반
작성일: 2025-03-31 (최종 수정일: 2025-05-28)
개선사항:
- Colab 성공 구조 기반으로 단순화
- 안정성 우선 설계
- 카테고리별 단일 시트 생성
- 첨부2의 classify_table_by_content, scrape_single_bank, save_bank_data 개선사항 반영 (2025-05-28 요청)
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
        self.VERSION = "2.13.0-improved-classification" # Updated version
        self.BASE_URL = "https://www.fsb.or.kr/busmagequar_0100.act"
        self.MAX_RETRIES = int(os.getenv('MAX_RETRIES', '2'))
        # Colab 수준의 성능 설정
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

# --- Colab 기반 스크래핑 함수들 ---
def extract_date_information(driver):
    """강화된 날짜 정보 추출 및 검증"""
    try:
        # 1차 추출 시도
        js_script = """
        var allMatches = [];
        var datePattern = /(\d{4})년\s*(\d{1,2})월말/g;
        var bodyText = document.body.innerText || "";
        var match;
        datePattern.lastIndex = 0;

        // 전체 텍스트에서 모든 날짜 패턴 추출
        while ((match = datePattern.exec(bodyText)) !== null) {
            allMatches.push({
                fullText: match[0],
                year: parseInt(match[1]),
                month: parseInt(match[2])
            });
        }

        // 우선순위가 높은 태그에서 날짜 검색
        var prioritySelectors = [
            'h1', 'h2', 'h3', 'caption', 'th[colspan]',
            '.title', '.date', '#publishDate', '.disclosure-date'
        ];

        for (var i = 0; i < prioritySelectors.length; i++) {
            var elements;
            try { elements = document.querySelectorAll(prioritySelectors[i]); }
            catch(e) { continue; }

            for (var j = 0; j < elements.length; j++) {
                var elText = elements[j].innerText || "";
                if (elText.length > 500) elText = elText.substring(0, 500);
                datePattern.lastIndex = 0;

                while ((match = datePattern.exec(elText)) !== null) {
                    var year = parseInt(match[1]);
                    var month = parseInt(match[2]);

                    // 중복 제거
                    if (!allMatches.some(m => m.year === year && m.month === month)) {
                        allMatches.push({
                            fullText: match[0],
                            year: year,
                            month: month,
                            priority: true  // 우선순위 태그에서 추출됨
                        });
                    }
                }
            }
        }

        return allMatches;
        """

        date_matches = driver.execute_script(js_script)

        if not date_matches:
            return "날짜 정보 없음"

        # 날짜 검증 및 필터링
        current_year = datetime.now().year
        valid_dates = []

        for match in date_matches:
            year = match.get('year', 0)
            month = match.get('month', 0)

            # 합리적인 날짜 범위 검증 (현재년도 기준 -3년 ~ +1년, 분기말 월 우선)
            if (current_year - 3) <= year <= (current_year + 1) and 1 <= month <= 12:
                # 분기말 월(3, 6, 9, 12) 우선순위 부여
                priority_score = 0
                if month in [3, 6, 9, 12]:
                    priority_score += 10
                if match.get('priority'):  # 우선순위 태그에서 추출
                    priority_score += 5
                if year >= current_year - 1:  # 최근 데이터 우선
                    priority_score += year - (current_year - 2)

                valid_dates.append({
                    'text': match['fullText'],
                    'year': year,
                    'month': month,
                    'score': priority_score
                })

        if valid_dates:
            # 점수 기준으로 정렬하여 가장 적절한 날짜 선택
            valid_dates.sort(key=lambda x: x['score'], reverse=True)
            best_date = valid_dates[0]['text'].replace(' ', '')

            logger.debug(f"날짜 추출 성공: {best_date} (검증된 {len(valid_dates)}개 중 선택)")
            return best_date

        # 검증 실패 시 경고 및 기본값 반환
        logger.warning(f"날짜 검증 실패. 추출된 모든 날짜가 유효하지 않음: {[m.get('fullText', '') for m in date_matches[:3]]}")
        return "날짜 검증 실패"

    except Exception as e:
        logger.error(f"날짜 정보 추출 중 오류: {e}")
        return "날짜 추출 실패"

def select_bank(driver, bank_name, config):
    """개선된 은행 선택 로직 - 정확한 매칭"""
    try:
        driver.get(config.BASE_URL)
        WebDriverWait(driver, config.PAGE_LOAD_TIMEOUT).until(lambda d: d.execute_script('return document.readyState') == 'complete')
        time.sleep(random.uniform(0.5, 1))

        # 정확한 은행명 매칭을 위한 JavaScript 로직
        js_script = f"""
        function selectExactBank(targetBankName) {{
            // 1단계: 정확한 텍스트 매칭 우선 시도
            var allElements = document.querySelectorAll('a, td');
            var exactMatches = [];
            var partialMatches = [];

            for (var i = 0; i < allElements.length; i++) {{
                var element = allElements[i];
                var text = element.textContent.trim();

                if (text === targetBankName) {{
                    exactMatches.push(element);
                }} else if (text.includes(targetBankName)) {{
                    partialMatches.push({{
                        element: element,
                        text: text,
                        score: text.length - targetBankName.length  // 짧을수록 더 정확한 매치
                    }});
                }}
            }}

            // 정확한 매치 우선 처리
            if (exactMatches.length > 0) {{
                var target = exactMatches[0];
                target.scrollIntoView({{block: 'center'}});

                // td 내부의 링크 확인
                if (target.tagName === 'TD') {{
                    var links = target.getElementsByTagName('a');
                    if (links.length > 0) {{
                        links[0].click();
                        return "exact_match_with_link";
                    }}
                }}

                target.click();
                return "exact_match";
            }}

            // 부분 매치 처리 (가장 짧은 텍스트 우선)
            if (partialMatches.length > 0) {{
                partialMatches.sort(function(a, b) {{ return a.score - b.score; }});

                // 타겟 은행명이 포함되어 있지만 너무 길지 않은 경우만 선택
                for (var j = 0; j < partialMatches.length; j++) {{
                    var match = partialMatches[j];

                    // 길이 차이가 5글자 이하인 경우만 허용 (예: "JT" vs "JT친애" 구분)
                    if (match.score <= 5) {{
                        var target = match.element;
                        target.scrollIntoView({{block: 'center'}});

                        if (target.tagName === 'TD') {{
                            var links = target.getElementsByTagName('a');
                            if (links.length > 0) {{
                                links[0].click();
                                return "partial_match_with_link: " + match.text;
                            }}
                        }}

                        target.click();
                        return "partial_match: " + match.text;
                    }}
                }}
            }}

            return false;
        }}

        return selectExactBank('{bank_name}');
        """

        result = driver.execute_script(js_script)
        if result:
            logger.debug(f"{bank_name} 은행 선택: {result}")
            time.sleep(random.uniform(0.5, 1))
            return True

        logger.warning(f"{bank_name} 은행을 찾을 수 없습니다.")
        return False

    except Exception as e:
        logger.error(f"{bank_name} 은행 선택 실패: {str(e)}")
        return False

def select_category(driver, category, config):
    """카테고리 선택 (Colab 기반 단순화)"""
    try:
        time.sleep(random.uniform(0.1, 0.3))

        # JavaScript로 카테고리 탭 클릭
        script = f"""
        var allElements = document.querySelectorAll('a, button, span, li, div');
        for (var k = 0; k < allElements.length; k++) {{
            if (allElements[k].innerText.trim() === '{category}') {{
                allElements[k].scrollIntoView({{block: 'center'}});
                allElements[k].click();
                return "exact_match";
            }}
        }}

        for (var j = 0; j < allElements.length; j++) {{
            if (allElements[j].innerText.includes('{category}')) {{
                allElements[j].scrollIntoView({{block: 'center'}});
                allElements[j].click();
                return "contains_match";
            }}
        }}
        return false;
        """

        result = driver.execute_script(script)
        if result:
            logger.debug(f"{category} 탭: {result} 성공")
            time.sleep(random.uniform(0.5, 1)) # Ensure content loads after click
            return True

        logger.debug(f"{category} 탭을 찾을 수 없습니다.")
        return False

    except Exception as e:
        logger.debug(f"{category} 탭 클릭 실패: {str(e)}")
        return False

def extract_tables_from_page(driver, config):
    """테이블 추출 (Colab 기반)"""
    try:
        WebDriverWait(driver, config.PAGE_LOAD_TIMEOUT).until(lambda d: d.execute_script('return document.readyState') == 'complete')
        time.sleep(random.uniform(0.5, 1)) # Allow dynamic content to render

        try:
            html_source = driver.page_source
            dfs = pd.read_html(StringIO(html_source))

            if dfs:
                valid_dfs = []
                # pd.read_html can return DataFrames with no useful data, filter them.
                for df_raw in dfs:
                    df = df_raw.copy() # Avoid SettingWithCopyWarning
                    if not df.empty and df.shape[0] > 0 and df.shape[1] > 0:
                        # Minimal cleaning: drop fully NaN rows/cols that sometimes appear
                        df.dropna(axis=0, how='all', inplace=True)
                        df.dropna(axis=1, how='all', inplace=True)
                        if not df.empty:
                             valid_dfs.append(df)
                return valid_dfs
        except ValueError: # pandas.io.common.EmptyDataError is a subclass of ValueError
            logger.debug("pandas 테이블 추출 실패: 페이지에 테이블이 없거나 분석할 수 없음.")
        except Exception as e:
            logger.debug(f"pandas 테이블 추출 실패: {str(e)}")

        return []

    except Exception as e:
        logger.debug(f"테이블 추출 실패: {str(e)}")
        return []

# --- 스크래퍼 클래스 (단순화) ---
class BankScraper:
    def __init__(self, config, driver_manager, progress_manager):
        self.config = config; self.driver_manager = driver_manager; self.progress_manager = progress_manager
        self.email_sender = EmailSender()

    # --- START: ATTACHMENT 2 MODIFICATIONS ---
    def classify_table_by_content(self, table_df, source_category):
        """
        테이블 내용을 분석하여 적절한 카테고리 결정 (개선된 버전)
        source_category를 우선적으로 고려하되, 내용 분석으로 검증
        """
        try:
            if table_df is None or table_df.empty:
                return "기타"

            # 테이블 텍스트 내용 추출
            table_text = ""

            # 컬럼명에서 텍스트 추출
            try:
                if hasattr(table_df, 'columns'):
                    col_text = " ".join(str(col).lower() for col in table_df.columns if col is not None)
                    table_text += col_text + " "
            except:
                pass

            # 데이터 내용에서 텍스트 추출 (상위 5행, 모든 컬럼)
            try:
                max_rows = min(5, len(table_df))
                for i in range(max_rows):
                    for val in table_df.iloc[i].values:
                        try:
                            if pd.notna(val) and val is not None:
                                table_text += str(val).lower() + " "
                        except:
                            continue
            except:
                pass

            if not table_text.strip():
                return source_category if source_category != "기타" else "기타"

            # 더 정확한 카테고리별 키워드 정의 (스크린샷 기반)
            category_keywords = {
                "영업개황": {
                    "strong": ["영업점", "직원", "점포", "지점", "임직원", "조직", "본점", "점포수", "직원수", "임원", "영업망", "지역본부", "영업소", "인원", "명"],
                    "medium": ["영업", "개황", "현황", "조직도", "인사", "직원현황"],
                    "weak": ["총", "현재", "명", "개", "점"]
                },
                "재무현황": {
                    "strong": ["자산", "부채", "자본", "총자산", "총부채", "자기자본", "대차대조표", "재무상태표", "대출", "예금", "유가증권", "고정자산", "유동자산", "차입금"],
                    "medium": ["재무", "현황", "자본금", "적립금", "충당금", "대출채권", "예수금"],
                    "weak": ["합계", "계", "총", "금액", "백만원", "천원"]
                },
                "손익현황": {
                    "strong": ["수익", "비용", "손익", "이익", "손실", "매출", "영업이익", "순이익", "손익계산서", "이자수익", "이자비용", "당기순이익", "영업수익", "영업비용"],
                    "medium": ["손익", "수익성", "이자", "수수료", "영업", "경비"],
                    "weak": ["총", "순", "계", "백만원", "천원"]
                },
                "기타": {
                    "strong": ["기타", "부가정보", "주요사항", "특기사항", "공시사항", "참고사항", "비고", "주석", "설명", "분기공시"],
                    "medium": ["기타", "참고", "주석", "설명"], # Removed some very generic "strong" ones to medium
                    "weak": ["정보", "내용", "사항"]
                }
            }

            # 카테고리별 점수 계산 (가중치 적용)
            category_scores = {}
            for category, keywords_dict in category_keywords.items():
                score = 0
                # Strong 키워드 (가중치 10)
                for keyword in keywords_dict.get("strong", []): # Use .get for safety
                    count = table_text.count(keyword)
                    score += count * 10
                # Medium 키워드 (가중치 5)
                for keyword in keywords_dict.get("medium", []):
                    count = table_text.count(keyword)
                    score += count * 5
                # Weak 키워드 (가중치 1)
                for keyword in keywords_dict.get("weak", []):
                    count = table_text.count(keyword)
                    score += count * 1
                category_scores[category] = score

            # source_category에 보너스 점수 부여 (원래 탭에서 가져온 것이므로 신뢰도 높음)
            if source_category in category_scores:
                category_scores[source_category] += 20

            # 최고 점수 카테고리 선택
            if category_scores and max(category_scores.values()) > 0:
                best_category = max(category_scores, key=category_scores.get)

                # 점수 차이가 너무 작으면 source_category 유지
                if source_category in category_scores and best_category != source_category:
                    score_diff = category_scores[best_category] - category_scores[source_category]
                    # If best_category score is not significantly higher (e.g. <10 points more), prefer source.
                    # This helps if keywords are sparse but source_category had a strong bonus.
                    if score_diff < 10 and category_scores[source_category] > 0 : # Ensure source_category had some score itself
                        return source_category
                return best_category

            # 점수가 모두 0이면 source_category 반환 (or 기타 if source was 기타)
            return source_category if source_category != "기타" else "기타"

        except Exception as e:
            logger.debug(f"테이블 분류 중 오류: {e}")
            return source_category if source_category != "기타" else "기타"

    def scrape_single_bank(self, bank_name, driver):
        """
        단일 은행 스크래핑 - 개선된 카테고리별 수집 및 분류
        """
        logger.info(f"[{bank_name}] 스크래핑 시작")

        try:
            # 은행 선택
            if not select_bank(driver, bank_name, self.config):
                logger.error(f"{bank_name} 은행 선택 실패")
                return None

            # 날짜 정보 추출
            date_info = extract_date_information(driver)
            logger.info(f"{bank_name} 은행 날짜 정보: {date_info}")

            # 날짜 검증
            normalized_date = normalize_datestr_for_comparison(date_info)
            if normalized_date == self.config.latest_due_period:
                logger.info(f"[{bank_name}] 날짜 일치: 공식적 최신")
            elif normalized_date == self.config.next_imminent_period:
                logger.info(f"[{bank_name}] 날짜 일치: 선제적 업데이트")
            elif normalized_date not in [None, "알 수 없는 형식"]:
                logger.warning(f"[{bank_name}] 날짜 불일치: {normalized_date} (예상: {self.config.latest_due_period} 또는 {self.config.next_imminent_period})")

            # 카테고리별 테이블 수집 (중복 제거 강화)
            categorized_tables = {category: [] for category in self.config.CATEGORIES}
            all_table_signatures = set() # 전역 중복 제거용

            # 각 카테고리 탭에서 순차적으로 테이블 수집
            for current_category_tab in self.config.CATEGORIES: # Renamed to avoid clash
                try:
                    logger.debug(f"[{bank_name}] {current_category_tab} 카테고리 처리 시작")

                    # 카테고리 탭 클릭
                    if not select_category(driver, current_category_tab, self.config):
                        logger.debug(f"[{bank_name}] {current_category_tab} 탭 클릭 실패 - 카테고리 없음으로 간주")
                        continue

                    # 페이지 로딩 대기 (select_category already has a small sleep, this is additional if needed)
                    time.sleep(random.uniform(1, 2)) # Increased sleep after category selection

                    # 테이블 추출
                    tables = extract_tables_from_page(driver, self.config)
                    if not tables:
                        logger.debug(f"[{bank_name}] {current_category_tab} 카테고리에서 테이블 없음")
                        continue

                    # 수집된 테이블 처리
                    category_table_count = 0
                    for df_original in tables:
                        df = df_original.copy() # Work on a copy
                        try:
                            # 테이블 서명 생성 (더 정확한 중복 검사)
                            shape_sig = f"shape_{df.shape[0]}x{df.shape[1]}"
                            header_sig = f"headers_{hash(str(list(df.columns)))}"

                            # 데이터 샘플링을 통한 내용 서명
                            content_sig_parts = []
                            if not df.empty:
                                sample_rows = min(3, len(df))
                                for i in range(sample_rows):
                                    row_data = []
                                    # Use min(5, len(df.columns)) to avoid error on tables with <5 cols
                                    for val in df.iloc[i].values[:min(5, len(df.columns))]:
                                        try:
                                            if pd.notna(val) and val is not None:
                                                row_data.append(str(val).strip().lower())
                                            else:
                                                row_data.append("") # Represent NaN/None consistently
                                        except:
                                            row_data.append("")
                                    content_sig_parts.append("_".join(row_data))
                            content_sig = f"content_{hash('|'.join(content_sig_parts))}"
                            table_signature = f"{shape_sig}_{header_sig}_{content_sig}"

                            # 중복 검사
                            if table_signature in all_table_signatures:
                                logger.debug(f"[{bank_name}] {current_category_tab}에서 중복 테이블 발견 (서명: {table_signature[:50]}...), 스킵")
                                continue

                            # 테이블 분류 (source_category 고려)
                            classified_category = self.classify_table_by_content(df, current_category_tab)

                            # 분류된 카테고리에 테이블 추가
                            categorized_tables[classified_category].append(df)
                            all_table_signatures.add(table_signature)
                            category_table_count += 1

                            # 재분류 로그
                            if classified_category != current_category_tab:
                                logger.info(f"[{bank_name}] 테이블 재분류: '{current_category_tab}' 탭에서 가져옴 -> 최종 '{classified_category}'로 분류")
                            else:
                                logger.debug(f"[{bank_name}] 테이블 분류 유지: '{current_category_tab}'")

                        except Exception as e:
                            logger.warning(f"[{bank_name}] {current_category_tab} 카테고리 테이블 처리 중 오류: {str(e)}", exc_info=True)
                            continue
                    logger.debug(f"[{bank_name}] {current_category_tab} 카테고리에서 {category_table_count}개 신규 테이블 수집")

                except Exception as e:
                    logger.error(f"[{bank_name}] {current_category_tab} 카테고리 처리 실패: {str(e)}", exc_info=True)
                    continue

            # 결과 검증
            total_tables = sum(len(tables_list) for tables_list in categorized_tables.values())
            if total_tables == 0:
                logger.error(f"[{bank_name}] 어떤 카테고리에서도 테이블을 수집하지 못했습니다")
                # It's possible the bank page has no tables, or all were filtered.
                # Still return date_info to mark progress if needed.
                return {'날짜정보': date_info} # Return at least date info


            # 결과 데이터 구성
            result_data = {'날짜정보': date_info}

            # 분류된 테이블만 포함 (빈 카테고리는 제외하지 않음, save_bank_data가 처리)
            for category_key, tables_list in categorized_tables.items():
                result_data[category_key] = tables_list # Keep empty lists for save_bank_data
                if tables_list:
                    logger.info(f"[{bank_name}] {category_key}: {len(tables_list)}개 테이블 최종 분류")
                else:
                    logger.debug(f"[{bank_name}] {category_key}: 최종 분류된 테이블 없음")


            logger.info(f"[{bank_name}] 데이터 수집 및 분류 완료 - 총 {total_tables}개 고유 테이블")
            return result_data

        except Exception as e:
            logger.error(f"[{bank_name}] 은행 처리 중 오류 발생: {str(e)}", exc_info=True)
            return None # Indicate failure for this bank

    def save_bank_data(self, bank_name, data_dict):
        """
        은행 데이터 저장 - 카테고리별 단일 시트 (개선된 버전)
        """
        if not data_dict: # Should not happen if scrape_single_bank returns at least date_info
            logger.warning(f"[{bank_name}] 저장할 데이터 없음.")
            return False

        try:
            # 날짜 정보 추출
            date_info = data_dict.get('날짜정보', '날짜정보없음')
            safe_date_info = re.sub(r'[^\w\-_.]', '', date_info)

            # 파일명에 날짜 정보 포함
            excel_path = self.config.output_dir / f"{bank_name}_{safe_date_info}.xlsx"

            with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
                # 정보 시트 생성
                category_counts_str = ", ".join(
                    [f"{cat}: {len(data_dict.get(cat, []))}개" for cat in self.config.CATEGORIES]
                )
                info_df = pd.DataFrame({
                    '은행명': [bank_name],
                    '공시 날짜': [date_info],
                    '추출 일시': [datetime.now().strftime("%Y-%m-%d %H:%M:%S")],
                    '스크래퍼 버전': [self.config.VERSION],
                    '카테고리별 테이블 수': [category_counts_str]
                })
                info_df.to_excel(writer, sheet_name='정보', index=False)

                # 각 카테고리별 데이터 저장
                for category in self.config.CATEGORIES:
                    tables = data_dict.get(category, [])
                    if not tables:
                        # 빈 카테고리의 경우 빈 시트 생성
                        empty_df = pd.DataFrame({'메시지': [f'{category} 카테고리에 해당하는 데이터가 없습니다.']})
                        empty_df.to_excel(writer, sheet_name=category, index=False)
                        logger.debug(f"[{bank_name}] {category} 시트: 데이터 없음, 메시지 저장.")
                        continue

                    combined_df_list = []
                    for i, table_original in enumerate(tables):
                        df = table_original.copy() # Work on a copy

                        # MultiIndex 컬럼 처리
                        if isinstance(df.columns, pd.MultiIndex):
                            new_cols = []
                            for col_tuple in df.columns:
                                if isinstance(col_tuple, tuple):
                                    # Filter out 'Unnamed: X_level_Y' and 'nan' strings, then join
                                    col_parts = [str(c).strip() for c in col_tuple
                                                 if pd.notna(c) and not (isinstance(c, str) and (c.strip().lower().startswith('unnamed:') or c.strip().lower() == 'nan'))]
                                    new_cols.append('_'.join(col_parts) if col_parts else f"자동컬럼_{len(new_cols)+1}")
                                else:
                                    new_cols.append(str(col_tuple).strip() if pd.notna(col_tuple) else f"자동컬럼_{len(new_cols)+1}")
                            df.columns = new_cols
                        else: # Single index columns, just clean them
                            df.columns = [str(c).strip() if pd.notna(c) else f"자동컬럼_{idx}" for idx, c in enumerate(df.columns)]


                        # 테이블 구분자 추가
                        # Add title row for the first table as well
                        title_data = [f'=== {category} 테이블 {i+1} ==='] + [''] * (len(df.columns)-1 if len(df.columns) > 0 else 0)
                        # Ensure title_df has columns, even if df is empty (shouldn't happen here as tables is not empty)
                        title_df_columns = df.columns if not df.columns.empty else [f"컬럼{j+1}" for j in range(max(1, len(title_data)))]


                        if i > 0: # Add separator for tables after the first one
                            separator_data = [''] * len(title_df_columns)
                            separator_df = pd.DataFrame([separator_data], columns=title_df_columns)
                            combined_df_list.append(separator_df)

                        title_df = pd.DataFrame([title_data], columns=title_df_columns)
                        combined_df_list.append(title_df)
                        combined_df_list.append(df)

                    # 모든 테이블을 세로로 연결
                    if combined_df_list:
                        try:
                            combined_df = pd.concat(combined_df_list, ignore_index=True)
                            combined_df.to_excel(writer, sheet_name=category, index=False)
                        except Exception as concat_err: # Handle cases where concat might fail (e.g. all dfs are empty after processing)
                            logger.error(f"[{bank_name}] {category} 시트 테이블 병합/저장 중 오류: {concat_err}")
                            empty_df = pd.DataFrame({'오류': [f'{category} 시트의 테이블을 병합/저장하는 중 문제가 발생했습니다: {concat_err}']})
                            empty_df.to_excel(writer, sheet_name=category, index=False)


            logger.info(f"[{bank_name}] 데이터 저장 완료: {excel_path}")

            # 저장된 데이터 요약 로그
            for category in self.config.CATEGORIES:
                table_count = len(data_dict.get(category, []))
                logger.debug(f"[{bank_name}] {category} 시트: {table_count}개 테이블 내용 저장됨")

            return True

        except Exception as e:
            logger.error(f"[{bank_name}] 데이터 저장 오류: {str(e)}", exc_info=True)
            return False
    # --- END: ATTACHMENT 2 MODIFICATIONS ---

    async def worker_process_bank(self, bank_name, pbar, semaphore):
        async with semaphore:
            driver, success, date_info = None, False, None
            try:
                driver = await asyncio.get_event_loop().run_in_executor(None, self.driver_manager.get_driver)
                if not driver:
                    self.progress_manager.mark_failed(bank_name)
                    return bank_name, False, date_info

                data = None
                for attempt in range(self.config.MAX_RETRIES):
                    try:
                        data = self.scrape_single_bank(bank_name, driver) # This now returns None on some failures, or dict with date_info for no tables
                        if data: # data will always be a dict if scrape_single_bank doesn't raise exception
                            date_info = data.get('날짜정보', '미확정')
                            # Check if any actual table data was collected beyond just date_info
                            if any(cat_data for cat, cat_data in data.items() if cat != '날짜정보' and isinstance(cat_data, list) and cat_data):
                                logger.info(f"[{bank_name}] 시도 {attempt+1}: 데이터 수집 성공.")
                                break # Successful scrape with data
                            elif attempt < self.config.MAX_RETRIES -1 :
                                logger.warning(f"[{bank_name}] 시도 {attempt+1}: 날짜 정보만 수집되었거나 테이블 없음, 재시도...")
                                # Potentially re-initialize driver or navigate to base URL if needed
                                driver.get(self.config.BASE_URL) # Navigate back to base before retry
                                await asyncio.sleep(random.uniform(1,3))
                            else:
                                logger.warning(f"[{bank_name}] 모든 시도 후에도 테이블 데이터 없음 (날짜정보: {date_info}).")
                                # Proceed to save if date_info is present, even if no tables
                        else: # scrape_single_bank returned None, indicating a more severe error
                            logger.error(f"[{bank_name}] 시도 {attempt+1} 실패 (scrape_single_bank 반환값 None).")
                            if attempt == self.config.MAX_RETRIES - 1:
                                 data = None # Ensure data is None if all retries fail critically
                            else: # Re-acquire driver for next attempt
                                await asyncio.get_event_loop().run_in_executor(None, self.driver_manager.return_driver, driver)
                                driver = None # Nullify before re-getting
                                driver = await asyncio.get_event_loop().run_in_executor(None, self.driver_manager.get_driver)
                                if not driver:
                                    logger.error(f"[{bank_name}] 재시도용 드라이버 획득 실패.")
                                    data = None; break


                    except Exception as e:
                        logger.warning(f"[{bank_name}] 시도 {attempt+1} 중 예외: {type(e).__name__} - {e}", exc_info=True)
                        if attempt < self.config.MAX_RETRIES-1:
                            await asyncio.get_event_loop().run_in_executor(None, self.driver_manager.return_driver, driver)
                            driver = None
                            driver = await asyncio.get_event_loop().run_in_executor(None, self.driver_manager.get_driver)
                            if not driver:
                                logger.error(f"[{bank_name}] 재시도용 드라이버 획득 실패.")
                                data = None; break # Break from retry loop if driver cannot be re-obtained
                        else:
                            data = None # Failed all retries
                            logger.error(f"[{bank_name}] 모든 재시도 실패.")

                if data: # Even if only date_info, try to save. save_bank_data handles empty tables.
                    if self.save_bank_data(bank_name, data):
                        self.progress_manager.mark_completed(bank_name, date_info)
                        success = True
                    else: # Save failed
                        self.progress_manager.mark_failed(bank_name)
                        success = False # Explicitly set success to False
                else: # No data after all retries
                    self.progress_manager.mark_failed(bank_name)
                    success = False


                return bank_name, success, date_info

            except Exception as e:
                logger.error(f"[{bank_name}] 작업자 예외: {type(e).__name__} - {e}", exc_info=True)
                self.progress_manager.mark_failed(bank_name)
                return bank_name, False, date_info # date_info might be None here
            finally:
                if driver:
                    await asyncio.get_event_loop().run_in_executor(None, self.driver_manager.return_driver, driver)
                if pbar:
                    pbar.update(1)
                # Log final status for this bank
                final_status_msg = '성공' if success else '실패'
                logger.info(f"[{bank_name}] 최종 처리: {final_status_msg}, 공시일: {date_info if date_info else '미확정'}")

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

        processed_count = sum(1 for r in results if not isinstance(r, Exception) and r is not None) # Ensure r is not None
        logger.info(f"asyncio.gather로 {processed_count}/{len(pending_banks)}개 작업 반환 완료.")
        
        # Additional logging for task results
        for i, res in enumerate(results):
            bank_name_for_log = pending_banks[i] if i < len(pending_banks) else "알수없는 은행"
            if isinstance(res, Exception):
                logger.error(f"작업 결과 [{bank_name_for_log}]: 예외 발생 - {res}")
            elif res is None:
                 logger.warning(f"작업 결과 [{bank_name_for_log}]: None 반환됨 (처리 중 문제 가능성)")
            else:
                _, success_flag, date_val = res
                logger.debug(f"작업 결과 [{bank_name_for_log}]: 성공={success_flag}, 날짜={date_val}")


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
                        date_match_status = "⚠️ 추출실패/정보없음"
                    elif normalized_disc_date == "알 수 없는 형식":
                        date_match_status = f"❓ 형식모름 ({original_disc_date})"
                    elif normalized_disc_date == expected_officially_due:
                        date_match_status = "✅ 일치 (기한내 최신)"
                    elif normalized_disc_date == expected_next_imminent:
                        date_match_status = "🟢 일치 (예정분 선반영)"
                    else:
                        date_match_status = f"❌ 불일치! (예상: {expected_officially_due} 또는 {expected_next_imminent}, 실제: {normalized_disc_date})"
                elif current_status == 'failed':
                    status, failed_count = '실패', failed_count + 1
                    failed_banks_names.append(bank_name_iter)
                    date_match_status = "N/A (실패)"
                # Handle cases where status might be something else or bank_detail is minimal
                elif not current_status : status = "상태 불명확"

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
            zip_file_path_obj = None # Set to None if zipping fails

        # Ensure processed_attempt_count includes all banks that had an attempt recorded in progress.json
        # This might be slightly different from len(results) if some tasks failed very early.
        # A more accurate count is from progress_manager stats if available.
        # For simplicity, using completed_count + failed_count from this summary loop.
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
        th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; word-break: break-word; }}
        th {{ background-color: #f2f2f2; }}
        </style></head><body>
        <h2>저축은행 스크래핑 결과 ({self.config.today})</h2>
        <p><strong>스크립트 실행일:</strong> {self.config.processing_date_kst.strftime('%Y-%m-%d')}</p>
        <p><strong>공식적 최신 예상일:</strong> {expected_officially_due}</p>
        <p><strong>다음 업로드 예상일:</strong> {expected_next_imminent}</p>
        <div class="summary-box">
            <p>총 대상 은행 설정: {len(all_banks_in_config)}개</p>
            <p>처리 시도 (완료/실패 기록): {processed_attempt_count}개</p>
            <p><span class="status-completed">✅ 성공: {completed_count}개</span></p>
            <p><span class="status-failed">❌ 실패: {failed_count}개</span> (기록된 작업 성공률: {success_rate:.1f}%)</p>
            <p>📂 데이터 저장 위치: {self.config.output_dir.name} (압축 파일: {zip_filename_str if zip_file_path_obj and zip_file_path_obj.exists() else '생성실패 또는 경로없음'})</p>
        </div>
        <h3>실패 은행 (최대 10개):</h3><ul>{failed_banks_display_html}</ul>
        <p>세부 결과는 첨부된 요약 파일 또는 압축 파일 내 로그/데이터를 확인해주십시오.</p>
        <h3>은행별 처리 현황 (요약):</h3>
        {summary_df.to_html(index=False, border=1, na_rep='').replace('<td>','<td style="word-break:normal;">') if not summary_df.empty else "<p>요약 데이터 없음.</p>"}
        <br><p><small>자동 발송 (v{self.config.VERSION})</small></p>
        </body></html>"""

        attachment_to_send = str(zip_file_path_obj) if zip_file_path_obj and zip_file_path_obj.exists() else (str(summary_file_path) if summary_file_path.exists() else None)
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
        logger.info("모든 스크래핑 작업 흐름 정상 완료.")
    except Exception as e:
        if logger:
            logger.critical(f"최상위 오류: {e}", exc_info=True)
        else:
            print(f"최상위 오류 (로거 미설정): {e}\n{traceback.format_exc()}")
        # Attempt to send a failure email if possible, minimalist
        try:
            email_sender_fallback = EmailSender() # Assumes ENV vars are loaded
            if email_sender_fallback.enabled:
                 subject = f"스크립트 실행 실패 - {config.today if config else datetime.now().strftime('%Y%m%d')}"
                 body = f"스크립트 실행 중 최상위 오류 발생:\n<pre>{traceback.format_exc()}</pre>"
                 email_sender_fallback.send_email_with_attachment(subject, body)
        except Exception as email_ex:
            if logger: logger.error(f"오류 보고 이메일 전송 실패: {email_ex}")
            else: print(f"오류 보고 이메일 전송 실패: {email_ex}")

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
