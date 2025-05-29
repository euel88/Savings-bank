#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
저축은행 중앙회 통일경영공시 데이터 자동 스크래핑 스크립트 (최소 수정 버전)
목적: 79개 저축은행의 재무정보를 빠르고 효율적으로 스크래핑
작성일: 2025-05-29
수정 전략: 기존 7-8분 성능 유지하면서 3가지 핵심 문제만 해결
- 날짜 추출 오류 해결 (사용자 지정 날짜 '2024년9월말', '2025년3월말'만 유효 처리)
- ZIP 파일 생성 안정화 (.bin 오류 해결)
- 스크린샷 형태 결과 테이블 생성 및 이메일 본문에 유사 테이블 추가
"""

import os
import sys
import time
import random
import json
import re
import asyncio
import concurrent.futures
import smtplib
import zipfile
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from io import StringIO
from tqdm import tqdm
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import pandas as pd

# =============================================================================
# 설정 및 상수 (기존과 동일)
# =============================================================================

TODAY = datetime.now().strftime("%Y%m%d")
BASE_URL = "https://www.fsb.or.kr/busmagequar_0100.act"
MAX_RETRIES = int(os.getenv('MAX_RETRIES', '2'))
PAGE_LOAD_TIMEOUT = int(os.getenv('PAGE_LOAD_TIMEOUT', '25'))
WAIT_TIMEOUT = int(os.getenv('WAIT_TIMEOUT', '15'))
MAX_WORKERS = int(os.getenv('MAX_WORKERS', '2'))

OUTPUT_BASE_DIR = os.getenv('OUTPUT_DIR', './output')
OUTPUT_DIR = os.path.join(OUTPUT_BASE_DIR, f'저축은행_데이터_{TODAY}')

GMAIL_ADDRESS = os.getenv('GMAIL_ADDRESS')
GMAIL_APP_PASSWORD = os.getenv('GMAIL_APP_PASSWORD')
RECIPIENT_EMAILS = os.getenv('RECIPIENT_EMAILS', '').split(',') if os.getenv('RECIPIENT_EMAILS') else []

BANKS = [
    "다올", "대신", "더케이", "민국", "바로", "스카이", "신한", "애큐온", "예가람", "웰컴",
    "유안타", "조은", "키움YES", "푸른", "하나", "DB", "HB", "JT", "친애", "KB",
    "NH", "OK", "OSB", "SBI", "금화", "남양", "모아", "부림", "삼정", "상상인",
    "세람", "안국", "안양", "영진", "융창", "인성", "인천", "키움", "페퍼", "평택",
    "한국투자", "한화", "고려", "국제", "동원제일", "솔브레인", "에스앤티", "우리", "조흥", "진주",
    "흥국", "BNK", "DH", "IBK", "대백", "대아", "대원", "드림", "라온", "머스트삼일",
    "엠에스", "오성", "유니온", "참", "CK", "대한", "더블", "동양", "삼호",
    "센트럴", "스마트", "스타", "대명", "상상인플러스", "아산", "오투", "우리금융", "청주", "한성"
]

CATEGORIES = ["영업개황", "재무현황", "손익현황", "기타"]

# 문제 은행 목록 (특별 처리 필요)
PROBLEM_BANKS = ["안국", "오투"]

PROGRESS_FILE = os.path.join(OUTPUT_DIR, 'bank_scraping_progress.json')
LOG_FILE = os.path.join(OUTPUT_DIR, f'scraping_log_{TODAY}.log')

os.makedirs(OUTPUT_DIR, exist_ok=True)

# =============================================================================
# 유틸리티 함수들
# =============================================================================

def log_message(message, print_to_console=True, verbose=True):
    """로그 메시지를 파일에 기록하고 필요한 경우 콘솔에 출력합니다."""
    if not verbose:
        return

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    # log_entry = f"[{timestamp}] {message}" # f-string 사용 최소화
    log_entry = "[{}] {}".format(timestamp, message)


    try:
        with open(LOG_FILE, 'a', encoding='utf-8') as f:
            f.write(log_entry + '\n')
    except Exception as e:
        print(f"로그 파일 쓰기 실패: {e}") # 여기 f-string은 유지 (표준 라이브러리 호출)

    if print_to_console:
        print(message)

def validate_data_freshness():
    """사용자가 지정한 목표 데이터 분기를 반환합니다."""
    possible_dates = ["2024년9월말", "2025년3월말"]
    log_message("사용자 지정 목표 데이터 분기: {}".format(', '.join(possible_dates)))
    return possible_dates

# =============================================================================
# 드라이버 관리 클래스 (기존과 동일)
# =============================================================================

class DriverManager:
    """웹 드라이버 풀을 관리하는 클래스입니다."""
    
    def __init__(self, max_drivers=MAX_WORKERS):
        self.max_drivers = max_drivers
        self.drivers = []
        self.available_drivers = []
        self.initialize_drivers()

    def initialize_drivers(self):
        """드라이버 풀을 초기화합니다."""
        log_message("{}개의 Chrome 드라이버 초기화 중...".format(self.max_drivers))
        for _ in range(self.max_drivers):
            driver = self.create_driver()
            if driver:
                self.drivers.append(driver)
                self.available_drivers.append(driver)
        log_message("총 {}개의 드라이버가 준비되었습니다.".format(len(self.drivers)))

    def create_driver(self):
        """GitHub Actions 환경에 최적화된 Chrome 웹드라이버를 생성합니다."""
        try:
            options = Options()
            
            options.add_argument('--headless=new')
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('--disable-gpu')
            options.add_argument('--window-size=1920,1080')
            options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
            options.add_argument('--disable-extensions')
            options.add_argument('--disable-browser-side-navigation')
            options.add_argument('--disable-infobars')
            options.add_argument('--disable-notifications')
            options.add_argument('--disable-popup-blocking')
            options.add_argument('--disable-background-networking')
            options.add_argument('--disable-background-timer-throttling')
            options.add_argument('--disable-backgrounding-occluded-windows')
            options.add_argument('--disable-renderer-backgrounding')
            options.add_argument('--memory-pressure-off')
            options.add_argument('--max_old_space_size=4096')
            options.add_argument('--disable-web-security')
            options.add_argument('--allow-running-insecure-content')
            
            prefs = {
                'profile.default_content_setting_values': {
                    'images': 1, 'plugins': 2, 'javascript': 1,
                    'notifications': 2, 'media_stream': 2,
                },
                'disk-cache-size': 4096,
            }
            options.add_experimental_option('prefs', prefs)
            
            driver = webdriver.Chrome(options=options)
            driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
            driver.implicitly_wait(WAIT_TIMEOUT)
            return driver
        except Exception as e:
            log_message("Chrome 드라이버 생성 실패: {}".format(str(e)))
            return None

    def get_driver(self):
        while not self.available_drivers:
            log_message("모든 드라이버가 사용 중입니다. 잠시 대기...", verbose=False)
            time.sleep(1)
        return self.available_drivers.pop(0)

    def return_driver(self, driver):
        if driver in self.drivers and driver not in self.available_drivers:
            try:
                driver.current_url
                self.available_drivers.append(driver)
            except:
                try: driver.quit()
                except: pass
                self.drivers.remove(driver)
                new_driver = self.create_driver()
                if new_driver:
                    self.drivers.append(new_driver)
                    self.available_drivers.append(new_driver)

    def close_all(self):
        for driver in self.drivers:
            try: driver.quit()
            except: pass
        self.drivers = []
        self.available_drivers = []

# =============================================================================
# 진행 상황 관리 클래스 (기존과 동일)
# =============================================================================
class ProgressManager:
    def __init__(self, file_path=None):
        self.file_path = file_path or PROGRESS_FILE
        self.progress = self.load()
    def load(self):
        if os.path.exists(self.file_path):
            try:
                with open(self.file_path, 'r', encoding='utf-8') as f: return json.load(f)
            except (json.JSONDecodeError, FileNotFoundError):
                log_message("진행 파일 손상 또는 없음: {}, 새로 생성합니다.".format(self.file_path))
        return {'completed': [], 'failed': [], 'data_validation': [], 'stats': {'last_run': None, 'success_count': 0, 'failure_count': 0, 'validation_count': 0}}
    def is_completed(self, bank_name): return bank_name in self.progress.get('completed', [])
    def mark_completed(self, bank_name):
        if bank_name not in self.progress.get('completed', []):
            self.progress.setdefault('completed', []).append(bank_name)
            self.progress['stats']['success_count'] = len(self.progress.get('completed', []))
        if bank_name in self.progress.get('failed', []): self.progress['failed'].remove(bank_name)
        self.save()
    def mark_failed(self, bank_name):
        if bank_name not in self.progress.get('failed', []) and bank_name not in self.progress.get('completed', []):
            self.progress.setdefault('failed', []).append(bank_name)
            self.progress['stats']['failure_count'] = len(self.progress.get('failed', []))
            self.save()
    def mark_data_validated(self, bank_name, date_info, is_fresh):
        entry = {'bank_name': bank_name, 'date_info': date_info, 'is_fresh': is_fresh, 'validated_at': datetime.now().isoformat()}
        self.progress.setdefault('data_validation', []).append(entry)
        self.progress['stats']['validation_count'] = len(self.progress.get('data_validation', []))
        self.save()
    def save(self):
        self.progress['stats']['last_run'] = datetime.now().isoformat()
        try:
            with open(self.file_path, 'w', encoding='utf-8') as f: json.dump(self.progress, f, ensure_ascii=False, indent=2)
        except Exception as e: log_message("진행 상황 저장 실패: {}".format(str(e)))
    def get_pending_banks(self, all_banks=BANKS):
        completed = set(self.progress.get('completed', []))
        return [bank for bank in all_banks if bank not in completed]

# =============================================================================
# 웹 스크래핑 유틸리티 클래스 (기존과 동일)
# =============================================================================
class WaitUtils:
    @staticmethod
    def wait_for_element(driver, locator, timeout=WAIT_TIMEOUT):
        try: return WebDriverWait(driver, timeout).until(EC.presence_of_element_located(locator))
        except TimeoutException: return None
    @staticmethod
    def wait_for_clickable(driver, locator, timeout=WAIT_TIMEOUT):
        try: return WebDriverWait(driver, timeout).until(EC.element_to_be_clickable(locator))
        except TimeoutException: return None
    @staticmethod
    def wait_for_page_load(driver, timeout=PAGE_LOAD_TIMEOUT):
        try:
            WebDriverWait(driver, timeout).until(lambda d: d.execute_script('return document.readyState') == 'complete')
            return True
        except TimeoutException: return False
    @staticmethod
    def wait_with_random(min_time=0.5, max_time=1.5): time.sleep(random.uniform(min_time, max_time))

# =============================================================================
# 핵심 수정 1: 스마트 날짜 추출 (사용자 지정 날짜 우선)
# =============================================================================
def extract_date_information(driver, bank_name=None):
    target_dates_normalized = {"2024년9월말", "2025년3월말"}
    def normalize_date_string(date_str):
        s = re.sub(r'\s+', '', date_str)
        return s.replace('년0', '년')
    try:
        page_source = driver.page_source
        date_candidates = re.findall(r'\d{4}년\s*\d{1,2}월말', page_source)
        for date_text in date_candidates:
            normalized = normalize_date_string(date_text)
            if normalized in target_dates_normalized:
                log_message("{} 기본 추출 (타겟 일치): {}".format(bank_name or '은행', normalized), verbose=False)
                return normalized
        
        log_message("{} 정밀 날짜 검색 시도 (타겟: {})".format(bank_name or '은행', target_dates_normalized), verbose=False)
        try:
            js_script = """
            var allText = document.body.innerText; var matches = allText.match(/\\d{4}년\\s*\\d{1,2}월말/g);
            var targetDates = ["2024년9월말", "2025년3월말"]; var found = "";
            if (matches) { for (var i = 0; i < matches.length; i++) {
                var normalized = matches[i].replace(/\\s+/g, '').replace('년0','년');
                if (targetDates.includes(normalized)) { found = normalized; break; }}}
            return found;"""
            js_result = driver.execute_script(js_script)
            if js_result:
                log_message("{} JavaScript 검색 (타겟 일치): {}".format(bank_name or '은행', js_result), verbose=False)
                return js_result
        except Exception as e_js:
            log_message("{} JavaScript 검색 실패: {}".format(bank_name or '은행', str(e_js)), verbose=False)

        if bank_name in PROBLEM_BANKS:
            selectors = ["//h1","//h2","//h3","//h4","//h5","//h6","//p","//span","//div","//th","//td"]
            for selector in selectors:
                try:
                    for element in driver.find_elements(By.XPATH, selector):
                        try:
                            text = element.text.strip()
                            if not text: continue
                            for match_text in re.findall(r'\d{4}년\s*\d{1,2}월말', text):
                                normalized = normalize_date_string(match_text)
                                if normalized in target_dates_normalized:
                                    log_message("{} 선택자 기반 검색 (타겟 일치 - {}): {}".format(bank_name, selector, normalized), verbose=False)
                                    return normalized
                        except StaleElementReferenceException: continue
                except Exception: continue
        log_message("{}에서 지정된 날짜 정보를 찾을 수 없음 (타겟: {})".format(bank_name or '은행', target_dates_normalized), verbose=True)
        return "날짜 정보 없음"
    except Exception as e:
        log_message("날짜 정보 추출 중 전체 오류 ({}): {}".format(bank_name or '은행', str(e)), verbose=True)
        return "날짜 추출 실패"

def validate_extracted_date(extracted_date, expected_dates):
    if not extracted_date or extracted_date in ["날짜 정보 없음", "날짜 추출 실패"]:
        return False, "날짜 정보를 추출할 수 없음"
    if extracted_date in expected_dates:
        return True, "최신 데이터 확인: {}".format(extracted_date)
    
    normalized_extracted = re.sub(r'\s+', '', extracted_date).replace('년0', '년')
    for expected in expected_dates:
        if normalized_extracted == expected:
            return True, "최신 데이터 확인 (추가 검증): {}".format(extracted_date)
    return False, "예상 날짜와 불일치: {} (예상: {})".format(extracted_date, ', '.join(expected_dates))

# =============================================================================
# 나머지 스크래핑 함수들 (select_bank, select_category, extract_tables_from_page) - 기존과 동일
# =============================================================================
def select_bank(driver, bank_name):
    try:
        driver.get(BASE_URL)
        WaitUtils.wait_for_page_load(driver)
        WaitUtils.wait_with_random(1, 2)
        js_script = "var targetBank = '{}'; var allElements = document.querySelectorAll('a, td, th, span, div'); var exactMatches = []; var limitedMatches = []; for(var i = 0; i < allElements.length; i++) {{ var element = allElements[i]; var text = element.textContent.trim(); if(text === targetBank) {{ exactMatches.push(element); }} else if(text.indexOf(targetBank) !== -1 && text.length <= targetBank.length * 2 && text.length > targetBank.length) {{ limitedMatches.push(element); }} }} var allCandidates = exactMatches.concat(limitedMatches); for(var i = 0; i < allCandidates.length; i++) {{ var element = allCandidates[i]; try {{ if(element.offsetParent === null) continue; element.scrollIntoView({{block: 'center'}}); if(element.tagName.toLowerCase() === 'a') {{ element.click(); return 'direct_link_' + (i < exactMatches.length ? 'exact' : 'limited'); }} var links = element.querySelectorAll('a'); if(links.length > 0) {{ links[0].click(); return 'nested_link_' + (i < exactMatches.length ? 'exact' : 'limited'); }} element.click(); return 'element_click_' + (i < exactMatches.length ? 'exact' : 'limited'); }} catch(e) {{ continue; }} }} return false;".format(bank_name)
        result = driver.execute_script(js_script)
        if result:
            log_message("{} 은행: JavaScript {} 성공".format(bank_name, result), verbose=False)
            WaitUtils.wait_with_random(1, 2)
            return True
        log_message("{} 은행: JavaScript 방법 실패, Selenium 시도".format(bank_name), verbose=False)
        exact_xpaths = ["//td[normalize-space(text())='{}']//a".format(bank_name), "//a[normalize-space(text())='{}']".format(bank_name), "//td[text()='{}']//a".format(bank_name), "//a[text()='{}']".format(bank_name)]
        for xpath in exact_xpaths:
            try:
                for element in driver.find_elements(By.XPATH, xpath):
                    if element.is_displayed():
                        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
                        WaitUtils.wait_with_random(0.5, 1)
                        driver.execute_script("arguments[0].click();", element)
                        WaitUtils.wait_with_random(1, 2)
                        log_message("{} 은행: Selenium 정확한 매칭 성공".format(bank_name), verbose=False)
                        return True
            except: continue
        log_message("{} 은행: 정확한 매칭 실패, 제한적 부분 매칭 시도".format(bank_name), verbose=False)
        try:
            for element in driver.find_elements(By.XPATH, "//td | //a"):
                try:
                    element_text = element.text.strip()
                    if (bank_name in element_text and len(element_text) <= len(bank_name) * 2 and len(element_text) > len(bank_name) and element.is_displayed()):
                        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
                        WaitUtils.wait_with_random(0.5, 1)
                        if element.tag_name.lower() == 'a': driver.execute_script("arguments[0].click();", element)
                        else:
                            links = element.find_elements(By.TAG_NAME, "a")
                            if links: driver.execute_script("arguments[0].click();", links[0])
                            else: driver.execute_script("arguments[0].click();", element)
                        WaitUtils.wait_with_random(1, 2)
                        log_message("{} 은행: Selenium 제한적 매칭 성공 (매칭된 텍스트: {})".format(bank_name, element_text), verbose=False)
                        return True
                except: continue
        except: pass
        log_message("{} 은행을 찾을 수 없습니다.".format(bank_name))
        return False
    except Exception as e:
        log_message("{} 은행 선택 실패: {}".format(bank_name, str(e)))
        return False

def select_category(driver, category):
    try:
        js_script = "var targetCategory = '{}'; var allElements = document.querySelectorAll('a, button, span, li, div, tab'); for(var i = 0; i < allElements.length; i++) {{ var element = allElements[i]; var text = element.textContent.trim(); if(text === targetCategory && element.offsetParent !== null) {{ element.scrollIntoView({{block: 'center'}}); element.click(); return 'exact_match'; }} }} for(var i = 0; i < allElements.length; i++) {{ var element = allElements[i]; var text = element.textContent.trim(); if(text.includes(targetCategory) && element.offsetParent !== null) {{ element.scrollIntoView({{block: 'center'}}); element.click(); return 'contains_match'; }} }} var categoryIndex = {{'영업개황': 0, '재무현황': 1, '손익현황': 2, '기타': 3}}; var index = categoryIndex[targetCategory]; if(index !== undefined) {{ var tabContainers = document.querySelectorAll('ul.tabs, .tab-container, nav, .tab-list, ul'); for(var i = 0; i < tabContainers.length; i++) {{ var tabs = tabContainers[i].querySelectorAll('a, li, button, span'); if(tabs.length > index && tabs[index].offsetParent !== null) {{ tabs[index].scrollIntoView({{block: 'center'}}); tabs[index].click(); return 'index_match'; }} }} }} return false;".format(category)
        result = driver.execute_script(js_script)
        if result:
            log_message("{} 탭: JavaScript {} 성공".format(category, result), verbose=False)
            WaitUtils.wait_with_random(1, 2)
            return True
        category_xpaths = ["//a[normalize-space(text())='{}']".format(category), "//*[contains(text(), '{}') and (self::a or self::button or self::span or self::li)]".format(category)]
        for xpath in category_xpaths:
            try:
                for element in driver.find_elements(By.XPATH, xpath):
                    if element.is_displayed():
                        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
                        WaitUtils.wait_with_random(0.5, 1)
                        driver.execute_script("arguments[0].click();", element)
                        WaitUtils.wait_with_random(1, 2)
                        return True
            except: continue
        log_message("{} 탭을 찾을 수 없습니다.".format(category), verbose=False)
        return False
    except Exception as e:
        log_message("{} 탭 클릭 실패: {}".format(category, str(e)), verbose=False)
        return False

def extract_tables_from_page(driver):
    try:
        WaitUtils.wait_for_page_load(driver)
        WaitUtils.wait_with_random(1, 2)
        try:
            dfs = pd.read_html(StringIO(driver.page_source))
            if dfs:
                valid_dfs, seen_hashes = [], set()
                for df in dfs:
                    if not df.empty and df.shape[0] > 0 and df.shape[1] > 0:
                        if isinstance(df.columns, pd.MultiIndex):
                            new_cols = []
                            for col in df.columns:
                                if isinstance(col, tuple):
                                    parts = [str(c).strip() for c in col if str(c).strip() and str(c).lower() != 'nan']
                                    new_cols.append('_'.join(parts) if parts else "Column_{}".format(len(new_cols)+1))
                                else: new_cols.append(str(col))
                            df.columns = new_cols
                        try:
                            df_hash = hash(str(df.shape) + str(list(df.columns)) + (str(df.iloc[0].values) if len(df) > 0 else ""))
                            if df_hash not in seen_hashes: valid_dfs.append(df); seen_hashes.add(df_hash)
                        except: valid_dfs.append(df)
                return valid_dfs
        except Exception as e: log_message("pandas 테이블 추출 실패: {}".format(str(e)), verbose=False)
        try:
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            extracted_dfs, seen_hashes = [], set()
            for table in soup.find_all('table'):
                try:
                    headers = [th.get_text(strip=True) for th in (table.select('thead th') or table.select('tr:first-child th'))]
                    if not headers: headers = [(td.get_text(strip=True) or "Column_{}".format(i+1)) for i, td in enumerate(table.select('tr:first-child td'))]
                    rows_data = []
                    for tr in (table.select('tbody tr') or table.select('tr')[1:]):
                        cells = [td.get_text(strip=True) for td in tr.select('td')]
                        if cells: rows_data.append(cells)
                    if rows_data and headers:
                        for i, row in enumerate(rows_data):
                            if len(row) < len(headers): rows_data[i] = row + [''] * (len(headers) - len(row))
                            elif len(row) > len(headers): rows_data[i] = row[:len(headers)]
                        df = pd.DataFrame(rows_data, columns=headers)
                        if not df.empty:
                            try:
                                df_hash = hash(str(df.shape) + (str(df.iloc[0].values) if len(df) > 0 else ""))
                                if df_hash not in seen_hashes: extracted_dfs.append(df); seen_hashes.add(df_hash)
                            except: extracted_dfs.append(df)
                except Exception as e: log_message("개별 테이블 파싱 실패: {}".format(str(e)), verbose=False)
            return extracted_dfs
        except Exception as e: log_message("BeautifulSoup 테이블 추출 실패: {}".format(str(e)), verbose=False)
        return []
    except Exception as e:
        log_message("페이지에서 테이블 추출 실패: {}".format(str(e)))
        return []

# =============================================================================
# 메인 스크래핑 로직
# =============================================================================
def scrape_bank_data(bank_name, driver, progress_manager, expected_dates):
    log_message("[시작] {} 은행 스크래핑 시작".format(bank_name))
    try:
        if not select_bank(driver, bank_name):
            log_message("{} 은행 선택 실패".format(bank_name)); return None
        try:
            driver.current_url; log_message("{} 은행 페이지 접속 성공".format(bank_name), verbose=False)
        except: log_message("{} 은행 페이지 URL 획득 실패".format(bank_name)); return None
        
        date_info = extract_date_information(driver, bank_name)
        is_fresh, validation_message = validate_extracted_date(date_info, expected_dates)
        progress_manager.mark_data_validated(bank_name, date_info, is_fresh)
        log_message("{} 은행 날짜 검증: {} (추출된 날짜: {})".format(bank_name, validation_message, date_info))

        result_data = {'날짜정보': date_info, '검증결과': validation_message, '신선도': is_fresh }
        all_table_hashes = set()
        for category in CATEGORIES:
            try:
                if not select_category(driver, category):
                    log_message("{} 은행 {} 탭 클릭 실패, 다음 카테고리로 진행".format(bank_name, category)); continue
                tables = extract_tables_from_page(driver)
                if not tables:
                    log_message("{} 은행 {} 카테고리에서 테이블을 찾을 수 없습니다.".format(bank_name, category)); continue
                valid_tables = []
                for df in tables:
                    try:
                        df_hash = hash(str(df.shape) + str(list(df.columns)) + (str(df.iloc[0].values) if len(df) > 0 else ""))
                        if df_hash not in all_table_hashes: valid_tables.append(df); all_table_hashes.add(df_hash)
                    except: valid_tables.append(df)
                if valid_tables:
                    result_data[category] = valid_tables
                    log_message("{} 은행 {} 카테고리에서 {}개 테이블 추출".format(bank_name, category, len(valid_tables)))
            except Exception as e: log_message("{} 은행 {} 카테고리 처리 실패: {}".format(bank_name, category, str(e)))
        
        table_categories = [k for k,v in result_data.items() if k not in ['날짜정보','검증결과','신선도'] and isinstance(v,list) and v]
        if not table_categories:
            log_message("{} 은행에서 테이블 데이터를 추출할 수 없습니다.".format(bank_name))
            if date_info not in ["날짜 정보 없음", "날짜 추출 실패"]:
                 log_message("[완료] {} 은행 테이블 데이터는 없으나 날짜 정보({})는 수집됨.".format(bank_name, date_info))
                 return result_data 
            return None
        log_message("[완료] {} 은행 데이터 수집 완료 (카테고리: {})".format(bank_name, ', '.join(table_categories)))
        return result_data
    except Exception as e:
        log_message("{} 은행 처리 중 오류 발생: {}".format(bank_name, str(e)))
        return None

def save_bank_data(bank_name, data_dict):
    if not data_dict: return False
    try:
        date_info = data_dict.get('날짜정보', '날짜정보없음')
        safe_date_info = re.sub(r'[^\w\-_년월말]', '_', date_info)
        excel_path = os.path.join(OUTPUT_DIR, "{}_{}.xlsx".format(bank_name, safe_date_info))
        with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
            info_df = pd.DataFrame({
                '은행명': [bank_name], '공시 날짜': [data_dict.get('날짜정보', '')],
                '검증 결과': [data_dict.get('검증결과', '')],
                '데이터 신선도': ['최신' if data_dict.get('신선도', False) else '구버전 또는 확인필요'],
                '추출 일시': [datetime.now().strftime("%Y-%m-%d %H:%M:%S")],
                '스크래핑 시스템': ['저축은행 스크래퍼 v2.3.2 (f-string 전면 수정)']
            })
            info_df.to_excel(writer, sheet_name='공시정보', index=False)
            for category, tables in data_dict.items():
                if category in ['날짜정보','검증결과','신선도'] or not isinstance(tables,list): continue
                for i, df in enumerate(tables):
                    sheet_name = category[:31] if i == 0 else "{}_{}".format(category, i+1)[:31]
                    if isinstance(df.columns, pd.MultiIndex):
                        new_cols = []
                        for col in df.columns:
                            if isinstance(col, tuple):
                                parts = [str(c).strip() for c in col if str(c).strip() and str(c).lower() != 'nan']
                                new_cols.append('_'.join(parts) if parts else "Column_{}".format(len(new_cols)+1))
                            else: new_cols.append(str(col))
                        df.columns = new_cols
                    try: df.to_excel(writer, sheet_name=sheet_name, index=False)
                    except Exception as e: log_message("{} - {} 시트 저장 실패: {}".format(bank_name, sheet_name, str(e)))
        log_message("{} 은행 데이터 저장 완료: {}".format(bank_name, excel_path))
        return True
    except Exception as e:
        log_message("{} 은행 데이터 저장 오류: {}".format(bank_name, str(e)))
        return False

def worker_process_bank(bank_name, driver_manager, progress_manager, expected_dates):
    driver = None
    try:
        driver = driver_manager.get_driver()
        for attempt in range(MAX_RETRIES + 1):
            try:
                result_data = scrape_bank_data(bank_name, driver, progress_manager, expected_dates)
                if result_data:
                    if save_bank_data(bank_name, result_data):
                        progress_manager.mark_completed(bank_name)
                        return bank_name, True, result_data.get('날짜정보', ''), result_data.get('신선도', False)
                    else: # 저장 실패
                        if attempt < MAX_RETRIES: log_message("{} 은행 데이터 저장 실패, 재시도 {}/{}...".format(bank_name, attempt+1, MAX_RETRIES)); WaitUtils.wait_with_random(2,4)
                        else: log_message("{} 은행 데이터 저장 실패, 최대 시도 횟수 초과".format(bank_name))
                else: # 스크래핑 실패 (result_data is None)
                    if attempt < MAX_RETRIES:
                        log_message("{} 은행 데이터 스크래핑 실패, 재시도 {}/{}...".format(bank_name, attempt+1, MAX_RETRIES))
                        try: driver.get("about:blank"); time.sleep(0.5)
                        except: pass
                        WaitUtils.wait_with_random(2,4)
                    else: log_message("{} 은행 데이터 스크래핑 실패, 최대 시도 횟수 초과".format(bank_name))
                if attempt == MAX_RETRIES and not result_data: break # 최종 실패
            except Exception as e: # worker_process_bank 내의 예외
                if attempt < MAX_RETRIES: log_message("{} 은행 처리 중 오류: {}, 재시도 {}/{}...".format(bank_name, str(e), attempt+1, MAX_RETRIES)); WaitUtils.wait_with_random(2,4)
                else: log_message("{} 은행 처리 실패: {}, 최대 시도 횟수 초과".format(bank_name, str(e)))
            if result_data and save_bank_data(bank_name, result_data): # 성공 시 즉시 반환
                 progress_manager.mark_completed(bank_name)
                 return bank_name, True, result_data.get('날짜정보', ''), result_data.get('신선도', False)
        # 모든 시도 실패
        progress_manager.mark_failed(bank_name)
        validation_info = progress_manager.progress.get('data_validation', [])
        bank_validation = next((item for item in validation_info if item['bank_name'] == bank_name), None)
        failed_date_info = bank_validation.get('date_info', '추출 실패') if bank_validation else '추출 실패'
        failed_is_fresh = bank_validation.get('is_fresh', False) if bank_validation else False
        return bank_name, False, failed_date_info, failed_is_fresh
    except Exception as e: # worker_process_bank의 가장 바깥쪽 예외
        log_message("{} 은행 처리 중 예상치 못한 오류: {}".format(bank_name, str(e)))
        progress_manager.mark_failed(bank_name)
        return bank_name, False, "예상치 못한 오류: {}".format(str(e)), False
    finally:
        if driver and driver_manager: driver_manager.return_driver(driver)

# =============================================================================
# 비동기 처리 및 메인 실행 로직
# =============================================================================
async def process_banks_async(banks, driver_manager, progress_manager, expected_dates):
    log_message("병렬 처리 시작: {}개 은행, {}개 워커".format(len(banks), MAX_WORKERS))
    all_results = []
    batch_size = MAX_WORKERS 
    bank_batches = [banks[i:i + batch_size] for i in range(0, len(banks), batch_size)]
    for batch_idx, batch in enumerate(bank_batches):
        log_message("배치 {}/{} 처리 중 ({}개 은행)".format(batch_idx+1, len(bank_batches), len(batch)))
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(MAX_WORKERS, len(batch))) as executor:
            loop = asyncio.get_event_loop()
            futures = [loop.run_in_executor(executor, worker_process_bank, bank, driver_manager, progress_manager, expected_dates) for bank in batch]
            progress_bar = tqdm(asyncio.as_completed(futures), total=len(futures), desc="배치 {}/{}".format(batch_idx+1, len(bank_batches)))
            batch_results = []
            for future in progress_bar:
                result = await future
                batch_results.append(result)
                success_count = len([r for r in batch_results if r[1]])
                progress_bar.set_postfix_str("완료: {}/{}".format(success_count, len(batch_results)))
            all_results.extend(batch_results)
            if batch_idx < len(bank_batches) - 1:
                log_message("배치 {} 완료. 다음 배치 전 잠시 대기...".format(batch_idx+1)); await asyncio.sleep(2)
    return all_results

def process_with_retry(banks, max_retries_main=1):
    expected_dates = validate_data_freshness()
    driver_manager = DriverManager(max_drivers=MAX_WORKERS)
    progress_manager = ProgressManager()
    pending_banks = progress_manager.get_pending_banks(banks)
    if not pending_banks:
        log_message("모든 은행 처리 완료! 일부 은행을 재검증합니다.")
        pending_banks = banks[:min(5, len(banks))]
    log_message("처리할 은행 수: {}/{}".format(len(pending_banks), len(banks)))
    all_run_results = []
    try:
        if sys.version_info >= (3, 7): results = asyncio.run(process_banks_async(pending_banks, driver_manager, progress_manager, expected_dates))
        else: 
            loop = asyncio.get_event_loop()
            results = loop.run_until_complete(process_banks_async(pending_banks, driver_manager, progress_manager, expected_dates))
        all_run_results.extend(results)
        failed_banks_names = [r[0] for r in results if not r[1]]
        retry_count = 0
        while failed_banks_names and retry_count < max_retries_main:
            retry_count += 1
            log_message("메인 재시도 {}/{}: {}개 은행 처리 중...".format(retry_count, max_retries_main, len(failed_banks_names)))
            if sys.version_info >= (3,7): retry_res = asyncio.run(process_banks_async(failed_banks_names, driver_manager, progress_manager, expected_dates))
            else: 
                loop = asyncio.get_event_loop()
                retry_res = loop.run_until_complete(process_banks_async(failed_banks_names, driver_manager, progress_manager, expected_dates))
            all_run_results.extend(retry_res)
            newly_successful = [r[0] for r in retry_res if r[1]]
            failed_banks_names = [r[0] for r in retry_res if not r[1]]
            log_message("메인 재시도 {} 결과: {}개 성공, {}개 실패".format(retry_count, len(newly_successful), len(failed_banks_names)))
        final_successful_banks = progress_manager.progress.get('completed', [])
        final_failed_banks = progress_manager.progress.get('failed', [])
        return final_successful_banks, final_failed_banks, all_run_results
    finally: driver_manager.close_all()

def generate_summary_report():
    try:
        progress_manager = ProgressManager()
        validation_data = progress_manager.progress.get('data_validation', [])
        validation_dict = {item['bank_name']: item for item in validation_data}
        bank_summary = []
        for bank in BANKS:
            bank_files = [f for f in os.listdir(OUTPUT_DIR) if f.startswith(bank + "_") and f.endswith(".xlsx")]
            if bank_files:
                try:
                    latest_file = sorted(bank_files)[-1]
                    xls = pd.ExcelFile(os.path.join(OUTPUT_DIR, latest_file))
                    categories = sorted(list(set([s.split('_')[0] if '_' in s else s for s in xls.sheet_names if s != '공시정보'])))
                    date_info, val_res, fresh_text = "날짜 정보 없음", "검증 없음", "알 수 없음"
                    if '공시정보' in xls.sheet_names:
                        info_df = pd.read_excel(os.path.join(OUTPUT_DIR, latest_file), sheet_name='공시정보')
                        if '공시 날짜' in info_df.columns and not info_df['공시 날짜'].empty: date_info = str(info_df['공시 날짜'].iloc[0])
                        if '검증 결과' in info_df.columns and not info_df['검증 결과'].empty: val_res = str(info_df['검증 결과'].iloc[0])
                        if '데이터 신선도' in info_df.columns and not info_df['데이터 신선도'].empty: fresh_text = str(info_df['데이터 신선도'].iloc[0])
                    status = '완료' if set(categories) >= set(CATEGORIES) else '부분 완료'
                    if not categories and date_info in ["날짜 정보 없음", "날짜 추출 실패"]: status = '실패'
                    bank_summary.append({'은행명': bank, '스크래핑 상태': status, '공시 날짜': date_info, '데이터 신선도': fresh_text, '검증 결과': val_res, '시트 수': len(xls.sheet_names) -1 if len(xls.sheet_names)>0 else 0, '스크래핑된 카테고리': ', '.join(categories)})
                except Exception as e: bank_summary.append({'은행명': bank, '스크래핑 상태': '파일 손상', '공시 날짜': '확인 불가', '데이터 신선도': '확인 불가', '검증 결과': '오류: {}'.format(str(e)), '시트 수': '확인 불가', '스크래핑된 카테고리': ''})
            else:
                val_item = validation_dict.get(bank, {})
                date_val, fresh_val = val_item.get('date_info', '정보 없음'), val_item.get('is_fresh', False)
                fresh_text_val = "알 수 없음"
                if date_val not in ["날짜 정보 없음", "날짜 추출 실패", "정보 없음"]: fresh_text_val = '최신' if fresh_val else '구버전 또는 확인필요'
                bank_summary.append({'은행명': bank, '스크래핑 상태': '실패' if bank in progress_manager.progress.get('failed', []) else '미처리', '공시 날짜': date_val, '데이터 신선도': fresh_text_val, '검증 결과': '검증 완료' if bank in validation_dict else '검증 안됨', '시트 수': 0, '스크래핑된 카테고리': ''})
        summary_df = pd.DataFrame(bank_summary)
        status_order = {'완료':0,'부분 완료':1,'파일 손상':2,'실패':3,'미처리':4}; summary_df['상태순서'] = summary_df['스크래핑 상태'].map(status_order)
        summary_df = summary_df.sort_values(['상태순서','은행명']).drop('상태순서',axis=1)
        summary_file_path = os.path.join(OUTPUT_DIR, "스크래핑_요약_{}.xlsx".format(TODAY))
        summary_df.to_excel(summary_file_path, index=False)
        stats = {'전체 은행 수':len(BANKS), '완료 은행 수':len([r for r in bank_summary if r['스크래핑 상태']=='완료']), '부분 완료 은행 수':len([r for r in bank_summary if r['스크래핑 상태']=='부분 완료']), '실패 은행 수':len([r for r in bank_summary if r['스크래핑 상태'] in ['실패','파일 손상','미처리']]), '최신 데이터 은행 수':len([r for r in bank_summary if r['데이터 신선도']=='최신']), '성공률':"{:.2f}%".format(len([r for r in bank_summary if r['스크래핑 상태'] in ['완료','부분 완료']])/len(BANKS)*100 if len(BANKS)>0 else 0)}
        log_message("\n===== 스크래핑 결과 요약 =====")
        for k,v in stats.items(): log_message("{}: {}".format(k,v))
        log_message("요약 파일 저장 완료: {}".format(summary_file_path))
        return summary_file_path, stats
    except Exception as e:
        log_message("요약 보고서 생성 오류: {}".format(str(e))); import traceback; log_message(traceback.format_exc())
        return None, {}

def create_zip_archive():
    try:
        zip_filename = '저축은행_데이터_{}.zip'.format(TODAY)
        zip_path = os.path.join(OUTPUT_BASE_DIR, zip_filename)
        if os.path.exists(zip_path): os.remove(zip_path); log_message("기존 ZIP 파일 삭제: {}".format(zip_path))
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED, compresslevel=6) as zipf:
            file_count, total_size = 0, 0
            for root, _, files in os.walk(OUTPUT_DIR):
                for file in files:
                    file_path_to_zip = os.path.join(root, file)
                    if os.path.exists(file_path_to_zip) and os.path.isfile(file_path_to_zip):
                        try:
                            arcname = os.path.relpath(file_path_to_zip, OUTPUT_BASE_DIR)
                            zipf.write(file_path_to_zip, arcname)
                            file_size = os.path.getsize(file_path_to_zip); total_size += file_size; file_count += 1
                            log_message("ZIP에 추가: {} ({} bytes)".format(arcname, file_size), verbose=False)
                        except Exception as e: log_message("파일 ZIP 추가 실패 ({}): {}".format(file, str(e)))
            info_lines = [
                "저축은행 중앙회 통일경영공시 데이터 스크래핑 결과 (v2.3.2 f-string 전면 수정)\n",
                "생성일시: {}".format(datetime.now().strftime('%Y년 %m월 %d일 %H시 %M분 %S초')),
                "포함 파일 수: {}개".format(file_count),
                "총 용량: {:,} bytes".format(total_size),
                "스크래핑 대상: {}개 저축은행".format(len(BANKS)),
                "데이터 기준일: 2024년 9월말 / 2025년 3월말\n",
                "🔧 이번 버전의 개선사항 (v2.3.2):",
                "✅ f-string 사용 최소화로 SyntaxError 해결 시도",
                "✅ 날짜 추출 정확도 향상 (사용자 지정 '2024년9월말', '2025년3월말'만 유효 처리)",
                "✅ ZIP 파일 생성 완전 안정화 (.bin 오류 근본 해결)",
                "✅ 스크린샷 형태 결과 테이블 생성 및 이메일 본문에 유사 테이블 추가\n",
                "파일 구성:", "- 각 은행별 Excel 파일 (.xlsx)", "- 스크래핑 요약 보고서 (Excel)",
                "- 은행별 날짜 확인 결과 (Excel)", "- 실행 로그 파일 (.log)", "- 진행 상황 파일 (.json)\n",
                "GitHub Actions 저축은행 스크래퍼 v2.3.2 (f-string 전면 수정)"
            ]
            zipf.writestr("README_{}.txt".format(TODAY), "\n".join(info_lines).encode('utf-8'))
        if os.path.exists(zip_path):
            with zipfile.ZipFile(zip_path, 'r') as test_zip:
                if test_zip.namelist():
                    log_message("✅ ZIP 아카이브 생성 완료: {}".format(zip_path)); return zip_path
            log_message("❌ ZIP 파일이 비어있습니다."); return None
        log_message("❌ ZIP 파일 생성에 실패했습니다."); return None
    except Exception as e:
        log_message("❌ ZIP 아카이브 생성 실패: {}".format(str(e))); import traceback; log_message("상세 오류: {}".format(traceback.format_exc()))
        return None

def generate_screenshot_format_report(all_run_results):
    try:
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        table_data, unique_bank_results = [], {}
        for bank_result in all_run_results: unique_bank_results[bank_result[0]] = bank_result
        for bank_name_key in BANKS:
            bank_result = unique_bank_results.get(bank_name_key)
            if not bank_result:
                table_data.append({'은행명': bank_name_key, '공시 날짜(월말)': '결과 없음', '날짜 확인': '❌ 미처리/오류', '처리상태': '실패', '확인 시간': current_time}); continue
            bank_name, success, date_info_raw, is_fresh = bank_result
            date_info_for_report = str(date_info_raw)
            processing_status = "완료" if success else "실패"
            if not success and ("오류" in date_info_for_report or "실패" in date_info_for_report or not date_info_for_report): date_info_for_report = "추출 실패"
            date_status = "⚠️ 확인필요"
            if date_info_for_report in ["날짜 정보 없음", "추출 실패", "결과 없음"]: date_status = "❌ 미처리/오류"
            elif is_fresh:
                if "2024년9월말" in date_info_for_report: date_status = "✅ 일치 (기한내최신)"
                elif "2025년3월말" in date_info_for_report: date_status = "🟢 일치 (예정보다선반영)"
                else: date_status = "✅ 일치 (확인됨)"
            elif date_info_for_report not in ["날짜 정보 없음", "추출 실패", "결과 없음"]: date_status = "⚠️ 불일치 (구버전)"
            table_data.append({'은행명': bank_name, '공시 날짜(월말)': date_info_for_report, '날짜 확인': date_status, '처리상태': processing_status, '확인 시간': current_time})
        result_df = pd.DataFrame(table_data)
        s_file = os.path.join(OUTPUT_DIR, "은행별_날짜확인_결과_{}.xlsx".format(TODAY))
        with pd.ExcelWriter(s_file, engine='openpyxl') as writer:
            result_df.to_excel(writer, sheet_name='은행별_날짜확인', index=False)
            stats_data = {'구분': ['전체 은행 수','완료된 은행 수 (처리상태 기준)','2024년9월말 데이터','2025년3월말 데이터','기타/오래된 날짜 데이터 (완료 건 중)','처리 실패 은행 (처리상태 기준)','성공률 (처리상태 기준)'],
                          '수량': [len(BANKS), len([r for r in table_data if r['처리상태']=='완료']), len([r for r in table_data if "2024년9월말" in r['공시 날짜(월말)'] and r['처리상태']=='완료']), len([r for r in table_data if "2025년3월말" in r['공시 날짜(월말)'] and r['처리상태']=='완료']), len([r for r in table_data if r['날짜 확인'] not in ["✅ 일치 (기한내최신)","🟢 일치 (예정보다선반영)","✅ 일치 (확인됨)"] and r['처리상태']=='완료']), len([r for r in table_data if r['처리상태']=='실패']), "{:.1f}%".format(len([r for r in table_data if r['처리상태']=='완료'])/len(BANKS)*100) if len(BANKS)>0 else "N/A"]}
            pd.DataFrame(stats_data).to_excel(writer, sheet_name='통계요약', index=False)
            problem_banks_report = [r for r in table_data if r['날짜 확인'] not in ["✅ 일치 (기한내최신)","🟢 일치 (예정보다선반영)","✅ 일치 (확인됨)"]]
            if problem_banks_report: pd.DataFrame(problem_banks_report).to_excel(writer, sheet_name='문제은행목록', index=False)
        log_message("스크린샷 형태 결과 파일 저장 완료: {}".format(s_file))
        log_message("\n📋 은행별 날짜 확인 결과 요약:")
        for status, count in result_df['처리상태'].value_counts().items(): log_message("  • {}: {}개".format(status, count))
        log_message("\n📅 날짜 확인 상태:")
        for status, count in result_df['날짜 확인'].value_counts().items(): log_message("  • {}: {}개".format(status, count))
        return s_file, table_data
    except Exception as e:
        log_message("스크린샷 형태 보고서 생성 오류: {}".format(str(e))); import traceback; log_message(traceback.format_exc())
        return None, []

def send_email_notification(subject, body_text_part, email_table_data=None, attachment_paths=None, is_overall_success=True, expected_dates_for_email=None):
    if not GMAIL_ADDRESS or not GMAIL_APP_PASSWORD or not RECIPIENT_EMAILS:
        log_message("이메일 설정이 불완전하여 알림을 발송하지 않습니다."); return False
    try:
        msg = MIMEMultipart('alternative')
        msg['From'] = GMAIL_ADDRESS; msg['To'] = ', '.join(RECIPIENT_EMAILS); msg['Subject'] = subject
        msg.attach(MIMEText(body_text_part, 'plain', 'utf-8'))
        
        html_parts = ["<html><head><meta charset=\"UTF-8\"><style>",
                      "body{font-family:Arial,sans-serif;margin:0;padding:0;color:#333}table{border-collapse:collapse;width:95%;margin:20px auto;font-size:10pt;box-shadow:0 2px 3px rgba(0,0,0,.1)}",
                      "th,td{border:1px solid #ddd;text-align:left;padding:8px}th{background-color:#f0f0f0;color:#333;font-weight:bold}tr:nth-child(even){background-color:#f9f9f9}",
                      "h2{color:#0056b3;margin-left:15px;border-bottom:2px solid #0056b3;padding-bottom:5px}p{margin-left:15px;line-height:1.6}",
                      ".summary-section{padding:15px;background-color:#fdfdfd;border-radius:5px;margin-bottom:15px}.summary-section p{margin-left:0;white-space:pre-wrap}",
                      "</style></head><body><div class=\"summary-section\"><p>{}</p></div>".format(body_text_part.replace("\n", "<br>"))]

        if expected_dates_for_email:
            html_parts.append("<h2>데이터 신선도 검증 기준</h2><p>📅 예상 최신 데이터 분기: {}<br>📅 조기 업로드 가능 분기 (또는 다른 유효 분기): {}<br>⚠️ 이 기준과 다른 날짜의 데이터는 구버전이거나 추출 오류일 가능성이 있습니다.</p>".format(expected_dates_for_email[0], expected_dates_for_email[1]))
        if email_table_data:
            html_parts.append("<h2>은행별 날짜 확인 결과 상세</h2><table><thead><tr>")
            headers = ['은행명', '공시 날짜(월말)', '날짜 확인', '처리상태', '확인 시간']
            for header in headers: html_parts.append("<th>{}</th>".format(header))
            html_parts.append("</tr></thead><tbody>")
            for item in email_table_data:
                html_parts.append("<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>".format(item.get('은행명',''), item.get('공시 날짜(월말)',''), item.get('날짜 확인',''), item.get('처리상태',''), item.get('확인 시간','')))
            html_parts.append("</tbody></table>")
        html_parts.append("</body></html>")
        msg.attach(MIMEText("".join(html_parts), 'html', 'utf-8'))
        
        if attachment_paths:
            for file_path in attachment_paths:
                if os.path.exists(file_path):
                    try:
                        filename=os.path.basename(file_path); maintype,subtype='application','octet-stream'
                        if filename.endswith('.zip'): subtype='zip'
                        elif filename.endswith('.xlsx'): subtype='vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                        with open(file_path,"rb") as fp: part=MIMEBase(maintype,subtype); part.set_payload(fp.read())
                        encoders.encode_base64(part); part.add_header('Content-Disposition','attachment',filename=filename); msg.attach(part)
                        log_message("📎 파일 첨부: {}".format(filename))
                    except Exception as e: log_message("첨부 파일 추가 실패 ({}): {}".format(file_path, str(e)))
        with smtplib.SMTP('smtp.gmail.com', 587) as server:
            server.starttls(); server.login(GMAIL_ADDRESS, GMAIL_APP_PASSWORD); server.send_message(msg)
        log_message("📧 이메일 알림 발송 완료: {}".format(', '.join(RECIPIENT_EMAILS))); return True
    except Exception as e:
        log_message("❌ 이메일 발송 실패: {}".format(str(e))); import traceback; log_message(traceback.format_exc()); return False

# =============================================================================
# 메인 함수
# =============================================================================
def main():
    try:
        with open(LOG_FILE,'w',encoding='utf-8') as f: f.write("") 
    except Exception as e: print("로그 파일 초기화 실패: {}".format(e))

    start_time = time.time()
    log_message("\n🚀 ===== 저축은행 중앙회 통일경영공시 데이터 스크래핑 시작 (v2.3.2 f-string 전면 수정) [{}] =====\n".format(TODAY))

    try:
        log_message("🔧 이번 버전의 수정사항 (v2.3.2):")
        log_message("  ✅ f-string 사용 최소화로 SyntaxError 해결 시도")
        log_message("  ✅ 날짜 추출 로직 개선: 사용자 지정 날짜('2024년9월말', '2025년3월말')만 유효 처리")
        log_message("  ✅ 이메일 본문에 스크린샷 형태의 결과 테이블 추가 (HTML 형식)")
        
        log_message("\n⚙️ 현재 설정값:")
        log_message("  • 최대 워커 수: {}개".format(MAX_WORKERS))
        log_message("  • 최대 재시도 횟수 (메인): 1회 (워커 내 재시도: {}회)".format(MAX_RETRIES))
        log_message("  • 이메일 알림: {}".format('✅ 활성화' if GMAIL_ADDRESS and GMAIL_APP_PASSWORD else '❌ 비활성화'))

        log_message("\n🏦 {}개 저축은행 데이터 스크래핑 시작...".format(len(BANKS)))
        successful_banks, failed_banks, all_run_results_for_report = process_with_retry(BANKS, max_retries_main=1)

        log_message("\n📊 결과 요약 보고서 생성 중...")
        summary_file, stats = generate_summary_report() 

        log_message("📋 스크린샷 형태 결과 테이블 생성 중...")
        screenshot_file, email_table_data_for_html = generate_screenshot_format_report(all_run_results_for_report)

        log_message("📦 ZIP 압축 파일 생성 중...")
        zip_file = create_zip_archive()

        end_time = time.time()
        total_duration = end_time - start_time
        minutes, seconds = divmod(total_duration, 60)
        target_achieved = total_duration <= 8 * 60

        log_message("\n🎉 ===== 스크래핑 완료 (v2.3.2 f-string 전면 수정) =====")
        log_message("⏰ 총 실행 시간: {}분 {}초".format(int(minutes), int(seconds)))
        log_message("🎯 성능 목표: {}".format('✅ 달성 (8분 이내)' if target_achieved else '⚠️ 목표 초과'))
        log_message("✅ 성공한 은행 (ProgressManager 기준): {}개".format(len(successful_banks)))
        log_message("❌ 실패한 은행 (ProgressManager 기준): {}개".format(len(failed_banks)))
        
        if failed_banks: log_message("🔍 실패한 은행 목록 (ProgressManager 기준): {}".format(', '.join(failed_banks)))

        if email_table_data_for_html: 
            problem_banks_from_report = [b for b in email_table_data_for_html if b['날짜 확인'] not in ["✅ 일치 (기한내최신)", "🟢 일치 (예정보다선반영)", "✅ 일치 (확인됨)"]]
            if problem_banks_from_report:
                log_message("⚠️ 날짜 확인 필요 은행 (보고서 기준): {}개".format(len(problem_banks_from_report)))
                for bank_item in problem_banks_from_report[:5]: log_message("    • {}: {} ({})".format(bank_item['은행명'], bank_item['공시 날짜(월말)'], bank_item['날짜 확인']))
                if len(problem_banks_from_report) > 5: log_message("    ... 기타 {}개 은행".format(len(problem_banks_from_report) - 5))

        log_message("\n📁 생성된 파일 목록:")
        log_message("  📦 ZIP 압축파일: {} {}".format(os.path.basename(zip_file) if zip_file else "생성 실패", "✅" if zip_file else "❌"))
        log_message("  📋 은행별 날짜확인 결과(Excel): {} {}".format(os.path.basename(screenshot_file) if screenshot_file else "생성 실패", "✅" if screenshot_file else "❌"))
        log_message("  📊 요약 보고서(Excel): {} {}".format(os.path.basename(summary_file) if summary_file else "생성 실패", "✅" if summary_file else "❌"))
        log_message("  📄 실행 로그: {} ✅".format(os.path.basename(LOG_FILE)))

        if GMAIL_ADDRESS and GMAIL_APP_PASSWORD and RECIPIENT_EMAILS:
            log_message("\n📧 이메일 알림 발송 중...")
            
            subject_email = "� 저축은행 데이터 스크래핑 {} (v2.3.2) - {}분{}초".format(('완료' if not failed_banks else '부분완료'), int(minutes), int(seconds))
            
            body_lines = [
                "저축은행 중앙회 통일경영공시 데이터 스크래핑이 완료되었습니다.\n",
                "🔧 v2.3.2 수정 버전의 특징:",
                "✅ f-string 사용 최소화로 SyntaxError 해결 시도",
                "✅ 날짜 추출 로직 개선: 사용자 지정 날짜('2024년9월말', '2025년3월말')만 유효 처리",
                "✅ 이메일 본문에 스크린샷 형태의 결과 테이블 추가 (HTML 형식)\n",
                "📊 실행 정보:",
                "- 🕐 실행 날짜: {}".format(datetime.now().strftime('%Y년 %m월 %d일 %H시 %M분')),
                "- ⏱️ 총 실행 시간: {}분 {}초 ({})".format(int(minutes), int(seconds), ('목표 달성' if target_achieved else '목표 초과')),
                "- 🎯 처리 대상: 전국 {}개 저축은행\n".format(len(BANKS)),
                "📈 스크래핑 결과 요약 (ProgressManager 기준):",
                "- 🏦 전체 은행 수: {}개".format(stats.get('전체 은행 수', len(BANKS))),
                "- ✅ 완료 은행 수: {}개".format(stats.get('완료 은행 수', len(successful_banks))),
                "- ⚠️ 부분 완료 은행 수: {}개".format(stats.get('부분 완료 은행 수', 0)),
                "- ❌ 실패 은행 수: {}개".format(stats.get('실패 은행 수', len(failed_banks))),
                "- 🟢 최신 데이터 은행 수: {}개".format(stats.get('최신 데이터 은행 수', 0)),
                "- 📊 전체 성공률 (완료+부분완료): {}\n".format(stats.get('성공률', '0.00%')),
                "📦 첨부 파일:", "1. 🗜️ ZIP 압축파일: 모든 데이터 포함", "2. 📋 은행별 날짜확인 결과 (Excel)",
                "3. 📊 종합 요약 보고서 (Excel)", "4. 📄 상세 실행 로그"
            ]
            body_text_email = "\n".join(body_lines)

            attachments_email = []
            if zip_file and os.path.exists(zip_file): attachments_email.append(zip_file)
            if screenshot_file and os.path.exists(screenshot_file): attachments_email.append(screenshot_file)
            if summary_file and os.path.exists(summary_file): attachments_email.append(summary_file)
            if os.path.exists(LOG_FILE): attachments_email.append(LOG_FILE)

            expected_dates_for_email_content = validate_data_freshness() 

            email_sent = send_email_notification(subject_email, body_text_email, email_table_data_for_html, attachments_email, not failed_banks, expected_dates_for_email_content)
            log_message("    {}".format("✅ 이메일 발송 성공" if email_sent else "❌ 이메일 발송 실패"))
        else:
            log_message("\n📧 이메일 알림: 설정되지 않음")
        log_message("\n🎊 ===== 저축은행 중앙회 통일경영공시 데이터 스크래핑 완료 (v2.3.2) [{}] =====".format(TODAY))

    except KeyboardInterrupt: log_message("\n⏹️ 사용자에 의해 중단되었습니다.")
    except Exception as e:
        log_message("\n💥 예상치 못한 오류 발생: {}".format(str(e)))
        import traceback
        log_message("상세 오류 정보:\n{}".format(traceback.format_exc()))
        if GMAIL_ADDRESS and GMAIL_APP_PASSWORD and RECIPIENT_EMAILS:
            error_body_lines = [
                "저축은행 데이터 스크래핑 v2.3.2 버전 실행 중 오류가 발생했습니다.\n",
                "- 🕐 발생 시간: {}".format(datetime.now().strftime('%Y년 %m월 %d일 %H시 %M분')),
                "- 🐛 오류 내용: {}".format(str(e)),
                "상세 내용은 첨부된 로그 파일을 확인해주세요."
            ]
            send_email_notification("❌ 저축은행 데이터 스크래핑 오류 발생 (v2.3.2) - {}".format(TODAY), "\n".join(error_body_lines), None, [LOG_FILE] if os.path.exists(LOG_FILE) else [], False)

if __name__ == "__main__":
    main()
�
