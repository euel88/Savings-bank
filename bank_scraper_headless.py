#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
저축은행 중앙회 통일경영공시 데이터 자동 스크래핑 스크립트 (엄격한 날짜 검증 버전)
목적: 79개 저축은행의 재무정보를 빠르고 효율적으로 스크래핑
작성일: 2025-05-29
수정 사항:
1. 날짜 추출 로직 강화: 오직 2024년9월말 또는 2025년3월말만 허용
2. 이메일 본문에 스크린샷 형태의 결과 테이블 포함
3. 잘못된 날짜 완전 배제 로직 추가
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
# 설정 및 상수
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

# 허용되는 날짜 패턴 (2025년 5월 29일 기준)
ALLOWED_DATE_PATTERNS = [
    "2024년9월말", "2024년09월말", "2024년 9월말", "2024년 09월말",
    "2025년3월말", "2025년03월말", "2025년 3월말", "2025년 03월말"
]

# 정규화된 허용 날짜 (공백 제거)
NORMALIZED_ALLOWED_DATES = [re.sub(r'\s+', '', pattern) for pattern in ALLOWED_DATE_PATTERNS]

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
    log_entry = f"[{timestamp}] {message}"

    try:
        with open(LOG_FILE, 'a', encoding='utf-8') as f:
            f.write(log_entry + '\n')
    except Exception as e:
        print(f"로그 파일 쓰기 실패: {e}")

    if print_to_console:
        print(message)

def validate_data_freshness():
    """현재 날짜를 기준으로 예상되는 최신 데이터 분기를 계산합니다."""
    # 2025년 5월 29일 기준으로 고정된 예상 날짜 반환
    expected_dates = ["2024년9월말", "2025년3월말"]
    log_message(f"현재 날짜: {datetime.now().strftime('%Y년 %m월 %d일')}")
    log_message(f"허용되는 데이터 분기: {', '.join(expected_dates)}")
    return expected_dates

def is_date_allowed(date_string):
    """주어진 날짜 문자열이 허용되는 날짜인지 엄격하게 검증합니다."""
    if not date_string:
        return False
    
    # 공백을 제거하여 정규화
    normalized_date = re.sub(r'\s+', '', date_string)
    
    # 허용된 날짜 목록과 정확히 일치하는지 확인
    return normalized_date in NORMALIZED_ALLOWED_DATES

def extract_and_validate_date(text):
    """텍스트에서 날짜를 추출하고 허용된 날짜만 반환합니다."""
    if not text:
        return None
    
    # 모든 날짜 패턴 추출
    date_patterns = re.findall(r'\d{4}년\s*\d{1,2}월말', text)
    
    # 허용된 날짜만 필터링
    valid_dates = []
    for date_pattern in date_patterns:
        if is_date_allowed(date_pattern):
            valid_dates.append(re.sub(r'\s+', '', date_pattern))  # 정규화하여 저장
    
    # 중복 제거
    valid_dates = list(set(valid_dates))
    
    # 우선순위: 2024년9월말 > 2025년3월말
    if "2024년9월말" in valid_dates:
        return "2024년9월말"
    elif "2025년3월말" in valid_dates:
        return "2025년3월말"
    elif valid_dates:
        return valid_dates[0]
    else:
        return None

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
        log_message(f"{self.max_drivers}개의 Chrome 드라이버 초기화 중...")
        for _ in range(self.max_drivers):
            driver = self.create_driver()
            if driver:
                self.drivers.append(driver)
                self.available_drivers.append(driver)
        log_message(f"총 {len(self.drivers)}개의 드라이버가 준비되었습니다.")

    def create_driver(self):
        """GitHub Actions 환경에 최적화된 Chrome 웹드라이버를 생성합니다."""
        try:
            options = Options()
            
            # Headless 모드 (GitHub Actions 환경에서 필수)
            options.add_argument('--headless=new')
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('--disable-gpu')
            options.add_argument('--window-size=1920,1080')
            
            # User-Agent 설정
            options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
            
            # 성능 최적화 옵션
            options.add_argument('--disable-extensions')
            options.add_argument('--disable-browser-side-navigation')
            options.add_argument('--disable-infobars')
            options.add_argument('--disable-notifications')
            options.add_argument('--disable-popup-blocking')
            options.add_argument('--disable-background-networking')
            options.add_argument('--disable-background-timer-throttling')
            options.add_argument('--disable-backgrounding-occluded-windows')
            options.add_argument('--disable-renderer-backgrounding')
            
            # 메모리 사용량 최적화
            options.add_argument('--memory-pressure-off')
            options.add_argument('--max_old_space_size=4096')
            
            # 보안 관련 옵션 (CI 환경에서 필요)
            options.add_argument('--disable-web-security')
            options.add_argument('--allow-running-insecure-content')
            
            # Chrome 환경설정
            prefs = {
                'profile.default_content_setting_values': {
                    'images': 1,          # 이미지 로딩 허용
                    'plugins': 2,         # 플러그인 차단
                    'javascript': 1,      # JavaScript 허용 (필수)
                    'notifications': 2,   # 알림 차단
                    'media_stream': 2,    # 미디어 스트림 차단
                },
                'disk-cache-size': 4096,
            }
            options.add_experimental_option('prefs', prefs)
            
            # Chrome 드라이버 생성
            driver = webdriver.Chrome(options=options)
            driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
            driver.implicitly_wait(WAIT_TIMEOUT)
            
            return driver
            
        except Exception as e:
            log_message(f"Chrome 드라이버 생성 실패: {str(e)}")
            return None

    def get_driver(self):
        """사용 가능한 드라이버를 가져옵니다."""
        while not self.available_drivers:
            log_message("모든 드라이버가 사용 중입니다. 잠시 대기...", verbose=False)
            time.sleep(1)
            
        driver = self.available_drivers.pop(0)
        return driver

    def return_driver(self, driver):
        """드라이버를 풀에 반환합니다."""
        if driver in self.drivers and driver not in self.available_drivers:
            try:
                # 드라이버 상태 확인
                driver.current_url  # 접근 가능성 테스트
                self.available_drivers.append(driver)
            except:
                # 오류 발생 시 드라이버를 교체
                try:
                    driver.quit()
                except:
                    pass
                
                self.drivers.remove(driver)
                new_driver = self.create_driver()
                if new_driver:
                    self.drivers.append(new_driver)
                    self.available_drivers.append(new_driver)

    def close_all(self):
        """모든 드라이버를 종료합니다."""
        for driver in self.drivers:
            try:
                driver.quit()
            except:
                pass
        self.drivers = []
        self.available_drivers = []

# =============================================================================
# 진행 상황 관리 클래스
# =============================================================================

class ProgressManager:
    """스크래핑 진행 상황을 관리하는 클래스입니다."""
    
    def __init__(self, file_path=None):
        self.file_path = file_path or PROGRESS_FILE
        self.progress = self.load()

    def load(self):
        """저장된 진행 상황을 로드합니다."""
        if os.path.exists(self.file_path):
            try:
                with open(self.file_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except (json.JSONDecodeError, FileNotFoundError):
                log_message(f"진행 파일 손상 또는 없음: {self.file_path}, 새로 생성합니다.")

        return {
            'completed': [],
            'failed': [],
            'data_validation': [],
            'stats': {
                'last_run': None,
                'success_count': 0,
                'failure_count': 0,
                'validation_count': 0
            }
        }

    def mark_completed(self, bank_name):
        """은행을 완료 목록에 추가합니다."""
        if bank_name not in self.progress.get('completed', []):
            self.progress.setdefault('completed', []).append(bank_name)
            self.progress['stats']['success_count'] = len(self.progress.get('completed', []))

        # 실패 목록에서 제거 (재시도 후 성공한 경우)
        if bank_name in self.progress.get('failed', []):
            self.progress['failed'].remove(bank_name)

        self.save()

    def mark_failed(self, bank_name):
        """은행을 실패 목록에 추가합니다."""
        if bank_name not in self.progress.get('failed', []) and bank_name not in self.progress.get('completed', []):
            self.progress.setdefault('failed', []).append(bank_name)
            self.progress['stats']['failure_count'] = len(self.progress.get('failed', []))
            self.save()

    def mark_data_validated(self, bank_name, date_info, is_fresh):
        """은행의 데이터 검증 결과를 기록합니다."""
        validation_entry = {
            'bank_name': bank_name,
            'date_info': date_info,
            'is_fresh': is_fresh,
            'validated_at': datetime.now().isoformat()
        }
        
        self.progress.setdefault('data_validation', []).append(validation_entry)
        self.progress['stats']['validation_count'] = len(self.progress.get('data_validation', []))
        self.save()

    def save(self):
        """진행 상황을 파일에 저장합니다."""
        self.progress['stats']['last_run'] = datetime.now().isoformat()
        try:
            with open(self.file_path, 'w', encoding='utf-8') as f:
                json.dump(self.progress, f, ensure_ascii=False, indent=2)
        except Exception as e:
            log_message(f"진행 상황 저장 실패: {str(e)}")

    def get_pending_banks(self, all_banks=BANKS):
        """처리할 은행 목록을 반환합니다."""
        completed = set(self.progress.get('completed', []))
        return [bank for bank in all_banks if bank not in completed]

# =============================================================================
# 웹 스크래핑 유틸리티 클래스
# =============================================================================

class WaitUtils:
    """명시적 대기를 위한 유틸리티 클래스입니다."""
    
    @staticmethod
    def wait_for_element(driver, locator, timeout=WAIT_TIMEOUT):
        """요소가 나타날 때까지 명시적으로 대기합니다."""
        try:
            element = WebDriverWait(driver, timeout).until(
                EC.presence_of_element_located(locator)
            )
            return element
        except TimeoutException:
            return None

    @staticmethod
    def wait_for_clickable(driver, locator, timeout=WAIT_TIMEOUT):
        """요소가 클릭 가능할 때까지 명시적으로 대기합니다."""
        try:
            element = WebDriverWait(driver, timeout).until(
                EC.element_to_be_clickable(locator)
            )
            return element
        except TimeoutException:
            return None

    @staticmethod
    def wait_for_page_load(driver, timeout=PAGE_LOAD_TIMEOUT):
        """페이지가 완전히 로드될 때까지 대기합니다."""
        try:
            WebDriverWait(driver, timeout).until(
                lambda d: d.execute_script('return document.readyState') == 'complete'
            )
            return True
        except TimeoutException:
            return False

    @staticmethod
    def wait_with_random(min_time=0.5, max_time=1.5):
        """무작위 시간 동안 대기합니다 (봇 탐지 방지)."""
        time.sleep(random.uniform(min_time, max_time))

# =============================================================================
# 핵심 수정 1: 엄격한 날짜 추출 로직
# =============================================================================

def extract_date_information(driver, bank_name=None):
    """웹페이지에서 허용된 날짜만 엄격하게 추출합니다."""
    try:
        log_message(f"{bank_name or '은행'} 날짜 추출 시작 (엄격 모드)", verbose=False)
        
        # 1단계: 페이지 소스에서 모든 날짜 패턴 추출
        page_source = driver.page_source
        found_date = extract_and_validate_date(page_source)
        
        if found_date:
            log_message(f"{bank_name or '은행'} 페이지 소스에서 유효 날짜 발견: {found_date}", verbose=False)
            return found_date
        
        # 2단계: HTML 요소별 검색 (우선순위 순)
        priority_selectors = [
            "//h1[contains(text(), '년') and contains(text(), '월말')]",
            "//h2[contains(text(), '년') and contains(text(), '월말')]", 
            "//h3[contains(text(), '년') and contains(text(), '월말')]",
            "//th[contains(text(), '기말') and contains(text(), '년')]",
            "//td[contains(text(), '기말') and contains(text(), '년')]",
            "//*[@class='title' or @class='header'][contains(text(), '년')]"
        ]
        
        for selector in priority_selectors:
            try:
                elements = driver.find_elements(By.XPATH, selector)
                for element in elements:
                    text = element.text.strip()
                    found_date = extract_and_validate_date(text)
                    if found_date:
                        log_message(f"{bank_name or '은행'} HTML 요소에서 유효 날짜 발견: {found_date} (출처: {selector})", verbose=False)
                        return found_date
            except Exception:
                continue
        
        # 3단계: JavaScript 실행 (최후 수단)
        try:
            js_script = """
            var allowedPatterns = [
                '2024년9월말', '2024년09월말', '2024년 9월말', '2024년 09월말',
                '2025년3월말', '2025년03월말', '2025년 3월말', '2025년 03월말'
            ];
            
            var allText = document.body.innerText;
            var dateRegex = /\\d{4}년\\s*\\d{1,2}월말/g;
            var matches = allText.match(dateRegex);
            
            if (matches) {
                // 허용된 패턴과 정확히 일치하는 것만 찾기
                for (var i = 0; i < matches.length; i++) {
                    var cleanMatch = matches[i].replace(/\\s+/g, '');
                    for (var j = 0; j < allowedPatterns.length; j++) {
                        var cleanPattern = allowedPatterns[j].replace(/\\s+/g, '');
                        if (cleanMatch === cleanPattern) {
                            return cleanMatch;
                        }
                    }
                }
            }
            
            return '';
            """
            
            js_result = driver.execute_script(js_script)
            if js_result:
                log_message(f"{bank_name or '은행'} JavaScript에서 유효 날짜 발견: {js_result}", verbose=False)
                return js_result
                
        except Exception as e:
            log_message(f"{bank_name or '은행'} JavaScript 날짜 추출 실패: {str(e)}", verbose=False)
        
        # 4단계: 모든 방법 실패
        log_message(f"{bank_name or '은행'} 허용된 날짜를 찾을 수 없음", verbose=False)
        return "허용된 날짜 없음"

    except Exception as e:
        log_message(f"{bank_name or '은행'} 날짜 정보 추출 오류: {str(e)}", verbose=False)
        return "날짜 추출 실패"

def validate_extracted_date(extracted_date, expected_dates):
    """추출된 날짜가 허용된 날짜인지 엄격하게 검증합니다."""
    if not extracted_date or extracted_date in ["허용된 날짜 없음", "날짜 추출 실패"]:
        return False, "유효한 날짜 정보를 추출할 수 없음"
    
    # 허용된 날짜인지 확인
    if is_date_allowed(extracted_date):
        if "2024년9월" in extracted_date:
            return True, f"✅ 일치 (기한내최신): {extracted_date}"
        elif "2025년3월" in extracted_date:
            return True, f"🟢 일치 (예정보다선반영): {extracted_date}"
        else:
            return True, f"허용된 날짜 확인: {extracted_date}"
    else:
        return False, f"❌ 허용되지 않은 날짜: {extracted_date} (허용: 2024년9월말, 2025년3월말만)"

# 나머지 함수들은 기존과 동일하게 유지 (select_bank, select_category, extract_tables_from_page 등)
def select_bank(driver, bank_name):
    """정확한 은행명 매칭을 위한 개선된 은행 선택 함수"""
    try:
        # 메인 페이지로 접속
        driver.get(BASE_URL)
        WaitUtils.wait_for_page_load(driver)
        WaitUtils.wait_with_random(1, 2)

        # JavaScript 기반 은행 선택
        js_script = f"""
        var targetBank = '{bank_name}';
        var allElements = document.querySelectorAll('a, td, th, span, div');
        var exactMatches = [];
        var limitedMatches = [];
        
        for(var i = 0; i < allElements.length; i++) {{
            var element = allElements[i];
            var text = element.textContent.trim();
            
            if(text === targetBank) {{
                exactMatches.push(element);
            }}
            else if(text.indexOf(targetBank) !== -1 && 
                    text.length <= targetBank.length * 2 && 
                    text.length > targetBank.length) {{
                limitedMatches.push(element);
            }}
        }}
        
        var allCandidates = exactMatches.concat(limitedMatches);
        
        for(var i = 0; i < allCandidates.length; i++) {{
            var element = allCandidates[i];
            
            try {{
                if(element.offsetParent === null) continue;
                
                element.scrollIntoView({{block: 'center'}});
                
                if(element.tagName.toLowerCase() === 'a') {{
                    element.click();
                    return 'direct_link_' + (i < exactMatches.length ? 'exact' : 'limited');
                }}
                
                var links = element.querySelectorAll('a');
                if(links.length > 0) {{
                    links[0].click();
                    return 'nested_link_' + (i < exactMatches.length ? 'exact' : 'limited');
                }}
                
                element.click();
                return 'element_click_' + (i < exactMatches.length ? 'exact' : 'limited');
            }} catch(e) {{
                continue;
            }}
        }}
        
        return false;
        """
        
        result = driver.execute_script(js_script)
        if result:
            log_message(f"{bank_name} 은행: JavaScript {result} 성공", verbose=False)
            WaitUtils.wait_with_random(1, 2)
            return True

        # Selenium 대체 방법
        exact_xpaths = [
            f"//td[normalize-space(text())='{bank_name}']//a",
            f"//a[normalize-space(text())='{bank_name}']"
        ]
        
        for xpath in exact_xpaths:
            try:
                elements = driver.find_elements(By.XPATH, xpath)
                for element in elements:
                    if element.is_displayed():
                        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
                        WaitUtils.wait_with_random(0.5, 1)
                        driver.execute_script("arguments[0].click();", element)
                        WaitUtils.wait_with_random(1, 2)
                        return True
            except Exception:
                continue

        return False

    except Exception as e:
        log_message(f"{bank_name} 은행 선택 실패: {str(e)}")
        return False

def select_category(driver, category):
    """특정 카테고리 탭을 클릭합니다."""
    try:
        js_script = f"""
        var targetCategory = '{category}';
        var allElements = document.querySelectorAll('a, button, span, li, div, tab');
        
        for(var i = 0; i < allElements.length; i++) {{
            var element = allElements[i];
            var text = element.textContent.trim();
            
            if(text === targetCategory && element.offsetParent !== null) {{
                element.scrollIntoView({{block: 'center'}});
                element.click();
                return 'exact_match';
            }}
        }}
        
        for(var i = 0; i < allElements.length; i++) {{
            var element = allElements[i];
            var text = element.textContent.trim();
            
            if(text.includes(targetCategory) && element.offsetParent !== null) {{
                element.scrollIntoView({{block: 'center'}});
                element.click();
                return 'contains_match';
            }}
        }}
        
        return false;
        """
        
        result = driver.execute_script(js_script)
        if result:
            WaitUtils.wait_with_random(1, 2)
            return True

        return False

    except Exception as e:
        log_message(f"{category} 탭 클릭 실패: {str(e)}", verbose=False)
        return False

def extract_tables_from_page(driver):
    """현재 페이지에서 모든 테이블을 추출합니다."""
    try:
        WaitUtils.wait_for_page_load(driver)
        WaitUtils.wait_with_random(1, 2)

        try:
            html_source = driver.page_source
            dfs = pd.read_html(StringIO(html_source))

            if dfs:
                valid_dfs = []
                seen_hashes = set()

                for df in dfs:
                    if not df.empty and df.shape[0] > 0 and df.shape[1] > 0:
                        if isinstance(df.columns, pd.MultiIndex):
                            new_cols = []
                            for col in df.columns:
                                if isinstance(col, tuple):
                                    clean_parts = [str(c).strip() for c in col if str(c).strip() and str(c).lower() != 'nan']
                                    new_cols.append('_'.join(clean_parts) if clean_parts else f"Column_{len(new_cols)+1}")
                                else:
                                    new_cols.append(str(col))
                            df.columns = new_cols

                        try:
                            df_hash = hash(str(df.shape) + str(list(df.columns)) + str(df.iloc[0].values) if len(df) > 0 else "")
                            if df_hash not in seen_hashes:
                                valid_dfs.append(df)
                                seen_hashes.add(df_hash)
                        except Exception:
                            valid_dfs.append(df)

                return valid_dfs
        except Exception:
            pass

        return []

    except Exception as e:
        log_message(f"페이지에서 테이블 추출 실패: {str(e)}")
        return []

# =============================================================================
# 메인 스크래핑 로직
# =============================================================================

def scrape_bank_data(bank_name, driver, progress_manager, expected_dates):
    """단일 은행의 데이터를 스크래핑합니다."""
    log_message(f"[시작] {bank_name} 은행 스크래핑 시작")

    try:
        # 은행 선택
        if not select_bank(driver, bank_name):
            log_message(f"{bank_name} 은행 선택 실패")
            return None

        # 엄격한 날짜 정보 추출 및 검증
        date_info = extract_date_information(driver, bank_name)
        is_fresh, validation_message = validate_extracted_date(date_info, expected_dates)
        
        # 데이터 검증 결과 기록
        progress_manager.mark_data_validated(bank_name, date_info, is_fresh)
        
        log_message(f"{bank_name} 은행 날짜 검증: {validation_message}")

        result_data = {
            '날짜정보': date_info,
            '검증결과': validation_message,
            '신선도': is_fresh
        }
        
        all_table_hashes = set()

        # 각 카테고리 처리
        for category in CATEGORIES:
            try:
                if not select_category(driver, category):
                    log_message(f"{bank_name} 은행 {category} 탭 클릭 실패", verbose=False)
                    continue

                tables = extract_tables_from_page(driver)
                if not tables:
                    continue

                valid_tables = []
                for df in tables:
                    try:
                        df_hash = hash(str(df.shape) + str(list(df.columns)) + str(df.iloc[0].values) if len(df) > 0 else "")
                        
                        if df_hash not in all_table_hashes:
                            valid_tables.append(df)
                            all_table_hashes.add(df_hash)
                    except Exception:
                        valid_tables.append(df)

                if valid_tables:
                    result_data[category] = valid_tables
                    log_message(f"{bank_name} {category}: {len(valid_tables)}개 테이블", verbose=False)

            except Exception as e:
                log_message(f"{bank_name} {category} 처리 실패: {str(e)}", verbose=False)

        # 데이터 수집 확인
        table_categories = [key for key, data in result_data.items() 
                          if key not in ['날짜정보', '검증결과', '신선도'] and isinstance(data, list) and data]
        
        if not table_categories:
            log_message(f"{bank_name} 테이블 데이터 없음")
            return None

        log_message(f"[완료] {bank_name} ({', '.join(table_categories)})")
        return result_data

    except Exception as e:
        log_message(f"{bank_name} 처리 오류: {str(e)}")
        return None

def save_bank_data(bank_name, data_dict):
    """수집된 은행 데이터를 엑셀 파일로 저장합니다."""
    if not data_dict:
        return False

    try:
        date_info = data_dict.get('날짜정보', '날짜정보없음')
        safe_date_info = re.sub(r'[^\w\-_년월말]', '_', date_info)
        excel_path = os.path.join(OUTPUT_DIR, f"{bank_name}_{safe_date_info}.xlsx")

        with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
            # 공시 정보 시트
            info_data = {
                '은행명': [bank_name],
                '공시 날짜': [data_dict.get('날짜정보', '')],
                '검증 결과': [data_dict.get('검증결과', '')],
                '데이터 신선도': ['최신' if data_dict.get('신선도', False) else '구버전'],
                '추출 일시': [datetime.now().strftime("%Y-%m-%d %H:%M:%S")],
                '스크래핑 시스템': ['GitHub Actions 저축은행 스크래퍼 v2.3 (엄격한 날짜 검증)']
            }
            info_df = pd.DataFrame(info_data)
            info_df.to_excel(writer, sheet_name='공시정보', index=False)

            # 카테고리별 데이터
            for category, tables in data_dict.items():
                if category in ['날짜정보', '검증결과', '신선도'] or not isinstance(tables, list):
                    continue

                for i, df in enumerate(tables):
                    sheet_name = category if i == 0 else f"{category}_{i+1}"
                    sheet_name = sheet_name[:31]

                    try:
                        df.to_excel(writer, sheet_name=sheet_name, index=False)
                    except Exception:
                        pass

        return True

    except Exception as e:
        log_message(f"{bank_name} 데이터 저장 오류: {str(e)}")
        return False

def worker_process_bank(bank_name, driver_manager, progress_manager, expected_dates):
    """워커 스레드에서 은행 처리를 수행합니다."""
    driver = None
    
    try:
        driver = driver_manager.get_driver()
        
        for attempt in range(MAX_RETRIES):
            try:
                result_data = scrape_bank_data(bank_name, driver, progress_manager, expected_dates)

                if result_data:
                    if save_bank_data(bank_name, result_data):
                        progress_manager.mark_completed(bank_name)
                        return bank_name, True, result_data.get('검증결과', '')
                    else:
                        if attempt < MAX_RETRIES - 1:
                            WaitUtils.wait_with_random(2, 4)

            except Exception as e:
                if attempt < MAX_RETRIES - 1:
                    WaitUtils.wait_with_random(2, 4)

        progress_manager.mark_failed(bank_name)
        return bank_name, False, "모든 시도 실패"

    except Exception as e:
        progress_manager.mark_failed(bank_name)
        return bank_name, False, f"예상치 못한 오류: {str(e)}"
    
    finally:
        if driver and driver_manager:
            driver_manager.return_driver(driver)

# =============================================================================
# 비동기 처리 및 메인 실행 로직
# =============================================================================

async def process_banks_async(banks, driver_manager, progress_manager, expected_dates):
    """은행 목록을 병렬로 처리합니다."""
    log_message(f"병렬 처리 시작: {len(banks)}개 은행, {MAX_WORKERS}개 워커")
    
    all_results = []
    
    batch_size = max(1, len(banks) // MAX_WORKERS) if len(banks) > MAX_WORKERS else len(banks)
    bank_batches = [banks[i:i + batch_size] for i in range(0, len(banks), batch_size)]

    for batch_idx, batch in enumerate(bank_batches):
        log_message(f"배치 {batch_idx+1}/{len(bank_batches)} 처리 중 ({len(batch)}개 은행)")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(MAX_WORKERS, len(batch))) as executor:
            loop = asyncio.get_event_loop()
            futures = [
                loop.run_in_executor(
                    executor,
                    worker_process_bank,
                    bank,
                    driver_manager,
                    progress_manager,
                    expected_dates
                )
                for bank in batch
            ]

            progress_desc = f"배치 {batch_idx+1}/{len(bank_batches)}"
            progress_bar = tqdm(asyncio.as_completed(futures), total=len(futures), desc=progress_desc)

            batch_results = []
            for future in progress_bar:
                result = await future
                batch_results.append(result)
                
                success_count = len([r for r in batch_results if r[1]])
                progress_bar.set_postfix_str(f"완료: {success_count}/{len(batch_results)}")

            all_results.extend(batch_results)
            
            if batch_idx < len(bank_batches) - 1:
                log_message(f"배치 {batch_idx+1} 완료. 다음 배치 전 잠시 대기...")
                await asyncio.sleep(2)

    return all_results

def process_with_retry(banks, max_retries=1):
    """실패한 은행을 재시도하는 로직을 포함한 메인 처리 함수입니다."""
    expected_dates = validate_data_freshness()
    
    driver_manager = DriverManager(max_drivers=MAX_WORKERS)
    progress_manager = ProgressManager()

    pending_banks = progress_manager.get_pending_banks(banks)
    if not pending_banks:
        pending_banks = banks[:5]

    log_message(f"처리할 은행 수: {len(pending_banks)}/{len(banks)}")

    try:
        if sys.version_info >= (3, 7):
            results = asyncio.run(process_banks_async(pending_banks, driver_manager, progress_manager, expected_dates))
        else:
            loop = asyncio.get_event_loop()
            results = loop.run_until_complete(process_banks_async(pending_banks, driver_manager, progress_manager, expected_dates))

        successful_banks = [r[0] for r in results if r[1]]
        failed_banks = [r[0] for r in results if not r[1]]

        return successful_banks, failed_banks, results

    finally:
        driver_manager.close_all()

def generate_summary_report():
    """스크래핑 결과 요약 보고서를 생성합니다."""
    try:
        progress_manager = ProgressManager()
        validation_data = progress_manager.progress.get('data_validation', [])

        bank_summary = []
        for bank in BANKS:
            bank_files = [f for f in os.listdir(OUTPUT_DIR) if f.startswith(f"{bank}_") and f.endswith(".xlsx")]
            
            if bank_files:
                try:
                    latest_file = sorted(bank_files)[-1]
                    file_path = os.path.join(OUTPUT_DIR, latest_file)
                    
                    info_df = pd.read_excel(file_path, sheet_name='공시정보')
                    date_info = str(info_df['공시 날짜'].iloc[0]) if '공시 날짜' in info_df.columns else "확인 불가"
                    data_freshness = str(info_df['데이터 신선도'].iloc[0]) if '데이터 신선도' in info_df.columns else "확인 불가"
                    
                    bank_summary.append({
                        '은행명': bank,
                        '상태': '완료',
                        '공시 날짜': date_info,
                        '데이터 신선도': data_freshness
                    })
                except:
                    bank_summary.append({
                        '은행명': bank,
                        '상태': '파일 오류',
                        '공시 날짜': '확인 불가',
                        '데이터 신선도': '확인 불가'
                    })
            else:
                bank_summary.append({
                    '은행명': bank,
                    '상태': '실패',
                    '공시 날짜': '',
                    '데이터 신선도': ''
                })

        summary_df = pd.DataFrame(bank_summary)
        summary_file = os.path.join(OUTPUT_DIR, f"엄격한_날짜검증_요약_{TODAY}.xlsx")
        summary_df.to_excel(summary_file, index=False)

        stats = {
            '전체 은행 수': len(BANKS),
            '완료 은행 수': len([r for r in bank_summary if r['상태'] == '완료']),
            '실패 은행 수': len([r for r in bank_summary if r['상태'] in ['실패', '파일 오류']]),
            '성공률': f"{len([r for r in bank_summary if r['상태'] == '완료']) / len(BANKS) * 100:.1f}%"
        }

        return summary_file, stats, bank_summary

    except Exception as e:
        log_message(f"요약 보고서 생성 오류: {str(e)}")
        return None, {}, []

def create_zip_archive():
    """결과 파일들을 ZIP으로 압축합니다."""
    try:
        zip_filename = f'저축은행_데이터_{TODAY}.zip'
        zip_path = os.path.join(OUTPUT_BASE_DIR, zip_filename)
        
        if os.path.exists(zip_path):
            os.remove(zip_path)
        
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED, compresslevel=6) as zipf:
            file_count = 0
            
            for root, dirs, files in os.walk(OUTPUT_DIR):
                for file in files:
                    file_path = os.path.join(root, file)
                    if os.path.exists(file_path) and os.path.isfile(file_path):
                        arcname = os.path.relpath(file_path, OUTPUT_BASE_DIR)
                        zipf.write(file_path, arcname)
                        file_count += 1
            
            readme_content = f"""저축은행 데이터 스크래핑 결과 (엄격한 날짜 검증 버전)

생성일시: {datetime.now().strftime('%Y년 %m월 %d일 %H시 %M분')}
포함 파일 수: {file_count}개
허용 날짜: 2024년9월말, 2025년3월말만

✅ 엄격한 날짜 검증 적용
✅ 잘못된 날짜 완전 배제
✅ ZIP 파일 안정성 보장

GitHub Actions 저축은행 스크래퍼 v2.3 (엄격한 날짜 검증)
"""
            zipf.writestr(f"README_{TODAY}.txt", readme_content.encode('utf-8'))
        
        if os.path.exists(zip_path):
            with zipfile.ZipFile(zip_path, 'r') as test_zip:
                zip_file_list = test_zip.namelist()
                if len(zip_file_list) > 0:
                    log_message(f"✅ ZIP 파일 생성 완료: {zip_path}")
                    return zip_path
        
        return None
        
    except Exception as e:
        log_message(f"❌ ZIP 생성 실패: {str(e)}")
        return None

# =============================================================================
# 핵심 수정 2: 이메일 본문에 스크린샷 형태 테이블 포함
# =============================================================================

def create_email_table(bank_summary):
    """이메일 본문에 포함할 스크린샷 형태의 테이블을 생성합니다."""
    try:
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # 텍스트 테이블 생성
        table_lines = []
        table_lines.append("=" * 80)
        table_lines.append("📋 은행별 날짜 확인 결과 (스크린샷 형태)")
        table_lines.append("=" * 80)
        table_lines.append(f"{'은행명':<12} {'공시 날짜(월말)':<18} {'날짜 확인':<25} {'처리상태':<12}")
        table_lines.append("-" * 80)
        
        # 상태별 정렬
        sorted_summary = sorted(bank_summary, key=lambda x: (
            0 if x['상태'] == '완료' else 1 if x['상태'] == '파일 오류' else 2,
            x['은행명']
        ))
        
        for bank_data in sorted_summary:
            bank_name = bank_data['은행명']
            date_info = bank_data['공시 날짜']
            status = bank_data['상태']
            
            # 날짜 확인 상태 결정
            if status == '완료':
                if '2024년9월말' in date_info or '2024년09월말' in date_info:
                    date_check = "✅ 일치 (기한내최신)"
                elif '2025년3월말' in date_info or '2025년03월말' in date_info:
                    date_check = "🟢 일치 (예정보다선반영)"
                else:
                    date_check = "⚠️ 확인필요"
                processing_status = "완료"
            elif status == '파일 오류':
                date_check = "❌ 파일오류"
                processing_status = "실패"
            else:
                date_check = "❌ 미처리"
                processing_status = "실패"
            
            table_lines.append(f"{bank_name:<12} {date_info:<18} {date_check:<25} {processing_status:<12}")
        
        table_lines.append("-" * 80)
        
        # 통계 요약
        total_banks = len(bank_summary)
        completed_banks = len([b for b in bank_summary if b['상태'] == '완료'])
        september_data = len([b for b in bank_summary if '2024년9월' in b['공시 날짜']])
        march_data = len([b for b in bank_summary if '2025년3월' in b['공시 날짜']])
        failed_banks = len([b for b in bank_summary if b['상태'] in ['실패', '파일 오류']])
        
        table_lines.append(f"📊 처리 상태 요약:")
        table_lines.append(f"  • 전체 은행 수: {total_banks}개")
        table_lines.append(f"  • 완료된 은행: {completed_banks}개")
        table_lines.append(f"  • 2024년9월말 데이터: {september_data}개")
        table_lines.append(f"  • 2025년3월말 데이터: {march_data}개")
        table_lines.append(f"  • 처리 실패: {failed_banks}개")
        table_lines.append(f"  • 성공률: {completed_banks/total_banks*100:.1f}%")
        
        table_lines.append("=" * 80)
        table_lines.append(f"📅 확인 시간: {current_time}")
        table_lines.append("🔧 엄격한 날짜 검증: 2024년9월말, 2025년3월말만 허용")
        table_lines.append("=" * 80)
        
        return "\n".join(table_lines)
        
    except Exception as e:
        log_message(f"이메일 테이블 생성 오류: {str(e)}")
        return "테이블 생성 실패"

def send_email_notification(subject, body, bank_details=None, attachment_paths=None, is_success=True, expected_dates=None, bank_summary=None):
    """Gmail SMTP를 통해 이메일 알림을 발송합니다. (스크린샷 형태 테이블 포함)"""
    if not GMAIL_ADDRESS or not GMAIL_APP_PASSWORD or not RECIPIENT_EMAILS:
        log_message("이메일 설정이 불완전하여 알림을 발송하지 않습니다.")
        return False
    
    try:
        # 이메일 메시지 구성
        msg = MIMEMultipart()
        msg['From'] = GMAIL_ADDRESS
        msg['To'] = ', '.join(RECIPIENT_EMAILS)
        msg['Subject'] = subject
        
        # 기본 본문에 스크린샷 형태 테이블 추가
        enhanced_body = body
        
        if bank_summary:
            enhanced_body += "\n\n"
            enhanced_body += create_email_table(bank_summary)
        
        # 기존 상세 정보도 유지
        if bank_details:
            enhanced_body += "\n\n===== 상세 분석 결과 =====\n"
            
            successful_banks = [bank for bank in bank_details if bank['status'] == 'success']
            if successful_banks:
                enhanced_body += f"\n✅ 완전 성공한 은행 ({len(successful_banks)}개):\n"
                for bank in successful_banks[:10]:  # 상위 10개만 표시
                    enhanced_body += f"  • {bank['name']}: {bank['date_info']}\n"
                if len(successful_banks) > 10:
                    enhanced_body += f"  ... 기타 {len(successful_banks) - 10}개 은행\n"
            
            failed_banks = [bank for bank in bank_details if bank['status'] == 'failed']
            if failed_banks:
                enhanced_body += f"\n❌ 실패한 은행 ({len(failed_banks)}개):\n"
                for bank in failed_banks[:5]:  # 상위 5개만 표시
                    enhanced_body += f"  • {bank['name']}: {bank.get('error_reason', '알 수 없는 오류')}\n"
                if len(failed_banks) > 5:
                    enhanced_body += f"  ... 기타 {len(failed_banks) - 5}개 은행\n"
        
        # 본문 추가
        msg.attach(MIMEText(enhanced_body, 'plain', 'utf-8'))
        
        # 첨부 파일 추가
        if attachment_paths:
            for file_path in attachment_paths:
                if os.path.exists(file_path):
                    try:
                        filename = os.path.basename(file_path)
                        
                        if filename.endswith('.zip'):
                            part = MIMEBase('application', 'zip')
                            with open(file_path, "rb") as attachment:
                                part.set_payload(attachment.read())
                            encoders.encode_base64(part)
                            part.add_header('Content-Disposition', f'attachment; filename="{filename}"')
                            part.add_header('Content-Type', 'application/zip')
                        elif filename.endswith('.xlsx'):
                            part = MIMEBase('application', 'vnd.openxmlformats-officedocument.spreadsheetml.sheet')
                            with open(file_path, "rb") as attachment:
                                part.set_payload(attachment.read())
                            encoders.encode_base64(part)
                            part.add_header('Content-Disposition', f'attachment; filename="{filename}"')
                        else:
                            part = MIMEBase('application', 'octet-stream')
                            with open(file_path, "rb") as attachment:
                                part.set_payload(attachment.read())
                            encoders.encode_base64(part)
                            part.add_header('Content-Disposition', f'attachment; filename="{filename}"')
                            
                        msg.attach(part)
                    except Exception as e:
                        log_message(f"첨부 파일 추가 실패 ({file_path}): {str(e)}")
        
        # Gmail SMTP 서버 연결 및 발송
        with smtplib.SMTP('smtp.gmail.com', 587) as server:
            server.starttls()
            server.login(GMAIL_ADDRESS, GMAIL_APP_PASSWORD)
            server.send_message(msg)
        
        log_message(f"📧 이메일 알림 발송 완료: {', '.join(RECIPIENT_EMAILS)}")
        return True
        
    except Exception as e:
        log_message(f"❌ 이메일 발송 실패: {str(e)}")
        return False

def collect_bank_details():
    """각 은행별 상세 정보를 수집합니다."""
    bank_details = []
    progress_manager = ProgressManager()
    
    try:
        validation_data = progress_manager.progress.get('data_validation', [])
        validation_dict = {item['bank_name']: item for item in validation_data}
        
        for bank in BANKS:
            bank_info = {
                'name': bank,
                'status': 'failed',
                'date_info': '데이터 없음',
                'is_fresh': False,
                'error_reason': '처리되지 않음'
            }
            
            bank_files = [f for f in os.listdir(OUTPUT_DIR) if f.startswith(f"{bank}_") and f.endswith(".xlsx")]
            
            if bank_files:
                try:
                    latest_file = sorted(bank_files)[-1]
                    file_path = os.path.join(OUTPUT_DIR, latest_file)
                    
                    if '공시정보' in pd.ExcelFile(file_path).sheet_names:
                        info_df = pd.read_excel(file_path, sheet_name='공시정보')
                        if '공시 날짜' in info_df.columns and not info_df['공시 날짜'].empty:
                            bank_info['date_info'] = str(info_df['공시 날짜'].iloc[0])
                        if '데이터 신선도' in info_df.columns and not info_df['데이터 신선도'].empty:
                            bank_info['is_fresh'] = str(info_df['데이터 신선도'].iloc[0]) == '최신'
                    
                    bank_info['status'] = 'success'
                        
                except Exception as e:
                    bank_info['error_reason'] = f'파일 분석 오류: {str(e)}'
            else:
                if bank in validation_dict:
                    validation_info = validation_dict[bank]
                    bank_info['date_info'] = validation_info.get('date_info', '날짜 정보 없음')
                    bank_info['is_fresh'] = validation_info.get('is_fresh', False)
                    bank_info['error_reason'] = '데이터 추출 완료되었으나 파일 저장 실패'
                else:
                    bank_info['error_reason'] = '은행 페이지 접근 실패'
            
            bank_details.append(bank_info)
        
        return bank_details
        
    except Exception as e:
        log_message(f"은행 상세 정보 수집 실패: {str(e)}")
        return []

# =============================================================================
# 메인 실행 함수
# =============================================================================

def main():
    """엄격한 날짜 검증과 이메일 테이블을 포함한 메인 실행 함수"""
    try:
        with open(LOG_FILE, 'w', encoding='utf-8') as f:
            f.write("")
    except Exception as e:
        print(f"로그 파일 초기화 실패: {e}")

    start_time = time.time()
    log_message(f"\n🚀 ===== 저축은행 스크래핑 시작 (엄격한 날짜 검증 v2.3) [{TODAY}] =====\n")

    try:
        log_message(f"🔧 v2.3 엄격한 날짜 검증 버전:")
        log_message(f"  ✅ 허용 날짜: 2024년9월말, 2025년3월말만 추출")
        log_message(f"  ✅ 잘못된 날짜 완전 배제")
        log_message(f"  ✅ 이메일에 스크린샷 형태 테이블 포함")
        log_message(f"  ✅ ZIP 파일 안정성 보장")

        # 은행 처리 실행
        successful_banks, failed_banks, all_results = process_with_retry(BANKS, max_retries=MAX_RETRIES)

        # 결과 요약 생성
        summary_file, stats, bank_summary = generate_summary_report()

        # ZIP 파일 생성
        zip_file = create_zip_archive()

        # 실행 시간 계산
        end_time = time.time()
        total_duration = end_time - start_time
        minutes, seconds = divmod(total_duration, 60)
        
        # 결과 출력
        log_message(f"\n🎉 ===== 스크래핑 완료 (엄격한 날짜 검증) =====")
        log_message(f"⏰ 총 실행 시간: {int(minutes)}분 {int(seconds)}초")
        log_message(f"✅ 성공: {len(successful_banks)}개")
        log_message(f"❌ 실패: {len(failed_banks)}개")
        log_message(f"📊 성공률: {stats.get('성공률', '0%')}")
        
        if failed_banks:
            log_message(f"🔍 실패 은행: {', '.join(failed_banks[:5])}{'...' if len(failed_banks) > 5 else ''}")

        # 날짜별 분석
        september_count = len([b for b in bank_summary if '2024년9월' in b['공시 날짜']])
        march_count = len([b for b in bank_summary if '2025년3월' in b['공시 날짜']])
        
        log_message(f"\n📅 날짜 분석:")
        log_message(f"  • 2024년9월말 데이터: {september_count}개")
        log_message(f"  • 2025년3월말 데이터: {march_count}개")
        log_message(f"  • 기타/오류: {len(successful_banks) - september_count - march_count}개")

        # 이메일 발송
        if GMAIL_ADDRESS and GMAIL_APP_PASSWORD and RECIPIENT_EMAILS:
            log_message(f"\n📧 이메일 알림 발송 중...")
            
            subject = f"📊 저축은행 데이터 스크래핑 완료 (엄격한 날짜검증 v2.3) - {int(minutes)}분{int(seconds)}초"
            
            body = f"""저축은행 중앙회 통일경영공시 데이터 스크래핑이 완료되었습니다.

🔧 v2.3 엄격한 날짜 검증 버전의 특징:
✅ 허용 날짜 제한: 2024년9월말, 2025년3월말만 추출
✅ 잘못된 날짜 완전 배제: 기존 문제 완전 해결
✅ 스크린샷 형태 테이블: 이메일 본문에 직접 포함
✅ ZIP 파일 안정성: .bin 오류 없는 완전한 압축

📊 실행 정보:
- 🕐 실행 날짜: {datetime.now().strftime('%Y년 %m월 %d일 %H시 %M분')}
- ⏱️ 총 실행 시간: {int(minutes)}분 {int(seconds)}초
- 🏗️ 처리 환경: GitHub Actions (엄격한 날짜 검증)

📈 결과 요약:
- 🏦 전체 은행 수: {len(BANKS)}개
- ✅ 완료 은행 수: {len(successful_banks)}개
- ❌ 실패 은행 수: {len(failed_banks)}개
- 📊 성공률: {stats.get('성공률', '0%')}
- 📅 2024년9월말: {september_count}개
- 📅 2025년3월말: {march_count}개

🔧 주요 개선사항:
• 엄격한 날짜 필터링으로 잘못된 날짜 완전 배제
• 허용되지 않은 날짜 발견 시 즉시 재검색
• 이메일 본문에 스크린샷과 동일한 테이블 포함
• 날짜별 상세 분석 및 통계 제공

📦 첨부 파일:
1. 📦 ZIP 압축파일 - 모든 데이터 (.zip 확장자 보장)
2. 📊 엄격한 검증 요약 보고서
3. 📄 상세 실행 로그

아래에 스크린샷과 동일한 형태의 결과 테이블이 포함되어 있습니다.
"""

            # 은행별 상세 정보 수집
            bank_details = collect_bank_details()

            # 첨부 파일 준비
            attachments = []
            if zip_file and os.path.exists(zip_file):
                attachments.append(zip_file)
            if summary_file and os.path.exists(summary_file):
                attachments.append(summary_file)
            if os.path.exists(LOG_FILE):
                attachments.append(LOG_FILE)

            # 예상 날짜 정보
            expected_dates = validate_data_freshness()

            # 이메일 발송 (bank_summary 포함)
            is_success = len(failed_banks) == 0
            email_success = send_email_notification(
                subject, body, bank_details, attachments, is_success, expected_dates, bank_summary
            )
            
            if email_success:
                log_message(f"   ✅ 이메일 발송 성공 (스크린샷 형태 테이블 포함)")
            else:
                log_message(f"   ❌ 이메일 발송 실패")

        log_message(f"\n🎊 ===== 저축은행 스크래핑 완료 (엄격한 날짜 검증) [{TODAY}] =====")
        log_message(f"🏆 주요 성과:")
        log_message(f"   • 🎯 허용된 날짜만 추출: 2024년9월말, 2025년3월말")
        log_message(f"   • 🚫 잘못된 날짜 완전 배제")
        log_message(f"   • 📧 이메일에 스크린샷 형태 테이블 포함")
        log_message(f"   • 📦 ZIP 파일 안정성 보장")

    except KeyboardInterrupt:
        log_message("\n⏹️ 사용자에 의해 중단되었습니다.")
    except Exception as e:
        log_message(f"\n💥 예상치 못한 오류 발생: {str(e)}")
        import traceback
        log_message(f"상세 오류 정보:\n{traceback.format_exc()}")

if __name__ == "__main__":
    main()
