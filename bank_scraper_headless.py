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
    log_entry = f"[{timestamp}] {message}"

    try:
        with open(LOG_FILE, 'a', encoding='utf-8') as f:
            f.write(log_entry + '\n')
    except Exception as e:
        print(f"로그 파일 쓰기 실패: {e}")

    if print_to_console:
        print(message)

def validate_data_freshness():
    """사용자가 지정한 목표 데이터 분기를 반환합니다."""
    # 사용자 요청: '2024년 9월말' (또는 09월말)과 '2025년 3월말' (또는 03월말)
    # 정규화된 형태로 저장하여 이후 비교 시 일관성 유지
    possible_dates = ["2024년9월말", "2025년3월말"]
    log_message(f"사용자 지정 목표 데이터 분기: {', '.join(possible_dates)}")
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
                    'images': 1,      # 이미지 로딩 허용
                    'plugins': 2,       # 플러그인 차단
                    'javascript': 1,    # JavaScript 허용 (필수)
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
# 진행 상황 관리 클래스 (기존과 동일)
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

    def is_completed(self, bank_name):
        """특정 은행의 스크래핑이 완료되었는지 확인합니다."""
        return bank_name in self.progress.get('completed', [])

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
# 웹 스크래핑 유틸리티 클래스 (기존과 동일)
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
# 핵심 수정 1: 스마트 날짜 추출 (사용자 지정 날짜 우선)
# =============================================================================

def extract_date_information(driver, bank_name=None):
    """웹페이지에서 공시 날짜 정보를 추출합니다. 사용자가 지정한 날짜 ('2024년9월말', '2025년3월말') 우선."""
    # 사용자가 원하는 목표 날짜 (정규화된 형태)
    target_dates_normalized = {"2024년9월말", "2025년3월말"}

    def normalize_date_string(date_str):
        # 공백 제거, "09월" -> "9월" 등으로 정규화
        s = re.sub(r'\s+', '', date_str)
        s = s.replace('년0', '년') # 예: 2024년09월말 -> 2024년9월말
        return s

    try:
        page_source = driver.page_source
        found_target_date = None

        # 1. 기본 빠른 추출 (정규 표현식) - 페이지 소스 전체에서 검색
        date_candidates_text = re.findall(r'\d{4}년\s*\d{1,2}월말', page_source)
        
        if date_candidates_text:
            for date_text in date_candidates_text:
                normalized_candidate = normalize_date_string(date_text)
                if normalized_candidate in target_dates_normalized:
                    log_message(f"{bank_name or '은행'} 기본 추출 (타겟 일치): {normalized_candidate}", verbose=False)
                    found_target_date = normalized_candidate
                    break 
            if found_target_date:
                return found_target_date # 정규화된 타겟 날짜 반환

        # 2. 문제 은행이거나 위에서 못 찾은 경우, 더 정밀한 검색 시도
        #    (JavaScript를 사용하여 페이지의 모든 텍스트 노드에서 검색)
        log_message(f"{bank_name or '은행'} 정밀 날짜 검색 시도 (타겟: {target_dates_normalized})", verbose=False)
        
        # JavaScript를 사용한 전체 텍스트 검색
        try:
            js_script_all_text = """
            var allText = document.body.innerText;
            var matches = allText.match(/\\d{4}년\\s*\\d{1,2}월말/g);
            var targetDates = ["2024년9월말", "2025년3월말"]; 
            var found = "";
            if (matches) {
                for (var i = 0; i < matches.length; i++) {
                    var rawMatch = matches[i];
                    var normalized = rawMatch.replace(/\\s+/g, '').replace('년0','년');
                    if (targetDates.includes(normalized)) {
                        found = normalized; // 타겟 날짜를 찾으면 저장
                        break; 
                    }
                }
            }
            return found; // 찾은 타겟 날짜 또는 빈 문자열 반환
            """
            js_result = driver.execute_script(js_script_all_text)
            if js_result: 
                log_message(f"{bank_name or '은행'} JavaScript 검색 (타겟 일치): {js_result}", verbose=False)
                return js_result # 이미 정규화된 타겟 날짜
        except Exception as e_js:
            log_message(f"{bank_name or '은행'} JavaScript 검색 실패: {str(e_js)}", verbose=False)

        # 3. (선택적) 기존의 선택자 기반 정밀 검색 (위 방법들이 실패했을 경우 대비)
        #    이 부분은 중복될 수 있으나, 특정 구조에 의존하는 경우를 위해 남겨둘 수 있음
        if bank_name in PROBLEM_BANKS: # 문제 은행에 대해서만 추가 시도
            priority_selectors = [
                "//h1", "//h2", "//h3", "//h4", "//h5", "//h6",
                "//p", "//span", "//div",
                "//th", "//td"
            ]
            for selector in priority_selectors:
                try:
                    elements = driver.find_elements(By.XPATH, selector)
                    for element in elements:
                        try:
                            text = element.text.strip()
                            if not text: continue
                            element_date_matches = re.findall(r'\d{4}년\s*\d{1,2}월말', text)
                            for match_text in element_date_matches:
                                normalized_match = normalize_date_string(match_text)
                                if normalized_match in target_dates_normalized:
                                    log_message(f"{bank_name} 선택자 기반 검색 (타겟 일치 - {selector}): {normalized_match}", verbose=False)
                                    return normalized_match
                        except StaleElementReferenceException:
                            continue
                except Exception:
                    continue
        
        log_message(f"{bank_name or '은행'}에서 지정된 날짜 정보를 찾을 수 없음 (타겟: {target_dates_normalized})", verbose=True)
        return "날짜 정보 없음"

    except Exception as e:
        log_message(f"날짜 정보 추출 중 전체 오류 ({bank_name or '은행'}): {str(e)}", verbose=True)
        return "날짜 추출 실패"


def validate_extracted_date(extracted_date, expected_dates):
    """추출된 날짜가 예상 날짜와 일치하는지 검증합니다."""
    if not extracted_date or extracted_date in ["날짜 정보 없음", "날짜 추출 실패"]:
        return False, "날짜 정보를 추출할 수 없음"
    
    # 추출된 날짜는 extract_date_information에서 이미 정규화된 타겟 날짜 중 하나이거나 "날짜 정보 없음" 등임
    # expected_dates도 ["2024년9월말", "2025년3월말"] 형태
    
    if extracted_date in expected_dates:
        return True, f"최신 데이터 확인: {extracted_date}"
    
    # 혹시 모를 다른 형태의 유효한 날짜 문자열 (예: "2024년 09월말")이 extract_date_information에서
    # 정규화되지 않고 반환되었을 경우를 대비한 추가 검증 (하지만 위 함수에서 정규화된 값만 반환하도록 수정됨)
    normalized_extracted = re.sub(r'\s+', '', extracted_date).replace('년0', '년')
    for expected in expected_dates:
        if normalized_extracted == expected:
            return True, f"최신 데이터 확인 (추가 검증): {extracted_date}"
            
    return False, f"예상 날짜와 불일치: {extracted_date} (예상: {', '.join(expected_dates)})"


# 나머지 함수들은 기존과 동일하게 유지
def select_bank(driver, bank_name):
    """정확한 은행명 매칭을 위한 개선된 은행 선택 함수 (기존과 동일)"""
    try:
        # 메인 페이지로 접속
        driver.get(BASE_URL)
        WaitUtils.wait_for_page_load(driver)
        WaitUtils.wait_with_random(1, 2)

        # 단순하고 안전한 JavaScript 기반 은행 선택
        js_script = f"""
        var targetBank = '{bank_name}';
        var allElements = document.querySelectorAll('a, td, th, span, div');
        var exactMatches = [];
        var limitedMatches = [];
        
        // 모든 요소를 스캔하여 매칭 우선순위별로 분류
        for(var i = 0; i < allElements.length; i++) {{
            var element = allElements[i];
            var text = element.textContent.trim();
            
            // 1순위: 완전히 동일한 텍스트
            if(text === targetBank) {{
                exactMatches.push(element);
            }}
            // 2순위: 은행명을 포함하되 길이가 제한된 텍스트 (중복 방지)
            else if(text.indexOf(targetBank) !== -1 && 
                    text.length <= targetBank.length * 2 && 
                    text.length > targetBank.length) {{
                limitedMatches.push(element);
            }}
        }}
        
        // 정확한 매칭부터 우선 시도
        var allCandidates = exactMatches.concat(limitedMatches);
        
        for(var i = 0; i < allCandidates.length; i++) {{
            var element = allCandidates[i];
            
            try {{
                // 요소가 화면에 보이는지 확인
                if(element.offsetParent === null) continue;
                
                element.scrollIntoView({{block: 'center'}});
                
                // 링크인 경우 직접 클릭
                if(element.tagName.toLowerCase() === 'a') {{
                    element.click();
                    return 'direct_link_' + (i < exactMatches.length ? 'exact' : 'limited');
                }}
                
                // 링크가 아닌 경우 내부에서 링크 찾기
                var links = element.querySelectorAll('a');
                if(links.length > 0) {{
                    links[0].click();
                    return 'nested_link_' + (i < exactMatches.length ? 'exact' : 'limited');
                }}
                
                // 그 외의 경우 요소 자체 클릭
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

        # JavaScript 실패 시 Selenium 기반 대체 방법
        log_message(f"{bank_name} 은행: JavaScript 방법 실패, Selenium 시도", verbose=False)
        
        # 1단계: 정확한 텍스트 매칭
        exact_xpaths = [
            f"//td[normalize-space(text())='{bank_name}']//a",
            f"//a[normalize-space(text())='{bank_name}']",
            f"//td[text()='{bank_name}']//a",
            f"//a[text()='{bank_name}']"
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
                        log_message(f"{bank_name} 은행: Selenium 정확한 매칭 성공", verbose=False)
                        return True
            except Exception:
                continue

        # 2단계: 제한적 부분 매칭 (정확한 매칭 실패 시에만)
        log_message(f"{bank_name} 은행: 정확한 매칭 실패, 제한적 부분 매칭 시도", verbose=False)
        
        try:
            # 모든 테이블 셀과 링크를 가져와서 수동으로 필터링
            all_elements = driver.find_elements(By.XPATH, "//td | //a")
            
            for element in all_elements:
                try:
                    element_text = element.text.strip()
                    # 은행명을 포함하고, 길이가 적절한 범위 내에 있는 경우만 허용
                    if (bank_name in element_text and 
                        len(element_text) <= len(bank_name) * 2 and
                        len(element_text) > len(bank_name) and
                        element.is_displayed()):
                        
                        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
                        WaitUtils.wait_with_random(0.5, 1)
                        
                        # 링크인 경우 직접 클릭
                        if element.tag_name.lower() == 'a':
                            driver.execute_script("arguments[0].click();", element)
                        else:
                            # 링크가 아닌 경우 내부 링크 찾기
                            links = element.find_elements(By.TAG_NAME, "a")
                            if links:
                                driver.execute_script("arguments[0].click();", links[0])
                            else:
                                driver.execute_script("arguments[0].click();", element)
                        
                        WaitUtils.wait_with_random(1, 2)
                        log_message(f"{bank_name} 은행: Selenium 제한적 매칭 성공 (매칭된 텍스트: {element_text})", verbose=False)
                        return True
                        
                except Exception:
                    continue
                    
        except Exception:
            pass

        log_message(f"{bank_name} 은행을 찾을 수 없습니다.")
        return False

    except Exception as e:
        log_message(f"{bank_name} 은행 선택 실패: {str(e)}")
        return False

def select_category(driver, category):
    """특정 카테고리 탭을 클릭합니다. (기존과 동일)"""
    try:
        # JavaScript를 사용한 카테고리 선택
        js_script = f"""
        var targetCategory = '{category}';
        var allElements = document.querySelectorAll('a, button, span, li, div, tab');
        
        // 정확한 텍스트 매칭 우선
        for(var i = 0; i < allElements.length; i++) {{
            var element = allElements[i];
            var text = element.textContent.trim();
            
            if(text === targetCategory && element.offsetParent !== null) {{
                element.scrollIntoView({{block: 'center'}});
                element.click();
                return 'exact_match';
            }}
        }}
        
        // 포함 검색
        for(var i = 0; i < allElements.length; i++) {{
            var element = allElements[i];
            var text = element.textContent.trim();
            
            if(text.includes(targetCategory) && element.offsetParent !== null) {{
                element.scrollIntoView({{block: 'center'}});
                element.click();
                return 'contains_match';
            }}
        }}
        
        // 탭 컨테이너에서 인덱스 기반 검색
        var categoryIndex = {{'영업개황': 0, '재무현황': 1, '손익현황': 2, '기타': 3}};
        var index = categoryIndex[targetCategory];
        
        if(index !== undefined) {{
            var tabContainers = document.querySelectorAll('ul.tabs, .tab-container, nav, .tab-list, ul');
            for(var i = 0; i < tabContainers.length; i++) {{
                var tabs = tabContainers[i].querySelectorAll('a, li, button, span');
                if(tabs.length > index && tabs[index].offsetParent !== null) {{
                    tabs[index].scrollIntoView({{block: 'center'}});
                    tabs[index].click();
                    return 'index_match';
                }}
            }}
        }}
        
        return false;
        """
        
        result = driver.execute_script(js_script)
        if result:
            log_message(f"{category} 탭: JavaScript {result} 성공", verbose=False)
            WaitUtils.wait_with_random(1, 2)
            return True

        # Selenium을 사용한 대체 방법
        category_xpaths = [
            f"//a[normalize-space(text())='{category}']",
            f"//*[contains(text(), '{category}') and (self::a or self::button or self::span or self::li)]"
        ]

        for xpath in category_xpaths:
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

        log_message(f"{category} 탭을 찾을 수 없습니다.", verbose=False)
        return False

    except Exception as e:
        log_message(f"{category} 탭 클릭 실패: {str(e)}", verbose=False)
        return False

def extract_tables_from_page(driver):
    """현재 페이지에서 모든 테이블을 추출합니다. (기존과 동일)"""
    try:
        WaitUtils.wait_for_page_load(driver)
        WaitUtils.wait_with_random(1, 2)

        # pandas를 사용한 테이블 추출 시도
        try:
            html_source = driver.page_source
            dfs = pd.read_html(StringIO(html_source))

            if dfs:
                valid_dfs = []
                seen_hashes = set()

                for df in dfs:
                    if not df.empty and df.shape[0] > 0 and df.shape[1] > 0:
                        # MultiIndex 컬럼 처리
                        if isinstance(df.columns, pd.MultiIndex):
                            new_cols = []
                            for col in df.columns:
                                if isinstance(col, tuple):
                                    clean_parts = [str(c).strip() for c in col if str(c).strip() and str(c).lower() != 'nan']
                                    new_cols.append('_'.join(clean_parts) if clean_parts else f"Column_{len(new_cols)+1}")
                                else:
                                    new_cols.append(str(col))
                            df.columns = new_cols

                        # 중복 테이블 제거를 위한 해시 생성
                        try:
                            df_hash = hash(str(df.shape) + str(list(df.columns)) + str(df.iloc[0].values) if len(df) > 0 else "")
                            if df_hash not in seen_hashes:
                                valid_dfs.append(df)
                                seen_hashes.add(df_hash)
                        except Exception:
                            valid_dfs.append(df)

                return valid_dfs
        except Exception as e:
            log_message(f"pandas 테이블 추출 실패: {str(e)}", verbose=False)

        # BeautifulSoup을 사용한 대체 추출 방법
        try:
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            tables = soup.find_all('table')
            
            extracted_dfs = []
            seen_hashes = set()

            for table in tables:
                try:
                    # 헤더 추출
                    headers = []
                    th_elements = table.select('thead th') or table.select('tr:first-child th')
                    if th_elements:
                        headers = [th.get_text(strip=True) for th in th_elements]

                    # 헤더가 없으면 첫 번째 행의 td를 헤더로 사용
                    if not headers:
                        first_row_tds = table.select('tr:first-child td')
                        if first_row_tds:
                            headers = [td.get_text(strip=True) or f"Column_{i+1}" for i, td in enumerate(first_row_tds)]

                    # 데이터 행 추출
                    rows = []
                    for tr in table.select('tbody tr') or table.select('tr')[1:]:
                        cells = tr.select('td')
                        if cells:
                            row_data = [td.get_text(strip=True) for td in cells]
                            if row_data:
                                rows.append(row_data)

                    # DataFrame 생성
                    if rows and headers:
                        # 열 개수 맞추기
                        for i, row in enumerate(rows):
                            if len(row) < len(headers):
                                rows[i] = row + [''] * (len(headers) - len(row))
                            elif len(row) > len(headers):
                                rows[i] = row[:len(headers)]

                        df = pd.DataFrame(rows, columns=headers)
                        
                        if not df.empty:
                            # 중복 제거를 위한 해시 생성
                            try:
                                df_hash = hash(str(df.shape) + str(df.iloc[0].values) if len(df) > 0 else "")
                                if df_hash not in seen_hashes:
                                    extracted_dfs.append(df)
                                    seen_hashes.add(df_hash)
                            except Exception:
                                extracted_dfs.append(df)
                                
                except Exception as e:
                    log_message(f"개별 테이블 파싱 실패: {str(e)}", verbose=False)
                    continue

            return extracted_dfs

        except Exception as e:
            log_message(f"BeautifulSoup 테이블 추출 실패: {str(e)}", verbose=False)

        return []

    except Exception as e:
        log_message(f"페이지에서 테이블 추출 실패: {str(e)}")
        return []

# =============================================================================
# 메인 스크래핑 로직 (기존과 거의 동일, 날짜 추출 함수 호출 시 bank_name 전달)
# =============================================================================

def scrape_bank_data(bank_name, driver, progress_manager, expected_dates):
    """단일 은행의 데이터를 스크래핑합니다."""
    log_message(f"[시작] {bank_name} 은행 스크래핑 시작")

    try:
        # 은행 선택
        if not select_bank(driver, bank_name):
            log_message(f"{bank_name} 은행 선택 실패")
            return None

        # 현재 페이지 URL 확인
        try:
            base_bank_url = driver.current_url
            log_message(f"{bank_name} 은행 페이지 접속 성공", verbose=False)
        except Exception:
            log_message(f"{bank_name} 은행 페이지 URL 획득 실패")
            return None

        # 날짜 정보 추출 및 검증 (수정된 extract_date_information 사용)
        date_info = extract_date_information(driver, bank_name)
        is_fresh, validation_message = validate_extracted_date(date_info, expected_dates)
        
        # 데이터 검증 결과 기록
        progress_manager.mark_data_validated(bank_name, date_info, is_fresh)
        
        log_message(f"{bank_name} 은행 날짜 검증: {validation_message} (추출된 날짜: {date_info})")


        result_data = {
            '날짜정보': date_info,
            '검증결과': validation_message,
            '신선도': is_fresh
        }
        
        all_table_hashes = set()

        # 각 카테고리 처리
        for category in CATEGORIES:
            try:
                # 카테고리 탭 클릭
                if not select_category(driver, category):
                    log_message(f"{bank_name} 은행 {category} 탭 클릭 실패, 다음 카테고리로 진행")
                    continue

                # 테이블 추출
                tables = extract_tables_from_page(driver)
                if not tables:
                    log_message(f"{bank_name} 은행 {category} 카테고리에서 테이블을 찾을 수 없습니다.")
                    continue

                # 중복 제거된 유효 테이블 저장
                valid_tables = []
                for df in tables:
                    try:
                        # 테이블 해시 생성 (전역 중복 확인용)
                        df_hash = hash(str(df.shape) + str(list(df.columns)) + str(df.iloc[0].values) if len(df) > 0 else "")
                        
                        if df_hash not in all_table_hashes:
                            valid_tables.append(df)
                            all_table_hashes.add(df_hash)
                    except Exception:
                        valid_tables.append(df)

                # 유효한 테이블 저장
                if valid_tables:
                    result_data[category] = valid_tables
                    log_message(f"{bank_name} 은행 {category} 카테고리에서 {len(valid_tables)}개 테이블 추출")

            except Exception as e:
                log_message(f"{bank_name} 은행 {category} 카테고리 처리 실패: {str(e)}")

        # 데이터 수집 여부 확인
        table_categories = [key for key, data in result_data.items() 
                            if key not in ['날짜정보', '검증결과', '신선도'] and isinstance(data, list) and data]
        
        if not table_categories:
            log_message(f"{bank_name} 은행에서 테이블 데이터를 추출할 수 없습니다.")
            # 날짜 정보만이라도 있는 경우 반환 (사용자 요청은 날짜가 중요)
            if date_info not in ["날짜 정보 없음", "날짜 추출 실패"]:
                 log_message(f"[완료] {bank_name} 은행 테이블 데이터는 없으나 날짜 정보({date_info})는 수집됨.")
                 return result_data 
            return None


        log_message(f"[완료] {bank_name} 은행 데이터 수집 완료 (카테고리: {', '.join(table_categories)})")
        return result_data

    except Exception as e:
        log_message(f"{bank_name} 은행 처리 중 오류 발생: {str(e)}")
        return None

def save_bank_data(bank_name, data_dict):
    """수집된 은행 데이터를 엑셀 파일로 저장합니다. (기존과 동일)"""
    if not data_dict:
        return False

    try:
        # 파일명 생성 (날짜 정보 포함)
        date_info = data_dict.get('날짜정보', '날짜정보없음')
        safe_date_info = re.sub(r'[^\w\-_년월말]', '_', date_info)
        excel_path = os.path.join(OUTPUT_DIR, f"{bank_name}_{safe_date_info}.xlsx")

        with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
            # 공시 정보 시트 생성
            info_data = {
                '은행명': [bank_name],
                '공시 날짜': [data_dict.get('날짜정보', '')],
                '검증 결과': [data_dict.get('검증결과', '')],
                '데이터 신선도': ['최신' if data_dict.get('신선도', False) else '구버전 또는 확인필요'], # 문구 수정
                '추출 일시': [datetime.now().strftime("%Y-%m-%d %H:%M:%S")],
                '스크래핑 시스템': ['GitHub Actions 저축은행 스크래퍼 v2.3 (날짜/이메일 수정)']
            }
            info_df = pd.DataFrame(info_data)
            info_df.to_excel(writer, sheet_name='공시정보', index=False)

            # 각 카테고리별 데이터 저장
            for category, tables in data_dict.items():
                if category in ['날짜정보', '검증결과', '신선도'] or not isinstance(tables, list):
                    continue

                # 각 카테고리의 테이블을 별도 시트로 저장
                for i, df in enumerate(tables):
                    # 시트명 생성 (엑셀 시트명 제한: 31자)
                    if i == 0:
                        sheet_name = category[:31]
                    else:
                        sheet_name = f"{category}_{i+1}"[:31]

                    # MultiIndex 컬럼 처리
                    if isinstance(df.columns, pd.MultiIndex):
                        new_cols = []
                        for col in df.columns:
                            if isinstance(col, tuple):
                                col_parts = [str(c).strip() for c in col if str(c).strip() and str(c).lower() != 'nan']
                                new_cols.append('_'.join(col_parts) if col_parts else f"Column_{len(new_cols)+1}")
                            else:
                                new_cols.append(str(col))
                        df.columns = new_cols

                    # 데이터프레임 저장
                    try:
                        df.to_excel(writer, sheet_name=sheet_name, index=False)
                        log_message(f"{bank_name} - {sheet_name} 시트 저장 완료", verbose=False)
                    except Exception as e:
                        log_message(f"{bank_name} - {sheet_name} 시트 저장 실패: {str(e)}")

        log_message(f"{bank_name} 은행 데이터 저장 완료: {excel_path}")
        return True

    except Exception as e:
        log_message(f"{bank_name} 은행 데이터 저장 오류: {str(e)}")
        return False

def worker_process_bank(bank_name, driver_manager, progress_manager, expected_dates):
    """워커 스레드에서 은행 처리를 수행합니다. (기존과 동일)"""
    driver = None
    
    try:
        driver = driver_manager.get_driver()
        
        # 최대 재시도 횟수만큼 시도
        for attempt in range(MAX_RETRIES + 1): # MAX_RETRIES 만큼 재시도 (총 MAX_RETRIES+1 번 시도)
            try:
                # 은행 데이터 스크래핑
                result_data = scrape_bank_data(bank_name, driver, progress_manager, expected_dates)

                if result_data:
                    # 데이터 저장
                    if save_bank_data(bank_name, result_data):
                        progress_manager.mark_completed(bank_name)
                        return bank_name, True, result_data.get('날짜정보', ''), result_data.get('신선도', False)
                    else:
                        if attempt < MAX_RETRIES:
                            log_message(f"{bank_name} 은행 데이터 저장 실패, 재시도 {attempt+1}/{MAX_RETRIES}...")
                            WaitUtils.wait_with_random(2, 4)
                        else:
                            log_message(f"{bank_name} 은행 데이터 저장 실패, 최대 시도 횟수 초과")
                else: # result_data가 None인 경우 (스크래핑 실패)
                    if attempt < MAX_RETRIES:
                        log_message(f"{bank_name} 은행 데이터 스크래핑 실패, 재시도 {attempt+1}/{MAX_RETRIES}...")
                        # 재시도 전 드라이버 상태 초기화 (예: 기본 URL로 이동)
                        try:
                            driver.get("about:blank") 
                            time.sleep(0.5)
                        except: pass
                        WaitUtils.wait_with_random(2, 4)
                    else:
                        log_message(f"{bank_name} 은행 데이터 스크래핑 실패, 최대 시도 횟수 초과")
                
                if attempt == MAX_RETRIES and not result_data: # 마지막 시도에도 실패하면 최종 실패 처리
                    break


            except Exception as e:
                if attempt < MAX_RETRIES:
                    log_message(f"{bank_name} 은행 처리 중 오류: {str(e)}, 재시도 {attempt+1}/{MAX_RETRIES}...")
                    WaitUtils.wait_with_random(2, 4)
                else:
                    log_message(f"{bank_name} 은행 처리 실패: {str(e)}, 최대 시도 횟수 초과")
            
            if result_data and save_bank_data(bank_name, result_data): # 루프 내에서 성공 시 바로 반환
                 progress_manager.mark_completed(bank_name)
                 return bank_name, True, result_data.get('날짜정보', ''), result_data.get('신선도', False)


        # 모든 시도 실패
        progress_manager.mark_failed(bank_name)
        # 실패 시 반환 값에 날짜 정보와 신선도 추가 (기본값으로)
        validation_info = progress_manager.progress.get('data_validation', [])
        bank_validation = next((item for item in validation_info if item['bank_name'] == bank_name), None)
        failed_date_info = bank_validation.get('date_info', '추출 실패') if bank_validation else '추출 실패'
        failed_is_fresh = bank_validation.get('is_fresh', False) if bank_validation else False
        return bank_name, False, failed_date_info, failed_is_fresh


    except Exception as e:
        log_message(f"{bank_name} 은행 처리 중 예상치 못한 오류: {str(e)}")
        progress_manager.mark_failed(bank_name)
        return bank_name, False, f"예상치 못한 오류: {str(e)}", False # 오류 발생 시 기본값
    
    finally:
        if driver and driver_manager:
            driver_manager.return_driver(driver)

# =============================================================================
# 비동기 처리 및 메인 실행 로직 (기존과 동일)
# =============================================================================

async def process_banks_async(banks, driver_manager, progress_manager, expected_dates):
    """은행 목록을 병렬로 처리합니다."""
    log_message(f"병렬 처리 시작: {len(banks)}개 은행, {MAX_WORKERS}개 워커")
    
    all_results = []
    
    # 배치 단위로 처리 (메모리 효율성)
    # batch_size = max(1, len(banks) // MAX_WORKERS) if len(banks) > MAX_WORKERS else len(banks)
    batch_size = MAX_WORKERS # 워커 수만큼 배치 크기 설정 또는 더 작은 값으로 조절 가능
    bank_batches = [banks[i:i + batch_size] for i in range(0, len(banks), batch_size)]


    for batch_idx, batch in enumerate(bank_batches):
        log_message(f"배치 {batch_idx+1}/{len(bank_batches)} 처리 중 ({len(batch)}개 은행)")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(MAX_WORKERS, len(batch))) as executor:
            # 비동기 작업 생성
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

            # 진행률 표시
            progress_desc = f"배치 {batch_idx+1}/{len(bank_batches)}"
            progress_bar = tqdm(asyncio.as_completed(futures), total=len(futures), desc=progress_desc)

            # 결과 수집
            batch_results = []
            for future in progress_bar:
                result = await future
                batch_results.append(result)
                
                # 진행률 업데이트
                success_count = len([r for r in batch_results if r[1]])
                progress_bar.set_postfix_str(f"완료: {success_count}/{len(batch_results)}")

            all_results.extend(batch_results)
            
            # 배치 간 휴식
            if batch_idx < len(bank_batches) - 1:
                log_message(f"배치 {batch_idx+1} 완료. 다음 배치 전 잠시 대기...")
                await asyncio.sleep(2)

    return all_results

def process_with_retry(banks, max_retries_main=1): # 파라미터 이름 변경 (MAX_RETRIES와 혼동 방지)
    """실패한 은행을 재시도하는 로직을 포함한 메인 처리 함수입니다."""
    # 예상 데이터 날짜 검증
    expected_dates = validate_data_freshness()
    
    # 매니저 초기화
    driver_manager = DriverManager(max_drivers=MAX_WORKERS)
    progress_manager = ProgressManager()

    # 처리할 은행 목록 결정
    pending_banks = progress_manager.get_pending_banks(banks)
    if not pending_banks:
        log_message("모든 은행 처리 완료! 일부 은행을 재검증합니다.")
        pending_banks = banks[:min(5, len(banks))]  # 검증용으로 소수만 재처리

    log_message(f"처리할 은행 수: {len(pending_banks)}/{len(banks)}")
    
    all_run_results = [] # 모든 실행 결과를 저장 (재시도 포함)

    try:
        # 비동기 이벤트 루프 실행
        if sys.version_info >= (3, 7):
            results = asyncio.run(process_banks_async(pending_banks, driver_manager, progress_manager, expected_dates))
        else:
            loop = asyncio.get_event_loop()
            results = loop.run_until_complete(process_banks_async(pending_banks, driver_manager, progress_manager, expected_dates))
        
        all_run_results.extend(results)

        # 결과 분석
        successful_banks_names = [r[0] for r in results if r[1]]
        failed_banks_names = [r[0] for r in results if not r[1]]

        # 실패한 은행 재시도
        retry_count = 0
        while failed_banks_names and retry_count < max_retries_main: # worker_process_bank 내 재시도와 별개
            retry_count += 1
            log_message(f"메인 재시도 {retry_count}/{max_retries_main}: {len(failed_banks_names)}개 은행 처리 중...")

            # 재시도 실행
            if sys.version_info >= (3, 7):
                retry_results = asyncio.run(process_banks_async(failed_banks_names, driver_manager, progress_manager, expected_dates))
            else:
                loop = asyncio.get_event_loop()
                retry_results = loop.run_until_complete(process_banks_async(failed_banks_names, driver_manager, progress_manager, expected_dates))
            
            all_run_results.extend(retry_results)

            # 결과 갱신
            newly_successful_names = [r[0] for r in retry_results if r[1]]
            failed_banks_names = [r[0] for r in retry_results if not r[1]] # 재시도 후에도 실패한 은행 목록 갱신

            successful_banks_names.extend(newly_successful_names)
            log_message(f"메인 재시도 {retry_count} 결과: {len(newly_successful_names)}개 성공, {len(failed_banks_names)}개 실패")

        # 최종 성공/실패 은행 목록은 progress_manager를 통해 얻는 것이 더 정확할 수 있음
        final_successful_banks = progress_manager.progress.get('completed', [])
        final_failed_banks = progress_manager.progress.get('failed', [])
        
        return final_successful_banks, final_failed_banks, all_run_results # 모든 실행 결과 반환

    finally:
        # 드라이버 정리
        driver_manager.close_all()


def generate_summary_report():
    """스크래핑 결과 요약 보고서를 생성합니다. (기존과 동일, 데이터 신선도 문구 수정 가능성)"""
    try:
        progress_manager = ProgressManager()
        completed_banks = progress_manager.progress.get('completed', [])
        failed_banks = progress_manager.progress.get('failed', [])
        validation_data = progress_manager.progress.get('data_validation', [])

        # 은행별 데이터 요약
        bank_summary = []
        validation_dict = {item['bank_name']: item for item in validation_data}

        for bank in BANKS:
            # 각 은행의 엑셀 파일 찾기
            bank_files = [f for f in os.listdir(OUTPUT_DIR) if f.startswith(f"{bank}_") and f.endswith(".xlsx")]

            if bank_files:
                try:
                    # 가장 최근 파일 선택
                    latest_file = sorted(bank_files)[-1]
                    file_path = os.path.join(OUTPUT_DIR, latest_file)

                    # 엑셀 파일 분석
                    xls = pd.ExcelFile(file_path)
                    sheet_count = len(xls.sheet_names)

                    # 카테고리 추출
                    categories = []
                    for sheet in xls.sheet_names:
                        if sheet != '공시정보':
                            category = sheet.split('_')[0] if '_' in sheet else sheet
                            categories.append(category)

                    categories = sorted(list(set(categories)))

                    # 공시 정보에서 날짜 및 검증 결과 추출
                    date_info = "날짜 정보 없음"
                    validation_result = "검증 없음"
                    data_freshness_text = "알 수 없음" 
                    
                    if '공시정보' in xls.sheet_names:
                        info_df = pd.read_excel(file_path, sheet_name='공시정보')
                        if '공시 날짜' in info_df.columns and not info_df['공시 날짜'].empty:
                            date_info = str(info_df['공시 날짜'].iloc[0])
                        if '검증 결과' in info_df.columns and not info_df['검증 결과'].empty:
                            validation_result = str(info_df['검증 결과'].iloc[0])
                        if '데이터 신선도' in info_df.columns and not info_df['데이터 신선도'].empty:
                            data_freshness_text = str(info_df['데이터 신선도'].iloc[0]) # '최신' 또는 '구버전 또는 확인필요'

                    # 상태 결정
                    status = '완료' if set(categories) >= set(CATEGORIES) else '부분 완료'
                    if not categories and date_info in ["날짜 정보 없음", "날짜 추출 실패"]: # 테이블 없고 날짜도 없으면
                        status = '실패'


                    bank_summary.append({
                        '은행명': bank,
                        '스크래핑 상태': status,
                        '공시 날짜': date_info,
                        '데이터 신선도': data_freshness_text,
                        '검증 결과': validation_result,
                        '시트 수': sheet_count - 1 if sheet_count > 0 else 0,  # 공시정보 시트 제외
                        '스크래핑된 카테고리': ', '.join(categories)
                    })
                    
                except Exception as e:
                    bank_summary.append({
                        '은행명': bank,
                        '스크래핑 상태': '파일 손상',
                        '공시 날짜': '확인 불가',
                        '데이터 신선도': '확인 불가',
                        '검증 결과': f'오류: {str(e)}',
                        '시트 수': '확인 불가',
                        '스크래핑된 카테고리': ''
                    })
            else: # 파일이 없는 경우
                status = '실패' if bank in failed_banks else '미처리'
                validation_info_item = validation_dict.get(bank, {})
                
                date_info_val = validation_info_item.get('date_info', '정보 없음')
                is_fresh_val = validation_info_item.get('is_fresh', False)
                
                freshness_text_val = "알 수 없음"
                if date_info_val not in ["날짜 정보 없음", "날짜 추출 실패", "정보 없음"]:
                    freshness_text_val = '최신' if is_fresh_val else '구버전 또는 확인필요'


                bank_summary.append({
                    '은행명': bank,
                    '스크래핑 상태': status,
                    '공시 날짜': date_info_val,
                    '데이터 신선도': freshness_text_val,
                    '검증 결과': '검증 완료' if bank in validation_dict else '검증 안됨',
                    '시트 수': 0,
                    '스크래핑된 카테고리': ''
                })

        # 요약 DataFrame 생성 및 정렬
        summary_df = pd.DataFrame(bank_summary)
        status_order = {'완료': 0, '부분 완료': 1, '파일 손상': 2, '실패': 3, '미처리': 4}
        summary_df['상태순서'] = summary_df['스크래핑 상태'].map(status_order)
        summary_df = summary_df.sort_values(['상태순서', '은행명']).drop('상태순서', axis=1)

        # 요약 파일 저장
        summary_file = os.path.join(OUTPUT_DIR, f"스크래핑_요약_{TODAY}.xlsx")
        summary_df.to_excel(summary_file, index=False)

        # 통계 정보 계산
        stats = {
            '전체 은행 수': len(BANKS),
            '완료 은행 수': len([r for r in bank_summary if r['스크래핑 상태'] == '완료']),
            '부분 완료 은행 수': len([r for r in bank_summary if r['스크래핑 상태'] == '부분 완료']),
            '실패 은행 수': len([r for r in bank_summary if r['스크래핑 상태'] in ['실패', '파일 손상', '미처리']]), # 미처리도 실패로 간주
            '최신 데이터 은행 수': len([r for r in bank_summary if r['데이터 신선도'] == '최신']),
            '성공률': f"{len([r for r in bank_summary if r['스크래핑 상태'] in ['완료', '부분 완료']]) / len(BANKS) * 100:.2f}%"
        }

        log_message("\n===== 스크래핑 결과 요약 =====")
        for key, value in stats.items():
            log_message(f"{key}: {value}")

        log_message(f"요약 파일 저장 완료: {summary_file}")
        return summary_file, stats

    except Exception as e:
        log_message(f"요약 보고서 생성 오류: {str(e)}")
        return None, {}

# =============================================================================
# 핵심 수정 2: 안정화된 ZIP 파일 생성 (기존과 동일)
# =============================================================================

def create_zip_archive():
    """결과 파일들을 ZIP으로 압축합니다. (.bin 오류 완전 해결)"""
    try:
        # ZIP 파일명을 명확하게 .zip 확장자로 설정
        zip_filename = f'저축은행_데이터_{TODAY}.zip'
        zip_path = os.path.join(OUTPUT_BASE_DIR, zip_filename)
        
        # 기존 ZIP 파일이 있다면 삭제
        if os.path.exists(zip_path):
            os.remove(zip_path)
            log_message(f"기존 ZIP 파일 삭제: {zip_path}")
        
        # ZIP 파일 생성 시 명시적으로 압축 방식과 압축 레벨 설정
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED, compresslevel=6) as zipf:
            file_count = 0
            total_size = 0
            
            # OUTPUT_DIR 내의 모든 파일을 ZIP에 추가
            for root, dirs, files in os.walk(OUTPUT_DIR):
                for file in files:
                    file_path_to_zip = os.path.join(root, file) # 변수명 변경
                    
                    # 파일이 존재하고 읽을 수 있는지 확인
                    if os.path.exists(file_path_to_zip) and os.path.isfile(file_path_to_zip):
                        try:
                            # ZIP 내에서의 상대 경로 계산
                            arcname = os.path.relpath(file_path_to_zip, OUTPUT_BASE_DIR)
                            
                            # 파일을 ZIP에 추가
                            zipf.write(file_path_to_zip, arcname)
                            
                            file_size = os.path.getsize(file_path_to_zip)
                            total_size += file_size
                            file_count += 1
                            
                            log_message(f"ZIP에 추가: {arcname} ({file_size:,} bytes)", verbose=False)
                            
                        except Exception as e:
                            log_message(f"파일 ZIP 추가 실패 ({file}): {str(e)}")
            
            # ZIP 파일 정보 추가 (메타데이터)
            info_content = f"""저축은행 중앙회 통일경영공시 데이터 스크래핑 결과 (v2.3 날짜/이메일 수정)

생성일시: {datetime.now().strftime('%Y년 %m월 %d일 %H시 %M분 %S초')}
포함 파일 수: {file_count}개
총 용량: {total_size:,} bytes
스크래핑 대상: {len(BANKS)}개 저축은행
데이터 기준일: 2024년 9월말 / 2025년 3월말

🔧 이번 버전의 개선사항 (v2.3):
✅ 날짜 추출 정확도 향상 (사용자 지정 '2024년9월말', '2025년3월말'만 유효 처리)
✅ ZIP 파일 생성 완전 안정화 (.bin 오류 근본 해결)
✅ 스크린샷 형태 결과 테이블 생성 및 이메일 본문에 유사 테이블 추가

파일 구성:
- 각 은행별 Excel 파일 (.xlsx)
- 스크래핑 요약 보고서 (Excel)
- 은행별 날짜 확인 결과 (Excel)
- 실행 로그 파일 (.log)
- 진행 상황 파일 (.json)

GitHub Actions 저축은행 스크래퍼 v2.3 (날짜/이메일 수정)
"""
            
            # 정보 파일을 ZIP에 추가
            zipf.writestr(f"README_{TODAY}.txt", info_content.encode('utf-8'))
        
        # 생성된 ZIP 파일 검증
        if os.path.exists(zip_path):
            zip_size = os.path.getsize(zip_path)
            
            # ZIP 파일이 올바르게 생성되었는지 테스트
            with zipfile.ZipFile(zip_path, 'r') as test_zip:
                zip_file_list = test_zip.namelist()
                if len(zip_file_list) > 0:
                    log_message(f"✅ ZIP 아카이브 생성 완료: {zip_path}")
                    log_message(f"📦 ZIP 파일 크기: {zip_size:,} bytes")
                    log_message(f"📄 포함된 파일 수: {len(zip_file_list)}개")
                    log_message(f"🗂️ 주요 파일들: {', '.join(zip_file_list[:5])}{'...' if len(zip_file_list) > 5 else ''}")
                    return zip_path
                else:
                    log_message("❌ ZIP 파일이 비어있습니다.")
                    return None
        else:
            log_message("❌ ZIP 파일 생성에 실패했습니다.")
            return None
            
    except Exception as e:
        log_message(f"❌ ZIP 아카이브 생성 실패: {str(e)}")
        import traceback
        log_message(f"상세 오류: {traceback.format_exc()}")
        return None

# =============================================================================
# 핵심 수정 3: 스크린샷 형태 결과 테이블 (Excel 및 이메일용 데이터 생성)
# =============================================================================

def generate_screenshot_format_report(all_run_results): # worker_process_bank 결과 사용
    """스크린샷과 동일한 형태의 은행별 날짜 확인 테이블을 생성하고, 이메일용 데이터도 반환합니다."""
    try:
        # all_run_results: [(bank_name, success_bool, date_info_str, is_fresh_bool), ...]
        
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        table_data = [] # Excel 및 이메일용 데이터

        unique_bank_results = {} # 은행별 마지막 결과만 저장하기 위함
        for bank_result in all_run_results:
            unique_bank_results[bank_result[0]] = bank_result # 은행 이름으로 덮어쓰면 마지막 결과만 남음

        for bank_name_key in BANKS: # 원래 은행 순서대로 처리
            bank_result = unique_bank_results.get(bank_name_key)
            if not bank_result: # 혹시 결과에 없는 은행이 있다면 (실패 등으로)
                table_data.append({
                    '은행명': bank_name_key,
                    '공시 날짜(월말)': '결과 없음',
                    '날짜 확인': '❌ 미처리/오류',
                    '처리상태': '실패',
                    '확인 시간': current_time
                })
                continue

            bank_name, success, date_info_raw, is_fresh = bank_result
            
            date_info_for_report = str(date_info_raw) # 문자열로 변환
            
            # 처리상태 결정
            if success:
                processing_status = "완료"
            else: 
                processing_status = "실패"
                if "오류" in date_info_for_report or "실패" in date_info_for_report or not date_info_for_report:
                     date_info_for_report = "추출 실패"


            # 날짜 확인 상태 결정
            date_status = "⚠️ 확인필요" # 기본값
            if date_info_for_report in ["날짜 정보 없음", "추출 실패", "결과 없음"]:
                date_status = "❌ 미처리/오류"
            elif is_fresh: 
                if "2024년9월말" in date_info_for_report: 
                    date_status = "✅ 일치 (기한내최신)"
                elif "2025년3월말" in date_info_for_report:
                    date_status = "🟢 일치 (예정보다선반영)"
                else: 
                    date_status = "✅ 일치 (확인됨)" 
            else: 
                if date_info_for_report not in ["날짜 정보 없음", "추출 실패", "결과 없음"]: 
                     date_status = "⚠️ 불일치 (구버전)"
                # else: 이미 위에서 "❌ 미처리/오류"로 처리됨


            table_data.append({
                '은행명': bank_name,
                '공시 날짜(월말)': date_info_for_report,
                '날짜 확인': date_status,
                '처리상태': processing_status,
                '확인 시간': current_time 
            })
        
        # DataFrame 생성
        result_df = pd.DataFrame(table_data)
        
        # 상태별로 정렬 (원래 은행 순서 유지를 위해 정렬 제거 또는 은행명 기준 정렬)
        # result_df = result_df.sort_values(['은행명']).reset_index(drop=True) # 은행명 순으로 정렬
        
        # 스크린샷 형태의 결과 파일 저장
        screenshot_format_file = os.path.join(OUTPUT_DIR, f"은행별_날짜확인_결과_{TODAY}.xlsx")
        
        with pd.ExcelWriter(screenshot_format_file, engine='openpyxl') as writer:
            # 메인 결과 시트
            result_df.to_excel(writer, sheet_name='은행별_날짜확인', index=False)
            
            # 통계 요약 시트
            stats_data = {
                '구분': [
                    '전체 은행 수',
                    '완료된 은행 수 (처리상태 기준)',
                    '2024년9월말 데이터',
                    '2025년3월말 데이터',
                    '기타/오래된 날짜 데이터 (완료 건 중)',
                    '처리 실패 은행 (처리상태 기준)',
                    '성공률 (처리상태 기준)'
                ],
                '수량': [
                    len(BANKS),
                    len([r for r in table_data if r['처리상태'] == '완료']),
                    len([r for r in table_data if "2024년9월말" in r['공시 날짜(월말)'] and r['처리상태'] == '완료']),
                    len([r for r in table_data if "2025년3월말" in r['공시 날짜(월말)'] and r['처리상태'] == '완료']),
                    len([r for r in table_data if r['날짜 확인'] not in ["✅ 일치 (기한내최신)", "🟢 일치 (예정보다선반영)", "✅ 일치 (확인됨)"] and r['처리상태'] == '완료']),
                    len([r for r in table_data if r['처리상태'] == '실패']),
                    f"{len([r for r in table_data if r['처리상태'] == '완료']) / len(BANKS) * 100:.1f}%" if len(BANKS) > 0 else "N/A"
                ]
            }
            
            stats_df = pd.DataFrame(stats_data)
            stats_df.to_excel(writer, sheet_name='통계요약', index=False)
            
            # 문제 은행 목록 (날짜 확인이 '확인필요', '불일치', '미처리/오류' 인 경우)
            problem_banks_report = [
                r for r in table_data 
                if r['날짜 확인'] not in ["✅ 일치 (기한내최신)", "🟢 일치 (예정보다선반영)", "✅ 일치 (확인됨)"]
            ]
            
            if problem_banks_report:
                problem_df = pd.DataFrame(problem_banks_report)
                problem_df.to_excel(writer, sheet_name='문제은행목록', index=False)
        
        log_message(f"스크린샷 형태 결과 파일 저장 완료: {screenshot_format_file}")
        
        # 콘솔에 간단한 요약 출력
        log_message(f"\n📋 은행별 날짜 확인 결과 요약:")
        status_summary_report = result_df['처리상태'].value_counts()
        for status, count in status_summary_report.items():
            log_message(f"  • {status}: {count}개")
        
        date_check_summary_report = result_df['날짜 확인'].value_counts()
        log_message(f"\n📅 날짜 확인 상태:")
        for status, count in date_check_summary_report.items():
            log_message(f"  • {status}: {count}개")
        
        return screenshot_format_file, table_data # Excel 파일 경로와 테이블 데이터 반환
            
    except Exception as e:
        log_message(f"스크린샷 형태 보고서 생성 오류: {str(e)}")
        import traceback
        log_message(traceback.format_exc())
        return None, []


def send_email_notification(subject, body_text_part, email_table_data=None, attachment_paths=None, is_overall_success=True, expected_dates_for_email=None):
    """Gmail SMTP를 통해 이메일 알림을 발송합니다. HTML 테이블 포함."""
    if not GMAIL_ADDRESS or not GMAIL_APP_PASSWORD or not RECIPIENT_EMAILS:
        log_message("이메일 설정이 불완전하여 알림을 발송하지 않습니다.")
        return False
    
    try:
        msg = MIMEMultipart('alternative') # HTML과 Plain Text를 모두 지원하기 위해 'alternative' 사용
        msg['From'] = GMAIL_ADDRESS
        msg['To'] = ', '.join(RECIPIENT_EMAILS)
        msg['Subject'] = subject
        
        # Plain text 본문 (기존 body_text_part 사용)
        msg.attach(MIMEText(body_text_part, 'plain', 'utf-8'))

        # HTML 본문 생성
        html_body_content = f"""
        <html>
          <head>
            <meta charset="UTF-8">
            <style>
              body {{ font-family: Arial, sans-serif; margin: 0; padding: 0; color: #333; }}
              table {{ border-collapse: collapse; width: 95%; margin: 20px auto; font-size: 10pt; box-shadow: 0 2px 3px rgba(0,0,0,0.1); }}
              th, td {{ border: 1px solid #ddd; text-align: left; padding: 8px; }}
              th {{ background-color: #f0f0f0; color: #333; font-weight: bold; }}
              tr:nth-child(even) {{ background-color: #f9f9f9; }}
              h2 {{ color: #0056b3; margin-left: 15px; border-bottom: 2px solid #0056b3; padding-bottom: 5px;}}
              p {{ margin-left: 15px; line-height: 1.6; }}
              .summary-section {{ padding:15px; background-color: #fdfdfd; border-radius: 5px; margin-bottom:15px;}}
              .summary-section p {{ margin-left: 0; white-space: pre-wrap; }}
            </style>
          </head>
          <body>
            <div class="summary-section">
              <p>{body_text_part.replace("\n", "<br>")}</p>
            </div>
        """

        if expected_dates_for_email:
            html_body_content += f"""
            <h2>데이터 신선도 검증 기준</h2>
            <p>
              📅 예상 최신 데이터 분기: {expected_dates_for_email[0]}<br>
              📅 조기 업로드 가능 분기 (또는 다른 유효 분기): {expected_dates_for_email[1]}<br>
              ⚠️ 이 기준과 다른 날짜의 데이터는 구버전이거나 추출 오류일 가능성이 있습니다.
            </p>
            """

        if email_table_data:
            html_body_content += "<h2>은행별 날짜 확인 결과 상세</h2><table><thead><tr>"
            headers = ['은행명', '공시 날짜(월말)', '날짜 확인', '처리상태', '확인 시간']
            for header in headers:
                html_body_content += f"<th>{header}</th>"
            html_body_content += "</tr></thead><tbody>"

            for item in email_table_data: # email_table_data는 generate_screenshot_format_report에서 정렬된 순서
                html_body_content += "<tr>"
                html_body_content += f"<td>{item.get('은행명', '')}</td>"
                html_body_content += f"<td>{item.get('공시 날짜(월말)', '')}</td>"
                html_body_content += f"<td>{item.get('날짜 확인', '')}</td>"
                html_body_content += f"<td>{item.get('처리상태', '')}</td>"
                html_body_content += f"<td>{item.get('확인 시간', '')}</td>"
                html_body_content += "</tr>"
            
            html_body_content += "</tbody></table>"
        
        html_body_content += "</body></html>"
        msg.attach(MIMEText(html_body_content, 'html', 'utf-8'))
        
        # 첨부 파일 추가
        if attachment_paths:
            for file_path in attachment_paths:
                if os.path.exists(file_path):
                    try:
                        filename = os.path.basename(file_path)
                        ctype, encoding = encoders.encode_base64, None # 기본값
                        
                        if filename.endswith('.zip'):
                            maintype, subtype = 'application', 'zip'
                        elif filename.endswith('.xlsx'):
                            maintype, subtype = 'application', 'vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                        else: # 기타 파일
                            maintype, subtype = 'application', 'octet-stream'

                        with open(file_path, "rb") as fp:
                            part = MIMEBase(maintype, subtype)
                            part.set_payload(fp.read())
                        encoders.encode_base64(part)
                        part.add_header('Content-Disposition', 'attachment', filename=filename)
                        msg.attach(part)
                        log_message(f"📎 파일 첨부: {filename}")
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
        import traceback
        log_message(traceback.format_exc())
        return False

# =============================================================================
# 최소 수정 메인 함수
# =============================================================================

def main():
    """최소 수정으로 문제 해결한 메인 실행 함수"""
    # 로그 파일 초기화
    try:
        with open(LOG_FILE, 'w', encoding='utf-8') as f:
            f.write("") 
    except Exception as e:
        print(f"로그 파일 초기화 실패: {e}")

    start_time = time.time()
    log_message(f"\n🚀 ===== 저축은행 중앙회 통일경영공시 데이터 스크래핑 시작 (v2.3.1 f-string 수정) [{TODAY}] =====\n")

    try:
        log_message(f"🔧 이번 버전의 수정사항 (v2.3.1):")
        log_message(f"  ✅ f-string 구성 방식 변경으로 SyntaxError 해결 시도")
        log_message(f"  ✅ 날짜 추출 로직 개선: 사용자 지정 날짜('2024년9월말', '2025년3월말')만 유효 처리")
        log_message(f"  ✅ 이메일 본문에 스크린샷 형태의 결과 테이블 추가 (HTML 형식)")
        
        log_message(f"\n⚙️ 현재 설정값:")
        log_message(f"  • 최대 워커 수: {MAX_WORKERS}개")
        log_message(f"  • 최대 재시도 횟수 (메인): 1회 (워커 내 재시도: {MAX_RETRIES}회)")
        log_message(f"  • 이메일 알림: {'✅ 활성화' if GMAIL_ADDRESS and GMAIL_APP_PASSWORD else '❌ 비활성화'}")

        log_message(f"\n🏦 {len(BANKS)}개 저축은행 데이터 스크래핑 시작...")
        successful_banks, failed_banks, all_run_results_for_report = process_with_retry(BANKS, max_retries_main=1)


        log_message(f"\n📊 결과 요약 보고서 생성 중...")
        summary_file, stats = generate_summary_report() 

        log_message(f"📋 스크린샷 형태 결과 테이블 생성 중...")
        screenshot_file, email_table_data_for_html = generate_screenshot_format_report(all_run_results_for_report)


        log_message(f"📦 ZIP 압축 파일 생성 중...")
        zip_file = create_zip_archive()

        end_time = time.time()
        total_duration = end_time - start_time
        minutes, seconds = divmod(total_duration, 60)
        
        target_achieved = total_duration <= 8 * 60

        log_message(f"\n🎉 ===== 스크래핑 완료 (v2.3.1 f-string 수정) =====")
        log_message(f"⏰ 총 실행 시간: {int(minutes)}분 {int(seconds)}초")
        log_message(f"🎯 성능 목표: {'✅ 달성 (8분 이내)' if target_achieved else '⚠️ 목표 초과'}")
        log_message(f"✅ 성공한 은행 (ProgressManager 기준): {len(successful_banks)}개")
        log_message(f"❌ 실패한 은행 (ProgressManager 기준): {len(failed_banks)}개")
        
        if failed_banks:
            log_message(f"🔍 실패한 은행 목록 (ProgressManager 기준): {', '.join(failed_banks)}")

        if email_table_data_for_html: 
            problem_banks_from_report = [b for b in email_table_data_for_html if b['날짜 확인'] not in ["✅ 일치 (기한내최신)", "🟢 일치 (예정보다선반영)", "✅ 일치 (확인됨)"]]
            if problem_banks_from_report:
                log_message(f"⚠️ 날짜 확인 필요 은행 (보고서 기준): {len(problem_banks_from_report)}개")
                for bank_report_item in problem_banks_from_report[:5]:
                    log_message(f"    • {bank_report_item['은행명']}: {bank_report_item['공시 날짜(월말)']} ({bank_report_item['날짜 확인']})")
                if len(problem_banks_from_report) > 5:
                    log_message(f"    ... 기타 {len(problem_banks_from_report) - 5}개 은행")


        log_message(f"\n📁 생성된 파일 목록:")
        if zip_file: log_message(f"  📦 ZIP 압축파일: {os.path.basename(zip_file)} ✅")
        else: log_message(f"  📦 ZIP 압축파일: 생성 실패 ❌")
        if screenshot_file: log_message(f"  📋 은행별 날짜확인 결과(Excel): {os.path.basename(screenshot_file)} ✅")
        else: log_message(f"  📋 은행별 날짜확인 결과(Excel): 생성 실패 ❌")
        if summary_file: log_message(f"  📊 요약 보고서(Excel): {os.path.basename(summary_file)} ✅")
        else: log_message(f"  📊 요약 보고서(Excel): 생성 실패 ❌")
        log_message(f"  📄 실행 로그: {os.path.basename(LOG_FILE)} ✅")

        if GMAIL_ADDRESS and GMAIL_APP_PASSWORD and RECIPIENT_EMAILS:
            log_message(f"\n📧 이메일 알림 발송 중...")
            
            subject_email = f"📊 저축은행 데이터 스크래핑 {'완료' if not failed_banks else '부분완료'} (v2.3.1) - {int(minutes)}분{int(seconds)}초"
            
            # 이메일 본문의 Plain Text 부분 - f-string 대신 format 또는 concatenation 사용
            lines = []
            lines.append("저축은행 중앙회 통일경영공시 데이터 스크래핑이 완료되었습니다.\n")
            lines.append("🔧 v2.3.1 수정 버전의 특징:")
            lines.append("✅ f-string 구성 방식 변경으로 SyntaxError 해결 시도")
            lines.append("✅ 날짜 추출 로직 개선: 사용자 지정 날짜('2024년9월말', '2025년3월말')만 유효 처리")
            lines.append("✅ 이메일 본문에 스크린샷 형태의 결과 테이블 추가 (HTML 형식)\n")
            lines.append("📊 실행 정보:")
            lines.append(f"- 🕐 실행 날짜: {datetime.now().strftime('%Y년 %m월 %d일 %H시 %M분')}")
            lines.append(f"- ⏱️ 총 실행 시간: {int(minutes)}분 {int(seconds)}초 ({'목표 달성' if target_achieved else '목표 초과'})")
            lines.append(f"- 🎯 처리 대상: 전국 {len(BANKS)}개 저축은행\n")
            lines.append("📈 스크래핑 결과 요약 (ProgressManager 기준):")
            lines.append(f"- 🏦 전체 은행 수: {stats.get('전체 은행 수', len(BANKS))}개")
            lines.append(f"- ✅ 완료 은행 수: {stats.get('완료 은행 수', len(successful_banks))}개")
            lines.append(f"- ⚠️ 부분 완료 은행 수: {stats.get('부분 완료 은행 수', 0)}개")
            lines.append(f"- ❌ 실패 은행 수: {stats.get('실패 은행 수', len(failed_banks))}개")
            lines.append(f"- 🟢 최신 데이터 은행 수: {stats.get('최신 데이터 은행 수', 0)}개") # 키 수정
            lines.append(f"- 📊 전체 성공률 (완료+부분완료): {stats.get('성공률', '0.00%')}\n")
            lines.append("📦 첨부 파일:")
            lines.append("1. 🗜️ ZIP 압축파일: 모든 데이터 포함")
            lines.append("2. 📋 은행별 날짜확인 결과 (Excel)")
            lines.append("3. 📊 종합 요약 보고서 (Excel)")
            lines.append("4. 📄 상세 실행 로그")
            body_text_email = "\n".join(lines)

            attachments_email = []
            if zip_file and os.path.exists(zip_file): attachments_email.append(zip_file)
            if screenshot_file and os.path.exists(screenshot_file): attachments_email.append(screenshot_file)
            if summary_file and os.path.exists(summary_file): attachments_email.append(summary_file)
            if os.path.exists(LOG_FILE): attachments_email.append(LOG_FILE)

            expected_dates_for_email_content = validate_data_freshness() 

            email_sent_successfully = send_email_notification(
                subject_email, 
                body_text_email, 
                email_table_data_for_html, 
                attachments_email, 
                not failed_banks, 
                expected_dates_for_email_content
            )
            
            if email_sent_successfully: log_message(f"    ✅ 이메일 발송 성공")
            else: log_message(f"    ❌ 이메일 발송 실패")
        else:
            log_message(f"\n📧 이메일 알림: 설정되지 않음")

        log_message(f"\n🎊 ===== 저축은행 중앙회 통일경영공시 데이터 스크래핑 완료 (v2.3.1) [{TODAY}] =====")

    except KeyboardInterrupt:
        log_message("\n⏹️ 사용자에 의해 중단되었습니다.")
    except Exception as e:
        log_message(f"\n💥 예상치 못한 오류 발생: {str(e)}")
        import traceback
        log_message(f"상세 오류 정보:\n{traceback.format_exc()}")
        
        if GMAIL_ADDRESS and GMAIL_APP_PASSWORD and RECIPIENT_EMAILS:
            error_lines = []
            error_lines.append("저축은행 데이터 스크래핑 v2.3.1 버전 실행 중 오류가 발생했습니다.\n")
            error_lines.append(f"- 🕐 발생 시간: {datetime.now().strftime('%Y년 %m월 %d일 %H시 %M분')}")
            error_lines.append(f"- 🐛 오류 내용: {str(e)}")
            error_lines.append("상세 내용은 첨부된 로그 파일을 확인해주세요.")
            error_body_email = "\n".join(error_lines)
            
            error_attachments = [LOG_FILE] if os.path.exists(LOG_FILE) else []
            send_email_notification(
                f"❌ 저축은행 데이터 스크래핑 오류 발생 (v2.3.1) - {TODAY}", 
                error_body_email, 
                None, 
                error_attachments, 
                False
            )

# =============================================================================
# 프로그램 진입점
# =============================================================================

if __name__ == "__main__":
    main()
