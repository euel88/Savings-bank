#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
저축은행 중앙회 통일경영공시 데이터 자동 스크래핑 스크립트 (GitHub Actions 버전)
목적: 79개 저축은행의 재무정보를 빠르고 효율적으로 스크래핑
작성일: 2025-05-28
GitHub Actions 환경용 개선사항:
- Colab 종속성 제거
- 환경 변수 기반 설정
- 이메일 알림 기능 추가
- 데이터 날짜 검증 로직 추가
- 상대 경로 사용
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

# 환경 변수에서 설정값 가져오기 (기본값 포함)
TODAY = datetime.now().strftime("%Y%m%d")
BASE_URL = "https://www.fsb.or.kr/busmagequar_0100.act"
MAX_RETRIES = int(os.getenv('MAX_RETRIES', '2'))
PAGE_LOAD_TIMEOUT = int(os.getenv('PAGE_LOAD_TIMEOUT', '25'))
WAIT_TIMEOUT = int(os.getenv('WAIT_TIMEOUT', '15'))
MAX_WORKERS = int(os.getenv('MAX_WORKERS', '2'))

# 출력 디렉토리 설정 (환경 변수 또는 기본값)
OUTPUT_BASE_DIR = os.getenv('OUTPUT_DIR', './output')
OUTPUT_DIR = os.path.join(OUTPUT_BASE_DIR, f'저축은행_데이터_{TODAY}')

# 이메일 설정
GMAIL_ADDRESS = os.getenv('GMAIL_ADDRESS')
GMAIL_APP_PASSWORD = os.getenv('GMAIL_APP_PASSWORD')
RECIPIENT_EMAILS = os.getenv('RECIPIENT_EMAILS', '').split(',') if os.getenv('RECIPIENT_EMAILS') else []

# 전체 79개 저축은행 목록 (업데이트: 머스트삼일 통합)
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

# 카테고리 목록
CATEGORIES = ["영업개황", "재무현황", "손익현황", "기타"]

# 파일 경로 설정
PROGRESS_FILE = os.path.join(OUTPUT_DIR, 'bank_scraping_progress.json')
LOG_FILE = os.path.join(OUTPUT_DIR, f'scraping_log_{TODAY}.log')

# 출력 디렉토리 생성
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

    # 로그 파일에 기록
    try:
        with open(LOG_FILE, 'a', encoding='utf-8') as f:
            f.write(log_entry + '\n')
    except Exception as e:
        print(f"로그 파일 쓰기 실패: {e}")

    # 콘솔에 출력
    if print_to_console:
        print(message)

def validate_data_freshness():
    """
    현재 날짜를 기준으로 예상되는 최신 데이터 분기를 계산합니다.
    각 분기말(3월말, 6월말, 9월말, 12월말) 종료 후 2개월 후 마지막 평일에 업로드됨.
    
    업로드 스케줄:
    - 3월말 데이터 → 5월 마지막 평일
    - 6월말 데이터 → 8월 마지막 평일
    - 9월말 데이터 → 11월 마지막 평일
    - 12월말 데이터 → 2월 마지막 평일
    """
    try:
        current_date = datetime.now()
        current_year = current_date.year
        current_month = current_date.month
        current_day = current_date.day
        
        # 마지막 평일 계산을 위한 함수
        def get_last_weekday_of_month(year, month):
            import calendar
            last_day = calendar.monthrange(year, month)[1]
            for day in range(last_day, 0, -1):
                if datetime(year, month, day).weekday() < 5:  # 월요일(0) ~ 금요일(4)
                    return day
            return last_day
        
        # 현재 월의 마지막 평일이 지났는지 확인
        last_weekday_current_month = get_last_weekday_of_month(current_year, current_month)
        is_past_last_weekday = current_day > last_weekday_current_month
        
        # 분기별 예상 업로드 월과 데이터 분기 계산
        if current_month == 11 or (current_month == 12) or (current_month == 1) or (current_month == 2 and not is_past_last_weekday):
            # 11월, 12월, 1월, 2월 마지막평일 전 → 9월말 데이터가 최신
            expected_quarter_end = f"{current_year if current_month >= 11 else current_year-1}년9월말"
            next_expected_quarter_end = f"{current_year if current_month <= 2 else current_year-1}년12월말"
        elif current_month == 2 and is_past_last_weekday:
            # 2월 마지막평일 후 → 12월말 데이터가 업로드됨
            expected_quarter_end = f"{current_year-1}년12월말"
            next_expected_quarter_end = f"{current_year}년3월말"
        elif current_month in [3, 4] or (current_month == 5 and not is_past_last_weekday):
            # 3월, 4월, 5월 마지막평일 전 → 12월말 데이터가 최신
            expected_quarter_end = f"{current_year-1}년12월말"
            next_expected_quarter_end = f"{current_year}년3월말"
        elif current_month == 5 and is_past_last_weekday:
            # 5월 마지막평일 후 → 3월말 데이터가 업로드됨
            expected_quarter_end = f"{current_year}년3월말"
            next_expected_quarter_end = f"{current_year}년6월말"
        elif current_month in [6, 7] or (current_month == 8 and not is_past_last_weekday):
            # 6월, 7월, 8월 마지막평일 전 → 3월말 데이터가 최신
            expected_quarter_end = f"{current_year}년3월말"
            next_expected_quarter_end = f"{current_year}년6월말"
        elif current_month == 8 and is_past_last_weekday:
            # 8월 마지막평일 후 → 6월말 데이터가 업로드됨
            expected_quarter_end = f"{current_year}년6월말"
            next_expected_quarter_end = f"{current_year}년9월말"
        elif current_month in [9, 10] or (current_month == 11 and not is_past_last_weekday):
            # 9월, 10월, 11월 마지막평일 전 → 6월말 데이터가 최신
            expected_quarter_end = f"{current_year}년6월말"
            next_expected_quarter_end = f"{current_year}년9월말"
        else:
            # 기본값 (예외 상황)
            expected_quarter_end = f"{current_year-1}년12월말"
            next_expected_quarter_end = f"{current_year}년3월말"
        
        # 현재가 5월이고 아직 마지막 평일이 지나지 않았다면, 2024년 9월말이 최신
        if current_month == 5 and not is_past_last_weekday:
            expected_quarter_end = f"{current_year-1}년9월말"
            next_expected_quarter_end = f"{current_year}년3월말"
        
        possible_dates = [expected_quarter_end, next_expected_quarter_end]
        
        log_message(f"현재 날짜: {current_date.strftime('%Y년 %m월 %d일')}")
        log_message(f"이번 달 마지막 평일: {current_month}월 {last_weekday_current_month}일")
        log_message(f"마지막 평일 경과 여부: {'예' if is_past_last_weekday else '아니오'}")
        log_message(f"예상 최신 데이터 분기: {expected_quarter_end}")
        log_message(f"조기 업로드 가능 분기: {next_expected_quarter_end}")
        
        return possible_dates
        
    except Exception as e:
        log_message(f"데이터 신선도 검증 오류: {str(e)}")
        return [f"{current_year-1}년9월말", f"{current_year}년3월말"]

def send_email_notification(subject, body, bank_details=None, attachment_paths=None, is_success=True):
    """Gmail SMTP를 통해 이메일 알림을 발송합니다."""
    if not GMAIL_ADDRESS or not GMAIL_APP_PASSWORD or not RECIPIENT_EMAILS:
        log_message("이메일 설정이 불완전하여 알림을 발송하지 않습니다.")
        return False
    
    try:
        # 이메일 메시지 구성
        msg = MIMEMultipart()
        msg['From'] = GMAIL_ADDRESS
        msg['To'] = ', '.join(RECIPIENT_EMAILS)
        msg['Subject'] = subject
        
        # 은행별 상세 정보를 본문에 추가
        enhanced_body = body
        if bank_details:
            enhanced_body += "\n\n===== 은행별 상세 결과 =====\n"
            
            # 성공한 은행들
            successful_banks = [bank for bank in bank_details if bank['status'] == 'success']
            if successful_banks:
                enhanced_body += f"\n✅ 성공한 은행 ({len(successful_banks)}개):\n"
                for bank in successful_banks:
                    enhanced_body += f"  • {bank['name']}: {bank['date_info']}\n"
            
            # 부분 성공한 은행들  
            partial_banks = [bank for bank in bank_details if bank['status'] == 'partial']
            if partial_banks:
                enhanced_body += f"\n⚠️ 부분 성공한 은행 ({len(partial_banks)}개):\n"
                for bank in partial_banks:
                    enhanced_body += f"  • {bank['name']}: {bank['date_info']} (일부 카테고리 누락)\n"
            
            # 실패한 은행들
            failed_banks = [bank for bank in bank_details if bank['status'] == 'failed']
            if failed_banks:
                enhanced_body += f"\n❌ 실패한 은행 ({len(failed_banks)}개):\n"
                for bank in failed_banks:
                    enhanced_body += f"  • {bank['name']}: {bank.get('error_reason', '알 수 없는 오류')}\n"
            
            # 데이터 신선도별 분류
            enhanced_body += "\n\n===== 데이터 신선도별 분류 =====\n"
            fresh_banks = [bank for bank in bank_details if bank.get('is_fresh', False)]
            old_banks = [bank for bank in bank_details if not bank.get('is_fresh', False) and bank['status'] in ['success', 'partial']]
            
            if fresh_banks:
                enhanced_body += f"\n🟢 최신 데이터 은행 ({len(fresh_banks)}개):\n"
                for bank in fresh_banks:
                    enhanced_body += f"  • {bank['name']}: {bank['date_info']}\n"
            
            if old_banks:
                enhanced_body += f"\n🟡 구버전 데이터 은행 ({len(old_banks)}개):\n"
                for bank in old_banks:
                    enhanced_body += f"  • {bank['name']}: {bank['date_info']}\n"
        
        # 본문 추가
        msg.attach(MIMEText(enhanced_body, 'plain', 'utf-8'))
        
        # 첨부 파일 추가
        if attachment_paths:
            for file_path in attachment_paths:
                if os.path.exists(file_path):
                    try:
                        with open(file_path, "rb") as attachment:
                            part = MIMEBase('application', 'octet-stream')
                            part.set_payload(attachment.read())
                        
                        encoders.encode_base64(part)
                        filename = os.path.basename(file_path)
                        part.add_header(
                            'Content-Disposition',
                            f'attachment; filename= {filename}',
                        )
                        msg.attach(part)
                        log_message(f"첨부 파일 추가: {filename}")
                    except Exception as e:
                        log_message(f"첨부 파일 추가 실패 ({file_path}): {str(e)}")
        
        # Gmail SMTP 서버 연결 및 발송
        with smtplib.SMTP('smtp.gmail.com', 587) as server:
            server.starttls()
            server.login(GMAIL_ADDRESS, GMAIL_APP_PASSWORD)
            server.send_message(msg)
        
        log_message(f"이메일 알림 발송 완료: {', '.join(RECIPIENT_EMAILS)}")
        return True
        
    except Exception as e:
        log_message(f"이메일 발송 실패: {str(e)}")
        return False

# =============================================================================
# 드라이버 관리 클래스
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
# 데이터 추출 및 검증 함수들
# =============================================================================

def extract_date_information(driver):
    """웹페이지에서 공시 날짜 정보를 추출합니다."""
    try:
        # 날짜 정보가 포함된 요소들을 다양한 방법으로 찾기
        date_selectors = [
            "//*[contains(text(), '기말') and contains(text(), '년')]",
            "//h1 | //h2 | //h3 | //h4 | //h5 | //th[contains(text(), '년')]",
            "//*[contains(text(), '년') and contains(text(), '월말')]"
        ]
        
        for selector in date_selectors:
            try:
                date_elements = driver.find_elements(By.XPATH, selector)
                for element in date_elements:
                    text = element.text
                    # 정규식으로 날짜 패턴 추출 (XXXX년XX월말)
                    date_pattern = re.compile(r'\d{4}년\s*\d{1,2}월말')
                    matches = date_pattern.findall(text)
                    
                    if matches:
                        # 공백 제거하여 표준화
                        return matches[0].replace(' ', '')
            except Exception:
                continue
        
        # JavaScript로 전체 페이지 텍스트에서 날짜 추출 시도
        try:
            js_script = """
            var allText = document.body.innerText;
            var match = allText.match(/\\d{4}년\\s*\\d{1,2}월말/);
            return match ? match[0].replace(/\\s+/g, '') : '';
            """
            date_text = driver.execute_script(js_script)
            if date_text:
                return date_text
        except Exception:
            pass

        return "날짜 정보 없음"

    except Exception as e:
        log_message(f"날짜 정보 추출 오류: {str(e)}", verbose=False)
        return "날짜 추출 실패"

def validate_extracted_date(extracted_date, expected_dates):
    """추출된 날짜가 예상 날짜와 일치하는지 검증합니다."""
    if not extracted_date or extracted_date in ["날짜 정보 없음", "날짜 추출 실패"]:
        return False, "날짜 정보를 추출할 수 없음"
    
    # 추출된 날짜 정규화
    normalized_extracted = extracted_date.replace(' ', '')
    
    for expected_date in expected_dates:
        normalized_expected = expected_date.replace(' ', '')
        if normalized_extracted == normalized_expected:
            return True, f"최신 데이터 확인: {extracted_date}"
    
    # 부분 일치 확인 (년도와 월 확인)
    try:
        # 추출된 날짜에서 년도와 월 추출
        match = re.search(r'(\d{4})년(\d{1,2})월말', normalized_extracted)
        if match:
            year, month = match.groups()
            for expected_date in expected_dates:
                exp_match = re.search(r'(\d{4})년(\d{1,2})월말', expected_date)
                if exp_match:
                    exp_year, exp_month = exp_match.groups()
                    if year == exp_year and month.zfill(2) == exp_month.zfill(2):
                        return True, f"예상보다 빠른 업로드: {extracted_date}"
    except Exception:
        pass
    
    return False, f"예상 날짜와 불일치: {extracted_date} (예상: {', '.join(expected_dates)})"

def select_bank(driver, bank_name):
    """다양한 방법으로 은행을 선택합니다."""
    try:
        # 메인 페이지로 접속
        driver.get(BASE_URL)
        WaitUtils.wait_for_page_load(driver)
        WaitUtils.wait_with_random(1, 2)

        # JavaScript를 사용한 은행 선택 (가장 확실한 방법)
        js_script = f"""
        // 모든 링크와 테이블 셀에서 은행명 검색
        var allElements = document.querySelectorAll('a, td, th, span, div');
        var targetBank = '{bank_name}';
        
        for(var i = 0; i < allElements.length; i++) {{
            var element = allElements[i];
            var text = element.textContent.trim();
            
            // 정확한 일치 우선
            if(text === targetBank) {{
                element.scrollIntoView({{block: 'center'}});
                if(element.tagName.toLowerCase() === 'a') {{
                    element.click();
                    return 'exact_link_click';
                }}
                // 링크가 아니면 내부 링크 찾기
                var links = element.querySelectorAll('a');
                if(links.length > 0) {{
                    links[0].click();
                    return 'exact_nested_link_click';
                }}
                element.click();
                return 'exact_element_click';
            }}
            
            // 부분 일치
            if(text.includes(targetBank) && text.length < targetBank.length + 10) {{
                element.scrollIntoView({{block: 'center'}});
                if(element.tagName.toLowerCase() === 'a') {{
                    element.click();
                    return 'partial_link_click';
                }}
                var links = element.querySelectorAll('a');
                if(links.length > 0) {{
                    links[0].click();
                    return 'partial_nested_link_click';
                }}
                element.click();
                return 'partial_element_click';
            }}
        }}
        
        return false;
        """
        
        result = driver.execute_script(js_script)
        if result:
            log_message(f"{bank_name} 은행: JavaScript {result} 성공", verbose=False)
            WaitUtils.wait_with_random(1, 2)
            return True

        # Selenium을 사용한 대체 방법들
        bank_xpaths = [
            f"//td[text()='{bank_name}']//a | //a[text()='{bank_name}']",
            f"//td[contains(text(), '{bank_name}')]//a | //a[contains(text(), '{bank_name}')]",
            f"//*[contains(text(), '{bank_name}') and (self::a or descendant::a)]"
        ]

        for xpath in bank_xpaths:
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

        log_message(f"{bank_name} 은행을 찾을 수 없습니다.")
        return False

    except Exception as e:
        log_message(f"{bank_name} 은행 선택 실패: {str(e)}")
        return False

def select_category(driver, category):
    """특정 카테고리 탭을 클릭합니다."""
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
    """현재 페이지에서 모든 테이블을 추출합니다."""
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

        # 현재 페이지 URL 확인
        try:
            base_bank_url = driver.current_url
            log_message(f"{bank_name} 은행 페이지 접속 성공", verbose=False)
        except Exception:
            log_message(f"{bank_name} 은행 페이지 URL 획득 실패")
            return None

        # 날짜 정보 추출 및 검증
        date_info = extract_date_information(driver)
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
            return None

        log_message(f"[완료] {bank_name} 은행 데이터 수집 완료 (카테고리: {', '.join(table_categories)})")
        return result_data

    except Exception as e:
        log_message(f"{bank_name} 은행 처리 중 오류 발생: {str(e)}")
        return None

def save_bank_data(bank_name, data_dict):
    """수집된 은행 데이터를 엑셀 파일로 저장합니다."""
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
                '데이터 신선도': ['최신' if data_dict.get('신선도', False) else '구버전'],
                '추출 일시': [datetime.now().strftime("%Y-%m-%d %H:%M:%S")],
                '스크래핑 시스템': ['GitHub Actions 저축은행 스크래퍼 v1.0']
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
    """워커 스레드에서 은행 처리를 수행합니다."""
    driver = None
    
    try:
        driver = driver_manager.get_driver()
        
        # 최대 재시도 횟수만큼 시도
        for attempt in range(MAX_RETRIES):
            try:
                # 은행 데이터 스크래핑
                result_data = scrape_bank_data(bank_name, driver, progress_manager, expected_dates)

                if result_data:
                    # 데이터 저장
                    if save_bank_data(bank_name, result_data):
                        progress_manager.mark_completed(bank_name)
                        return bank_name, True, result_data.get('검증결과', '')
                    else:
                        if attempt < MAX_RETRIES - 1:
                            log_message(f"{bank_name} 은행 데이터 저장 실패, 재시도 {attempt+1}/{MAX_RETRIES}...")
                            WaitUtils.wait_with_random(2, 4)
                        else:
                            log_message(f"{bank_name} 은행 데이터 저장 실패, 최대 시도 횟수 초과")
                else:
                    if attempt < MAX_RETRIES - 1:
                        log_message(f"{bank_name} 은행 데이터 스크래핑 실패, 재시도 {attempt+1}/{MAX_RETRIES}...")
                        WaitUtils.wait_with_random(2, 4)
                    else:
                        log_message(f"{bank_name} 은행 데이터 스크래핑 실패, 최대 시도 횟수 초과")

            except Exception as e:
                if attempt < MAX_RETRIES - 1:
                    log_message(f"{bank_name} 은행 처리 중 오류: {str(e)}, 재시도 {attempt+1}/{MAX_RETRIES}...")
                    WaitUtils.wait_with_random(2, 4)
                else:
                    log_message(f"{bank_name} 은행 처리 실패: {str(e)}, 최대 시도 횟수 초과")

        # 모든 시도 실패
        progress_manager.mark_failed(bank_name)
        return bank_name, False, "모든 시도 실패"

    except Exception as e:
        log_message(f"{bank_name} 은행 처리 중 예상치 못한 오류: {str(e)}")
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
    
    # 배치 단위로 처리 (메모리 효율성)
    batch_size = max(1, len(banks) // MAX_WORKERS) if len(banks) > MAX_WORKERS else len(banks)
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

def process_with_retry(banks, max_retries=1):
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

    try:
        # 비동기 이벤트 루프 실행
        if sys.version_info >= (3, 7):
            results = asyncio.run(process_banks_async(pending_banks, driver_manager, progress_manager, expected_dates))
        else:
            loop = asyncio.get_event_loop()
            results = loop.run_until_complete(process_banks_async(pending_banks, driver_manager, progress_manager, expected_dates))

        # 결과 분석
        successful_banks = [r[0] for r in results if r[1]]
        failed_banks = [r[0] for r in results if not r[1]]

        # 실패한 은행 재시도
        retry_count = 0
        while failed_banks and retry_count < max_retries:
            retry_count += 1
            log_message(f"재시도 {retry_count}/{max_retries}: {len(failed_banks)}개 은행 처리 중...")

            # 재시도 실행
            if sys.version_info >= (3, 7):
                retry_results = asyncio.run(process_banks_async(failed_banks, driver_manager, progress_manager, expected_dates))
            else:
                loop = asyncio.get_event_loop()
                retry_results = loop.run_until_complete(process_banks_async(failed_banks, driver_manager, progress_manager, expected_dates))

            # 결과 갱신
            newly_successful = [r[0] for r in retry_results if r[1]]
            failed_banks = [r[0] for r in retry_results if not r[1]]

            successful_banks.extend(newly_successful)
            log_message(f"재시도 {retry_count} 결과: {len(newly_successful)}개 성공, {len(failed_banks)}개 실패")

        return successful_banks, failed_banks, results

    finally:
        # 드라이버 정리
        driver_manager.close_all()

def collect_bank_details():
    """각 은행별 상세 정보를 수집합니다."""
    bank_details = []
    progress_manager = ProgressManager()
    
    try:
        # 진행 상황에서 검증 데이터 가져오기
        validation_data = progress_manager.progress.get('data_validation', [])
        validation_dict = {item['bank_name']: item for item in validation_data}
        
        for bank in BANKS:
            bank_info = {
                'name': bank,
                'status': 'failed',
                'date_info': '데이터 없음',
                'is_fresh': False,
                'categories': [],
                'error_reason': '처리되지 않음'
            }
            
            # 각 은행의 엑셀 파일 찾기
            bank_files = [f for f in os.listdir(OUTPUT_DIR) if f.startswith(f"{bank}_") and f.endswith(".xlsx")]
            
            if bank_files:
                try:
                    # 가장 최근 파일 선택
                    latest_file = sorted(bank_files)[-1]
                    file_path = os.path.join(OUTPUT_DIR, latest_file)
                    
                    # 엑셀 파일 분석
                    xls = pd.ExcelFile(file_path)
                    
                    # 카테고리 추출
                    categories = []
                    for sheet in xls.sheet_names:
                        if sheet != '공시정보':
                            category = sheet.split('_')[0] if '_' in sheet else sheet
                            categories.append(category)
                    
                    categories = sorted(list(set(categories)))
                    bank_info['categories'] = categories
                    
                    # 공시 정보에서 상세 데이터 추출
                    if '공시정보' in xls.sheet_names:
                        info_df = pd.read_excel(file_path, sheet_name='공시정보')
                        if '공시 날짜' in info_df.columns and not info_df['공시 날짜'].empty:
                            bank_info['date_info'] = str(info_df['공시 날짜'].iloc[0])
                        if '데이터 신선도' in info_df.columns and not info_df['데이터 신선도'].empty:
                            bank_info['is_fresh'] = str(info_df['데이터 신선도'].iloc[0]) == '최신'
                    
                    # 상태 결정
                    if set(categories) >= set(CATEGORIES):
                        bank_info['status'] = 'success'
                    elif categories:
                        bank_info['status'] = 'partial'
                        bank_info['error_reason'] = f"누락된 카테고리: {', '.join(set(CATEGORIES) - set(categories))}"
                    else:
                        bank_info['status'] = 'failed'
                        bank_info['error_reason'] = '테이블 추출 실패'
                        
                except Exception as e:
                    bank_info['error_reason'] = f'파일 분석 오류: {str(e)}'
            else:
                # 검증 데이터에서 정보 추출 시도
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
    """스크래핑 결과 요약 보고서를 생성합니다."""
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
                    data_freshness = "알 수 없음"
                    
                    if '공시정보' in xls.sheet_names:
                        info_df = pd.read_excel(file_path, sheet_name='공시정보')
                        if '공시 날짜' in info_df.columns and not info_df['공시 날짜'].empty:
                            date_info = str(info_df['공시 날짜'].iloc[0])
                        if '검증 결과' in info_df.columns and not info_df['검증 결과'].empty:
                            validation_result = str(info_df['검증 결과'].iloc[0])
                        if '데이터 신선도' in info_df.columns and not info_df['데이터 신선도'].empty:
                            data_freshness = str(info_df['데이터 신선도'].iloc[0])

                    # 상태 결정
                    status = '완료' if set(categories) >= set(CATEGORIES) else '부분 완료'

                    bank_summary.append({
                        '은행명': bank,
                        '스크래핑 상태': status,
                        '공시 날짜': date_info,
                        '데이터 신선도': data_freshness,
                        '검증 결과': validation_result,
                        '시트 수': sheet_count - 1,  # 공시정보 시트 제외
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
            else:
                status = '실패' if bank in failed_banks else '미처리'
                validation_info = validation_dict.get(bank, {})
                
                bank_summary.append({
                    '은행명': bank,
                    '스크래핑 상태': status,
                    '공시 날짜': validation_info.get('date_info', ''),
                    '데이터 신선도': '최신' if validation_info.get('is_fresh', False) else '구버전',
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
            '실패 은행 수': len([r for r in bank_summary if r['스크래핑 상태'] in ['실패', '파일 손상']]),
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

def create_zip_archive():
    """결과 파일들을 ZIP으로 압축합니다."""
    try:
        zip_filename = f'저축은행_데이터_{TODAY}.zip'
        zip_path = os.path.join(OUTPUT_BASE_DIR, zip_filename)
        
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            # OUTPUT_DIR 내의 모든 파일을 ZIP에 추가
            for root, dirs, files in os.walk(OUTPUT_DIR):
                for file in files:
                    file_path = os.path.join(root, file)
                    # ZIP 내에서의 상대 경로 계산
                    arcname = os.path.relpath(file_path, OUTPUT_BASE_DIR)
                    zipf.write(file_path, arcname)
        
        log_message(f"ZIP 아카이브 생성 완료: {zip_path}")
        return zip_path
        
    except Exception as e:
        log_message(f"ZIP 아카이브 생성 실패: {str(e)}")
        return None

# =============================================================================
# 메인 실행 함수
# =============================================================================

def main():
    """메인 실행 함수"""
    # 로그 파일 초기화
    try:
        with open(LOG_FILE, 'w', encoding='utf-8') as f:
            f.write("")
    except Exception as e:
        print(f"로그 파일 초기화 실패: {e}")

    start_time = time.time()
    log_message(f"\n===== 저축은행 중앙회 통일경영공시 데이터 스크래핑 시작 (GitHub Actions 버전) [{TODAY}] =====\n")

    try:
        # 환경 설정 로그
        log_message(f"설정값:")
        log_message(f"- 최대 워커 수: {MAX_WORKERS}")
        log_message(f"- 최대 재시도 횟수: {MAX_RETRIES}")
        log_message(f"- 페이지 로드 타임아웃: {PAGE_LOAD_TIMEOUT}초")
        log_message(f"- 대기 타임아웃: {WAIT_TIMEOUT}초")
        log_message(f"- 출력 디렉토리: {OUTPUT_DIR}")
        log_message(f"- 이메일 알림: {'활성화' if GMAIL_ADDRESS and GMAIL_APP_PASSWORD else '비활성화'}")

        # 은행 처리 실행
        successful_banks, failed_banks, all_results = process_with_retry(BANKS, max_retries=MAX_RETRIES)

        # 결과 요약 생성
        summary_file, stats = generate_summary_report()

        # ZIP 아카이브 생성
        zip_file = create_zip_archive()

        # 실행 시간 계산
        end_time = time.time()
        total_duration = end_time - start_time
        minutes, seconds = divmod(total_duration, 60)
        
        # 최종 결과 로그
        log_message(f"\n===== 스크래핑 완료 =====")
        log_message(f"총 실행 시간: {int(minutes)}분 {int(seconds)}초")
        log_message(f"성공한 은행: {len(successful_banks)}개")
        log_message(f"실패한 은행: {len(failed_banks)}개")
        
        if failed_banks:
            log_message(f"실패한 은행 목록: {', '.join(failed_banks)}")

        # 이메일 알림 발송
        if GMAIL_ADDRESS and GMAIL_APP_PASSWORD and RECIPIENT_EMAILS:
            # 은행별 상세 정보 수집
            bank_details = collect_bank_details()
            
            subject = f"저축은행 데이터 스크래핑 {'완료' if not failed_banks else '부분완료'} - {TODAY}"
            
            body = f"""저축은행 중앙회 통일경영공시 데이터 스크래핑이 완료되었습니다.

실행 정보:
- 실행 날짜: {datetime.now().strftime('%Y년 %m월 %d일 %H시 %M분')}
- 총 실행 시간: {int(minutes)}분 {int(seconds)}초
- 처리 환경: GitHub Actions

결과 요약:
- 전체 은행 수: {stats.get('전체 은행 수', len(BANKS))}개
- 완료 은행 수: {stats.get('완료 은행 수', len(successful_banks))}개
- 부분 완료 은행 수: {stats.get('부분 완료 은행 수', 0)}개
- 실패 은행 수: {stats.get('실패 은행 수', len(failed_banks))}개
- 최신 데이터 은행 수: {stats.get('최신 데이터 은행 수', 0)}개
- 성공률: {stats.get('성공률', '0.00%')}

첨부 파일:
- 모든 은행 데이터 (ZIP 압축파일)
- 요약 보고서 (Excel)
- 실행 로그 파일
"""

            # 첨부 파일 준비 (ZIP 파일 우선)
            attachments = []
            if zip_file and os.path.exists(zip_file):
                attachments.append(zip_file)
            if summary_file and os.path.exists(summary_file):
                attachments.append(summary_file)
            if os.path.exists(LOG_FILE):
                attachments.append(LOG_FILE)

            # 이메일 발송
            is_success = len(failed_banks) == 0
            send_email_notification(subject, body, bank_details, attachments, is_success)

        log_message(f"\n===== 저축은행 중앙회 통일경영공시 데이터 스크래핑 완료 [{TODAY}] =====")

    except KeyboardInterrupt:
        log_message("\n사용자에 의해 중단되었습니다.")
    except Exception as e:
        log_message(f"예상치 못한 오류 발생: {str(e)}")
        import traceback
        log_message(traceback.format_exc())
        
        # 오류 발생 시에도 이메일 알림 발송
        if GMAIL_ADDRESS and GMAIL_APP_PASSWORD and RECIPIENT_EMAILS:
            error_subject = f"저축은행 데이터 스크래핑 오류 발생 - {TODAY}"
            error_body = f"""저축은행 데이터 스크래핑 중 오류가 발생했습니다.

오류 정보:
- 발생 시간: {datetime.now().strftime('%Y년 %m월 %d일 %H시 %M분')}
- 오류 내용: {str(e)}

자세한 내용은 첨부된 로그 파일을 확인해주세요.
"""
            attachments = [LOG_FILE] if os.path.exists(LOG_FILE) else []
            send_email_notification(error_subject, error_body, attachments, False)

if __name__ == "__main__":
    main()
