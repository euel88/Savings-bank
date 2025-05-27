"""
저축은행 중앙회 통일경영공시 데이터 자동 스크래핑 도구 (Headless 버전)
목적: GitHub Actions에서 자동 실행을 위한 헤드리스 버전
작성일: 2025-03-31
특징:
- GUI 제거 및 CLI 기반 실행
- GitHub Actions 환경에 최적화
- 환경 변수를 통한 설정
- 자동 재시도 및 에러 핸들링
"""

import os
import sys
import time
import random
import json
import re
import concurrent.futures
import zipfile
from datetime import datetime
from io import StringIO
import argparse
import logging
from pathlib import Path

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
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options

# 데이터 처리 관련 임포트
from bs4 import BeautifulSoup
import pandas as pd
import warnings
warnings.filterwarnings("ignore")

# 로깅 설정
def setup_logging(log_level="INFO"):
    """로깅 시스템을 설정합니다."""
    logging.basicConfig(
        level=getattr(logging, log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),  # 콘솔 출력
            logging.FileHandler('scraping.log', encoding='utf-8')  # 파일 출력
        ]
    )
    return logging.getLogger(__name__)

# 전역 로거
logger = setup_logging()

# 이메일 전송 클래스
class EmailSender:
    """Gmail을 통해 이메일을 전송하는 클래스"""
    
    def __init__(self):
        # 환경 변수에서 이메일 설정 읽기
        self.smtp_server = "smtp.gmail.com"
        self.smtp_port = 587
        self.sender_email = os.getenv('GMAIL_ADDRESS')
        self.sender_password = os.getenv('GMAIL_APP_PASSWORD')
        self.recipient_emails = os.getenv('RECIPIENT_EMAILS', '').split(',')
        
        # 설정 확인
        if not self.sender_email or not self.sender_password:
            logger.warning("이메일 설정이 없습니다. 이메일 전송을 건너뜁니다.")
            self.enabled = False
        else:
            self.enabled = True
            logger.info(f"이메일 전송 설정 완료: {self.sender_email}")
    
    def send_email_with_attachment(self, subject, body, attachment_path=None):
        """첨부 파일과 함께 이메일을 전송합니다."""
        if not self.enabled:
            logger.info("이메일 전송이 비활성화되어 있습니다.")
            return False
        
        try:
            # 이메일 메시지 생성
            msg = MIMEMultipart()
            msg['From'] = self.sender_email
            msg['To'] = ', '.join(self.recipient_emails)
            msg['Subject'] = subject
            
            # 본문 추가
            msg.attach(MIMEText(body, 'html'))
            
            # 첨부 파일 추가
            if attachment_path and os.path.exists(attachment_path):
                with open(attachment_path, 'rb') as attachment:
                    part = MIMEBase('application', 'octet-stream')
                    part.set_payload(attachment.read())
                    encoders.encode_base64(part)
                    part.add_header(
                        'Content-Disposition',
                        f'attachment; filename= {os.path.basename(attachment_path)}'
                    )
                    msg.attach(part)
                logger.info(f"첨부 파일 추가: {attachment_path}")
            
            # Gmail 서버에 연결 및 전송
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls()  # TLS 보안 연결
                server.login(self.sender_email, self.sender_password)
                server.send_message(msg)
            
            logger.info(f"이메일 전송 성공: {', '.join(self.recipient_emails)}")
            return True
            
        except Exception as e:
            logger.error(f"이메일 전송 실패: {str(e)}")
            return False

# 설정 클래스
class Config:
    """환경 변수와 기본값을 관리하는 설정 클래스"""
    
    def __init__(self):
        # 환경 변수에서 설정 읽기
        self.VERSION = "2.0"
        self.BASE_URL = "https://www.fsb.or.kr/busmagequar_0100.act"
        self.MAX_RETRIES = int(os.getenv('MAX_RETRIES', '3'))
        self.PAGE_LOAD_TIMEOUT = int(os.getenv('PAGE_LOAD_TIMEOUT', '30'))
        self.WAIT_TIMEOUT = int(os.getenv('WAIT_TIMEOUT', '10'))
        self.MAX_WORKERS = int(os.getenv('MAX_WORKERS', '2'))  # GitHub Actions에서는 적은 수 권장
        
        # 출력 디렉토리 설정
        self.today = datetime.now().strftime("%Y%m%d")
        self.output_dir = os.getenv('OUTPUT_DIR', f"./output/저축은행_데이터_{self.today}")
        os.makedirs(self.output_dir, exist_ok=True)
        
        # 전체 79개 저축은행 목록
        self.BANKS = [
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
        self.CATEGORIES = ["영업개황", "재무현황", "손익현황", "기타"]
        
        # 진행 상황 파일
        self.progress_file = os.path.join(self.output_dir, 'progress.json')
        
        logger.info(f"설정 초기화 완료: 출력 디렉토리={self.output_dir}, 워커 수={self.MAX_WORKERS}")

# 웹드라이버 관리 클래스
class DriverManager:
    """헤드리스 Chrome 웹드라이버를 관리합니다."""
    
    def __init__(self, config):
        self.config = config
        
    def create_driver(self):
        """GitHub Actions 환경에 최적화된 Chrome 드라이버를 생성합니다."""
        options = Options()
        
        # 헤드리스 모드 설정 (필수)
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')  # GitHub Actions에서 필수
        options.add_argument('--disable-dev-shm-usage')  # 메모리 문제 방지
        options.add_argument('--disable-gpu')  # GPU 사용 안함
        
        # 성능 최적화 옵션
        options.add_argument('--disable-web-security')
        options.add_argument('--disable-features=VizDisplayCompositor')
        options.add_argument('--disable-blink-features=AutomationControlled')
        
        # 브라우저 크기 설정
        options.add_argument('--window-size=1920,1080')
        
        # User-Agent 설정
        options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
        
        # 추가 최적화
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        
        # 프리퍼런스 설정
        prefs = {
            'profile.default_content_setting_values': {
                'images': 2,  # 이미지 로딩 비활성화로 속도 향상
                'plugins': 2,
                'popups': 2,
                'geolocation': 2,
                'notifications': 2,
                'media_stream': 2,
            }
        }
        options.add_experimental_option('prefs', prefs)
        
        try:
            driver = webdriver.Chrome(options=options)
            driver.set_page_load_timeout(self.config.PAGE_LOAD_TIMEOUT)
            logger.info("Chrome 드라이버 생성 성공")
            return driver
        except Exception as e:
            logger.error(f"Chrome 드라이버 생성 실패: {str(e)}")
            raise

# 진행 상황 관리자
class ProgressManager:
    """스크래핑 진행 상황을 추적하고 저장합니다."""
    
    def __init__(self, config):
        self.config = config
        self.progress = self.load()
        
    def load(self):
        """저장된 진행 상황을 로드합니다."""
        if os.path.exists(self.config.progress_file):
            try:
                with open(self.config.progress_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except:
                logger.warning("진행 상황 파일 로드 실패, 새로 시작합니다.")
        
        return {
            'completed': [],
            'failed': [],
            'timestamp': datetime.now().isoformat()
        }
    
    def save(self):
        """진행 상황을 파일에 저장합니다."""
        with open(self.config.progress_file, 'w', encoding='utf-8') as f:
            json.dump(self.progress, f, ensure_ascii=False, indent=2)
    
    def mark_completed(self, bank_name):
        """은행을 완료 목록에 추가합니다."""
        if bank_name not in self.progress['completed']:
            self.progress['completed'].append(bank_name)
            if bank_name in self.progress['failed']:
                self.progress['failed'].remove(bank_name)
            self.save()
            logger.info(f"{bank_name} 은행 완료 처리")
    
    def mark_failed(self, bank_name):
        """은행을 실패 목록에 추가합니다."""
        if bank_name not in self.progress['failed'] and bank_name not in self.progress['completed']:
            self.progress['failed'].append(bank_name)
            self.save()
            logger.warning(f"{bank_name} 은행 실패 처리")
    
    def get_pending_banks(self, banks):
        """아직 처리하지 않은 은행 목록을 반환합니다."""
        completed = set(self.progress['completed'])
        return [bank for bank in banks if bank not in completed]

# 스크래퍼 클래스
class BankScraper:
    """실제 스크래핑을 수행하는 클래스"""
    
    def __init__(self, config, driver_manager, progress_manager):
        self.config = config
        self.driver_manager = driver_manager
        self.progress_manager = progress_manager
        
    def extract_date_information(self, driver):
        """웹페이지에서 공시 날짜 정보를 추출합니다."""
        try:
            # JavaScript로 날짜 패턴 검색
            js_script = """
            var allText = document.body.innerText;
            var match = allText.match(/\\d{4}년\\d{1,2}월말/);
            return match ? match[0] : '날짜 정보 없음';
            """
            return driver.execute_script(js_script)
        except Exception as e:
            logger.error(f"날짜 정보 추출 실패: {str(e)}")
            return "날짜 추출 실패"
    
    def select_bank(self, driver, bank_name):
        """은행을 선택합니다."""
        try:
            # 메인 페이지 접속
            driver.get(self.config.BASE_URL)
            time.sleep(2)  # 페이지 로딩 대기
            
            # JavaScript로 은행 선택
            js_script = f"""
            var links = document.querySelectorAll('a, td');
            for(var i = 0; i < links.length; i++) {{
                if(links[i].textContent.trim() === '{bank_name}') {{
                    links[i].click();
                    return true;
                }}
            }}
            return false;
            """
            
            result = driver.execute_script(js_script)
            if result:
                time.sleep(2)  # 페이지 전환 대기
                return True
            
            logger.warning(f"{bank_name} 은행을 찾을 수 없습니다.")
            return False
            
        except Exception as e:
            logger.error(f"{bank_name} 은행 선택 실패: {str(e)}")
            return False
    
    def select_category(self, driver, category):
        """카테고리 탭을 선택합니다."""
        try:
            # JavaScript로 카테고리 선택
            js_script = f"""
            var elements = document.querySelectorAll('a, button, li, span');
            for(var i = 0; i < elements.length; i++) {{
                if(elements[i].textContent.includes('{category}')) {{
                    elements[i].click();
                    return true;
                }}
            }}
            return false;
            """
            
            result = driver.execute_script(js_script)
            if result:
                time.sleep(1)  # 탭 전환 대기
                return True
                
            return False
            
        except Exception as e:
            logger.error(f"{category} 카테고리 선택 실패: {str(e)}")
            return False
    
    def extract_tables_from_page(self, driver):
        """현재 페이지에서 테이블을 추출합니다."""
        try:
            html = driver.page_source
            
            # pandas로 테이블 추출
            dfs = pd.read_html(StringIO(html))
            
            # 유효한 테이블만 필터링
            valid_dfs = []
            for df in dfs:
                if not df.empty and df.shape[0] > 0 and df.shape[1] > 0:
                    # MultiIndex 처리
                    if isinstance(df.columns, pd.MultiIndex):
                        df.columns = ['_'.join(map(str, col)).strip() for col in df.columns]
                    valid_dfs.append(df)
            
            return valid_dfs
            
        except Exception as e:
            logger.error(f"테이블 추출 실패: {str(e)}")
            return []
    
    def scrape_bank(self, bank_name):
        """단일 은행의 데이터를 스크래핑합니다."""
        driver = None
        try:
            logger.info(f"{bank_name} 은행 스크래핑 시작")
            driver = self.driver_manager.create_driver()
            
            # 은행 선택
            if not self.select_bank(driver, bank_name):
                raise Exception("은행 선택 실패")
            
            # 날짜 정보 추출
            date_info = self.extract_date_information(driver)
            result_data = {'날짜정보': date_info}
            
            # 각 카테고리별로 데이터 수집
            for category in self.config.CATEGORIES:
                if self.select_category(driver, category):
                    tables = self.extract_tables_from_page(driver)
                    if tables:
                        result_data[category] = tables
                        logger.info(f"{bank_name} - {category}: {len(tables)}개 테이블 추출")
            
            # 데이터 저장
            if len(result_data) > 1:  # 날짜정보 외에 다른 데이터가 있는 경우
                self.save_bank_data(bank_name, result_data)
                self.progress_manager.mark_completed(bank_name)
                return True
            else:
                raise Exception("데이터 추출 실패")
                
        except Exception as e:
            logger.error(f"{bank_name} 은행 처리 실패: {str(e)}")
            self.progress_manager.mark_failed(bank_name)
            return False
        finally:
            if driver:
                driver.quit()
    
    def save_bank_data(self, bank_name, data_dict):
        """수집된 데이터를 엑셀 파일로 저장합니다."""
        try:
            date_info = data_dict.get('날짜정보', '날짜정보없음')
            date_info = date_info.replace('/', '-').replace('\\', '-')
            
            excel_path = os.path.join(self.config.output_dir, f"{bank_name}_{date_info}.xlsx")
            
            with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
                # 정보 시트
                info_df = pd.DataFrame({
                    '은행명': [bank_name],
                    '공시 날짜': [date_info],
                    '추출 일시': [datetime.now().strftime("%Y-%m-%d %H:%M:%S")]
                })
                info_df.to_excel(writer, sheet_name='정보', index=False)
                
                # 카테고리별 데이터 저장
                for category, tables in data_dict.items():
                    if category == '날짜정보':
                        continue
                    
                    for i, df in enumerate(tables):
                        sheet_name = f"{category}_{i+1}" if i > 0 else category
                        sheet_name = sheet_name[:31]  # 엑셀 시트명 길이 제한
                        df.to_excel(writer, sheet_name=sheet_name, index=False)
            
            logger.info(f"{bank_name} 데이터 저장 완료: {excel_path}")
            return True
            
        except Exception as e:
            logger.error(f"{bank_name} 데이터 저장 실패: {str(e)}")
            return False
    
    def run(self, banks=None):
        """스크래핑을 실행합니다."""
        if banks is None:
            banks = self.config.BANKS
        
        # 이미 완료된 은행 제외
        pending_banks = self.progress_manager.get_pending_banks(banks)
        
        if not pending_banks:
            logger.info("모든 은행 처리 완료")
            return
        
        logger.info(f"처리할 은행 수: {len(pending_banks)}")
        
        # 순차 처리 (GitHub Actions에서는 병렬 처리보다 안정적)
        for bank in pending_banks:
            for retry in range(self.config.MAX_RETRIES):
                if self.scrape_bank(bank):
                    break
                else:
                    if retry < self.config.MAX_RETRIES - 1:
                        logger.info(f"{bank} 은행 재시도 {retry + 1}/{self.config.MAX_RETRIES}")
                        time.sleep(5)  # 재시도 전 대기
        
        # 결과 요약
        completed = len(self.progress_manager.progress['completed'])
        failed = len(self.progress_manager.progress['failed'])
        logger.info(f"스크래핑 완료: 성공 {completed}개, 실패 {failed}개")
        
        # 요약 보고서 생성
        self.generate_summary_report()
    
    def generate_summary_report(self):
        """스크래핑 결과 요약 보고서를 생성하고 이메일로 전송합니다."""
        try:
            summary_data = []
            
            for bank in self.config.BANKS:
                status = '미처리'
                if bank in self.progress_manager.progress['completed']:
                    status = '완료'
                elif bank in self.progress_manager.progress['failed']:
                    status = '실패'
                
                summary_data.append({
                    '은행명': bank,
                    '상태': status,
                    '처리시간': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                })
            
            # 요약 DataFrame 생성
            summary_df = pd.DataFrame(summary_data)
            summary_file = os.path.join(self.config.output_dir, f"스크래핑_요약_{self.config.today}.xlsx")
            summary_df.to_excel(summary_file, index=False)
            
            logger.info(f"요약 보고서 생성 완료: {summary_file}")
            
            # 압축 파일 생성
            zip_file_path = self.create_zip_file()
            
            # 이메일 전송
            if zip_file_path:
                self.send_result_email(summary_df, zip_file_path)
            
        except Exception as e:
            logger.error(f"요약 보고서 생성 실패: {str(e)}")
    
    def send_result_email(self, summary_df, zip_file_path):
        """스크래핑 결과를 이메일로 전송합니다."""
        try:
            # 이메일 전송기 초기화
            email_sender = EmailSender()
            
            if not email_sender.enabled:
                return
            
            # 통계 계산
            total_banks = len(self.config.BANKS)
            completed_banks = len(self.progress_manager.progress['completed'])
            failed_banks = len(self.progress_manager.progress['failed'])
            success_rate = (completed_banks / total_banks * 100) if total_banks > 0 else 0
            
            # 이메일 제목
            subject = f"[저축은행 데이터] {self.config.today} 스크래핑 결과 - 성공률 {success_rate:.1f}%"
            
            # 이메일 본문 HTML
            body = f"""
            <html>
            <head>
                <style>
                    body {{ font-family: Arial, sans-serif; line-height: 1.6; }}
                    h2 {{ color: #333; }}
                    .summary {{ background-color: #f4f4f4; padding: 15px; border-radius: 5px; margin: 10px 0; }}
                    .stats {{ margin: 10px 0; }}
                    .stat-item {{ margin: 5px 0; }}
                    .success {{ color: #4CAF50; font-weight: bold; }}
                    .fail {{ color: #f44336; font-weight: bold; }}
                    table {{ border-collapse: collapse; width: 100%; margin-top: 20px; }}
                    th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                    th {{ background-color: #4CAF50; color: white; }}
                    tr:nth-child(even) {{ background-color: #f2f2f2; }}
                    .footer {{ margin-top: 30px; font-size: 12px; color: #666; }}
                </style>
            </head>
            <body>
                <h2>저축은행 중앙회 데이터 스크래핑 완료</h2>
                
                <div class="summary">
                    <h3>📊 스크래핑 요약</h3>
                    <div class="stats">
                        <div class="stat-item">📅 실행 일자: {datetime.now().strftime("%Y년 %m월 %d일 %H:%M")}</div>
                        <div class="stat-item">🏦 전체 은행 수: {total_banks}개</div>
                        <div class="stat-item"><span class="success">✅ 성공</span>: {completed_banks}개</div>
                        <div class="stat-item"><span class="fail">❌ 실패</span>: {failed_banks}개</div>
                        <div class="stat-item">📈 성공률: <span class="{'success' if success_rate >= 80 else 'fail'}">{success_rate:.1f}%</span></div>
                    </div>
                </div>
                
                <h3>🏦 은행별 처리 상태</h3>
                <table>
                    <tr>
                        <th>은행명</th>
                        <th>상태</th>
                        <th>처리시간</th>
                    </tr>
            """
            
            # 실패한 은행들을 먼저 표시
            failed_banks_list = [row for _, row in summary_df.iterrows() if row['상태'] == '실패']
            for row in failed_banks_list:
                body += f"""
                    <tr style="background-color: #ffebee;">
                        <td>{row['은행명']}</td>
                        <td><span class="fail">{row['상태']}</span></td>
                        <td>{row['처리시간']}</td>
                    </tr>
                """
            
            # 성공한 은행들은 처음 10개만 표시
            completed_banks_list = [row for _, row in summary_df.iterrows() if row['상태'] == '완료'][:10]
            for row in completed_banks_list:
                body += f"""
                    <tr>
                        <td>{row['은행명']}</td>
                        <td><span class="success">{row['상태']}</span></td>
                        <td>{row['처리시간']}</td>
                    </tr>
                """
            
            if len(completed_banks_list) < completed_banks:
                body += f"""
                    <tr>
                        <td colspan="3" style="text-align: center; font-style: italic;">
                            ... 그 외 {completed_banks - len(completed_banks_list)}개 은행 성공
                        </td>
                    </tr>
                """
            
            body += """
                </table>
                
                <div class="footer">
                    <p>💾 첨부된 ZIP 파일에 전체 데이터가 포함되어 있습니다.</p>
                    <p>📧 이 메일은 GitHub Actions에서 자동으로 발송되었습니다.</p>
                    <p>⚙️ 저축은행 중앙회 통일경영공시 자동 스크래퍼 v2.0</p>
                </div>
            </body>
            </html>
            """
            
            # 이메일 전송
            email_sender.send_email_with_attachment(subject, body, zip_file_path)
            
        except Exception as e:
            logger.error(f"이메일 전송 중 오류: {str(e)}")
    
    def create_zip_file(self):
        """결과를 압축 파일로 만듭니다."""
        try:
            zip_file = f"저축은행_데이터_{self.config.today}.zip"
            
            with zipfile.ZipFile(zip_file, 'w', zipfile.ZIP_DEFLATED) as zipf:
                for root, dirs, files in os.walk(self.config.output_dir):
                    for file in files:
                        file_path = os.path.join(root, file)
                        arcname = os.path.relpath(file_path, self.config.output_dir)
                        zipf.write(file_path, arcname)
            
            logger.info(f"압축 파일 생성 완료: {zip_file}")
            return zip_file
            
        except Exception as e:
            logger.error(f"압축 파일 생성 실패: {str(e)}")
            return None

# 메인 함수
def main():
    """스크립트의 진입점"""
    parser = argparse.ArgumentParser(description='저축은행 중앙회 데이터 스크래퍼')
    parser.add_argument('--banks', nargs='+', help='스크래핑할 은행 목록')
    parser.add_argument('--workers', type=int, help='병렬 처리 워커 수')
    parser.add_argument('--output', help='출력 디렉토리')
    
    args = parser.parse_args()
    
    try:
        # 설정 초기화
        config = Config()
        
        # 명령줄 인수 처리
        if args.workers:
            config.MAX_WORKERS = args.workers
        if args.output:
            config.output_dir = args.output
            os.makedirs(config.output_dir, exist_ok=True)
        
        # 스크래핑 실행
        driver_manager = DriverManager(config)
        progress_manager = ProgressManager(config)
        scraper = BankScraper(config, driver_manager, progress_manager)
        
        banks = args.banks if args.banks else config.BANKS
        scraper.run(banks)
        
        logger.info("스크래핑 프로세스 완료")
        
    except Exception as e:
        logger.error(f"스크립트 실행 실패: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
