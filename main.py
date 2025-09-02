"""
저축은행 통합 데이터 스크래퍼 (MD 기능 포함)
결산공시와 분기공시를 하나의 GUI에서 관리하며 마크다운 보고서 생성 지원
버전: 3.1 (완전 통합 버전)
작성일: 2025-01-31
"""

import tkinter as tk
from tkinter import ttk, messagebox, filedialog, scrolledtext
import sys
import os
import pandas as pd
from datetime import datetime
import threading
import json
import zipfile
import webbrowser
from pathlib import Path
import subprocess

# 현재 디렉토리를 Python 경로에 추가
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# 각 스크래퍼 모듈 import (에러 처리 포함)
try:
    import settlement_scraper
    SETTLEMENT_AVAILABLE = True
    print("✅ 결산공시 스크래퍼 모듈 로드 성공")
except ImportError as e:
    print(f"❌ 결산공시 스크래퍼 모듈 로드 실패: {e}")
    SETTLEMENT_AVAILABLE = False

try:
    import quarterly_scraper
    QUARTERLY_AVAILABLE = True
    print("✅ 분기공시 스크래퍼 모듈 로드 성공")
except ImportError as e:
    print(f"❌ 분기공시 스크래퍼 모듈 로드 실패: {e}")
    QUARTERLY_AVAILABLE = False


class IntegratedBankScraperGUI:
    """통합 저축은행 스크래퍼 메인 GUI 클래스"""
    
    def __init__(self, root):
        self.root = root
        self.root.title("🏦 저축은행 통합 데이터 스크래퍼 v3.1 (MD 지원)")
        self.root.geometry("1200x800")
        self.root.resizable(True, True)
        
        # 앱 아이콘 설정 (선택사항)
        try:
            # Windows
            if sys.platform == 'win32':
                self.root.iconbitmap(default='bank_icon.ico')
        except:
            pass
        
        # 전역 설정
        self.config_dir = os.path.join(os.path.expanduser("~"), ".bank_scraper")
        self.config_file = os.path.join(self.config_dir, "main_settings.json")
        self.settings = self.load_main_settings()
        
        # 탭 인스턴스 저장
        self.settlement_tab = None
        self.quarterly_tab = None
        
        # 스타일 설정
        self.setup_styles()
        
        # 메인 UI 생성
        self.create_main_ui()
        
        # 초기 설정 로드
        self.load_initial_settings()
        
        # 종료 이벤트 바인딩
        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)
    
    def setup_styles(self):
        """전체 앱 스타일 설정"""
        style = ttk.Style()
        
        # 사용 가능한 테마 확인 및 설정
        available_themes = style.theme_names()
        if 'clam' in available_themes:
            style.theme_use('clam')
        elif 'vista' in available_themes:
            style.theme_use('vista')
        elif 'alt' in available_themes:
            style.theme_use('alt')
        
        # 탭 스타일 커스터마이징
        style.configure('TNotebook', tabposition='n')
        style.configure('TNotebook.Tab', padding=[15, 8])
        
        # 버튼 스타일
        style.configure('Accent.TButton', font=('', 9, 'bold'))
        
        # 상태바 스타일
        style.configure('Status.TLabel', relief='sunken', padding=5)
    
    def create_main_ui(self):
        """메인 UI 구성 요소 생성"""
        # 메인 컨테이너
        main_container = ttk.Frame(self.root)
        main_container.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # 메뉴바 생성
        self.create_menu_bar()
        
        # 탭 컨트롤 생성
        self.create_notebook()
        
        # 상태바 생성
        self.create_status_bar()
        
        # 하단 정보 패널
        self.create_info_panel()
    
    def create_menu_bar(self):
        """상세한 메뉴바 생성"""
        menubar = tk.Menu(self.root)
        self.root.config(menu=menubar)
        
        # 파일 메뉴
        file_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="📁 파일", menu=file_menu)
        
        file_menu.add_command(
            label="🏦 결산공시 폴더 열기", 
            command=self.open_settlement_folder, 
            state=tk.NORMAL if SETTLEMENT_AVAILABLE else tk.DISABLED
        )
        file_menu.add_command(
            label="📊 분기공시 폴더 열기", 
            command=self.open_quarterly_folder,
            state=tk.NORMAL if QUARTERLY_AVAILABLE else tk.DISABLED
        )
        file_menu.add_separator()
        file_menu.add_command(label="📋 설정 내보내기", command=self.export_settings)
        file_menu.add_command(label="📥 설정 가져오기", command=self.import_settings)
        file_menu.add_separator()
        file_menu.add_command(label="❌ 종료", command=self.on_closing)
        
        # MD 보고서 메뉴
        md_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="📝 MD 보고서", menu=md_menu)
        
        md_menu.add_command(
            label="📊 통합 비교 분석 보고서", 
            command=self.create_integrated_comparison_md
        )
        md_menu.add_command(
            label="📈 트렌드 분석 보고서", 
            command=self.create_trend_analysis_md
        )
        md_menu.add_command(
            label="📋 전체 데이터 MD 요약", 
            command=self.create_comprehensive_md_summary
        )
        md_menu.add_separator()
        md_menu.add_command(
            label="🔄 기존 엑셀을 MD로 변환", 
            command=self.convert_excel_to_md
        )
        md_menu.add_command(
            label="📄 MD 뷰어 열기", 
            command=self.open_md_viewer
        )
        
        # 데이터 분석 메뉴
        analysis_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="📊 데이터 분석", menu=analysis_menu)
        
        analysis_menu.add_command(
            label="📈 통합 재무 보고서 생성", 
            command=self.create_integrated_financial_report
        )
        analysis_menu.add_command(
            label="🔍 두 데이터 비교 분석", 
            command=self.compare_datasets
        )
        analysis_menu.add_command(
            label="📊 은행별 성과 분석", 
            command=self.analyze_bank_performance
        )
        analysis_menu.add_separator()
        analysis_menu.add_command(
            label="📉 리스크 분석 보고서", 
            command=self.create_risk_analysis
        )
        analysis_menu.add_command(
            label="💹 시장 동향 분석", 
            command=self.create_market_trend_analysis
        )
        
        # 도구 메뉴
        tools_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="🛠️ 도구", menu=tools_menu)
        
        tools_menu.add_command(
            label="🗜️ 데이터 압축 및 아카이브", 
            command=self.compress_all_data
        )
        tools_menu.add_command(
            label="🧹 임시 파일 정리", 
            command=self.cleanup_temp_files
        )
        tools_menu.add_command(
            label="🔄 모든 탭 새로고침", 
            command=self.refresh_all_tabs
        )
        tools_menu.add_separator()
        tools_menu.add_command(
            label="⚙️ 고급 설정", 
            command=self.show_advanced_settings
        )
        tools_menu.add_command(
            label="📊 시스템 상태", 
            command=self.show_system_status
        )
        
        # 도움말 메뉴
        help_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="❓ 도움말", menu=help_menu)
        
        help_menu.add_command(label="📖 사용 방법", command=self.show_help)
        help_menu.add_command(label="🚀 빠른 시작 가이드", command=self.show_quick_start)
        help_menu.add_command(label="❓ FAQ", command=self.show_faq)
        help_menu.add_separator()
        help_menu.add_command(label="🌐 온라인 문서", command=self.open_online_docs)
        help_menu.add_command(label="🐛 버그 신고", command=self.report_bug)
        help_menu.add_separator()
        help_menu.add_command(label="ℹ️ 정보", command=self.show_about)
    
    def create_notebook(self):
        """탭 컨트롤 및 탭들 생성"""
        # 메인 컨테이너에서 탭 영역 생성
        main_container = self.root.children['!frame']
        
        self.notebook = ttk.Notebook(main_container)
        self.notebook.pack(fill=tk.BOTH, expand=True, pady=(0, 5))
        
        # 결산공시 탭 추가
        if SETTLEMENT_AVAILABLE:
            try:
                self.settlement_tab = settlement_scraper.SettlementScraperTab(self.notebook)
                self.notebook.add(
                    self.settlement_tab.frame, 
                    text="🏦 결산공시 (연말)",
                    padding=5
                )
                self.update_status("결산공시 탭 로드 완료")
            except Exception as e:
                self.update_status(f"결산공시 탭 로드 실패: {str(e)}")
                messagebox.showerror("오류", f"결산공시 탭을 로드할 수 없습니다: {str(e)}")
        else:
            # 대체 탭 생성
            placeholder_frame = ttk.Frame(self.notebook)
            ttk.Label(
                placeholder_frame, 
                text="❌ 결산공시 스크래퍼 모듈을 사용할 수 없습니다.\n\nsettlement_scraper.py 파일을 확인해주세요.",
                font=("", 12), 
                justify=tk.CENTER
            ).pack(expand=True)
            self.notebook.add(placeholder_frame, text="🏦 결산공시 (사용 불가)")
        
        # 분기공시 탭 추가  
        if QUARTERLY_AVAILABLE:
            try:
                self.quarterly_tab = quarterly_scraper.QuarterlyScraperTab(self.notebook)
                self.notebook.add(
                    self.quarterly_tab.frame,
                    text="📊 분기공시 (3개월)", 
                    padding=5
                )
                self.update_status("분기공시 탭 로드 완료")
            except Exception as e:
                self.update_status(f"분기공시 탭 로드 실패: {str(e)}")
                messagebox.showerror("오류", f"분기공시 탭을 로드할 수 없습니다: {str(e)}")
        else:
            # 대체 탭 생성
            placeholder_frame = ttk.Frame(self.notebook)
            ttk.Label(
                placeholder_frame, 
                text="❌ 분기공시 스크래퍼 모듈을 사용할 수 없습니다.\n\nquarterly_scraper.py 파일을 확인해주세요.",
                font=("", 12), 
                justify=tk.CENTER
            ).pack(expand=True)
            self.notebook.add(placeholder_frame, text="📊 분기공시 (사용 불가)")
        
        # 통합 분석 탭 추가
        self.create_analysis_tab()
        
        # 탭 변경 이벤트 바인딩
        self.notebook.bind("<<NotebookTabChanged>>", self.on_tab_changed)
    
    def create_analysis_tab(self):
        """통합 분석 탭 생성"""
        analysis_frame = ttk.Frame(self.notebook)
        self.notebook.add(analysis_frame, text="📈 통합 분석", padding=5)
        
        # 메인 컨테이너
        main_frame = ttk.Frame(analysis_frame, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # 제목
        title_label = ttk.Label(
            main_frame, 
            text="📈 저축은행 통합 데이터 분석", 
            font=("", 16, "bold")
        )
        title_label.pack(pady=(0, 20))
        
        # 기능 섹션들을 위한 프레임
        sections_frame = ttk.Frame(main_frame)
        sections_frame.pack(fill=tk.BOTH, expand=True)
        
        # 왼쪽 섹션
        left_frame = ttk.Frame(sections_frame)
        left_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(0, 10))
        
        # 오른쪽 섹션
        right_frame = ttk.Frame(sections_frame)
        right_frame.pack(side=tk.RIGHT, fill=tk.BOTH, expand=True, padx=(10, 0))
        
        # 보고서 생성 섹션
        report_section = ttk.LabelFrame(left_frame, text="📝 보고서 생성", padding="10")
        report_section.pack(fill=tk.X, pady=(0, 10))
        
        ttk.Button(
            report_section, 
            text="📊 통합 재무 보고서", 
            command=self.create_integrated_financial_report,
            style='Accent.TButton'
        ).pack(fill=tk.X, pady=2)
        
        ttk.Button(
            report_section, 
            text="📋 MD 통합 요약", 
            command=self.create_comprehensive_md_summary
        ).pack(fill=tk.X, pady=2)
        
        ttk.Button(
            report_section, 
            text="🔍 비교 분석 보고서", 
            command=self.create_integrated_comparison_md
        ).pack(fill=tk.X, pady=2)
        
        # 데이터 변환 섹션
        convert_section = ttk.LabelFrame(left_frame, text="🔄 데이터 변환", padding="10")
        convert_section.pack(fill=tk.X, pady=(0, 10))
        
        ttk.Button(
            convert_section, 
            text="📄 Excel → MD 변환", 
            command=self.convert_excel_to_md
        ).pack(fill=tk.X, pady=2)
        
        ttk.Button(
            convert_section, 
            text="🗜️ 데이터 압축", 
            command=self.compress_all_data
        ).pack(fill=tk.X, pady=2)
        
        # 시스템 정보 섹션
        system_section = ttk.LabelFrame(right_frame, text="🖥️ 시스템 정보", padding="10")
        system_section.pack(fill=tk.X, pady=(0, 10))
        
        # 시스템 정보 텍스트
        self.system_info_text = tk.Text(
            system_section, 
            height=8, 
            wrap=tk.WORD, 
            font=("Consolas", 9)
        )
        self.system_info_text.pack(fill=tk.BOTH, expand=True)
        
        # 퀵 액션 섹션
        quick_section = ttk.LabelFrame(right_frame, text="⚡ 빠른 작업", padding="10")
        quick_section.pack(fill=tk.X, pady=(0, 10))
        
        ttk.Button(
            quick_section, 
            text="🔄 모든 탭 새로고침", 
            command=self.refresh_all_tabs
        ).pack(fill=tk.X, pady=2)
        
        ttk.Button(
            quick_section, 
            text="🧹 임시 파일 정리", 
            command=self.cleanup_temp_files
        ).pack(fill=tk.X, pady=2)
        
        ttk.Button(
            quick_section, 
            text="📊 시스템 상태 새로고침", 
            command=self.update_system_info
        ).pack(fill=tk.X, pady=2)
        
        # 초기 시스템 정보 표시
        self.update_system_info()
    
    def update_system_info(self):
        """시스템 정보 업데이트"""
        try:
            info = []
            info.append("🏦 저축은행 통합 스크래퍼 v3.1")
            info.append(f"📅 현재 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            info.append("")
            
            # 모듈 상태
            info.append("📦 모듈 상태:")
            info.append(f"  결산공시: {'✅ 사용 가능' if SETTLEMENT_AVAILABLE else '❌ 사용 불가'}")
            info.append(f"  분기공시: {'✅ 사용 가능' if QUARTERLY_AVAILABLE else '❌ 사용 불가'}")
            info.append("")
            
            # 설정 정보
            if hasattr(self, 'settlement_tab') and self.settlement_tab:
                settlement_dir = getattr(self.settlement_tab.config, 'output_dir', '미설정')
                info.append(f"🏦 결산공시 폴더: {os.path.basename(settlement_dir)}")
            
            if hasattr(self, 'quarterly_tab') and self.quarterly_tab:
                quarterly_dir = getattr(self.quarterly_tab.config, 'output_dir', '미설정')
                info.append(f"📊 분기공시 폴더: {os.path.basename(quarterly_dir)}")
            
            info.append("")
            info.append("💡 팁: 메뉴에서 다양한 분석 도구를 사용해보세요!")
            
            # 텍스트 위젯에 표시
            self.system_info_text.delete(1.0, tk.END)
            self.system_info_text.insert(1.0, "\n".join(info))
            self.system_info_text.config(state=tk.DISABLED)
            
        except Exception as e:
            self.update_status(f"시스템 정보 업데이트 실패: {str(e)}")
    
    def create_status_bar(self):
        """상태바 생성"""
        main_container = self.root.children['!frame']
        
        status_frame = ttk.Frame(main_container)
        status_frame.pack(side=tk.BOTTOM, fill=tk.X, pady=(5, 0))
        
        # 현재 탭 표시
        self.current_tab_label = ttk.Label(
            status_frame, 
            text="현재 탭: 🏦 결산공시",
            style='Status.TLabel'
        )
        self.current_tab_label.pack(side=tk.LEFT, padx=(0, 5))
        
        # 구분선
        ttk.Separator(status_frame, orient=tk.VERTICAL).pack(
            side=tk.LEFT, fill=tk.Y, padx=5
        )
        
        # 전체 상태
        self.status_label = ttk.Label(
            status_frame,
            text="준비 완료 - MD 기능 지원",
            style='Status.TLabel'
        )
        self.status_label.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 5))
        
        # 시간 표시
        self.time_label = ttk.Label(
            status_frame,
            text="",
            style='Status.TLabel'
        )
        self.time_label.pack(side=tk.RIGHT)
        
        # 시간 업데이트 시작
        self.update_time()
    
    def update_time(self):
        """시간 업데이트"""
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.time_label.config(text=current_time)
        self.root.after(1000, self.update_time)  # 1초마다 업데이트
    
    def create_info_panel(self):
        """하단 정보 패널 생성 (접을 수 있는 형태)"""
        main_container = self.root.children['!frame']
        
        # 정보 패널 토글을 위한 프레임
        self.info_panel_frame = ttk.Frame(main_container)
        self.info_panel_frame.pack(side=tk.BOTTOM, fill=tk.X, pady=(5, 0))
        
        # 토글 버튼
        self.info_panel_visible = tk.BooleanVar(value=False)
        toggle_button = ttk.Button(
            self.info_panel_frame,
            text="ℹ️ 정보 패널 표시",
            command=self.toggle_info_panel
        )
        toggle_button.pack(pady=2)
        
        # 정보 내용 프레임 (처음에는 숨김)
        self.info_content_frame = ttk.Frame(self.info_panel_frame)
        
        # 정보 텍스트
        info_text = ttk.Label(
            self.info_content_frame,
            text=("💡 팁: 각 탭에서 스크래핑을 진행하고, MD 보고서 메뉴에서 다양한 분석 보고서를 생성할 수 있습니다.\n"
                  "🔧 고급 기능: 통합 분석 탭에서 데이터 변환 및 시스템 관리 도구를 사용하세요."),
            font=("", 9),
            foreground="gray"
        )
        info_text.pack(pady=5)
    
    def toggle_info_panel(self):
        """정보 패널 표시/숨김 토글"""
        if self.info_panel_visible.get():
            self.info_content_frame.pack_forget()
            self.info_panel_visible.set(False)
            button = self.info_panel_frame.children['!button']
            button.config(text="ℹ️ 정보 패널 표시")
        else:
            self.info_content_frame.pack(side=tk.BOTTOM, fill=tk.X)
            self.info_panel_visible.set(True)
            button = self.info_panel_frame.children['!button']
            button.config(text="ℹ️ 정보 패널 숨김")
    
    def on_tab_changed(self, event):
        """탭 변경 이벤트 처리"""
        try:
            current_tab = self.notebook.tab(self.notebook.select(), "text")
            self.current_tab_label.config(text=f"현재 탭: {current_tab}")
            self.update_status(f"{current_tab} 탭으로 변경됨")
        except Exception as e:
            print(f"탭 변경 이벤트 처리 중 오류: {e}")
    
    def update_status(self, message):
        """상태바 메시지 업데이트"""
        if hasattr(self, 'status_label'):
            self.status_label.config(text=message)
        print(f"📝 {message}")
    
    # =====================================
    # 파일 메뉴 관련 메서드들
    # =====================================
    
    def open_settlement_folder(self):
        """결산공시 출력 폴더 열기"""
        if not self.settlement_tab:
            messagebox.showwarning("경고", "결산공시 탭이 사용 불가능합니다.")
            return
        
        try:
            output_dir = self.settlement_tab.config.output_dir
            self._open_folder(output_dir, "결산공시")
        except Exception as e:
            messagebox.showerror("오류", f"결산공시 폴더를 열 수 없습니다: {str(e)}")
    
    def open_quarterly_folder(self):
        """분기공시 출력 폴더 열기"""
        if not self.quarterly_tab:
            messagebox.showwarning("경고", "분기공시 탭이 사용 불가능합니다.")
            return
        
        try:
            output_dir = self.quarterly_tab.config.output_dir
            self._open_folder(output_dir, "분기공시")
        except Exception as e:
            messagebox.showerror("오류", f"분기공시 폴더를 열 수 없습니다: {str(e)}")
    
    def _open_folder(self, folder_path, folder_type):
        """폴더 열기 공통 메서드"""
        if os.path.exists(folder_path):
            try:
                if sys.platform == 'win32':
                    os.startfile(folder_path)
                elif sys.platform == 'darwin':
                    subprocess.Popen(['open', folder_path])
                else:
                    subprocess.Popen(['xdg-open', folder_path])
                self.update_status(f"{folder_type} 폴더 열기 완료")
            except Exception as e:
                raise Exception(f"폴더를 열 수 없습니다: {str(e)}")
        else:
            messagebox.showwarning("경고", f"{folder_type} 폴더가 존재하지 않습니다: {folder_path}")
    
    def export_settings(self):
        """설정 내보내기"""
        try:
            # 저장할 파일 선택
            file_path = filedialog.asksaveasfilename(
                title="설정 파일 저장",
                defaultextension=".json",
                filetypes=[("JSON 파일", "*.json"), ("모든 파일", "*.*")]
            )
            
            if not file_path:
                return
            
            # 모든 설정 수집
            settings = {
                'main_settings': self.settings,
                'export_time': datetime.now().isoformat(),
                'version': '3.1'
            }
            
            if self.settlement_tab:
                settings['settlement_settings'] = {
                    'output_dir': getattr(self.settlement_tab.config, 'output_dir', ''),
                    'chrome_driver_path': getattr(self.settlement_tab.config, 'chrome_driver_path', ''),
                    'max_workers': getattr(self.settlement_tab.config, 'MAX_WORKERS', 3),
                    'auto_zip': getattr(self.settlement_tab.config, 'auto_zip', True)
                }
            
            if self.quarterly_tab:
                settings['quarterly_settings'] = {
                    'output_dir': getattr(self.quarterly_tab.config, 'output_dir', ''),
                    'chrome_driver_path': getattr(self.quarterly_tab.config, 'chrome_driver_path', ''),
                    'max_workers': getattr(self.quarterly_tab.config, 'MAX_WORKERS', 3),
                    'auto_zip': getattr(self.quarterly_tab.config, 'auto_zip', True)
                }
            
            # 파일 저장
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(settings, f, ensure_ascii=False, indent=2)
            
            messagebox.showinfo("완료", f"설정이 저장되었습니다: {file_path}")
            self.update_status("설정 내보내기 완료")
            
        except Exception as e:
            messagebox.showerror("오류", f"설정 내보내기 실패: {str(e)}")
    
    def import_settings(self):
        """설정 가져오기"""
        try:
            # 파일 선택
            file_path = filedialog.askopenfilename(
                title="설정 파일 선택",
                filetypes=[("JSON 파일", "*.json"), ("모든 파일", "*.*")]
            )
            
            if not file_path:
                return
            
            # 설정 파일 로드
            with open(file_path, 'r', encoding='utf-8') as f:
                settings = json.load(f)
            
            # 설정 적용 확인
            if messagebox.askyesno(
                "설정 가져오기", 
                f"설정 파일을 가져오시겠습니까?\n파일: {os.path.basename(file_path)}\n\n"
                "기존 설정이 덮어쓰여집니다."
            ):
                # 메인 설정 적용
                if 'main_settings' in settings:
                    self.settings.update(settings['main_settings'])
                    self.save_main_settings()
                
                messagebox.showinfo(
                    "완료", 
                    "설정을 가져왔습니다. 프로그램을 재시작하면 모든 설정이 적용됩니다."
                )
                self.update_status("설정 가져오기 완료")
            
        except Exception as e:
            messagebox.showerror("오류", f"설정 가져오기 실패: {str(e)}")
    
    # =====================================
    # MD 보고서 메뉴 관련 메서드들
    # =====================================
    
    def create_integrated_comparison_md(self):
        """통합 비교 분석 MD 보고서 생성"""
        try:
            self.update_status("통합 비교 분석 보고서 생성 중...")
            
            # 별도 스레드에서 실행
            threading.Thread(
                target=self._generate_integrated_comparison_md, 
                daemon=True
            ).start()
            
        except Exception as e:
            messagebox.showerror("오류", f"통합 비교 분석 보고서 생성 중 오류: {str(e)}")
            self.update_status("통합 비교 분석 실패")
    
    def _generate_integrated_comparison_md(self):
        """통합 비교 분석 MD 보고서 생성 (백그라운드)"""
        try:
            # 결산공시와 분기공시 데이터 수집
            settlement_data = self._collect_financial_data("settlement")
            quarterly_data = self._collect_financial_data("quarterly")
            
            if not settlement_data and not quarterly_data:
                self.root.after(
                    0, 
                    lambda: messagebox.showwarning(
                        "경고", 
                        "비교할 데이터가 없습니다. 먼저 스크래핑을 실행해주세요."
                    )
                )
                return
            
            # 출력 파일 경로
            today = datetime.now().strftime("%Y%m%d")
            output_file = os.path.join(
                os.path.expanduser("~"), 
                "Downloads", 
                f"저축은행_통합비교분석_{today}.md"
            )
            
            # MD 파일 생성
            self._write_comparison_md_file(output_file, settlement_data, quarterly_data)
            
            # 완료 알림
            self.root.after(
                0, 
                lambda: self._show_md_completion_dialog(output_file, "통합 비교 분석 보고서")
            )
            
        except Exception as e:
            self.root.after(
                0, 
                lambda: messagebox.showerror(
                    "오류", 
                    f"통합 비교 분석 보고서 생성 실패: {str(e)}"
                )
            )
        finally:
            self.root.after(0, lambda: self.update_status("준비 완료"))
    
    def _collect_financial_data(self, data_type):
        """지정된 타입의 재무 데이터 수집"""
        try:
            if data_type == "settlement" and self.settlement_tab:
                output_dir = self.settlement_tab.config.output_dir
            elif data_type == "quarterly" and self.quarterly_tab:
                output_dir = self.quarterly_tab.config.output_dir
            else:
                return []
            
            if not os.path.exists(output_dir):
                return []
            
            financial_data = []
            excel_files = [
                f for f in os.listdir(output_dir) 
                if f.endswith('.xlsx') and not f.startswith('~')
            ]
            
            for excel_file in excel_files:
                file_path = os.path.join(output_dir, excel_file)
                try:
                    # 간단한 재무 데이터 추출
                    data = self._extract_basic_financial_data(file_path, data_type)
                    if data:
                        financial_data.append(data)
                except Exception as e:
                    print(f"파일 처리 실패 {excel_file}: {str(e)}")
                    continue
            
            return financial_data
            
        except Exception as e:
            print(f"데이터 수집 실패: {str(e)}")
            return []
    
    def _extract_basic_financial_data(self, excel_path, data_type):
        """엑셀 파일에서 기본 재무 데이터 추출"""
        try:
            # 파일명에서 은행명 추출
            filename = os.path.basename(excel_path)
            bank_name = filename.split('_')[0] if '_' in filename else filename.replace('.xlsx', '')
            
            # 기본 데이터 구조
            data = {
                '은행명': bank_name,
                '데이터타입': data_type,
                '총자산': None,
                '자기자본': None,
                '위험가중자산에 대한 자기자본비율(%)': None,
                '고정이하여신비율(%)': None,
                '파일경로': excel_path
            }
            
            # 간단한 데이터 추출 로직
            # (실제 구현에서는 더 정교한 파싱 필요)
            xls = pd.ExcelFile(excel_path)
            
            # 공시정보 시트에서 날짜 정보 추출
            if '공시정보' in xls.sheet_names:
                info_df = pd.read_excel(excel_path, sheet_name='공시정보')
                if '공시 날짜' in info_df.columns and not info_df['공시 날짜'].empty:
                    data['공시날짜'] = str(info_df['공시 날짜'].iloc[0])
            
            return data
            
        except Exception as e:
            return None
    
    def _write_comparison_md_file(self, output_file, settlement_data, quarterly_data):
        """비교 분석 MD 파일 작성"""
        with open(output_file, 'w', encoding='utf-8') as f:
            # 헤더
            f.write("# 🏦 저축은행 통합 비교 분석 보고서\n\n")
            f.write(f"**생성일시**: {datetime.now().strftime('%Y년 %m월 %d일 %H:%M:%S')}\n\n")
            
            # 목차
            f.write("## 📋 목차\n\n")
            f.write("1. [개요](#개요)\n")
            f.write("2. [데이터 현황](#데이터-현황)\n")
            f.write("3. [결산공시 vs 분기공시 비교](#결산공시-vs-분기공시-비교)\n")
            f.write("4. [주요 지표 분석](#주요-지표-분석)\n")
            f.write("5. [권장사항](#권장사항)\n\n")
            
            # 개요
            f.write("## 📊 개요\n\n")
            f.write("이 보고서는 저축은행 중앙회의 결산공시와 분기공시 데이터를 통합하여 분석한 결과입니다.\n\n")
            
            # 데이터 현황
            f.write("## 📈 데이터 현황\n\n")
            f.write("### 결산공시 데이터\n\n")
            if settlement_data:
                f.write(f"- **수집된 은행 수**: {len(settlement_data)}개\n")
                f.write(
                    f"- **데이터 보유 은행**: "
                    f"{len([d for d in settlement_data if d.get('총자산')])}개\n"
                )
            else:
                f.write("- **상태**: 데이터 없음\n")
            
            f.write("\n### 분기공시 데이터\n\n")
            if quarterly_data:
                f.write(f"- **수집된 은행 수**: {len(quarterly_data)}개\n")
                f.write(
                    f"- **데이터 보유 은행**: "
                    f"{len([d for d in quarterly_data if d.get('총자산')])}개\n"
                )
            else:
                f.write("- **상태**: 데이터 없음\n")
            
            # 비교 분석
            if settlement_data and quarterly_data:
                f.write("\n## 🔍 결산공시 vs 분기공시 비교\n\n")
                self._write_comparison_analysis(f, settlement_data, quarterly_data)
            
            # 권장사항
            f.write("\n## 💡 권장사항\n\n")
            f.write("- 정기적인 데이터 모니터링을 통해 은행별 재무 건전성을 추적하세요.\n")
            f.write("- 자기자본비율이 낮은 은행들에 대해 추가 분석을 고려하세요.\n")
            f.write("- 분기공시와 결산공시의 차이가 큰 은행들을 주의 깊게 살펴보세요.\n\n")
            
            # 푸터
            f.write("---\n")
            f.write(f"*이 보고서는 저축은행 통합 데이터 스크래퍼 v3.1에 의해 자동 생성되었습니다.*\n")
    
    def _write_comparison_analysis(self, f, settlement_data, quarterly_data):
        """비교 분석 내용 작성"""
        try:
            # 공통 은행 찾기
            settlement_banks = {d['은행명'] for d in settlement_data}
            quarterly_banks = {d['은행명'] for d in quarterly_data}
            common_banks = settlement_banks & quarterly_banks
            
            f.write(f"### 📊 데이터 비교 개요\n\n")
            f.write(f"- **결산공시 은행 수**: {len(settlement_banks)}개\n")
            f.write(f"- **분기공시 은행 수**: {len(quarterly_banks)}개\n")
            f.write(f"- **공통 은행 수**: {len(common_banks)}개\n")
            f.write(f"- **결산공시만 있는 은행**: {len(settlement_banks - quarterly_banks)}개\n")
            f.write(f"- **분기공시만 있는 은행**: {len(quarterly_banks - settlement_banks)}개\n\n")
            
            if common_banks:
                f.write("### 🔍 공통 은행 목록\n\n")
                f.write("| 은행명 | 결산공시 | 분기공시 | 상태 |\n")
                f.write("| --- | --- | --- | --- |\n")
                
                for bank in sorted(common_banks):
                    f.write(f"| {bank} | ✅ | ✅ | 데이터 완비 |\n")
                
                f.write("\n")
            
        except Exception as e:
            f.write(f"*비교 분석 중 오류 발생: {str(e)}*\n\n")
    
    def create_trend_analysis_md(self):
        """트렌드 분석 MD 보고서 생성"""
        messagebox.showinfo(
            "정보", 
            "트렌드 분석 기능은 시계열 데이터가 축적된 후 구현 예정입니다.\n\n"
            "현재는 통합 비교 분석 보고서를 사용해주세요."
        )
    
    def convert_excel_to_md(self):
        """기존 엑셀 파일을 MD로 변환"""
        try:
            # 파일 선택 대화상자
            excel_files = filedialog.askopenfilenames(
                title="MD로 변환할 엑셀 파일 선택",
                filetypes=[("Excel 파일", "*.xlsx"), ("모든 파일", "*.*")]
            )
            
            if not excel_files:
                return
            
            self.update_status(f"엑셀 파일 {len(excel_files)}개 MD 변환 중...")
            
            # 별도 스레드에서 변환 실행
            threading.Thread(
                target=self._convert_excel_files_to_md, 
                args=(excel_files,),
                daemon=True
            ).start()
            
        except Exception as e:
            messagebox.showerror("오류", f"엑셀 to MD 변환 중 오류: {str(e)}")
    
    def _convert_excel_files_to_md(self, excel_files):
        """엑셀 파일들을 MD로 변환 (백그라운드)"""
        try:
            converted_files = []
            
            for i, excel_file in enumerate(excel_files):
                try:
                    # 진행 상황 업데이트
                    progress = int((i / len(excel_files)) * 100)
                    self.root.after(
                        0, 
                        lambda p=progress: self.update_status(f"MD 변환 중... {p}%")
                    )
                    
                    # 파일명에서 은행명과 타입 추출
                    filename = os.path.basename(excel_file)
                    bank_name = filename.split('_')[0] if '_' in filename else filename.replace('.xlsx', '')
                    is_settlement = "결산" in filename
                    
                    # MD 파일 경로 생성
                    md_file = excel_file.replace('.xlsx', '.md')
                    
                    # 엑셀에서 데이터 읽기 및 MD로 변환
                    self._convert_single_excel_to_md(excel_file, md_file, bank_name, is_settlement)
                    
                    converted_files.append(md_file)
                    
                except Exception as e:
                    print(f"파일 변환 실패 {excel_file}: {str(e)}")
                    continue
            
            # 완료 알림
            if converted_files:
                self.root.after(
                    0, 
                    lambda: messagebox.showinfo(
                        "완료", 
                        f"{len(converted_files)}개 파일이 MD로 변환되었습니다.\n\n" + 
                        "\n".join([os.path.basename(f) for f in converted_files[:5]]) +
                        (f"\n... 외 {len(converted_files)-5}개" if len(converted_files) > 5 else "")
                    )
                )
            else:
                self.root.after(0, lambda: messagebox.showwarning("경고", "변환된 파일이 없습니다."))
                
        except Exception as e:
            self.root.after(0, lambda: messagebox.showerror("오류", f"변환 중 오류: {str(e)}"))
        finally:
            self.root.after(0, lambda: self.update_status("MD 변환 완료"))
    
    def _convert_single_excel_to_md(self, excel_file, md_file, bank_name, is_settlement):
        """단일 엑셀 파일을 MD로 변환"""
        try:
            xls = pd.ExcelFile(excel_file)
            
            with open(md_file, 'w', encoding='utf-8') as f:
                # 헤더
                data_type = "결산공시" if is_settlement else "분기공시"
                f.write(f"# {bank_name} 저축은행 {data_type} 데이터\n\n")
                f.write(f"**변환일시**: {datetime.now().strftime('%Y년 %m월 %d일 %H:%M:%S')}\n")
                f.write(f"**원본 파일**: {os.path.basename(excel_file)}\n\n")
                
                # 각 시트를 MD로 변환
                for sheet_name in xls.sheet_names:
                    f.write(f"## {sheet_name}\n\n")
                    
                    try:
                        df = pd.read_excel(excel_file, sheet_name=sheet_name)
                        
                        if not df.empty:
                            # 마크다운 테이블로 변환
                            df_clean = df.copy()
                            df_clean.columns = [
                                str(col).replace('\n', ' ').replace('|', '\\|') 
                                for col in df_clean.columns
                            ]
                            
                            # 테이블 헤더
                            headers = '| ' + ' | '.join(df_clean.columns) + ' |\n'
                            separator = '|' + '|'.join([' --- ' for _ in df_clean.columns]) + '|\n'
                            f.write(headers)
                            f.write(separator)
                            
                            # 데이터 행 (최대 100행까지만)
                            max_rows = min(100, len(df_clean))
                            for idx in range(max_rows):
                                row = df_clean.iloc[idx]
                                row_data = []
                                for value in row:
                                    str_value = str(value).replace('|', '\\|').replace('\n', ' ')
                                    if str_value == 'nan' or str_value == 'None':
                                        str_value = ''
                                    row_data.append(str_value)
                                f.write('| ' + ' | '.join(row_data) + ' |\n')
                            
                            if len(df_clean) > 100:
                                f.write(f"\n*({len(df_clean) - 100}개 행 더 있음...)*\n")
                            
                            f.write('\n')
                        else:
                            f.write("*데이터가 없습니다.*\n\n")
                            
                    except Exception as e:
                        f.write(f"*시트 변환 중 오류: {str(e)}*\n\n")
                
                # 푸터
                f.write("---\n")
                f.write(f"*이 파일은 저축은행 통합 데이터 스크래퍼 v3.1에 의해 변환되었습니다.*\n")
            
        except Exception as e:
            raise Exception(f"MD 변환 실패: {str(e)}")
    
    def create_comprehensive_md_summary(self):
        """전체 데이터 MD 요약 보고서 생성"""
        try:
            self.update_status("전체 데이터 MD 요약 생성 중...")
            
            # 별도 스레드에서 실행
            threading.Thread(target=self._generate_comprehensive_summary, daemon=True).start()
            
        except Exception as e:
            messagebox.showerror("오류", f"전체 요약 생성 중 오류: {str(e)}")
    
    def _generate_comprehensive_summary(self):
        """종합 요약 보고서 생성 (백그라운드)"""
        try:
            today = datetime.now().strftime("%Y%m%d")
            output_file = os.path.join(
                os.path.expanduser("~"), 
                "Downloads", 
                f"저축은행_종합요약_{today}.md"
            )
            
            # 데이터 수집
            settlement_data = self._collect_financial_data("settlement")
            quarterly_data = self._collect_financial_data("quarterly")
            
            with open(output_file, 'w', encoding='utf-8') as f:
                # 헤더
                f.write("# 🏦 저축은행 종합 데이터 요약 보고서\n\n")
                f.write(f"## 기본 정보\n\n")
                f.write(f"- **생성일시**: {datetime.now().strftime('%Y년 %m월 %d일 %H:%M:%S')}\n")
                f.write(f"- **스크래퍼 버전**: v3.1\n")
                f.write(f"- **데이터 출처**: 저축은행중앙회\n\n")
                
                # 전체 현황
                f.write("## 📊 전체 현황\n\n")
                f.write(f"- **결산공시 데이터**: {len(settlement_data) if settlement_data else 0}개 은행\n")
                f.write(f"- **분기공시 데이터**: {len(quarterly_data) if quarterly_data else 0}개 은행\n")
                
                total_banks = len(set([d['은행명'] for d in settlement_data + quarterly_data]))
                f.write(f"- **전체 은행 수**: {total_banks}개\n\n")
                
                # 데이터 품질 분석
                f.write("## 🔍 데이터 품질 분석\n\n")
                
                if settlement_data:
                    complete_settlement = len([
                        d for d in settlement_data 
                        if d.get('총자산') and d.get('자기자본')
                    ])
                    f.write(
                        f"- **결산공시 완전 데이터**: {complete_settlement}/{len(settlement_data)} "
                        f"({complete_settlement/len(settlement_data)*100:.1f}%)\n"
                    )
                
                if quarterly_data:
                    complete_quarterly = len([
                        d for d in quarterly_data 
                        if d.get('총자산') and d.get('자기자본')
                    ])
                    f.write(
                        f"- **분기공시 완전 데이터**: {complete_quarterly}/{len(quarterly_data)} "
                        f"({complete_quarterly/len(quarterly_data)*100:.1f}%)\n"
                    )
                
                f.write("\n")
                
                # 권장사항
                f.write("## 💡 권장사항\n\n")
                f.write("1. **정기 모니터링**: 월별 또는 분기별로 데이터를 수집하여 트렌드를 파악하세요.\n")
                f.write("2. **데이터 검증**: 수집된 데이터의 정확성을 주기적으로 검증하세요.\n")
                f.write("3. **백업 관리**: 중요한 재무 데이터는 정기적으로 백업하세요.\n")
                f.write("4. **보고서 활용**: MD 파일을 GitHub, Notion 등에서 활용하세요.\n\n")
                
                # 푸터
                f.write("---\n")
                f.write(f"*이 요약 보고서는 저축은행 통합 데이터 스크래퍼 v3.1에 의해 자동 생성되었습니다.*\n")
            
            # 완료 알림
            self.root.after(
                0, 
                lambda: self._show_md_completion_dialog(output_file, "종합 요약 보고서")
            )
            
        except Exception as e:
            self.root.after(
                0, 
                lambda: messagebox.showerror("오류", f"종합 요약 생성 실패: {str(e)}")
            )
        finally:
            self.root.after(0, lambda: self.update_status("준비 완료"))
    
    def open_md_viewer(self):
        """MD 뷰어 열기"""
        try:
            # MD 파일 선택
            md_files = filedialog.askopenfilenames(
                title="보기할 MD 파일 선택",
                filetypes=[("Markdown 파일", "*.md"), ("모든 파일", "*.*")]
            )
            
            if not md_files:
                return
            
            # 선택된 파일들 열기
            for md_file in md_files:
                self._open_file(md_file)
            
            self.update_status(f"{len(md_files)}개 MD 파일 열기 완료")
            
        except Exception as e:
            messagebox.showerror("오류", f"MD 뷰어 열기 실패: {str(e)}")
    
    def _show_md_completion_dialog(self, file_path, report_type):
        """MD 보고서 완료 대화상자"""
        result = messagebox.askyesno(
            "완료", 
            f"{report_type}가 생성되었습니다!\n\n"
            f"파일: {os.path.basename(file_path)}\n"
            f"위치: {os.path.dirname(file_path)}\n\n"
            f"파일을 열어보시겠습니까?"
        )
        
        if result:
            self._open_file(file_path)
    
    def _open_file(self, file_path):
        """파일 열기 공통 메서드"""
        try:
            if sys.platform == 'win32':
                os.startfile(file_path)
            elif sys.platform == 'darwin':
                subprocess.Popen(['open', file_path])
            else:
                subprocess.Popen(['xdg-open', file_path])
        except Exception as e:
            messagebox.showerror("오류", f"파일을 열 수 없습니다: {str(e)}")
    
    # =====================================
    # 데이터 분석 메뉴 관련 메서드들
    # =====================================
    
    def create_integrated_financial_report(self):
        """통합 재무 보고서 생성"""
        try:
            # 간단한 선택 대화상자
            result = messagebox.askyesno(
                "통합 보고서", 
                "결산공시와 분기공시 데이터를 통합하여 재무 보고서를 생성하시겠습니까?\n\n"
                "• 결산공시 데이터와 분기공시 데이터를 하나의 파일로 통합\n"
                "• 은행별 비교 분석 포함\n"
                "• MD 요약 보고서도 함께 생성"
            )
            
            if result:
                self.update_status("통합 재무 보고서 생성 중...")
                threading.Thread(
                    target=self._create_integrated_financial_report, 
                    daemon=True
                ).start()
                
        except Exception as e:
            messagebox.showerror("오류", f"통합 보고서 생성 중 오류: {str(e)}")
    
    def _create_integrated_financial_report(self):
        """통합 재무 보고서 생성 (백그라운드)"""
        try:
            # 데이터 수집
            settlement_data = self._collect_financial_data("settlement")
            quarterly_data = self._collect_financial_data("quarterly")
            
            if not settlement_data and not quarterly_data:
                self.root.after(
                    0, 
                    lambda: messagebox.showwarning("경고", "통합할 데이터가 없습니다.")
                )
                return
            
            # 출력 파일 설정
            today = datetime.now().strftime("%Y%m%d")
            output_file = os.path.join(
                os.path.expanduser("~"), 
                "Downloads", 
                f"저축은행_통합재무보고서_{today}.xlsx"
            )
            
            # 엑셀 파일 생성
            with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
                # 결산공시 데이터
                if settlement_data:
                    settlement_df = pd.DataFrame(settlement_data)
                    settlement_df.to_excel(writer, sheet_name='결산공시', index=False)
                
                # 분기공시 데이터  
                if quarterly_data:
                    quarterly_df = pd.DataFrame(quarterly_data)
                    quarterly_df.to_excel(writer, sheet_name='분기공시', index=False)
                
                # 통합 비교 시트
                if settlement_data and quarterly_data:
                    comparison_data = self._create_comparison_data(settlement_data, quarterly_data)
                    if comparison_data:
                        comparison_df = pd.DataFrame(comparison_data)
                        comparison_df.to_excel(writer, sheet_name='결산vs분기비교', index=False)
            
            # MD 요약도 함께 생성
            md_file = output_file.replace('.xlsx', '_요약.md')
            self._create_quick_md_summary(settlement_data, quarterly_data, md_file)
            
            # 완료 알림
            self.root.after(
                0, 
                lambda: messagebox.showinfo(
                    "완료", 
                    f"통합 재무 보고서가 생성되었습니다!\n\n"
                    f"엑셀 파일: {os.path.basename(output_file)}\n"
                    f"MD 요약: {os.path.basename(md_file)}\n"
                    f"위치: {os.path.dirname(output_file)}"
                )
            )
            
        except Exception as e:
            self.root.after(
                0, 
                lambda: messagebox.showerror("오류", f"통합 보고서 생성 실패: {str(e)}")
            )
        finally:
            self.root.after(0, lambda: self.update_status("준비 완료"))
    
    def _create_comparison_data(self, settlement_data, quarterly_data):
        """비교 데이터 생성"""
        try:
            comparison_data = []
            
            # 모든 은행명 수집
            all_banks = set([d['은행명'] for d in settlement_data + quarterly_data])
            
            for bank in sorted(all_banks):
                settlement_bank = next((d for d in settlement_data if d['은행명'] == bank), None)
                quarterly_bank = next((d for d in quarterly_data if d['은행명'] == bank), None)
                
                row = {'은행명': bank}
                
                if settlement_bank:
                    row['결산_총자산'] = settlement_bank.get('총자산')
                    row['결산_자기자본'] = settlement_bank.get('자기자본')
                    row['결산_자기자본비율'] = settlement_bank.get('위험가중자산에 대한 자기자본비율(%)')
                
                if quarterly_bank:
                    row['분기_총자산'] = quarterly_bank.get('총자산')
                    row['분기_자기자본'] = quarterly_bank.get('자기자본')
                    row['분기_자기자본비율'] = quarterly_bank.get('위험가중자산에 대한 자기자본비율(%)')
                
                # 차이 계산
                if settlement_bank and quarterly_bank:
                    if settlement_bank.get('총자산') and quarterly_bank.get('총자산'):
                        row['총자산_차이율'] = (
                            (settlement_bank['총자산'] - quarterly_bank['총자산']) / 
                            quarterly_bank['총자산'] * 100
                        )
                
                row['데이터상태'] = (
                    '결산+분기' if settlement_bank and quarterly_bank else 
                    '결산만' if settlement_bank else '분기만'
                )
                
                comparison_data.append(row)
            
            return comparison_data
            
        except Exception as e:
            return []
    
    def _create_quick_md_summary(self, settlement_data, quarterly_data, output_file):
        """빠른 MD 요약 생성"""
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write("# 📊 저축은행 통합 재무 보고서 요약\n\n")
                f.write(f"**생성일시**: {datetime.now().strftime('%Y년 %m월 %d일 %H:%M:%S')}\n\n")
                
                f.write("## 📈 데이터 현황\n\n")
                f.write(f"- 결산공시 데이터: {len(settlement_data) if settlement_data else 0}개 은행\n")
                f.write(f"- 분기공시 데이터: {len(quarterly_data) if quarterly_data else 0}개 은행\n\n")
                
                if settlement_data and quarterly_data:
                    common_banks = (
                        set([d['은행명'] for d in settlement_data]) & 
                        set([d['은행명'] for d in quarterly_data])
                    )
                    f.write(f"- 공통 데이터 보유 은행: {len(common_banks)}개\n\n")
                
                f.write("## 💡 주요 발견사항\n\n")
                f.write("- 상세한 분석 내용은 함께 생성된 엑셀 파일을 참조하세요.\n")
                f.write("- 각 탭별로 결산공시, 분기공시, 비교분석 데이터가 정리되어 있습니다.\n\n")
                
                f.write("---\n")
                f.write("*저축은행 통합 데이터 스크래퍼 v3.1*\n")
                
        except Exception as e:
            pass  # MD 생성 실패해도 엑셀은 생성됨
    
    def compare_datasets(self):
        """두 데이터 비교 분석"""
        self.create_integrated_comparison_md()  # 이미 구현된 기능 호출
    
    def analyze_bank_performance(self):
        """은행별 성과 분석"""
        messagebox.showinfo(
            "정보", 
            "은행별 성과 분석 기능은 개발 중입니다.\n\n"
            "현재는 통합 재무 보고서를 사용해주세요."
        )
    
    def create_risk_analysis(self):
        """리스크 분석 보고서"""
        messagebox.showinfo(
            "정보", 
            "리스크 분석 보고서 기능은 개발 중입니다.\n\n"
            "자기자본비율과 고정이하여신비율 데이터를 통합 보고서에서 확인하세요."
        )
    
    def create_market_trend_analysis(self):
        """시장 동향 분석"""
        messagebox.showinfo(
            "정보", 
            "시장 동향 분석 기능은 시계열 데이터 축적 후 구현 예정입니다.\n\n"
            "정기적으로 데이터를 수집하여 트렌드를 분석해보세요."
        )
    
    # =====================================
    # 도구 메뉴 관련 메서드들
    # =====================================
    
    def compress_all_data(self):
        """모든 데이터 압축 및 아카이브"""
        try:
            result = messagebox.askyesno(
                "데이터 압축", 
                "모든 출력 폴더의 데이터를 압축하시겠습니까?\n\n"
                "• 결산공시 데이터\n"
                "• 분기공시 데이터\n"
                "• 생성된 보고서들\n\n"
                "압축 파일은 Downloads 폴더에 저장됩니다."
            )
            
            if result:
                self.update_status("데이터 압축 중...")
                threading.Thread(target=self._compress_all_data, daemon=True).start()
                
        except Exception as e:
            messagebox.showerror("오류", f"데이터 압축 중 오류: {str(e)}")
    
    def _compress_all_data(self):
        """모든 데이터 압축 (백그라운드)"""
        try:
            today = datetime.now().strftime("%Y%m%d_%H%M%S")
            zip_filename = os.path.join(
                os.path.expanduser("~"), 
                "Downloads", 
                f'저축은행_전체_데이터_{today}.zip'
            )
            
            with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:
                # 결산공시 데이터 압축
                if self.settlement_tab and hasattr(self.settlement_tab, 'config'):
                    output_dir = self.settlement_tab.config.output_dir
                    if os.path.exists(output_dir):
                        self._add_folder_to_zip(zipf, output_dir, "결산공시")
                
                # 분기공시 데이터 압축
                if self.quarterly_tab and hasattr(self.quarterly_tab, 'config'):
                    output_dir = self.quarterly_tab.config.output_dir
                    if os.path.exists(output_dir):
                        self._add_folder_to_zip(zipf, output_dir, "분기공시")
            
            # 완료 알림
            self.root.after(
                0, 
                lambda: messagebox.showinfo(
                    "완료", 
                    f"데이터 압축이 완료되었습니다!\n\n"
                    f"파일: {os.path.basename(zip_filename)}\n"
                    f"위치: {os.path.dirname(zip_filename)}"
                )
            )
            
        except Exception as e:
            self.root.after(
                0, 
                lambda: messagebox.showerror("오류", f"데이터 압축 실패: {str(e)}")
            )
        finally:
            self.root.after(0, lambda: self.update_status("데이터 압축 완료"))
    
    def _add_folder_to_zip(self, zipf, folder_path, folder_name):
        """폴더를 ZIP에 추가"""
        for root, dirs, files in os.walk(folder_path):
            for file in files:
                file_path = os.path.join(root, file)
                arcname = os.path.join(folder_name, os.path.relpath(file_path, folder_path))
                zipf.write(file_path, arcname)
    
    def cleanup_temp_files(self):
        """임시 파일 정리"""
        try:
            result = messagebox.askyesno(
                "임시 파일 정리", 
                "임시 파일들을 정리하시겠습니까?\n\n"
                "• 로그 파일 (7일 이상 된 것)\n"
                "• 임시 다운로드 파일\n"
                "• 중복 파일\n\n"
                "이 작업은 되돌릴 수 없습니다."
            )
            
            if result:
                self.update_status("임시 파일 정리 중...")
                threading.Thread(target=self._cleanup_temp_files, daemon=True).start()
                
        except Exception as e:
            messagebox.showerror("오류", f"임시 파일 정리 중 오류: {str(e)}")
    
    def _cleanup_temp_files(self):
        """임시 파일 정리 (백그라운드)"""
        try:
            cleaned_count = 0
            
            # 결산공시 폴더 정리
            if self.settlement_tab and hasattr(self.settlement_tab, 'config'):
                output_dir = self.settlement_tab.config.output_dir
                if os.path.exists(output_dir):
                    cleaned_count += self._clean_single_folder(output_dir)
            
            # 분기공시 폴더 정리
            if self.quarterly_tab and hasattr(self.quarterly_tab, 'config'):
                output_dir = self.quarterly_tab.config.output_dir
                if os.path.exists(output_dir):
                    cleaned_count += self._clean_single_folder(output_dir)
            
            # Downloads 폴더의 임시 파일 정리
            downloads_dir = os.path.join(os.path.expanduser("~"), "Downloads")
            if os.path.exists(downloads_dir):
                cleaned_count += self._clean_downloads_folder(downloads_dir)
            
            # 완료 알림
            self.root.after(
                0, 
                lambda: messagebox.showinfo(
                    "완료", 
                    f"임시 파일 정리가 완료되었습니다!\n\n"
                    f"정리된 파일: {cleaned_count}개"
                )
            )
            
        except Exception as e:
            self.root.after(
                0, 
                lambda: messagebox.showerror("오류", f"임시 파일 정리 실패: {str(e)}")
            )
        finally:
            self.root.after(0, lambda: self.update_status("임시 파일 정리 완료"))
    
    def _clean_single_folder(self, folder_path):
        """단일 폴더 정리"""
        cleaned_count = 0
        try:
            for file in os.listdir(folder_path):
                file_path = os.path.join(folder_path, file)
                
                # 임시 파일 삭제
                if file.startswith('~') or file.startswith('.tmp') or file.endswith('.tmp'):
                    try:
                        os.remove(file_path)
                        cleaned_count += 1
                    except:
                        pass
                
                # 오래된 로그 파일 정리
                elif file.endswith('.txt') and 'log' in file.lower():
                    try:
                        import time
                        file_time = os.path.getmtime(file_path)
                        if time.time() - file_time > 7 * 24 * 3600:  # 7일
                            os.remove(file_path)
                            cleaned_count += 1
                    except:
                        pass
            
            return cleaned_count
            
        except Exception as e:
            return 0
    
    def _clean_downloads_folder(self, downloads_dir):
        """Downloads 폴더 정리"""
        cleaned_count = 0
        try:
            for file in os.listdir(downloads_dir):
                if file.startswith('저축은행_') and file.endswith('.tmp'):
                    try:
                        file_path = os.path.join(downloads_dir, file)
                        os.remove(file_path)
                        cleaned_count += 1
                    except:
                        pass
            
            return cleaned_count
            
        except Exception as e:
            return 0
    
    def refresh_all_tabs(self):
        """모든 탭 새로고침"""
        try:
            self.update_status("모든 탭 새로고침 중...")
            
            # 각 탭의 은행 목록 새로고침
            if self.settlement_tab and hasattr(self.settlement_tab, 'load_bank_list'):
                self.settlement_tab.load_bank_list()
            
            if self.quarterly_tab and hasattr(self.quarterly_tab, 'load_bank_list'):
                self.quarterly_tab.load_bank_list()
            
            # 시스템 정보 업데이트
            self.update_system_info()
            
            messagebox.showinfo("완료", "모든 탭이 새로고침되었습니다.")
            self.update_status("탭 새로고침 완료")
            
        except Exception as e:
            messagebox.showerror("오류", f"탭 새로고침 중 오류: {str(e)}")
    
    def show_advanced_settings(self):
        """고급 설정 창 표시"""
        try:
            # 고급 설정 창 생성
            settings_window = tk.Toplevel(self.root)
            settings_window.title("⚙️ 고급 설정")
            settings_window.geometry("500x400")
            settings_window.resizable(False, False)
            
            # 메인 프레임
            main_frame = ttk.Frame(settings_window, padding="20")
            main_frame.pack(fill=tk.BOTH, expand=True)
            
            # 설정 항목들
            ttk.Label(
                main_frame, 
                text="⚙️ 고급 설정", 
                font=("", 14, "bold")
            ).pack(pady=(0, 20))
            
            # 자동 백업 설정
            backup_frame = ttk.LabelFrame(main_frame, text="백업 설정", padding="10")
            backup_frame.pack(fill=tk.X, pady=(0, 10))
            
            self.auto_backup_var = tk.BooleanVar(value=self.settings.get('auto_backup', True))
            ttk.Checkbutton(
                backup_frame, 
                text="자동 백업 활성화", 
                variable=self.auto_backup_var
            ).pack(anchor=tk.W)
            
            # 로그 레벨 설정
            log_frame = ttk.LabelFrame(main_frame, text="로그 설정", padding="10")
            log_frame.pack(fill=tk.X, pady=(0, 10))
            
            self.log_level_var = tk.StringVar(value=self.settings.get('log_level', 'INFO'))
            ttk.Label(log_frame, text="로그 레벨:").pack(anchor=tk.W)
            log_combo = ttk.Combobox(
                log_frame, 
                textvariable=self.log_level_var, 
                values=['DEBUG', 'INFO', 'WARNING', 'ERROR'], 
                state='readonly'
            )
            log_combo.pack(fill=tk.X, pady=2)
            
            # 테마 설정
            theme_frame = ttk.LabelFrame(main_frame, text="테마 설정", padding="10")
            theme_frame.pack(fill=tk.X, pady=(0, 20))
            
            self.theme_var = tk.StringVar(value=self.settings.get('theme', 'clam'))
            ttk.Label(theme_frame, text="UI 테마:").pack(anchor=tk.W)
            style = ttk.Style()
            theme_combo = ttk.Combobox(
                theme_frame, 
                textvariable=self.theme_var, 
                values=list(style.theme_names()), 
                state='readonly'
            )
            theme_combo.pack(fill=tk.X, pady=2)
            
            # 버튼 프레임
            button_frame = ttk.Frame(main_frame)
            button_frame.pack(fill=tk.X)
            
            ttk.Button(
                button_frame, 
                text="저장", 
                command=lambda: self._save_advanced_settings(settings_window)
            ).pack(side=tk.LEFT, padx=(0, 5))
            
            ttk.Button(
                button_frame, 
                text="취소", 
                command=settings_window.destroy
            ).pack(side=tk.LEFT)
            
        except Exception as e:
            messagebox.showerror("오류", f"고급 설정 창 표시 중 오류: {str(e)}")
    
    def _save_advanced_settings(self, window):
        """고급 설정 저장"""
        try:
            self.settings['auto_backup'] = self.auto_backup_var.get()
            self.settings['log_level'] = self.log_level_var.get()
            self.settings['theme'] = self.theme_var.get()
            
            self.save_main_settings()
            
            # 테마 즉시 적용
            style = ttk.Style()
            try:
                style.theme_use(self.theme_var.get())
            except:
                pass
            
            messagebox.showinfo("완료", "설정이 저장되었습니다.")
            window.destroy()
            self.update_status("고급 설정 저장 완료")
            
        except Exception as e:
            messagebox.showerror("오류", f"설정 저장 중 오류: {str(e)}")
    
    def show_system_status(self):
        """시스템 상태 창 표시"""
        try:
            # 시스템 상태 창 생성
            status_window = tk.Toplevel(self.root)
            status_window.title("📊 시스템 상태")
            status_window.geometry("600x500")
            
            # 메인 프레임
            main_frame = ttk.Frame(status_window, padding="20")
            main_frame.pack(fill=tk.BOTH, expand=True)
            
            # 제목
            ttk.Label(
                main_frame, 
                text="📊 시스템 상태", 
                font=("", 14, "bold")
            ).pack(pady=(0, 20))
            
            # 스크롤 가능한 텍스트 영역
            text_frame = ttk.Frame(main_frame)
            text_frame.pack(fill=tk.BOTH, expand=True)
            
            status_text = scrolledtext.ScrolledText(
                text_frame, 
                wrap=tk.WORD, 
                font=("Consolas", 10)
            )
            status_text.pack(fill=tk.BOTH, expand=True)
            
            # 시스템 정보 수집 및 표시
            status_info = self._collect_system_status()
            status_text.insert(tk.END, status_info)
            status_text.config(state=tk.DISABLED)
            
            # 새로고침 버튼
            ttk.Button(
                main_frame, 
                text="🔄 새로고침", 
                command=lambda: self._refresh_system_status(status_text)
            ).pack(pady=(10, 0))
            
        except Exception as e:
            messagebox.showerror("오류", f"시스템 상태 창 표시 중 오류: {str(e)}")
    
    def _collect_system_status(self):
        """시스템 상태 정보 수집"""
        try:
            import platform
            
            status_info = []
            status_info.append("🖥️ 시스템 정보")
            status_info.append("=" * 50)
            status_info.append(f"운영체제: {platform.system()} {platform.release()}")
            status_info.append(f"Python 버전: {platform.python_version()}")
            status_info.append(f"아키텍처: {platform.architecture()[0]}")
            status_info.append("")
            
            # 하드웨어 정보
            status_info.append("🔧 하드웨어 정보")
            status_info.append("=" * 50)
            try:
                import psutil
                status_info.append(f"CPU: {platform.processor()}")
                status_info.append(
                    f"CPU 코어 수: {psutil.cpu_count(logical=False)} "
                    f"(논리: {psutil.cpu_count()})"
                )
                status_info.append(f"메모리: {psutil.virtual_memory().total // (1024**3)} GB")
                status_info.append(
                    f"사용 가능한 메모리: {psutil.virtual_memory().available // (1024**3)} GB"
                )
            except:
                status_info.append("하드웨어 정보를 가져올 수 없습니다.")
            status_info.append("")
            
            # 애플리케이션 상태
            status_info.append("🏦 애플리케이션 상태")
            status_info.append("=" * 50)
            status_info.append(f"스크래퍼 버전: v3.1")
            status_info.append(f"시작 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            status_info.append(
                f"결산공시 모듈: {'✅ 사용 가능' if SETTLEMENT_AVAILABLE else '❌ 사용 불가'}"
            )
            status_info.append(
                f"분기공시 모듈: {'✅ 사용 가능' if QUARTERLY_AVAILABLE else '❌ 사용 불가'}"
            )
            status_info.append("")
            
            # 폴더 상태
            status_info.append("📁 폴더 상태")
            status_info.append("=" * 50)
            
            if self.settlement_tab and hasattr(self.settlement_tab, 'config'):
                settlement_dir = self.settlement_tab.config.output_dir
                if os.path.exists(settlement_dir):
                    file_count = len([f for f in os.listdir(settlement_dir) if f.endswith('.xlsx')])
                    status_info.append(f"결산공시 폴더: {settlement_dir}")
                    status_info.append(f"  └─ 엑셀 파일: {file_count}개")
                else:
                    status_info.append(f"결산공시 폴더: 없음")
            
            if self.quarterly_tab and hasattr(self.quarterly_tab, 'config'):
                quarterly_dir = self.quarterly_tab.config.output_dir
                if os.path.exists(quarterly_dir):
                    file_count = len([f for f in os.listdir(quarterly_dir) if f.endswith('.xlsx')])
                    status_info.append(f"분기공시 폴더: {quarterly_dir}")
                    status_info.append(f"  └─ 엑셀 파일: {file_count}개")
                else:
                    status_info.append(f"분기공시 폴더: 없음")
            
            status_info.append("")
            status_info.append("📝 로그 파일")
            status_info.append("=" * 50)
            
            # 로그 파일 확인
            log_files_found = 0
            for root, dirs, files in os.walk(os.path.expanduser("~")):
                for file in files:
                    if 'bank_scraping_log' in file or 'bank_settlement_scraping_log' in file:
                        log_files_found += 1
                        if log_files_found <= 5:  # 최대 5개만 표시
                            status_info.append(f"  {file}")
            
            if log_files_found == 0:
                status_info.append("로그 파일을 찾을 수 없습니다.")
            elif log_files_found > 5:
                status_info.append(f"  ... 외 {log_files_found - 5}개 더")
            
            return "\n".join(status_info)
            
        except Exception as e:
            return f"시스템 상태 정보 수집 중 오류 발생: {str(e)}"
    
    def _refresh_system_status(self, text_widget):
        """시스템 상태 새로고침"""
        try:
            text_widget.config(state=tk.NORMAL)
            text_widget.delete(1.0, tk.END)
            
            status_info = self._collect_system_status()
            text_widget.insert(tk.END, status_info)
            text_widget.config(state=tk.DISABLED)
            
        except Exception as e:
            messagebox.showerror("오류", f"시스템 상태 새로고침 중 오류: {str(e)}")
    
    # =====================================
    # 도움말 메뉴 관련 메서드들
    # =====================================
    
    def show_help(self):
        """도움말 표시"""
        help_text = (
            "🏦 저축은행 통합 데이터 스크래퍼 v3.1 사용 방법\n\n"
            "📋 기본 사용법:\n"
            "1. 원하는 탭(결산공시/분기공시)을 선택합니다.\n"
            "2. 은행을 선택하고 스크래핑을 시작합니다.\n"
            "3. 완료 후 결과를 확인하고 보고서를 생성합니다.\n\n"
            "📝 MD 기능:\n"
            "• \"MD 파일도 함께 생성\" 체크박스를 선택하면 엑셀과 함께 마크다운 파일도 생성됩니다.\n"
            "• \"MD 보고서\" 메뉴에서 다양한 분석 보고서를 생성할 수 있습니다.\n"
            "• 기존 엑셀 파일을 MD로 변환하는 기능도 제공됩니다.\n\n"
            "📊 데이터 분석:\n"
            "• 통합 분석 탭에서 다양한 분석 도구를 사용할 수 있습니다.\n"
            "• 결산공시와 분기공시 데이터를 비교 분석할 수 있습니다.\n"
            "• 자동으로 생성되는 요약 보고서를 활용하세요.\n\n"
            "🛠️ 주요 기능:\n"
            "• 결산공시: 연말 결산 데이터 수집\n"
            "• 분기공시: 3개월 주기 데이터 수집\n"
            "• 통합 보고서: 두 데이터를 통합 분석\n"
            "• MD 보고서: 마크다운 형식의 보고서 생성\n"
            "• 데이터 비교: 결산공시와 분기공시 데이터 비교\n"
            "• 자동 압축: 수집된 데이터의 자동 압축 및 백업\n\n"
            "⚙️ 고급 기능:\n"
            "• 고급 설정에서 테마, 로그 레벨 등을 조정할 수 있습니다.\n"
            "• 시스템 상태에서 애플리케이션의 전반적인 상태를 확인할 수 있습니다.\n"
            "• 임시 파일 정리 기능으로 불필요한 파일들을 제거할 수 있습니다.\n\n"
            "💡 팁:\n"
            "• MD 파일은 GitHub, Notion 등에서 바로 읽을 수 있습니다.\n"
            "• 통합 보고서를 통해 은행별 재무 상태를 한눈에 비교할 수 있습니다.\n"
            "• 정기적으로 데이터를 수집하여 트렌드를 분석해보세요.\n"
            "• 설정 내보내기/가져오기 기능으로 설정을 백업하고 공유할 수 있습니다.\n\n"
            "🔧 문제 해결:\n"
            "• 스크래핑이 실패하면 시스템 상태를 확인해보세요.\n"
            "• ChromeDriver 경로를 정확히 설정했는지 확인하세요.\n"
            "• 네트워크 연결 상태를 점검하세요.\n"
            "• 임시 파일 정리를 통해 공간을 확보하세요."
        )
        
        # 새 창에서 도움말 표시
        help_window = tk.Toplevel(self.root)
        help_window.title("📖 사용 방법")
        help_window.geometry("700x600")
        
        # 메인 프레임
        main_frame = ttk.Frame(help_window, padding="20")
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # 텍스트 위젯
        text_widget = scrolledtext.ScrolledText(
            main_frame, 
            wrap=tk.WORD, 
            font=("", 10)
        )
        text_widget.pack(fill=tk.BOTH, expand=True)
        text_widget.insert(tk.END, help_text)
        text_widget.config(state=tk.DISABLED)
        
        # 닫기 버튼
        ttk.Button(main_frame, text="닫기", command=help_window.destroy).pack(pady=(10, 0))
    
    def show_quick_start(self):
        """빠른 시작 가이드"""
        quick_start_text = (
            "🚀 빠른 시작 가이드\n\n"
            "📋 1단계: 준비\n"
            "• ChromeDriver를 다운로드하고 경로를 설정하세요\n"
            "• 출력 폴더를 원하는 위치로 설정하세요\n"
            "• 네트워크 연결을 확인하세요\n\n"
            "📊 2단계: 데이터 수집\n"
            "• 결산공시 또는 분기공시 탭을 선택하세요\n"
            "• 수집할 은행들을 선택하세요 (전체 선택 권장)\n"
            "• \"MD 파일도 함께 생성\"을 체크하세요\n"
            "• \"스크래핑 시작\" 버튼을 클릭하세요\n\n"
            "📝 3단계: 보고서 생성\n"
            "• 스크래핑 완료 후 \"MD 보고서\" 메뉴를 사용하세요\n"
            "• \"통합 비교 분석 보고서\"를 생성하세요\n"
            "• \"전체 데이터 MD 요약\"을 생성하세요\n\n"
            "🔧 4단계: 데이터 관리\n"
            "• \"도구\" 메뉴에서 \"데이터 압축 및 아카이브\"를 실행하세요\n"
            "• 정기적으로 \"임시 파일 정리\"를 수행하세요\n"
            "• \"설정 내보내기\"로 설정을 백업하세요\n\n"
            "💡 추가 팁:\n"
            "• 통합 분석 탭에서 한 번에 여러 작업을 수행할 수 있습니다\n"
            "• 시스템 상태를 정기적으로 확인하세요\n"
            "• 문제 발생 시 도움말을 참조하세요"
        )
        
        # 빠른 시작 창 표시
        quick_window = tk.Toplevel(self.root)
        quick_window.title("🚀 빠른 시작 가이드")
        quick_window.geometry("600x500")
        
        main_frame = ttk.Frame(quick_window, padding="20")
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        text_widget = scrolledtext.ScrolledText(main_frame, wrap=tk.WORD, font=("", 10))
        text_widget.pack(fill=tk.BOTH, expand=True)
        text_widget.insert(tk.END, quick_start_text)
        text_widget.config(state=tk.DISABLED)
        
        ttk.Button(main_frame, text="닫기", command=quick_window.destroy).pack(pady=(10, 0))
    
    def show_faq(self):
        """FAQ 표시"""
        faq_text = (
            "❓ 자주 묻는 질문 (FAQ)\n\n"
            "Q1: ChromeDriver를 어디서 다운로드하나요?\n"
            "A1: https://chromedriver.chromium.org 에서 Chrome 버전에 맞는 드라이버를 다운로드하세요.\n\n"
            "Q2: 스크래핑이 중간에 멈춥니다.\n"
            "A2: 네트워크 연결을 확인하고, ChromeDriver 경로가 올바른지 확인하세요. "
            "병렬 작업자 수를 줄여보세요.\n\n"
            "Q3: MD 파일을 어떻게 활용하나요?\n"
            "A3: GitHub에 업로드하거나, Notion으로 임포트하거나, VS Code의 마크다운 프리뷰를 사용하세요.\n\n"
            "Q4: 데이터가 정확한가요?\n"
            "A4: 저축은행중앙회의 공식 공시 데이터를 수집하므로 정확합니다. "
            "단, 웹사이트 구조 변경 시 영향을 받을 수 있습니다.\n\n"
            "Q5: 여러 번 실행해도 되나요?\n"
            "A5: 네, 이미 완료된 은행은 건너뛰므로 안전합니다. "
            "\"진행 상태 초기화\"로 처음부터 다시 할 수도 있습니다.\n\n"
            "Q6: 오래된 데이터는 어떻게 관리하나요?\n"
            "A6: \"임시 파일 정리\" 기능을 사용하거나, \"데이터 압축 및 아카이브\"로 백업하세요.\n\n"
            "Q7: 설정을 다른 컴퓨터에서도 사용할 수 있나요?\n"
            "A7: \"설정 내보내기/가져오기\" 기능을 사용하여 설정을 이전할 수 있습니다.\n\n"
            "Q8: 에러가 발생하면 어떻게 하나요?\n"
            "A8: 로그 파일을 확인하고, \"시스템 상태\"에서 문제를 진단하세요. "
            "지속되면 프로그램을 재시작하세요.\n\n"
            "Q9: 속도를 높이려면?\n"
            "A9: 병렬 작업자 수를 늘리세요. 단, 너무 많이 늘리면 서버에 부하를 줄 수 있습니다.\n\n"
            "Q10: 특정 은행만 수집할 수 있나요?\n"
            "A10: 네, 은행 목록에서 원하는 은행만 선택하여 스크래핑할 수 있습니다."
        )
        
        # FAQ 창 표시
        faq_window = tk.Toplevel(self.root)
        faq_window.title("❓ 자주 묻는 질문")
        faq_window.geometry("700x600")
        
        main_frame = ttk.Frame(faq_window, padding="20")
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        text_widget = scrolledtext.ScrolledText(main_frame, wrap=tk.WORD, font=("", 10))
        text_widget.pack(fill=tk.BOTH, expand=True)
        text_widget.insert(tk.END, faq_text)
        text_widget.config(state=tk.DISABLED)
        
        ttk.Button(main_frame, text="닫기", command=faq_window.destroy).pack(pady=(10, 0))
    
    def open_online_docs(self):
        """온라인 문서 열기"""
        try:
            # 저축은행중앙회 웹사이트 열기
            webbrowser.open("https://www.fsb.or.kr")
            self.update_status("온라인 문서 열기 완료")
        except Exception as e:
            messagebox.showerror("오류", f"온라인 문서를 열 수 없습니다: {str(e)}")
    
    def report_bug(self):
        """버그 신고"""
        bug_info = (
            f"버그 신고용 시스템 정보:\n\n"
            f"스크래퍼 버전: v3.1\n"
            f"운영체제: {sys.platform}\n"
            f"Python 버전: {sys.version}\n"
            f"설치된 모듈:\n"
            f"- 결산공시: {'사용 가능' if SETTLEMENT_AVAILABLE else '사용 불가'}\n"
            f"- 분기공시: {'사용 가능' if QUARTERLY_AVAILABLE else '사용 불가'}\n\n"
            f"위 정보를 개발팀에 전달해 주세요."
        )
        
        messagebox.showinfo("🐛 버그 신고", bug_info)
    
    def show_about(self):
        """프로그램 정보 표시"""
        about_text = (
            f"🏦 저축은행 통합 데이터 스크래퍼\n"
            f"버전: 3.1 (완전 통합 버전)\n"
            f"작성일: 2025-01-31\n\n"
            f"📊 주요 기능:\n"
            f"• 79개 저축은행의 결산공시 및 분기공시 데이터 자동 수집\n"
            f"• 엑셀 및 마크다운(MD) 형식으로 데이터 저장\n"
            f"• 통합 비교 분석 보고서 생성\n"
            f"• 데이터 트렌드 분석 및 시각화\n"
            f"• 자동 압축 및 백업 기능\n\n"
            f"📝 MD 기능:\n"
            f"• 개별 은행 데이터를 마크다운 테이블로 저장\n"
            f"• 통합 분석 보고서를 MD 형식으로 생성\n"
            f"• 기존 엑셀 파일을 MD로 변환\n"
            f"• GitHub, Notion 등에서 바로 활용 가능\n\n"
            f"🛠️ 기술 스택:\n"
            f"• Python 3.8+\n"
            f"• Selenium WebDriver (Chrome)\n"
            f"• Pandas, BeautifulSoup, OpenPyXL\n"
            f"• Tkinter GUI\n"
            f"• 멀티스레딩 지원\n\n"
            f"🔧 새로운 기능 (v3.1):\n"
            f"• 완전 통합된 GUI 인터페이스\n"
            f"• 고급 설정 및 시스템 상태 모니터링\n"
            f"• 향상된 오류 처리 및 복구 기능\n"
            f"• 설정 내보내기/가져오기\n"
            f"• 임시 파일 자동 정리\n"
            f"• 다양한 테마 지원\n\n"
            f"📞 지원:\n"
            f"현재 상태: {'✅ 모든 기능 사용 가능' if SETTLEMENT_AVAILABLE and QUARTERLY_AVAILABLE else '⚠️ 일부 기능 제한'}\n"
            f"문의사항이 있으시면 시스템 관리자에게 연락하세요.\n\n"
            f"© 2025 저축은행 데이터 분석팀"
        )
        
        messagebox.showinfo("ℹ️ 프로그램 정보", about_text)
    
    # =====================================
    # 설정 관리 메서드들
    # =====================================
    
    def load_main_settings(self):
        """메인 설정 로드"""
        try:
            if os.path.exists(self.config_file):
                with open(self.config_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            else:
                return {
                    'auto_backup': True,
                    'log_level': 'INFO',
                    'theme': 'clam',
                    'last_used': datetime.now().isoformat()
                }
        except Exception as e:
            print(f"메인 설정 로드 실패: {e}")
            return {}
    
    def save_main_settings(self):
        """메인 설정 저장"""
        try:
            os.makedirs(self.config_dir, exist_ok=True)
            self.settings['last_used'] = datetime.now().isoformat()
            
            with open(self.config_file, 'w', encoding='utf-8') as f:
                json.dump(self.settings, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"메인 설정 저장 실패: {e}")
    
    def load_initial_settings(self):
        """초기 설정 로드"""
        try:
            # 테마 적용
            if 'theme' in self.settings:
                style = ttk.Style()
                try:
                    style.theme_use(self.settings['theme'])
                except:
                    pass
            
            self.update_status("초기 설정 로드 완료")
        except Exception as e:
            print(f"초기 설정 로드 중 오류: {e}")
    
    # =====================================
    # 애플리케이션 종료 관련
    # =====================================
    
    def on_closing(self):
        """애플리케이션 종료 처리"""
        try:
            # 설정 저장
            self.save_main_settings()
            
            # 실행 중인 작업 확인
            running_tasks = []
            if (self.settlement_tab and 
                hasattr(self.settlement_tab, 'running') and 
                self.settlement_tab.running):
                running_tasks.append("결산공시 스크래핑")
            if (self.quarterly_tab and 
                hasattr(self.quarterly_tab, 'running') and 
                self.quarterly_tab.running):
                running_tasks.append("분기공시 스크래핑")
            
            if running_tasks:
                result = messagebox.askyesno(
                    "실행 중인 작업 확인", 
                    f"다음 작업이 실행 중입니다:\n{', '.join(running_tasks)}\n\n"
                    f"정말 종료하시겠습니까? 진행 중인 작업이 중단됩니다."
                )
                
                if not result:
                    return
                
                # 실행 중인 작업 중지
                if self.settlement_tab and hasattr(self.settlement_tab, 'running'):
                    self.settlement_tab.running = False
                if self.quarterly_tab and hasattr(self.quarterly_tab, 'running'):
                    self.quarterly_tab.running = False
            
            # 종료
            self.root.destroy()
            
        except Exception as e:
            print(f"종료 처리 중 오류: {e}")
            self.root.destroy()


def main():
    """프로그램 진입점"""
    try:
        print("🏦 저축은행 통합 데이터 스크래퍼 v3.1 시작")
        print(f"📅 시작 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Tkinter 루트 윈도우 생성
        root = tk.Tk()
        
        # 통합 GUI 앱 생성
        app = IntegratedBankScraperGUI(root)
        
        print("✅ GUI 초기화 완료")
        
        # 이벤트 루프 시작
        root.mainloop()
        
        print("📝 프로그램 정상 종료")
        
    except Exception as e:
        print(f"❌ 프로그램 실행 중 오류 발생: {str(e)}")
        import traceback
        traceback.print_exc()
        
        # 에러 대화상자 표시 (GUI가 가능한 경우)
        try:
            messagebox.showerror(
                "심각한 오류", 
                f"프로그램 실행 중 예상치 못한 오류가 발생했습니다:\n\n{str(e)}\n\n"
                f"프로그램을 다시 시작해주세요."
            )
        except:
            pass


if __name__ == "__main__":
    main()
