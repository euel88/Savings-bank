def generate_screenshot_format_report():
    """스크린샷과 동일한 형태의 은행별 날짜 확인 테이블을 생성합니다."""
    try:
        progress_manager = ProgressManager()
        validation_data = progress_manager.progress.get('data_validation', [])
        validation_dict = {item['bank_name']: item for item in validation_data}
        
        # 현재 시간
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # 테이블 데이터 생성
        table_data = []
        
        for bank in BANKS:
            # 각 은행의 엑셀 파일 찾기
            bank_files = [f for f in os.listdir(OUTPUT_DIR) if f.startswith(f"{bank}_") and f.endswith(".xlsx")]
            
            date_info = "데이터 없음"
            date_status = "❌ 안료"  # 완료되지 않음
            processing_status = "완료"
            
            if bank_files:
                try:
                    # 가장 최근 파일 선택
                    latest_file = sorted(bank_files)[-1]
                    file_path = os.path.join(OUTPUT_DIR, latest_file)
                    
                    # 공시 정보에서 날짜 추출
                    if os.path.exists(file_path):
                        try:
                            info_df = pd.read_excel(file_path, sheet_name='공시정보')
                            if '공시 날짜' in info_df.columns and not info_df['공시 날짜'].empty:
                                date_info = str(info_df['공시 날짜'].iloc[0])
                                
                                # 날짜에 따른 상태 결정
                                if '2024년9월말' in date_info or '2024년09월말' in date_info:
                                    date_status = "✅ 일치 (기한내최신)"
                                elif '2025년3월말' in date_info or '2025년03월말' in date_info:
                                    date_status = "🟢 일치 (예정보다선반영)"
                                else:
                                    date_status = "⚠️ 확인필요"
                                    
                        except Exception as e:
                            date_info = f"파일 읽기 오류: {str(e)}"
                            date_status = "❌ 오류"
                            processing_status = "실패"
                            
                except Exception as e:
                    date_info = f"파일 처리 오류: {str(e)}"
                    date_status = "❌ 오류"
                    processing_status = "실패"
            else:
                # 검증 데이터에서 정보 가져오기
                if bank in validation_dict:
                    validation_info = validation_dict[bank]
                    date_info = validation_info.get('date_info', '추출 실패')
                    
                    if validation_info.get('is_fresh', False):
                        if '2024년9월' in date_info:
                            date_status = "✅ 일치 (기한내최신)"
                        elif '2025년3월' in date_info:
                            date_status = "🟢 일치 (예정보다선반영)"
                        else:
                            date_status = "⚠️ 확인필요"
                    else:
                        date_status = "❌ 불일치"
                        
                    processing_status = "부분완료"
                else:
                    date_info = "처리되지 않음"
                    date_status = "❌ 미처리"
                    processing_status = "실패"
            
            table_data.append({
                '은행명': bank,
                '공시 날짜(월말)': date_info,
                '날짜 확인': date_status,
                '처리상태': processing_status,
                '확인 시간': current_time
            })
        
        # DataFrame 생성
        result_df = pd.DataFrame(table_data)
        
        # 상태별로 정렬 (성공 > 부분완료 > 실패)
        status_order = {'완료': 0, '부분완료': 1, '실패': 2}
        result_df['정렬순서'] = result_df['처리상태'].map(status_order)
        result_df = result_df.sort_values(['정렬순서', '은행명']).drop('정렬순서', axis=1)
        
        # 스크린샷 형태의 결과 파일 저장
        screenshot_format_file = os.path.join(OUTPUT_DIR, f"은행별_날짜확인_결과_{TODAY}.xlsx")
        
        with pd.ExcelWriter(screenshot_format_file, engine='openpyxl') as writer:
            # 메인 결과 시트
            result_df.to_excel(writer, sheet_name='은행별_날짜확인', index=False)
            
            # 통계 요약 시트
            stats_data = {
                '구분': [
                    '전체 은행 수',
                    '완료된 은행 수',
                    '2024년9월말 데이터',
                    '2025년3월말 데이터',
                    '기타 날짜 데이터',
                    '처리 실패 은행',
                    '성공률'
                ],
                '수량': [
                    len(BANKS),
                    len([r for r in table_data if r['처리상태'] == '완료']),
                    len([r for r in table_data if '2024년9월' in r['공시 날짜(월말)']]),
                    len([r for r in table_data if '2025년3월' in r['공시 날짜(월말)']]),
                    len([r for r in table_data if '2024년9월' not in r['공시 날짜(월말)'] and '2025년3월' not in r['공시 날짜(월말)'] and r['처리상태'] != '실패']),
                    len([r for r in table_data if r['처리상태'] == '실패']),
                    f"{len([r for r in table_data if r['처리상태'] in ['완료', '부분완료']]) / len(BANKS) * 100:.1f}%"
                ]
            }
            
            stats_df = pd.DataFrame(stats_data)
            stats_df.to_excel(writer, sheet_name='통계요약', index=False)
            
            # 문제 은행 목록 (날짜가 예상과 다른 은행들)
            problem_banks = [
                r for r in table_data 
                if r['날짜 확인'] in ['⚠️ 확인필요', '❌ 불일치', '❌ 미처리', '❌ 오류']
            ]
            
            if problem_banks:
                problem_df = pd.DataFrame(problem_banks)
                problem_df.to_excel(writer, sheet_name='문제은행목록', index=False)
        
        log_message(f"스크린샷 형태 결과 파일 저장 완료: {screenshot_format_file}")
        
        # 콘솔에 요약 출력 (스크린샷과 유사한 형태)
        log_message("\n" + "="*80)
        log_message("📋 은행별 날짜 확인 결과 (스크린샷 형태)")
        log_message("="*80)
        log_message(f"{'은행명':<10} {'공시 날짜':<15} {'날짜 확인':<20} {'처리상태':<10}")
        log_message("-"*80)
        
        for _, row in result_df.head(20).iterrows():  # 상위 20개만 표시
            log_message(f"{row['은행명']:<10} {row['공시 날짜(월말)']:<15} {row['날짜 확인']:<20} {row['처리상태']:<10}")
        
        if len(result_df) > 20:
            log_message(f"... 총 {len(result_df)}개 은행 (상위 20개만 표시)")
        
        log_message("-"*80)
        
        # 상태별 요약
        status_summary = result_df['처리상태'].value_counts()
        date_check_summary = result_df['날짜 확인'].value_counts()
        
        log_message("📊 처리 상태 요약:")
        for status, count in status_summary.items():
            log_message(f"  • {status}: {count}개")
            
        log_message("\n📅 날짜 확인 요약:")
        for status, count in date_check_summary.items():
            log_message(f"  • {status}: {count}개")
        
        log_message("="*80)
        
        return screenshot_format_file, problem_banks
        
    except Exception as e:
        log_message(f"스크린샷 형태 보고서 생성 오류: {str(e)}")
        import traceback
        log_message(f"상세 오류: {traceback.format_exc()}")
        return None, []


def enhanced_main():
    """개선된 메인 실행 함수 (3가지 문제점 해결 적용)"""
    # 로그 파일 초기화
    try:
        with open(LOG_FILE, 'w', encoding='utf-8') as f:
            f.write("")
    except Exception as e:
        print(f"로그 파일 초기화 실패: {e}")

    start_time = time.time()
    log_message(f"\n===== 저축은행 중앙회 통일경영공시 데이터 스크래핑 시작 (개선 버전) [{TODAY}] =====\n")

    try:
        # 환경 설정 로그
        log_message(f"🔧 개선사항 적용:")
        log_message(f"  • 날짜 추출 정확도 향상 (안국, 오투 은행 오류 해결)")
        log_message(f"  • ZIP 파일 생성 안정화 (.bin 오류 해결)")
        log_message(f"  • 스크린샷 형태 결과 테이블 생성")
        
        log_message(f"\n⚙️ 설정값:")
        log_message(f"  • 최대 워커 수: {MAX_WORKERS}")
        log_message(f"  • 최대 재시도 횟수: {MAX_RETRIES}")
        log_message(f"  • 페이지 로드 타임아웃: {PAGE_LOAD_TIMEOUT}초")
        log_message(f"  • 대기 타임아웃: {WAIT_TIMEOUT}초")
        log_message(f"  • 출력 디렉토리: {OUTPUT_DIR}")
        log_message(f"  • 이메일 알림: {'활성화' if GMAIL_ADDRESS and GMAIL_APP_PASSWORD else '비활성화'}")

        # 은행 처리 실행
        successful_banks, failed_banks, all_results = process_with_retry(BANKS, max_retries=MAX_RETRIES)

        # 결과 요약 생성
        summary_file, stats = generate_summary_report()

        # 스크린샷 형태 결과 생성 (신규 기능)
        screenshot_file, problem_banks = generate_screenshot_format_report()

        # 개선된 ZIP 아카이브 생성
        zip_file = create_zip_archive()

        # 실행 시간 계산
        end_time = time.time()
        total_duration = end_time - start_time
        minutes, seconds = divmod(total_duration, 60)
        
        # 최종 결과 로그
        log_message(f"\n🎉 ===== 스크래핑 완료 (개선 버전) =====")
        log_message(f"⏰ 총 실행 시간: {int(minutes)}분 {int(seconds)}초")
        log_message(f"✅ 성공한 은행: {len(successful_banks)}개")
        log_message(f"❌ 실패한 은행: {len(failed_banks)}개")
        
        if failed_banks:
            log_message(f"🔍 실패한 은행 목록: {', '.join(failed_banks)}")

        if problem_banks:
            log_message(f"⚠️ 날짜 확인 필요 은행: {len(problem_banks)}개")
            for bank in problem_banks[:5]:  # 상위 5개만 표시
                log_message(f"   • {bank['은행명']}: {bank['공시 날짜(월말)']} ({bank['날짜 확인']})")

        # 생성된 파일 목록
        log_message(f"\n📁 생성된 파일:")
        log_message(f"  • 📦 ZIP 압축파일: {os.path.basename(zip_file) if zip_file else '생성 실패'}")
        log_message(f"  • 📊 요약 보고서: {os.path.basename(summary_file) if summary_file else '생성 실패'}")
        log_message(f"  • 📋 스크린샷 형태 결과: {os.path.basename(screenshot_file) if screenshot_file else '생성 실패'}")

        # 이메일 알림 발송
        if GMAIL_ADDRESS and GMAIL_APP_PASSWORD and RECIPIENT_EMAILS:
            # 은행별 상세 정보 수집
            bank_details = collect_bank_details()
            
            subject = f"📊 저축은행 데이터 스크래핑 {'완료' if not failed_banks else '부분완료'} (개선버전) - {TODAY}"
            
            body = f"""저축은행 중앙회 통일경영공시 데이터 스크래핑이 완료되었습니다.

🔧 이번 실행의 개선사항:
✅ 날짜 추출 정확도 향상 (안국, 오투 은행 등 문제 해결)
✅ ZIP 파일 생성 안정화 (.bin 오류 해결)  
✅ 스크린샷 형태 결과 테이블 추가

📊 실행 정보:
- 실행 날짜: {datetime.now().strftime('%Y년 %m월 %d일 %H시 %M분')}
- 총 실행 시간: {int(minutes)}분 {int(seconds)}초
- 처리 환경: GitHub Actions (개선 버전)

📈 결과 요약:
- 전체 은행 수: {stats.get('전체 은행 수', len(BANKS))}개
- 완료 은행 수: {stats.get('완료 은행 수', len(successful_banks))}개
- 부분 완료 은행 수: {stats.get('부분 완료 은행 수', 0)}개
- 실패 은행 수: {stats.get('실패 은행 수', len(failed_banks))}개
- 최신 데이터 은행 수: {stats.get('최신 데이터 은행 수', 0)}개
- 성공률: {stats.get('성공률', '0.00%')}

📦 첨부 파일:
- ZIP 압축파일 (모든 은행 데이터) - 메인 첨부파일
- 스크린샷 형태 결과 테이블 (Excel)
- 요약 보고서 (Excel)
- 실행 로그 파일
"""

            # 첨부 파일 준비 (ZIP 파일을 최우선으로)
            attachments = []
            if zip_file and os.path.exists(zip_file):
                attachments.append(zip_file)
                log_message(f"📎 ZIP 파일 첨부 준비: {zip_file}")
            if screenshot_file and os.path.exists(screenshot_file):
                attachments.append(screenshot_file)
                log_message(f"📎 스크린샷 형태 결과 첨부 준비: {screenshot_file}")
            if summary_file and os.path.exists(summary_file):
                attachments.append(summary_file)
            if os.path.exists(LOG_FILE):
                attachments.append(LOG_FILE)

            # 예상 날짜 정보 가져오기
            expected_dates = validate_data_freshness()

            # 이메일 발송
            is_success = len(failed_banks) == 0
            send_email_notification(subject, body, bank_details, attachments, is_success, expected_dates)

        log_message(f"\n🎊 ===== 저축은행 중앙회 통일경영공시 데이터 스크래핑 완료 (개선 버전) [{TODAY}] =====")

    except KeyboardInterrupt:
        log_message("\n⏹️ 사용자에 의해 중단되었습니다.")
    except Exception as e:
        log_message(f"💥 예상치 못한 오류 발생: {str(e)}")
        import traceback
        log_message(traceback.format_exc())
        
        # 오류 발생 시에도 이메일 알림 발송
        if GMAIL_ADDRESS and GMAIL_APP_PASSWORD and RECIPIENT_EMAILS:
            error_subject = f"❌ 저축은행 데이터 스크래핑 오류 발생 (개선버전) - {TODAY}"
            error_body = f"""저축은행 데이터 스크래핑 중 오류가 발생했습니다.

🔧 개선 버전 실행 중 오류:
- 발생 시간: {datetime.now().strftime('%Y년 %m월 %d일 %H시 %M분')}
- 오류 내용: {str(e)}

개선사항이 적용된 버전에서도 오류가 발생했습니다.
자세한 내용은 첨부된 로그 파일을 확인해주세요.
"""
            attachments = [LOG_FILE] if os.path.exists(LOG_FILE) else []
            send_email_notification(error_subject, error_body, None, attachments, False)


# 기존 main() 함수를 enhanced_main()으로 교체
if __name__ == "__main__":
    enhanced_main()
