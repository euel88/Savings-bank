# 저축은행 중앙회 데이터 자동 스크래퍼

## 📋 프로젝트 소개
저축은행 중앙회 통일경영공시 데이터를 자동으로 수집하고 이메일로 전송하는 시스템입니다.

### 주요 기능
- 79개 저축은행의 재무 데이터 자동 수집
- 매년 5월, 8월, 11월 마지막 평일에 자동 실행
- 수집된 데이터를 Excel 파일로 저장
- 전체 데이터를 ZIP으로 압축하여 Gmail로 자동 전송
- GitHub Actions를 통한 완전 자동화

## 🚀 빠른 시작

### 1. 리포지토리 Fork 또는 Clone
```bash
git clone https://github.com/your-username/bank-scraper.git
```

### 2. Gmail 앱 비밀번호 생성
1. [Google 계정 설정](https://myaccount.google.com) 접속
2. 보안 → 2단계 인증 활성화
3. 앱 비밀번호 생성
4. 16자리 비밀번호 복사

### 3. GitHub Secrets 설정
리포지토리 Settings → Secrets and variables → Actions에서:
- `GMAIL_ADDRESS`: 발신 Gmail 주소
- `GMAIL_APP_PASSWORD`: 앱 비밀번호
- `RECIPIENT_EMAILS`: 수신자 이메일 (쉼표로 구분)

### 4. 테스트 실행
Actions 탭 → "저축은행 데이터 스크래핑" → Run workflow

## 📅 자동 실행 일정
- 매년 5월 마지막 평일
- 매년 8월 마지막 평일  
- 매년 11월 마지막 평일

## 📁 프로젝트 구조
```
bank-scraper/
├── .github/
│   └── workflows/
│       └── bank-scraper.yml    # GitHub Actions 워크플로우
├── bank_scraper_headless.py    # 메인 스크래퍼 코드
├── requirements.txt             # Python 패키지 목록
└── README.md                    # 이 파일
```

## 🔧 환경 변수 설정
| 변수명 | 설명 | 기본값 |
|--------|------|--------|
| MAX_WORKERS | 병렬 처리 워커 수 | 2 |
| MAX_RETRIES | 재시도 횟수 | 3 |
| PAGE_LOAD_TIMEOUT | 페이지 로딩 타임아웃(초) | 30 |

## 📧 이메일 형식
스크래핑이 완료되면 다음과 같은 형식의 이메일이 발송됩니다:
- 제목: [저축은행 데이터] YYYYMMDD 스크래핑 결과 - 성공률 XX.X%
- 내용: HTML 형식의 요약 보고서
- 첨부: 전체 데이터가 포함된 ZIP 파일

## 🐛 문제 해결

### Chrome 드라이버 오류
```
Error: Chrome driver not found
```
해결: GitHub Actions는 자동으로 Chrome을 설치합니다. 로컬에서는 Chrome 브라우저가 설치되어 있어야 합니다.

### 이메일 전송 실패
```
Error: Authentication failed
```
해결: Gmail 앱 비밀번호와 2단계 인증을 확인하세요.

### 타임아웃 오류
```
TimeoutException
```
해결: `PAGE_LOAD_TIMEOUT` 환경 변수를 60으로 늘려보세요.

## 📝 라이선스
이 프로젝트는 MIT 라이선스를 따릅니다.

## 🤝 기여
버그 리포트나 기능 제안은 Issues를 통해 제출해주세요.

## 📞 연락처
문의사항이 있으시면 Issues를 통해 연락주세요.
