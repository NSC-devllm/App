# 🟦 Naver X Dunamu Crypto MvP
이 프로젝트는 **Naver Stable Coin (NSC)**을 이용한 차세대 결제 시스템의 프로토타입 데모입니다.  
사용자 잔액을 **동결(Freeze)**하고 결제가 확정되면 **정산(Settle)**하는 2단계 결제 로직을 구현하였습니다.

## 🟦 시스템 아키텍처 (Architecture)

이 프로젝트는 **FastAPI (Backend)**와 **React + Vite (Frontend)**로 구성된 Full-Stack 애플리케이션입니다.

###  Backend (Python/FastAPI)
- **Framework**: FastAPI (비동기 지원)
- **Database**: SQLite (SQLAlchemy 비동기 세션 사용 2.0+)
- **핵심 로직**:
  - `UserWallet`: 잔액 및 동결 금액 관리
  - `PaymentOrder`: 결제 주문 상태 관리 (READY -> COMPLETED/CANCELED)
  - `TransactionLedger`: 모든 거래 내역 기록 (불변 원장)

###  Frontend (React/Vite)
- **Framework**: React 19
- **Build Tool**: Vite
- **Styling**: Vanilla CSS (네이버 파이낸셜 스타일)
- **Routing**: React Router v7

---

## 🟦 핵심 기능 (Key Features)

1.  **내 지갑 (Wallet)**
    - 현재 잔액 및 동결 금액 확인
    - 최근 거래 내역 조회
2.  **결제 시뮬레이션 (Payment Demo)**
    - **NSC 충전**: 가상 계좌에서 코인 충전
    - **결제 준비 (Prepare)**: 결제 요청 시 잔액을 즉시 차감하지 않고 **동결(Freeze)** 처리
    - **결제 확정 (Confirm)**: 동결된 금액을 차감하고 거래 완료 처리
    - **결제 취소 (Cancel)**: 동결을 해제하고 잔액으로 환불
3.  **관리자 (Admin)**
    - 전체 원장(Ledger) 조회
    - 전체 주문(Order) 상태 조회

---

## 🟦 환경 구축 및 실행 (Setup Guide)

터미널 2개를 열어서 각각 실행해야 합니다.

### 1. Backend 실행
```bash
cd backend
# 가상환경 생성 (선택사항)
python -m venv .venv
# 가상환경 활성화 (Windows)
.venv\Scripts\activate

# 라이브러리 설치
pip install -r requirements.txt

# 서버 실행 (http://127.0.0.1:8000)
uvicorn main:app --reload
```

### 2. Frontend 실행
```bash
cd frontend
# 의존성 설치
npm install

# 개발 서버 실행 (http://localhost:5173)
npm run dev
```

---

## 🟦 파일 디렉토리 구조 (File Directory Structure)

```
CryptoSection/
├── backend/
│   ├── main.py          # API 엔드포인트 핵심 로직
│   ├── database.py      # DB 연결 설정
│   ├── models.py        # SQLAlchemy DB 모델
│   ├── requirements.txt # 파이썬 의존성 패키지
│   └── naver_finance.db # SQLite 데이터베이스 파일
│
└── frontend/
    ├── public/          # 정적 파일 보관 (빌드 시 루트로 그대로 복사됨, 예: favicon)
    ├── node_modules/    # 설치된 라이브러리 저장소 (Git 제외, 자동 생성됨)
    ├── src/
    │   ├── components/  # React 컴포넌트 (WalletView, PaymentDemo, AdminDashboard)
    │   ├── App.jsx      # 메인 라우팅 설정
    │   └── index.css    # 전역 스타일
    └── package.json     # Node.js 패키지 설정
```
