# Frontend - Naver Finance Payment Demo

이 디렉토리는 **React + Vite**로 구축된 프론트엔드 애플리케이션입니다.  
사용자 인터페이스를 담당하며, FastAPI 백엔드와 통신하여 결제 시나리오를 시각화합니다.

## 🚀 실행 방법 (Quick Start)

```bash
# 의존성 설치
npm install

# 개발 서버 실행
npm run dev
```

브라우저에서 [http://localhost:5173](http://localhost:5173)으로 접속하세요.

## 🧩 주요 컴포넌트 (Components)

- **`WalletView.jsx`**: 사용자의 지갑 잔액과 최근 거래 내역을 보여줍니다.
- **`PaymentDemo.jsx`**: 결제 프로세스(준비 -> 확정/취소)를 시뮬레이션하는 핵심 컴포넌트입니다.
- **`AdminDashboard.jsx`**: 시스템 전체의 거래 원장과 주문 상태를 모니터링합니다.

## 📡 백엔드 연동

이 프론트엔드는 기본적으로 `http://127.0.0.1:8000`에서 실행되는 백엔드 API를 호출하도록 설정되어 있습니다.  
`vite.config.js`의 proxy 설정을 확인하세요.
