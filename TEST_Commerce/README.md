---

# 🟦 Naver x Dunamu Commerce MVP

이 레포지토리는 **K-Won Sentinel** 프로젝트의 핵심인 **커머스 주문 관리 시스템**을 담당합니다. 사용자의 주문 요청을 처리하고, NSC(Naver Stable Coin) 결제 상태를 추적하며, 정산 정합성을 보장하는 중추적인 역할을 수행합니다.

## 🟦 시스템 아키텍처 (Architecture)

우리 프로젝트는 마이크로서비스 형태의 협업 구조를 가집니다.

* **Commerce (Java/Spring Boot):** 주문 마스터 및 상세 정보 관리, 재고 가예약 로직 담당.
* **Crypto (Python/FastAPI):** 지갑 잔액 동결(Freeze) 및 실제 차감(Settle) 처리.
* **Frontend (React/Vite):** 사용자 UI 및 두 서비스 간의 API 오케스트레이션.

---

## 🟦 핵심 기능 (Key Features)

1. **2단계 결제 프로세스 지원:**
* **READY:** 주문 생성 시 재고를 즉시 차감하고 주문 상태를 `PENDING`으로 설정합니다.
* **PAID:** 크립토 결제가 최종 확정되면 주문 상태를 `PAID`로 전환하여 배송 대기 상태로 만듭니다.


2. **데이터 정합성 보장:** 트랜잭션을 통해 주문 실패 시 차감된 재고가 자동으로 복구되도록 설계되었습니다.
3. **분석용 로그 발행:** 사빈님의 리스크 모니터링을 위해 모든 주문 전환 시점에 상세 로그를 남깁니다.

---

## 🟦 API 명세서 (API Specification)

팀원들이 바로 테스트해 볼 수 있는 주요 엔드포인트입니다.

| Method | Endpoint | Description | Status Code |
| --- | --- | --- | --- |
| `POST` | `/api/orders` | 새로운 주문 생성 및 재고 가예약 | `200 OK` |
| `POST` | `/api/orders/{id}/confirm` | 결제 완료 후 주문 상태를 `PAID`로 확정 | `200 OK` |
| `GET` | `/api/orders/{id}` | 특정 주문의 현재 상태 조회 | `200 OK` |

---

## 🟦 환경 구축 및 실행 (Setup Guide)

시스템 엔지니어의 로컬 환경에서도 바로 돌아갈 수 있게 표준화된 세팅을 권장합니다.

1. **Prerequisites:**
* Java 17 이상
* Maven (또는 IntelliJ 내장 Maven)


2. **Run Application:**
```bash
mvn clean install
mvn spring-boot:run

```


3. **Database:**
* 기본적으로 **H2 In-memory DB**를 사용합니다.
* 콘솔 접속: `http://localhost:8080/h2-console`
* JDBC URL: `jdbc:h2:mem:testdb` (ID: `sa`, PW: 없음)



---

## 💡 팀 협업 가이드 (Notice for Team)

* 결제 준비 API(`/api/pay/prepare`) 호출 시, 여기서 생성된 `orderId`를 `related_id`로 넘겨주시면 나중에 데이터 조인이 수월합니다.
* `orders` 테이블의 `updated_at` 컬럼을 관찰하시면 결제 지연 시간(Latency) 분석이 가능합니다.
* 도메인 엔티티 확장이 필요하면 언제든 말씀해 주세요. 제 구역(`yechan_commerce`) 폴더 안에서 유연하게 대응하겠습니다.

---
