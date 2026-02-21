# Performance And Partitioning Checklist

Last updated: 2026-02-06

## 1. Partition Baseline

| Table | Partition |
|---|---|
| `silver.wallet_snapshot` | `snapshot_date_kst` |
| `silver.ledger_entries` | `event_date_kst` |
| `silver.order_events` | `event_date_kst` |
| `gold.recon_daily_snapshot_flow` | `date_kst` |
| `gold.ledger_supply_balance_daily` | `date_kst` |
| `gold.fact_payment_anonymized` | `date_kst` (overwrite partition) |
| `gold.exception_ledger` | `date_kst` |

## 2. Preflight Checks

1. 대상 `date_kst` 파티션 row count 확인
2. 최근 7일 파티션 편중 여부 확인
3. 조인 키 null 비율 확인 (`user_id`, `order_ref`, `related_id`)

## 3. Runtime Checks

1. 잡 실행 시간(시작/종료) 기록
2. 입력 대비 출력 row count 급감/급증 여부
3. `gold.exception_ledger` CRITICAL 급증 여부

## 4. Optimization Candidates

1. 반복 조회가 많은 Gold 테이블:
- `OPTIMIZE ... ZORDER BY (date_kst, user_id)`

2. 작은 차원/룰 테이블:
- 캐시 고려 (`gold.dim_rule_scd2`)

3. Backfill 대량 구간:
- 날짜 구간 분할 실행

## 5. Evidence Template

기록 파일 예시:
- `.agents/logs/verification/20260206_phase10_L2.txt`
- `.agents/logs/verification/e2e_smoke_*.txt`
