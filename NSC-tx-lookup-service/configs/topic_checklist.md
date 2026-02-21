# Integration Topic Checklist

> **이벤트 계약**: 이 체크리스트는 업스트림 프로듀서 팀(CryptoSvc, AccountSvc, CommerceSvc)이
> 발행해야 하는 이벤트 계약이다. tx-lookup-service는 컨슈머로서 이 계약에 따라 이벤트를 수신한다.
> (DEC-112 참조)

## 계약 원칙

- **Upsert 전용**: 삭제(tombstone) 이벤트는 지원하지 않는다. 취소/환불은 별도 이벤트로 표현한다 (DEC-224).
- **발행 트리거**: INSERT와 UPDATE 모두. 레코드 상태가 변경될 때마다 최신 스냅샷을 발행한다.
- **멱등성**: Consumer는 at-least-once + 멱등 upsert. 중복 발행해도 결과 동일.
- **순서**: `updated_at` 또는 `version` 기준 latest-wins. 메시지 순서 보장 불필요(권장은 키 기준 파티셔닝).

## Required topics

1) Ledger entry upsert topic (default: `ledger.entry.upserted`)

Required fields
- `tx_id`
- `wallet_id`
- `entry_type` — 자유 텍스트. 단, 페어링에 사용되는 `PAYMENT`/`RECEIVE`는 반드시 이 값으로 태깅 (DEC-222)
- `amount`
- `related_id` (recommended — 페어링의 조인 키. 없으면 `pairing_status=UNKNOWN`)
- `event_time` or `source_created_at`
- `updated_at` or `version` (강력 권장 — 없으면 ingested_at 기준 fallback, 순서 역전 가능)

Optional fields
- `amount_signed` (없으면 NULL 저장, DEC-204)
- `related_type` (페어링은 `PAYMENT_ORDER`만 지원, DEC-222)
- `schema_version`
- `occurred_at`

Partition key recommendation: `tx_id`

Sample message
```json
{"tx_id":"tx-001","wallet_id":"wallet-001","entry_type":"PAYMENT","amount":"10000.00","amount_signed":"-10000.00","related_id":"po-001","related_type":"PAYMENT_ORDER","event_time":"2026-02-05T01:00:00Z","updated_at":"2026-02-05T01:00:01Z","version":1}
```

2) Payment order upsert topic (default: `payment.order.upserted`)

Required fields
- `order_id`
- `amount`
- `status` — 자유 텍스트. 매핑 안 되는 값은 `status_group=UNKNOWN`으로 그룹핑 (DEC-223)
- `created_at`
- `updated_at` or `version` (강력 권장)

Optional fields
- `user_id`
- `merchant_name`
- `schema_version`
- `occurred_at`

Partition key recommendation: `order_id`

Sample message
```json
{"order_id":"po-001","user_id":"user-001","merchant_name":"MERCHANT-001","amount":"10000.00","status":"SETTLED","created_at":"2026-02-05T01:00:00Z","updated_at":"2026-02-05T01:00:01Z","version":1}
```

취소/환불 처리 예시:
```json
{"order_id":"po-001","user_id":"user-001","merchant_name":"MERCHANT-001","amount":"10000.00","status":"CANCELLED","created_at":"2026-02-05T01:00:00Z","updated_at":"2026-02-05T02:00:00Z","version":2}
```
> 별도 삭제 이벤트 불필요. `status` 업데이트만 보내면 기존 upsert 경로에서 처리됨 (DEC-224).

## Optional topic

3) Payment ledger paired topic (if service emits)
Required fields
- `payment_order_id`
- `payment_tx_id`
- `receive_tx_id`
- `payer_wallet_id`
- `payee_wallet_id`
- `amount`
- `updated_at`

## 필드명 alias 지원

Consumer는 아래 alias를 자동 인식한다. 업스트림 기존 필드명이 다르더라도 아래 중 하나면 파싱 가능:

| 계약 필드명 | 허용 alias |
|-----------|-----------|
| `event_time` | `source_created_at`, `created_at` |
| `entry_type` | `type` |
| `version` | `source_version` |

## status_group 매핑표 (v1, DEC-206)

업스트림 `status` 원문이 아래 값에 해당하면 자동 그룹핑된다 (대소문자 무시):

| status_group | 매핑되는 status 값 |
|-------------|------------------|
| `SUCCESS` | `SETTLED`, `COMPLETED`, `SUCCESS`, `SUCCEEDED`, `PAID` |
| `FAIL` | `FAILED`, `CANCELLED`, `CANCELED`, `REJECTED`, `DECLINED` |
| `IN_PROGRESS` | `CREATED`, `PENDING`, `PROCESSING`, `AUTHORIZED` |
| `UNKNOWN` | 그 외 또는 NULL |

> 매핑에 없는 값도 Consumer 처리에 문제없음. `status` 원문은 항상 보존됨.
> 새 값을 추가하려면 `src/api/service.py`의 `_resolve_status_group()`만 수정.

