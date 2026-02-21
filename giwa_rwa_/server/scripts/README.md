# Server Smoke E2E

Run a minimal API smoke flow for the custodial server:

1. `POST /auth/signup` (seller, buyer)
2. `POST /admin/compliance/approve` (seller, buyer)
3. `POST /admin/complexes/import`
4. `POST /admin/classes/{classId}/tokenize`
5. `POST /admin/faucet/musd`
6. `POST /admin/distribute/shares`
7. `POST /trade/list`
8. `POST /trade/buy`

## Prerequisites
- server is running (`http://localhost:8080` by default)
- `ADMIN_API_TOKEN` configured on server
- `curl`, `jq` installed

## Usage

```bash
cd /Users/kanghyoseung/Desktop/project/rwa/server
BASE_URL="http://localhost:8080" \
ADMIN_TOKEN="<admin-token>" \
BUY_LISTING_ID="1" \
./scripts/smoke_e2e.sh
```

## Notes
- 모든 mutating API에 `Idempotency-Key`를 자동으로 부여합니다.
- 체인 tx는 `/tx/outbox/{outboxId}`를 폴링해 `MINED`까지 대기합니다.
- `BUY_LISTING_ID`는 환경 상태에 맞게 조정해야 합니다.
