# CODEX SPEC 2 - MERGED v4.1 (Baseline: 2026-02-15 post-live deploy)

Title: Spring Boot Custodial Ops Server (Off-chain compliance) + TxOrchestrator + CDC-friendly Transactional Outbox
+ API Idempotency + Distributed Nonce Lock + On-chain listing fallback

## 0) Baseline alignment (MUST)

### A) Compliance model
- On-chain KYC allowlist enforcement is REMOVED.
- Do NOT reference KYCRegistry anywhere.
- Backend must enforce compliance OFF-CHAIN before allowing user actions.
- Contracts do not block transfers/listing/buying via on-chain allowlist.

### B) Deployed contracts set (EXACTLY 5)
1. MockUSD
2. PropertyRegistry
3. PropertyShare1155
4. PropertyTokenizer
5. FixedPriceMarketDvP

### C) Ownership state (post-bootstrap, confirmed)
- MockUSD.owner = ISSUER_ADDRESS
- PropertyTokenizer.owner = ISSUER_ADDRESS
- PropertyRegistry.owner = PropertyTokenizer (contract)
- PropertyShare1155.owner = PropertyTokenizer (contract)
- Tokenization MUST be executed through PropertyTokenizer by ISSUER key.

### D) Runtime source of truth (MUST)
- Contract addresses: `shared/deployments/giwa-sepolia.json`
- Contract ABIs: `shared/abi/*.json` (no legacy KYC ABI)
- Constants: `shared/config/constants.json`
  - `SHARE_SCALE = 1e18`
  - `CONFIRMATIONS = 12`
- Do NOT hardcode deployed addresses in code.

### E) Network config
- `chainId=91342`, `RPC=https://sepolia-rpc.giwa.io` (GIWA Sepolia)
- Nonce source: `eth_getTransactionCount(from, "pending")`

## 1) Architecture / Responsibilities

### Server responsibilities
- Custodial wallet issuance and secure key storage
- Off-chain compliance approval/revocation + enforcement gates
- Public data import (complex/class/unit creation) including UNKNOWN class (Option A)
- Tokenization orchestration via PropertyTokenizer (reserve+register+mint chunking)
- Faucet mUSD and share distribution
- Trading orchestration (list/buy) as user-signed txs (custodial)
- Tx Orchestrator (nonce safety, outbox_tx, receipts, retries)
- Read-only query APIs powered by Postgres read-models updated by Flink (listings/trades/balances)
- Transactional Outbox Pattern (CDC-friendly) for domain events -> Kafka (optional downstream consumers)
- API Idempotency for ALL mutating endpoints
- Distributed per-address nonce lock for multi-instance safety

### Indexing responsibilities (NOT in this server)
- On-chain events -> Kafka -> Flink -> DB read models handled by Ingester/Flink modules.

## 2) Tech stack (recommended)
- Java 21 + Spring Boot 3.x (project runtime currently Java 17)
- Spring Web, Validation
- Spring Data JPA (Hibernate)
- Postgres 15+
- Web3j
- Spring Kafka
- Flyway
- AES-GCM
- @Async + ThreadPoolTaskExecutor
- @Scheduled for retry jobs

## 3) Database schema (minimum; Flyway)

### A) Custodial + compliance
- `users(user_id uuid PK default uuidv7(), created_at timestamptz, compliance_status text, compliance_updated_at timestamptz)`
- `wallets(user_id uuid PK FK users, address varchar(42) unique, encrypted_privkey bytea, enc_version int, created_at timestamptz)`

### B) Public data store
- `complexes(kapt_code PK, kapt_name, kapt_addr, doro_juso, ho_cnt, raw_json jsonb, fetched_at timestamptz)`
- `classes(class_id PK hex bytes32, kapt_code FK, class_key, unit_count, status, doc_hash, base_token_id numeric(78,0), issued_at, created_at, updated_at)`
- `units(unit_id PK, class_id FK, unit_no int, token_id numeric(78,0), status, UNIQUE(class_id, unit_no))`

### C) Tx Orchestrator outbox
- `outbox_tx(outbox_id uuid PK, request_id text UNIQUE NOT NULL, from_address varchar(42) NOT NULL, to_address varchar(42), nonce bigint, tx_hash varchar(66), raw_tx text, status text NOT NULL, tx_type text NOT NULL, payload jsonb, last_error text, created_at timestamptz default now(), updated_at timestamptz default now())`

### D) Read model (server reads only)
- `processed_events(...)`
- `listings(...)`
- `trades(...)`
- `balances(...)`

### E) API Idempotency table
- `api_idempotency(endpoint text NOT NULL, idempotency_key text NOT NULL, request_hash varchar(64) NOT NULL, status text NOT NULL, response_status int, response_body jsonb, created_at timestamptz not null default now(), updated_at timestamptz not null default now(), primary key(endpoint, idempotency_key))`
- Index `(status, created_at)`

### Idempotency rules
- All mutating endpoints require `Idempotency-Key`.
- Missing key -> `400 "Idempotency-Key required"`.
- Same key + different payload hash -> `409 "Idempotency-Key reuse with different payload"`.
- `COMPLETED` -> return stored status/body.
- `IN_PROGRESS` -> return `202` (status endpoint pointer).
- Success -> update `COMPLETED`.
- Failure -> update `FAILED` with minimal error payload.

## 4) CDC-friendly Transactional Outbox

### Tables
- `outbox_event` (INSERT-ONLY)
- `outbox_delivery` (mutable operational state)

### DomainEvent required fields
- `eventId`, `aggregateType`, `aggregateId`, `eventType`, `occurredAt`, `topic`, `partitionKey`, `payload`

### Publishing flow
1. `@Transactional` domain service publishes `DomainEvent`.
2. `@TransactionalEventListener(BEFORE_COMMIT)` persists `outbox_event + outbox_delivery(INIT)`.
3. `@TransactionalEventListener(AFTER_COMMIT)` + `@Async` publishes to Kafka.
   - success: `SEND_SUCCESS`
   - fail: `SEND_FAIL`, increment attempts, retry metadata update

### Retry scheduler rules
- CLAIM -> SEND -> COMPLETE pattern
- Must NOT hold DB locks during Kafka send
- reclaim expired locks via TTL
- config keys:
  - `OUTBOX_RETRY_MIN_AGE_MINUTES=10`
  - `OUTBOX_MAX_ATTEMPTS=30`
  - `OUTBOX_RETRY_INTERVAL_MS=60000`
  - `OUTBOX_LOCK_TTL_SECONDS=300`
  - `OUTBOX_CLAIM_BATCH_SIZE=100`

### Graceful shutdown
- outbox publisher executor:
  - `setWaitForTasksToCompleteOnShutdown(true)`
  - `setAwaitTerminationSeconds(10)`

## 5) Distributed per-address nonce lock (MUST)
- Multi-instance safe lock required.
- Postgres advisory lock:
  - acquire: `SELECT pg_advisory_lock(hashtext(lower(:from_address)));`
  - release: `SELECT pg_advisory_unlock(hashtext(lower(:from_address)));`
- Lock coverage:
  1. pending nonce read
  2. signing
  3. send to RPC
  4. outbox_tx persist/update
- Same DB connection for lock/unlock.
- Timeout (`NONCE_LOCK_TIMEOUT_MS`, default 5000ms) -> `409/429 "Address is busy"`.

## 6) Shared runtime config / secrets
- `GIWA_RPC_URL=https://sepolia-rpc.giwa.io`
- `GIWA_CHAIN_ID=91342`
- `SHARED_DIR_PATH=../shared`
- `MASTER_KEY_BASE64=...`
- `ISSUER_PRIVATE_KEY=...`
- `TREASURY_PRIVATE_KEY=...`
- `KAFKA_BOOTSTRAP_SERVERS=...`
- `OUTBOX_DEFAULT_TOPIC=server.domain.events`
- `NONCE_LOCK_TIMEOUT_MS=5000`
- `Idempotency-Key` required on all mutating endpoints

## 7) Core REST APIs (minimum)
- `POST /auth/signup`
- `POST /admin/compliance/approve`
- `POST /admin/compliance/revoke`
- `GET /admin/compliance/users`
- `POST /admin/complexes/import`
- `GET /complexes`
- `GET /complexes/{kaptCode}`
- `GET /complexes/{kaptCode}/classes`
- `GET /classes/{classId}/units`
- `POST /admin/classes/{classId}/tokenize`
- `POST /admin/faucet/musd`
- `POST /admin/distribute/shares`
- `POST /trade/list`
- `POST /trade/buy`
- `GET /tx/outbox/{outboxId}`
- `GET /tx/by-hash/{txHash}`
- `GET /market/listings`
- `GET /users/{userId}/holdings`
- `GET /tokens/{tokenId}/holders`

## 8) Tx Orchestrator (MUST)
- Must apply distributed address lock.
- Must use pending nonce.
- `outbox_tx` lifecycle: `CREATED -> SIGNED -> SENT -> MINED | FAILED | REPLACED`
- Chain final business state is from Flink read-model events, not receipt-only.

## 9) Acceptance criteria
- Idempotency correctness (same key same payload = same response; no duplicates)
- Same key different payload => 409
- Two instances same sender do not collide nonce
- Retry scheduler does not hold DB lock while sending Kafka
- Listing lag scenario still allows `/trade/buy` via on-chain listing fallback

## 10) JUnit test coverage (implemented: 2026-02-17)
- Test execution command: `cd server && ./gradlew test`
- Latest result: `18 tests, 0 failures, 0 errors`

### Unit tests
- `server/src/test/java/io/rwa/server/wallet/WalletServiceTest.java`
  - `signup()` persists `users/wallets`, encrypts private key, publishes `UserSignedUp` outbox event payload.
  - `assertApproved()` rejects non-approved users with `403`.
  - `getWallet()` returns `404` when wallet row is missing.
- `server/src/test/java/io/rwa/server/trade/TradeServiceTest.java`
  - `list()` submits approval tx first when `isApprovedForAll=false`, then submits list tx.
  - `list()` rejects missing `tokenId` and `unitId` with `400`.
  - `buy()` rejects non-ACTIVE listing with `409`.
  - `buy()` submits approve+buy tx sequence when allowance is insufficient and validates cost calculation.

### Web integration tests (MockMvc)
- `server/src/test/java/io/rwa/server/auth/AuthIdempotencyIntegrationTest.java`
  - `/auth/signup` without `Idempotency-Key` returns `400`.
  - First request with key stores completed idempotency response.
  - Repeated request with same key returns stored response (replay) and skips `walletService.signup()`.
- `server/src/test/java/io/rwa/server/auth/AuthSignupDbDefaultIntegrationTest.java`
  - Verifies `/auth/signup` persists `users/wallets` with DB-generated `users.user_id` (uuid v7).
  - Verifies returned `userId` is UUID v7 and persisted user compliance defaults are `PENDING`.
  - Runs only when `-DrunPg18Integration=true` (PG18 integration gate).
- `server/src/test/java/io/rwa/server/compliance/ComplianceAdminTokenIntegrationTest.java`
  - `/admin/compliance/users` without `X-Admin-Token` returns `401`.
  - Same endpoint with valid token returns `200` and mapped user compliance payload.

### JPA/Hibernate integration tests (`@DataJpaTest` + `@Transactional`)
- Test DB mode: H2 in-memory with DDL auto create/drop and JSONB domain alias for entity compatibility.
- Rollback policy: each test method runs in transaction and rolls back automatically on completion.
- `server/src/test/java/io/rwa/server/wallet/UserWalletRepositoryJpaIntegrationTest.java`
  - `UserRepository.findByComplianceStatus` 조회 정확성 검증.
  - `WalletRepository.findByAddress` 저장/조회 매핑 검증.
  - `wallets.address` unique 제약 위반 시 예외 발생 검증.
- `server/src/test/java/io/rwa/server/publicdata/PublicDataRepositoryJpaIntegrationTest.java`
  - `UnitRepository.findByClassIdOrderByUnitNoAsc` 정렬 조회 검증.
  - `UnitRepository.findByClassIdAndUnitNo` 단건 조회 검증.
  - `PropertyClassRepository.findByKaptCodeOrderByClassKeyAsc` 정렬 조회 검증.

## Implementation Notes (project decisions)
- Admin auth mode: `X-Admin-Token` header
- Deterministic IDs:
  - classId: `keccak256(lower(kaptCode)+":"+classKey)`
  - unitId: `{kaptCode}:{classKey}:{pad5(unitNo)}`
- User ID generation:
  - `users.user_id` is generated by DB default `uuidv7()`.
  - JPA mapping uses `@GeneratedValue(strategy = GenerationType.IDENTITY)` and consumes DB-generated UUID.
- Kafka mode: feature flag (`outbox.kafka.enabled`)
- Java baseline: 17 (repository/runtime aligned)

## Update Policy
- `server/codex_spec.md` is the server module's single source of truth for this request.
- Any server-side behavior change, schema change, endpoint contract change, or runtime policy change MUST update this file in the same change set.
- Change log entries must include date, scope, and impacted files/components.

## Change Log
- 2026-02-16: Initialized v4.1 server baseline spec in `server/codex_spec.md`.
  - Added implementation decisions fixed during planning (admin auth/header, deterministic IDs, Kafka feature flag, Java 17 baseline).
  - Marked outbox/nonce/idempotency/trading fallback requirements as hard constraints for server implementation.
- 2026-02-16: Implemented Spring Boot server baseline (MVP skeleton + core flows).
  - Added Flyway schema for write-model/outbox/idempotency.
  - Added shared deployment/ABI/constants runtime loaders (`shared/deployments/giwa-sepolia.json`, `shared/abi/*.json`, `shared/config/constants.json`).
  - Implemented global idempotency interceptor + status endpoint + cached response replay.
  - Implemented transactional outbox listeners (BEFORE_COMMIT persist, AFTER_COMMIT async publish) and retry scheduler (claim/send/complete).
  - Implemented Postgres advisory-lock nonce manager and `outbox_tx` orchestrator with receipt polling.
  - Implemented custodial wallet signup/encryption, compliance APIs, public-data import/query APIs, tokenize API, faucet/distribute APIs, trade list/buy APIs with on-chain listing fallback.
- 2026-02-16: Added execution tooling and smoke scenario.
  - Added Gradle Wrapper under `server/gradle/wrapper` and `server/gradlew*`.
  - Added smoke script `server/scripts/smoke_e2e.sh` and usage guide `server/scripts/README.md`.
  - Verified server module build/tests pass with wrapper (`./gradlew test`).
- 2026-02-17: Added `.env`-based server configuration workflow.
  - Enabled optional `.env` loading via `spring.config.import` in `server/src/main/resources/application.yml`.
  - Added template file `server/.env.example` for local runtime variable management.
  - Updated root `.gitignore` to explicitly ignore `server/.env` while allowing `server/.env.example`.
- 2026-02-17: Updated environment and local infra defaults for current setup.
  - Updated `server/.env` DB settings to Azure PostgreSQL endpoint (`giwapsqlserver.postgres.database.azure.com`) using provided administrator login credentials.
  - Updated local infra Postgres image to `postgres:17.7` in `infra/docker-compose.yml`.
- 2026-02-17: Replaced placeholder tests with JUnit unit/web integration coverage for live server behaviors.
  - Added `WalletServiceTest` and `TradeServiceTest` for core service logic and error branches.
  - Added `AuthIdempotencyIntegrationTest` and `ComplianceAdminTokenIntegrationTest` for interceptor/filter/controller integration via MockMvc.
  - Removed placeholder tests (`TradeBuyFallbackTest`, `IdempotencyIntegrationTest`, `OutboxRetrySchedulerTest`, `NonceLockConcurrencyTest`) and validated `./gradlew test` pass (`12/12`).
- 2026-02-17: Added JPA/Hibernate repository integration tests with transactional rollback.
  - Added H2 test runtime dependency in `server/build.gradle`.
  - Added `UserWalletRepositoryJpaIntegrationTest` and `PublicDataRepositoryJpaIntegrationTest` using `@DataJpaTest` + `@Transactional`.
  - Configured test datasource for JSONB-compatible entity DDL (`INIT=CREATE DOMAIN IF NOT EXISTS JSONB AS JSON`) and verified full test suite pass (`18/18`).
- 2026-02-17: Added migration SQL column/table usage comments for onboarding readability.
  - Updated `server/src/main/resources/db/migration/V1__core_write_model.sql`, `V2__tx_orchestrator.sql`, `V3__outbox_domain.sql`, `V4__api_idempotency.sql`.
  - Added table purpose descriptions and per-column meaning + concrete example value comments.
- 2026-02-17: Switched user ID generation to PostgreSQL 18 native UUID v7 defaults.
  - Updated schema to `users.user_id UUID PRIMARY KEY DEFAULT uuidv7()`.
  - Updated JPA mapping to `@GeneratedValue(strategy = GenerationType.IDENTITY)` on `UserEntity.userId`.
  - Updated signup flow to persist `UserEntity` first and consume DB-generated `user_id`.
  - Added `V6__users_uuidv7_default.sql` migration to enforce DB default on existing tables.
  - Added `AuthSignupDbDefaultIntegrationTest` (opt-in via `-DrunPg18Integration=true`) to verify `/auth/signup` DB-default UUID behavior.
