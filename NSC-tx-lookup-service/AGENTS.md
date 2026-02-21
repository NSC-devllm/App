Backoffice Serving Layer - LLM Coding Agent Instructions
========================================================

Instructions for LLM coding agents working with the tx-lookup-service codebase.


Project overview
----------------

Backoffice Serving Layer provides **admin transaction lookup (FR-ADM-02)** with
near real-time `tx_id` based queries.

**Components:**
 - **Admin API**: REST API for admin queries (FastAPI)
 - **Sync Consumer**: OLTP → Backoffice DB synchronization
 - **Backoffice DB**: Read-optimized serving store (PostgreSQL)

**Core principles:**
 - Backoffice DB is a **read-only derived store**
 - Write transactions only in OLTP; Backoffice receives via Sync Consumer
 - All upserts must be **idempotent**
 - This repository is **consumer-only** for event ingestion (producer ownership is upstream services)

See `.specs/backoffice_project_specs.md` for full project specs and decision locks.


Development environment
-----------------------

 - **API**: Python + FastAPI
 - **Consumer**: Python (custom service)
 - **DB**: PostgreSQL (local Docker / Azure)
 - **Events**: Kafka (or Azure Event Hubs)
 - **Testing**: pytest + Docker Compose


Local integration (Docker Compose)
----------------------------------

Use the repository `docker-compose.yml` for local DB + Kafka integration.

Start:
`docker compose up -d`

Stop (and remove volumes):
`docker compose down -v`

Local endpoints:
`postgresql://bo:bo@localhost:5432/bo`
`localhost:9092` (Kafka)


Repository structure
--------------------

```
├── src/
│   ├── api/           # Admin API (FastAPI)
│   ├── consumer/      # Sync Consumer
│   ├── db/            # Models & repositories
│   └── common/        # Shared utilities
├── tests/
│   ├── unit/
│   ├── integration/
│   └── e2e/
├── migrations/        # Alembic migrations
├── docker/
├── scripts/
├── configs/
└── .specs/            # Specifications (SSOT)
```


Key domain concepts
-------------------

 - `tx_id`: Ledger entry ID (PK)
 - `related_id`: Pairing key (= `payment_orders.order_id`)
 - Double-entry: PAYMENT + RECEIVE have different `tx_id`, same `related_id`

See `.specs/backoffice_db_admin_api.md` for DB schema and API design.


Code patterns
-------------

 - **Idempotency**: Upsert by key (`tx_id`, `order_id`), latest-wins by `updated_at`
 - **Pairing**: Allow partial pairs; track `pair_incomplete` status
 - **Admin API responses**:
   - `GET /admin/tx/{tx_id}`: 404 if not found; 200 with `pairing_status` for incomplete/unknown pairing
   - `GET /admin/payment-orders/{order_id}`: 404 if not found
   - `GET /admin/wallets/{wallet_id}/tx`: 200 with empty list allowed

See `.specs/backoffice_data_project.md` for sync and pairing details.


Testing strategy
----------------

**Test levels:**
 - **Unit**: pytest + mock (single function/class logic)
 - **Integration**: pytest + Docker DB (Repository + DB interaction)
 - **E2E**: Docker Compose (API → Consumer → DB full flow)

**Test case types:**
 - **Happy path**: Normal input → expected output
 - **Edge case**: Boundary values, empty data, NULL handling
 - **Error case**: Invalid input, auth failure → appropriate error response
 - **Idempotency**: Re-run same request → same result

**Test scope by component:**
 - `src/api/`: FastAPI TestClient for endpoint tests
 - `src/consumer/`: Message processing logic + pairing logic
 - `src/db/`: Repository CRUD + upsert idempotency

**Quality gates:**
 - L1: Unit tests pass
 - L2: Coverage gate pass (≥ 80%) and run integration tests for touched DB/API paths
 - L3: E2E smoke test pass (required for pairing logic or response schema changes)

**Test file naming:**
 - Unit: `tests/unit/test_<module>.py`
 - Integration: `tests/integration/test_<component>_integration.py`
 - E2E: `tests/e2e/test_<scenario>_e2e.py`


Verification policy
-------------------

### Verification contract

 - Verification is mandatory. Do not claim completion without running an
   appropriate verification level.
 - Python/pytest verification commands must run with the project virtualenv
   interpreter (`.venv/bin/python`). Do not use system `python`/`pytest` for
   gate evidence.
 - Store evidence under `.agents/logs/verification/`.
 - Do not encode verification level in commit messages.

### Verification ladder

| Level | When | Command |
|-------|------|---------|
| L0 | Per edit | `.venv/bin/python -m py_compile $(find src -name '*.py')` |
| L1 | Pre-commit | `.venv/bin/python -m pytest tests/unit/ -x` |
| L2 | Pre-PR | `.venv/bin/python -m pytest --cov-fail-under=80` |
| L3 | Pre-merge | Docker Compose E2E |

**Escalation:** L3 required for pairing logic or response schema changes.


Security & observability
------------------------

 - Auth: OIDC/JWT, RBAC (`ADMIN_READ`, `ADMIN_AUDIT`)
 - Audit logs required for all queries
 - SLO: API p95 < 200ms, data freshness p95 < 5s

See `.specs/backoffice_project_specs.md` sections 7-8.


Key references
--------------

**Consult `.specs/` as SSOT for all design decisions.**

| Document | Content |
|----------|---------|
| `.specs/requirements/SRS - Software Requirements Specification.md` | Requirements (FR-ADM-02) |
| `.specs/backoffice_project_specs.md` | Project specs, Decision Lock |
| `.specs/backoffice_db_admin_api.md` | DB + API design |
| `.specs/backoffice_data_project.md` | Sync specs |
| `.specs/reference/entire_architecture.md` | Architecture reference (non-SSOT) |

Decision hygiene
----------------

- If any part of the implementation is unclear or handled as an assumption,
  add it to `.specs/decision_open_items.md` immediately and update its status
  once a decision is made.
