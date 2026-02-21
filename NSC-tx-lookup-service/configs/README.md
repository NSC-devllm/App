# Environment configuration

Rules
- Use `APP_ENV` to select `local`, `dev`, or `prod`.
- `local` defaults are safe for Docker Compose (no secrets committed).
- `dev`/`prod` must set all connection values via environment variables or secret stores.
- Follow 12-factor practices: never commit credentials.

Required variables (current skeleton)
- `APP_ENV`: `local` | `dev` | `prod`
- `LOG_LEVEL`: `INFO` by default
- `DATABASE_URL`: Backoffice DB connection string
- `KAFKA_BROKERS`: Kafka bootstrap servers
- `KAFKA_SECURITY_PROTOCOL`: `PLAINTEXT` | `SSL` | `SASL_PLAINTEXT` | `SASL_SSL`
- `KAFKA_SASL_MECHANISM`: SASL mechanism (default `PLAIN`)
- `KAFKA_SASL_USERNAME`: SASL username (Event Hubs uses `$ConnectionString`)
- `KAFKA_SASL_PASSWORD`: SASL password (Event Hubs uses namespace connection string)
- `KAFKA_SSL_CA_LOCATION`: optional CA file path for SSL validation
- `SERVICE_NAME`: service identifier for logs/metrics
- `KAFKA_GROUP_ID`: consumer group id
- `LEDGER_TOPIC`: ledger entry topic
- `PAYMENT_ORDER_TOPIC`: payment order topic
- `DLQ_BACKEND`: `file` or `db` (default: `file` for local/dev, `db` for prod)
- `DLQ_PATH`: DLQ file path (when `DLQ_BACKEND=file`)
- `DLQ_RETENTION_DAYS`: DLQ retention window in days (default `14`, when `DLQ_BACKEND=db`)
- `CONSUMER_POLL_TIMEOUT_MS`: Kafka poll timeout in milliseconds
- `CONSUMER_OFFSET_RESET`: Kafka offset reset policy (`earliest`/`latest`)
- `AUTH_MODE`: `disabled` or `oidc`
- `AUTH_ISSUER`: OIDC issuer URL
- `AUTH_AUDIENCE`: expected audience
- `AUTH_JWKS_URL`: JWKS endpoint
- `AUTH_ALGORITHM`: JWT algorithm (default `RS256`)
- `AUTH_ROLES_CLAIM`: roles claim name (default `roles,scp`, comma-separated allowed)
- `AUTH_ACTOR_ID_CLAIMS`: actor id claim priority list (default `oid,sub`)
- `AUDIT_MASK_QUERY_KEYS`: query keys to mask in audit logs
- `DB_POOL_SIZE`: SQLAlchemy pool size (default `5`)
- `DB_MAX_OVERFLOW`: extra overflow connections (default `10`)
- `DB_POOL_TIMEOUT`: seconds to wait for a connection (default `30`)
- `DB_POOL_RECYCLE`: seconds before recycling a connection (default `1800`)
- `METRICS_HOST`: metrics server bind host (default `0.0.0.0`)
- `METRICS_PORT`: metrics server port (default `9108`)
- `DB_SLOW_QUERY_MS`: slow query log threshold in milliseconds (default `200`)

OIDC provider decision
- Provider: Microsoft Entra ID (Azure AD)
- Discovery: `https://login.microsoftonline.com/{tenant-id}/v2.0/.well-known/openid-configuration`
- `AUTH_ISSUER`: `https://login.microsoftonline.com/{tenant-id}/v2.0`
- `AUTH_JWKS_URL`: use `jwks_uri` from the discovery document
- `AUTH_AUDIENCE`: API App ID URI or client ID configured in Entra ID

Examples
- Copy `configs/env.example` and export values in your shell.
