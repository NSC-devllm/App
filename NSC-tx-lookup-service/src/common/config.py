from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Mapping

ALLOWED_ENVS = {"local", "dev", "prod"}
ALLOWED_AUTH_MODES = {"disabled", "oidc"}
ALLOWED_KAFKA_SECURITY_PROTOCOLS = {"PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"}
ALLOWED_DLQ_BACKENDS = {"file", "db"}


@dataclass(frozen=True)
class AppConfig:
    app_env: str
    log_level: str
    database_url: str
    kafka_brokers: str
    kafka_security_protocol: str
    kafka_sasl_mechanism: str
    kafka_sasl_username: str
    kafka_sasl_password: str
    kafka_ssl_ca_location: str
    service_name: str
    consumer_group_id: str
    ledger_topic: str
    payment_order_topic: str
    dlq_path: str
    dlq_backend: str
    dlq_retention_days: int
    consumer_poll_timeout_ms: int
    consumer_offset_reset: str
    auth_mode: str
    auth_issuer: str
    auth_audience: str
    auth_jwks_url: str
    auth_algorithm: str
    auth_roles_claim: str
    auth_actor_id_claims: str
    audit_mask_query_keys: str
    db_pool_size: int
    db_max_overflow: int
    db_pool_timeout: int
    db_pool_recycle: int
    appinsights_connection_string: str
    db_slow_query_ms: int


def _get_env(env: Mapping[str, str], key: str, default: str) -> str:
    value = env.get(key, default)
    return value.strip() if isinstance(value, str) else default


def _get_int(env: Mapping[str, str], key: str, default: int) -> int:
    value = env.get(key)
    if value is None:
        return default
    try:
        return int(str(value).strip())
    except ValueError as exc:
        raise ValueError(f"{key} must be an integer (got {value!r})") from exc


def load_config(env: Mapping[str, str] | None = None) -> AppConfig:
    source = env or os.environ
    app_env = _get_env(source, "APP_ENV", "local")
    if app_env not in ALLOWED_ENVS:
        raise ValueError(
            f"APP_ENV must be one of {sorted(ALLOWED_ENVS)} (got {app_env!r})"
        )

    auth_mode = _get_env(source, "AUTH_MODE", "disabled")
    if auth_mode not in ALLOWED_AUTH_MODES:
        raise ValueError(
            f"AUTH_MODE must be one of {sorted(ALLOWED_AUTH_MODES)} (got {auth_mode!r})"
        )

    kafka_security_protocol = _get_env(
        source, "KAFKA_SECURITY_PROTOCOL", "PLAINTEXT"
    ).upper()
    if kafka_security_protocol not in ALLOWED_KAFKA_SECURITY_PROTOCOLS:
        raise ValueError(
            "KAFKA_SECURITY_PROTOCOL must be one of "
            f"{sorted(ALLOWED_KAFKA_SECURITY_PROTOCOLS)} "
            f"(got {kafka_security_protocol!r})"
        )

    kafka_sasl_mechanism = _get_env(source, "KAFKA_SASL_MECHANISM", "PLAIN")
    kafka_sasl_username = _get_env(source, "KAFKA_SASL_USERNAME", "")
    kafka_sasl_password = _get_env(source, "KAFKA_SASL_PASSWORD", "")
    if kafka_security_protocol.startswith("SASL") and (
        not kafka_sasl_username or not kafka_sasl_password
    ):
        raise ValueError(
            "KAFKA_SASL_USERNAME and KAFKA_SASL_PASSWORD are required "
            "when KAFKA_SECURITY_PROTOCOL uses SASL"
        )

    dlq_backend_default = "db" if app_env == "prod" else "file"
    dlq_backend = _get_env(source, "DLQ_BACKEND", dlq_backend_default).lower()
    if dlq_backend not in ALLOWED_DLQ_BACKENDS:
        raise ValueError(
            f"DLQ_BACKEND must be one of {sorted(ALLOWED_DLQ_BACKENDS)} (got {dlq_backend!r})"
        )

    return AppConfig(
        app_env=app_env,
        log_level=_get_env(source, "LOG_LEVEL", "INFO"),
        database_url=_get_env(
            source, "DATABASE_URL", "postgresql+psycopg://bo:bo@localhost:5432/bo"
        ),
        kafka_brokers=_get_env(source, "KAFKA_BROKERS", "localhost:9092"),
        kafka_security_protocol=kafka_security_protocol,
        kafka_sasl_mechanism=kafka_sasl_mechanism,
        kafka_sasl_username=kafka_sasl_username,
        kafka_sasl_password=kafka_sasl_password,
        kafka_ssl_ca_location=_get_env(source, "KAFKA_SSL_CA_LOCATION", ""),
        service_name=_get_env(source, "SERVICE_NAME", "tx-lookup-service"),
        consumer_group_id=_get_env(source, "KAFKA_GROUP_ID", "bo-sync-consumer"),
        ledger_topic=_get_env(source, "LEDGER_TOPIC", "ledger.entry.upserted"),
        payment_order_topic=_get_env(
            source, "PAYMENT_ORDER_TOPIC", "payment.order.upserted"
        ),
        dlq_path=_get_env(source, "DLQ_PATH", "./dlq/failed_events.jsonl"),
        dlq_backend=dlq_backend,
        dlq_retention_days=_get_int(source, "DLQ_RETENTION_DAYS", 14),
        consumer_poll_timeout_ms=_get_int(source, "CONSUMER_POLL_TIMEOUT_MS", 1000),
        consumer_offset_reset=_get_env(source, "CONSUMER_OFFSET_RESET", "earliest"),
        auth_mode=auth_mode,
        auth_issuer=_get_env(source, "AUTH_ISSUER", ""),
        auth_audience=_get_env(source, "AUTH_AUDIENCE", ""),
        auth_jwks_url=_get_env(source, "AUTH_JWKS_URL", ""),
        auth_algorithm=_get_env(source, "AUTH_ALGORITHM", "RS256"),
        auth_roles_claim=_get_env(source, "AUTH_ROLES_CLAIM", "roles,scp"),
        auth_actor_id_claims=_get_env(source, "AUTH_ACTOR_ID_CLAIMS", "oid,sub"),
        audit_mask_query_keys=_get_env(
            source, "AUDIT_MASK_QUERY_KEYS", "access_token,token"
        ),
        db_pool_size=_get_int(source, "DB_POOL_SIZE", 5),
        db_max_overflow=_get_int(source, "DB_MAX_OVERFLOW", 10),
        db_pool_timeout=_get_int(source, "DB_POOL_TIMEOUT", 30),
        db_pool_recycle=_get_int(source, "DB_POOL_RECYCLE", 1800),
        appinsights_connection_string=_get_env(
            source, "APPLICATIONINSIGHTS_CONNECTION_STRING", ""
        ),
        db_slow_query_ms=_get_int(source, "DB_SLOW_QUERY_MS", 200),
    )
