from __future__ import annotations

from src.common.config import AppConfig
from src.common.kafka import build_kafka_client_config


def _make_config(**overrides) -> AppConfig:
    defaults = {
        "app_env": "local",
        "log_level": "INFO",
        "database_url": "postgresql+psycopg://bo:bo@localhost:5432/bo",
        "kafka_brokers": "localhost:9092",
        "kafka_security_protocol": "PLAINTEXT",
        "kafka_sasl_mechanism": "PLAIN",
        "kafka_sasl_username": "",
        "kafka_sasl_password": "",
        "kafka_ssl_ca_location": "",
        "service_name": "tx-lookup-service",
        "consumer_group_id": "bo-sync-consumer",
        "ledger_topic": "ledger.entry.upserted",
        "payment_order_topic": "payment.order.upserted",
        "dlq_path": "./dlq/failed_events.jsonl",
        "dlq_backend": "file",
        "dlq_retention_days": 14,
        "consumer_poll_timeout_ms": 1000,
        "consumer_offset_reset": "earliest",
        "auth_mode": "disabled",
        "auth_issuer": "",
        "auth_audience": "",
        "auth_jwks_url": "",
        "auth_algorithm": "RS256",
        "auth_roles_claim": "roles,scp",
        "auth_actor_id_claims": "oid,sub",
        "audit_mask_query_keys": "access_token,token",
        "db_pool_size": 5,
        "db_max_overflow": 10,
        "db_pool_timeout": 30,
        "db_pool_recycle": 1800,
        "appinsights_connection_string": "",
        "db_slow_query_ms": 200,
    }
    defaults.update(overrides)
    return AppConfig(**defaults)


def test_plaintext_config() -> None:
    config = _make_config(kafka_security_protocol="PLAINTEXT")
    result = build_kafka_client_config(config)

    assert result["bootstrap.servers"] == "localhost:9092"
    assert result["security.protocol"] == "PLAINTEXT"
    assert "sasl.mechanism" not in result
    assert "sasl.username" not in result
    assert "sasl.password" not in result


def test_sasl_ssl_config() -> None:
    config = _make_config(
        kafka_security_protocol="SASL_SSL",
        kafka_sasl_mechanism="PLAIN",
        kafka_sasl_username="user",
        kafka_sasl_password="pass",
    )
    result = build_kafka_client_config(config)

    assert result["security.protocol"] == "SASL_SSL"
    assert result["sasl.mechanism"] == "PLAIN"
    assert result["sasl.username"] == "user"
    assert result["sasl.password"] == "pass"


def test_ssl_ca_location() -> None:
    config = _make_config(kafka_ssl_ca_location="/etc/ssl/certs/ca.pem")
    result = build_kafka_client_config(config)

    assert result["ssl.ca.location"] == "/etc/ssl/certs/ca.pem"
