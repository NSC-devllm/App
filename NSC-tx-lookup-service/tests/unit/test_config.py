from __future__ import annotations

import pytest

from src.common.config import load_config


def test_load_config_defaults():
    config = load_config(env={})

    assert config.app_env == "local"
    assert config.auth_mode == "disabled"
    assert config.kafka_security_protocol == "PLAINTEXT"
    assert config.dlq_backend == "file"
    assert config.db_pool_size == 5
    assert config.log_level == "INFO"


def test_load_config_invalid_app_env():
    with pytest.raises(ValueError, match="APP_ENV"):
        load_config(env={"APP_ENV": "staging"})


def test_load_config_invalid_auth_mode():
    with pytest.raises(ValueError, match="AUTH_MODE"):
        load_config(env={"AUTH_MODE": "basic"})


def test_load_config_invalid_kafka_protocol():
    with pytest.raises(ValueError, match="KAFKA_SECURITY_PROTOCOL"):
        load_config(env={"KAFKA_SECURITY_PROTOCOL": "TLS"})


def test_load_config_sasl_requires_credentials():
    with pytest.raises(ValueError, match="KAFKA_SASL_USERNAME"):
        load_config(env={"KAFKA_SECURITY_PROTOCOL": "SASL_SSL"})


def test_load_config_sasl_with_credentials():
    config = load_config(
        env={
            "KAFKA_SECURITY_PROTOCOL": "SASL_SSL",
            "KAFKA_SASL_USERNAME": "user",
            "KAFKA_SASL_PASSWORD": "pass",
        }
    )
    assert config.kafka_security_protocol == "SASL_SSL"
    assert config.kafka_sasl_username == "user"


def test_load_config_invalid_dlq_backend():
    with pytest.raises(ValueError, match="DLQ_BACKEND"):
        load_config(env={"DLQ_BACKEND": "redis"})


def test_load_config_invalid_integer():
    with pytest.raises(ValueError, match="DB_POOL_SIZE"):
        load_config(env={"DB_POOL_SIZE": "abc"})


def test_load_config_prod_dlq_defaults_to_db():
    config = load_config(env={"APP_ENV": "prod"})
    assert config.dlq_backend == "db"
