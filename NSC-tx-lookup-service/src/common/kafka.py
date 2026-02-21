from __future__ import annotations

from typing import Any

from src.common.config import AppConfig


def build_kafka_client_config(config: AppConfig) -> dict[str, Any]:
    client_config: dict[str, Any] = {
        "bootstrap.servers": config.kafka_brokers,
    }

    if config.kafka_security_protocol:
        client_config["security.protocol"] = config.kafka_security_protocol

    if config.kafka_security_protocol.startswith("SASL"):
        client_config["sasl.mechanism"] = config.kafka_sasl_mechanism
        client_config["sasl.username"] = config.kafka_sasl_username
        client_config["sasl.password"] = config.kafka_sasl_password

    if config.kafka_ssl_ca_location:
        client_config["ssl.ca.location"] = config.kafka_ssl_ca_location

    return client_config
