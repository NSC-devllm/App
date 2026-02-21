from __future__ import annotations

import logging

from src.common.config import load_config

logger = logging.getLogger(__name__)
_CONFIGURED = False


def init_azure_monitor() -> None:
    global _CONFIGURED
    if _CONFIGURED:
        return
    config = load_config()
    conn_str = config.appinsights_connection_string
    if not conn_str:
        logger.info("APPLICATIONINSIGHTS_CONNECTION_STRING not set; OTel disabled")
        return
    from azure.monitor.opentelemetry import configure_azure_monitor

    configure_azure_monitor(
        connection_string=conn_str,
        enable_live_metrics=True,
    )
    logger.info("Azure Monitor OpenTelemetry configured")
    _CONFIGURED = True
