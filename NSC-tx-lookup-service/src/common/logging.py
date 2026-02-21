from __future__ import annotations

import logging
import logging.config
import time

from src.common.observability import get_correlation_id

from .config import load_config

LOG_FORMAT = "%(asctime)sZ %(levelname)s %(name)s corr=%(correlation_id)s %(message)s"
LOG_DATEFMT = "%Y-%m-%dT%H:%M:%S"


class CorrelationIdFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        record.correlation_id = get_correlation_id()
        return True


def _utc_formatter() -> logging.Formatter:
    formatter = logging.Formatter(LOG_FORMAT, datefmt=LOG_DATEFMT)
    formatter.converter = time.gmtime
    return formatter


def configure_logging() -> None:
    config = load_config()
    level_name = config.log_level.upper()
    level = getattr(logging, level_name, logging.INFO)

    handler = logging.StreamHandler()
    handler.setFormatter(_utc_formatter())
    handler.addFilter(CorrelationIdFilter())

    logging.basicConfig(level=level, handlers=[handler])

    root_logger = logging.getLogger()
    for existing_handler in root_logger.handlers:
        if not any(
            isinstance(existing_filter, CorrelationIdFilter)
            for existing_filter in existing_handler.filters
        ):
            existing_handler.addFilter(CorrelationIdFilter())


__all__ = ["configure_logging", "LOG_FORMAT", "LOG_DATEFMT"]
