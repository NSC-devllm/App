from __future__ import annotations

import logging
import time
from typing import Callable

from sqlalchemy import event, text
from sqlalchemy.engine import Engine

from src.common.config import load_config
from src.common.metrics import observe_db_query, register_replication_lag_provider

logger = logging.getLogger(__name__)

_PRIMARY_REPLICATION_LAG_SQL = text(
    """
    SELECT
      CASE
        WHEN COUNT(*) = 0 THEN NULL
        ELSE EXTRACT(EPOCH FROM COALESCE(MAX(replay_lag), INTERVAL '0 seconds'))
      END AS lag_seconds
    FROM pg_catalog.pg_stat_replication
    """
)

_STANDBY_REPLICATION_LAG_SQL = text(
    """
    SELECT
      CASE
        WHEN pg_is_in_recovery()
          THEN EXTRACT(EPOCH FROM now() - pg_last_xact_replay_timestamp())
        ELSE NULL
      END AS lag_seconds
    """
)


def _to_lag_seconds(value: object) -> float | None:
    if value is None:
        return None
    try:
        return max(0.0, float(value))
    except (TypeError, ValueError):
        return None


class _ReplicationLagSampler:
    def __init__(self, engine: Engine) -> None:
        self._engine = engine
        self._warned_primary = False
        self._warned_standby = False

    def sample(self) -> float | None:
        primary = self._sample_primary()
        if primary is not None:
            return primary
        return self._sample_standby()

    def _sample_primary(self) -> float | None:
        try:
            with self._engine.connect() as conn:
                lag = conn.execute(_PRIMARY_REPLICATION_LAG_SQL).scalar_one_or_none()
            return _to_lag_seconds(lag)
        except Exception as exc:
            if not self._warned_primary:
                logger.info(
                    "Primary replication lag metric unavailable: %s",
                    exc,
                )
                self._warned_primary = True
            return None

    def _sample_standby(self) -> float | None:
        try:
            with self._engine.connect() as conn:
                lag = conn.execute(_STANDBY_REPLICATION_LAG_SQL).scalar_one_or_none()
            return _to_lag_seconds(lag)
        except Exception as exc:
            if not self._warned_standby:
                logger.info(
                    "Standby replay lag metric unavailable: %s",
                    exc,
                )
                self._warned_standby = True
            return None


def build_replication_lag_provider(engine: Engine) -> Callable[[], float | None]:
    """Build a lazy sampler for db_replication_lag_seconds observable gauge."""
    sampler = _ReplicationLagSampler(engine)
    return sampler.sample


def _normalize_identifier(value: str) -> str:
    return value.strip().strip('"').strip("`")


def _parse_statement(statement: str) -> tuple[str, str]:
    if not statement:
        return "UNKNOWN", "unknown"
    tokens = statement.strip().split()
    if not tokens:
        return "UNKNOWN", "unknown"

    op = tokens[0].upper()
    table = "unknown"
    lowered = [token.lower() for token in tokens]

    if op == "SELECT":
        if "from" in lowered:
            idx = lowered.index("from") + 1
            if idx < len(tokens):
                table = tokens[idx]
    elif op == "INSERT":
        if "into" in lowered:
            idx = lowered.index("into") + 1
            if idx < len(tokens):
                table = tokens[idx]
    elif op == "UPDATE":
        if len(tokens) > 1:
            table = tokens[1]
    elif op == "DELETE":
        if "from" in lowered:
            idx = lowered.index("from") + 1
            if idx < len(tokens):
                table = tokens[idx]

    table = _normalize_identifier(table)
    if "." in table:
        table = table.split(".")[-1]
    return op, table or "unknown"


def _truncate_sql(statement: str, limit: int = 300) -> str:
    text = " ".join(statement.strip().split())
    if len(text) <= limit:
        return text
    return text[: limit - 3] + "..."


def install_sqlalchemy_observability(engine: Engine) -> None:
    if getattr(engine, "_observability_installed", False):
        return

    config = load_config()
    slow_ms = max(0, config.db_slow_query_ms)
    slow_sec = slow_ms / 1000.0

    @event.listens_for(engine, "before_cursor_execute")
    def before_cursor_execute(
        conn, cursor, statement, parameters, context, executemany
    ):
        context._query_start_time = time.perf_counter()

    @event.listens_for(engine, "after_cursor_execute")
    def after_cursor_execute(conn, cursor, statement, parameters, context, executemany):
        duration = time.perf_counter() - getattr(
            context, "_query_start_time", time.perf_counter()
        )
        op, table = _parse_statement(statement)
        observe_db_query(op, table, duration, success=True)

        if slow_sec and duration >= slow_sec:
            logger.warning(
                "Slow query %.2f ms op=%s table=%s sql=%s",
                duration * 1000,
                op,
                table,
                _truncate_sql(statement),
            )

    @event.listens_for(engine, "handle_error")
    def handle_error(exception_context):
        statement = exception_context.statement or ""
        execution_context = exception_context.execution_context
        start_time = None
        if execution_context is not None:
            start_time = getattr(execution_context, "_query_start_time", None)
        if start_time is None:
            start_time = time.perf_counter()
        duration = time.perf_counter() - start_time
        op, table = _parse_statement(statement)
        observe_db_query(op, table, duration, success=False)
        logger.warning(
            "DB error after %.2f ms op=%s table=%s",
            duration * 1000,
            op,
            table,
        )

    register_replication_lag_provider(build_replication_lag_provider(engine))

    setattr(engine, "_observability_installed", True)
