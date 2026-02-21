from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, Mapping

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


def _to_text(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    try:
        return json.dumps(value, ensure_ascii=False)
    except Exception:
        return str(value)


def write_dlq_file(path: str, payload: Mapping[str, Any]) -> None:
    dlq_path = Path(path)
    dlq_path.parent.mkdir(parents=True, exist_ok=True)
    with dlq_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False) + "\n")


def write_dlq_db(session: Session, payload: Mapping[str, Any]) -> None:
    from src.db.models import ConsumerDlqEvent

    topic = str(payload.get("topic") or "").strip() or "unknown"
    error = str(payload.get("error") or "").strip() or "unknown"

    event = ConsumerDlqEvent(
        topic=topic,
        partition=payload.get("partition"),
        offset=payload.get("offset"),
        key=_to_text(payload.get("key")),
        payload=_to_text(payload.get("payload")),
        error=error,
        correlation_id=_to_text(payload.get("correlation_id")),
        ingested_at=datetime.now(timezone.utc),
    )
    session.add(event)


def prune_dlq_db(session: Session, retention_days: int) -> int:
    from sqlalchemy import delete

    from src.db.models import ConsumerDlqEvent

    days = max(0, int(retention_days))
    if days <= 0:
        return 0
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    result = session.execute(
        delete(ConsumerDlqEvent).where(ConsumerDlqEvent.ingested_at < cutoff)
    )
    return int(result.rowcount or 0)


# Backward-compatible alias (legacy file-based DLQ).
write_dlq = write_dlq_file
