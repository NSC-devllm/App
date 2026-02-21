from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Producer
from sqlalchemy import select

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from src.common.config import load_config
from src.common.kafka import build_kafka_client_config
from src.db.models import ConsumerDlqEvent
from src.db.session import session_scope


def _parse_iso_datetime(value: str) -> datetime:
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    parsed = datetime.fromisoformat(text)
    return (
        parsed.astimezone(timezone.utc)
        if parsed.tzinfo
        else parsed.replace(tzinfo=timezone.utc)
    )


def replay_file(
    dlq_path: Path,
    *,
    topics: set[str] | None = None,
    limit: int | None = None,
) -> None:
    config = load_config()
    producer = Producer(build_kafka_client_config(config))

    remaining = None if limit is None else max(0, int(limit))
    with dlq_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            record = json.loads(line)
            topic = record.get("topic")
            payload = record.get("payload")
            if not topic or payload is None:
                continue
            if topics is not None and topic not in topics:
                continue
            if remaining is not None and remaining <= 0:
                break

            if isinstance(payload, str):
                value = payload.encode("utf-8")
            else:
                value = json.dumps(payload).encode("utf-8")

            producer.produce(topic, value=value)
            if remaining is not None:
                remaining -= 1

    producer.flush()


def replay_db(
    *,
    topics: set[str] | None = None,
    since: datetime | None = None,
    until: datetime | None = None,
    min_id: int | None = None,
    max_id: int | None = None,
    limit: int | None = None,
) -> None:
    config = load_config()
    producer = Producer(build_kafka_client_config(config))

    with session_scope() as session:
        stmt = select(ConsumerDlqEvent)
        if topics is not None:
            stmt = stmt.where(ConsumerDlqEvent.topic.in_(sorted(topics)))
        if since is not None:
            stmt = stmt.where(ConsumerDlqEvent.ingested_at >= since)
        if until is not None:
            stmt = stmt.where(ConsumerDlqEvent.ingested_at <= until)
        if min_id is not None:
            stmt = stmt.where(ConsumerDlqEvent.dlq_id >= int(min_id))
        if max_id is not None:
            stmt = stmt.where(ConsumerDlqEvent.dlq_id <= int(max_id))
        stmt = stmt.order_by(
            ConsumerDlqEvent.ingested_at.asc(), ConsumerDlqEvent.dlq_id.asc()
        )
        if limit is not None:
            stmt = stmt.limit(max(0, int(limit)))
        events = session.execute(stmt).scalars().all()

    for event in events:
        if not event.topic or event.payload is None:
            continue
        producer.produce(event.topic, value=event.payload.encode("utf-8"))

    producer.flush()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Replay DLQ events to Kafka topics")
    parser.add_argument(
        "--backend",
        choices=["file", "db"],
        help="Override DLQ backend (default: from config)",
    )
    parser.add_argument(
        "--dlq-file",
        type=Path,
        help="DLQ file path when backend=file (default: from config)",
    )
    parser.add_argument("--limit", type=int, help="Max number of events to replay")
    parser.add_argument(
        "--topic",
        action="append",
        help="Filter by topic (repeatable). When omitted, replays all topics.",
    )
    parser.add_argument(
        "--since",
        type=str,
        help="Filter by ingested_at >= since (ISO-8601, e.g. 2026-02-09T00:00:00Z). DB backend only.",
    )
    parser.add_argument(
        "--until",
        type=str,
        help="Filter by ingested_at <= until (ISO-8601). DB backend only.",
    )
    parser.add_argument(
        "--min-id",
        type=int,
        help="Filter by dlq_id >= min-id. DB backend only.",
    )
    parser.add_argument(
        "--max-id",
        type=int,
        help="Filter by dlq_id <= max-id. DB backend only.",
    )
    args = parser.parse_args()

    config = load_config()
    backend = (args.backend or config.dlq_backend).lower()
    topics = set(args.topic) if args.topic else None
    if backend == "db":
        replay_db(
            topics=topics,
            since=_parse_iso_datetime(args.since) if args.since else None,
            until=_parse_iso_datetime(args.until) if args.until else None,
            min_id=args.min_id,
            max_id=args.max_id,
            limit=args.limit,
        )
    else:
        path = args.dlq_file or Path(config.dlq_path)
        replay_file(path, topics=topics, limit=args.limit)
