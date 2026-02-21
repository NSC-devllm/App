from __future__ import annotations

import argparse
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from confluent_kafka import Consumer, KafkaError, KafkaException

from src.common.config import load_config
from src.common.kafka import build_kafka_client_config
from src.common.logging import configure_logging
from src.common.observability import (
    correlation_context,
    extract_correlation_id_from_headers,
    extract_correlation_id_from_payload,
)
from src.consumer.dlq import prune_dlq_db, write_dlq_db, write_dlq_file
from src.consumer.events import (
    EventValidationError,
    LedgerEntryUpserted,
    PaymentOrderUpserted,
)
from src.consumer.metrics import (
    FreshnessTracker,
    PairingMetrics,
    VersionMissingCounter,
    record_consumer_message,
    record_dlq,
    record_event_lag,
    record_kafka_lag,
    record_version_missing,
)
from src.consumer.processor import upsert_ledger_entry, upsert_payment_order
from src.db.session import session_scope

logger = logging.getLogger(__name__)


def _maybe_prune_dlq() -> None:
    config = load_config()
    if config.dlq_backend != "db":
        return
    try:
        with session_scope() as session:
            pruned = prune_dlq_db(session, config.dlq_retention_days)
        if pruned:
            logger.info(
                "Pruned %s old DLQ events (retention=%sd)",
                pruned,
                config.dlq_retention_days,
            )
    except Exception:
        logger.exception("Failed to prune DLQ events")


def _write_dlq_payload(payload: dict[str, Any]) -> None:
    config = load_config()
    if config.dlq_backend == "db":
        try:
            with session_scope() as session:
                write_dlq_db(session, payload)
            return
        except Exception:
            logger.exception("Failed to write DLQ to DB; falling back to file")
    try:
        write_dlq_file(config.dlq_path, payload)
    except Exception:
        logger.exception("Failed to write DLQ to file")


def _parse_iso_datetime(value: str | None) -> datetime | None:
    if value is None:
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    parsed = datetime.fromisoformat(text)
    return (
        parsed.astimezone(timezone.utc)
        if parsed.tzinfo
        else parsed.replace(tzinfo=timezone.utc)
    )


def _iter_json_lines(path: Path) -> Iterable[dict[str, Any]]:
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            yield json.loads(line)


def _build_consumer() -> Consumer:
    config = load_config()
    kafka_config = build_kafka_client_config(config)
    kafka_config.update(
        {
            "group.id": config.consumer_group_id,
            "enable.auto.commit": False,
            "auto.offset.reset": config.consumer_offset_reset,
        }
    )
    return Consumer(kafka_config)


def _init_observability() -> None:
    from src.common.otel import init_azure_monitor

    init_azure_monitor()


def _handle_payload(
    topic: str,
    payload: dict[str, Any],
    counter: VersionMissingCounter,
    pairing_metrics: PairingMetrics,
    freshness: FreshnessTracker | None = None,
) -> None:
    config = load_config()
    missing_version = False
    with session_scope() as session:
        if topic == config.ledger_topic:
            event = LedgerEntryUpserted.from_dict(payload)
            record_event_lag(topic, event.event_time)
            if freshness is not None:
                freshness.record(topic, event.event_time)
            missing_version, pairing_snapshot = upsert_ledger_entry(session, event)
            if pairing_snapshot:
                if pairing_metrics.record(
                    pairing_snapshot.complete,
                    pairing_snapshot.incomplete_age_sec,
                ):
                    logger.info(pairing_metrics.summary())
        elif topic == config.payment_order_topic:
            event = PaymentOrderUpserted.from_dict(payload)
            event_time = event.updated_at or event.created_at
            record_event_lag(topic, event_time)
            if freshness is not None:
                freshness.record(topic, event_time)
            missing_version = upsert_payment_order(session, event)
        else:
            raise EventValidationError(f"Unsupported topic {topic}")

    record_version_missing(topic, missing_version)
    if counter.record(missing_version):
        logger.info(counter.summary())


def run_consumer(
    max_messages: int | None = None,
    max_idle_seconds: int | None = None,
) -> None:
    config = load_config()
    _init_observability()
    _maybe_prune_dlq()
    consumer = _build_consumer()
    topics = [config.ledger_topic, config.payment_order_topic]
    consumer.subscribe(topics)

    counter = VersionMissingCounter()
    pairing_metrics = PairingMetrics()
    freshness = FreshnessTracker()
    processed = 0
    last_message_at = time.monotonic()

    def _idle_expired() -> bool:
        if max_idle_seconds is None:
            return False
        idle_for = time.monotonic() - last_message_at
        if idle_for >= max_idle_seconds:
            logger.info("Idle for %s seconds; exiting", max_idle_seconds)
            return True
        return False

    try:
        while True:
            if max_messages is not None and processed >= max_messages:
                logger.info("Processed %s messages; exiting", processed)
                break
            msg = consumer.poll(config.consumer_poll_timeout_ms / 1000.0)
            if msg is None:
                if _idle_expired():
                    break
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    if _idle_expired():
                        break
                    continue
                raise KafkaException(msg.error())

            correlation_id = extract_correlation_id_from_headers(msg.headers())
            try:
                payload = json.loads(msg.value() or b"{}")
                if correlation_id is None:
                    correlation_id = extract_correlation_id_from_payload(payload)
                with correlation_context(correlation_id):
                    _handle_payload(
                        msg.topic(), payload, counter, pairing_metrics, freshness
                    )
                    try:
                        _low, high = consumer.get_watermark_offsets(
                            msg.topic(), msg.partition(), cached=False
                        )
                        lag = max(0, high - msg.offset() - 1)
                        record_kafka_lag(msg.topic(), msg.partition(), lag)
                    except Exception:
                        logger.debug("Failed to read Kafka offsets", exc_info=True)
                    consumer.commit(msg)
                    record_consumer_message(msg.topic(), "success")
                processed += 1
                last_message_at = time.monotonic()
            except (json.JSONDecodeError, EventValidationError, Exception) as exc:
                with correlation_context(correlation_id):
                    record_consumer_message(msg.topic(), "error")
                    record_dlq(msg.topic())
                dlq_payload = {
                    "topic": msg.topic(),
                    "partition": msg.partition(),
                    "offset": msg.offset(),
                    "key": msg.key().decode("utf-8") if msg.key() else None,
                    "payload": (
                        msg.value().decode("utf-8", errors="replace")
                        if msg.value()
                        else None
                    ),
                    "error": str(exc),
                    "correlation_id": correlation_id,
                    "ingested_at": datetime.now(timezone.utc).isoformat(),
                }
                _write_dlq_payload(dlq_payload)
                logger.exception("Failed to process message; sent to DLQ")
                consumer.commit(msg)
    except KeyboardInterrupt:
        logger.info("Consumer interrupted; shutting down")
    finally:
        consumer.close()


def run_backfill(
    ledger_path: Path | None,
    payment_order_path: Path | None,
    since: datetime | None,
    until: datetime | None,
) -> None:
    _init_observability()
    config = load_config()
    _maybe_prune_dlq()
    counter = VersionMissingCounter()
    pairing_metrics = PairingMetrics()
    freshness = FreshnessTracker()

    def _within_range(value: datetime) -> bool:
        if since and value < since:
            return False
        if until and value > until:
            return False
        return True

    if ledger_path:
        for payload in _iter_json_lines(ledger_path):
            try:
                event = LedgerEntryUpserted.from_dict(payload)
                if not _within_range(event.event_time):
                    continue
                correlation_id = extract_correlation_id_from_payload(payload)
                with correlation_context(correlation_id):
                    with session_scope() as session:
                        missing, pairing_snapshot = upsert_ledger_entry(session, event)
                    record_event_lag(config.ledger_topic, event.event_time)
                    freshness.record(config.ledger_topic, event.event_time)
                    record_version_missing(config.ledger_topic, missing)
                    record_consumer_message(config.ledger_topic, "success")
                    if counter.record(missing):
                        logger.info(counter.summary())
                    if pairing_snapshot:
                        if pairing_metrics.record(
                            pairing_snapshot.complete,
                            pairing_snapshot.incomplete_age_sec,
                        ):
                            logger.info(pairing_metrics.summary())
            except Exception as exc:
                record_consumer_message("backfill-ledger", "error")
                record_dlq("backfill-ledger")
                _write_dlq_payload(
                    {
                        "topic": "backfill-ledger",
                        "payload": payload,
                        "error": str(exc),
                        "correlation_id": extract_correlation_id_from_payload(payload),
                        "ingested_at": datetime.now(timezone.utc).isoformat(),
                    }
                )
                logger.exception("Backfill ledger failed; sent to DLQ")

    if payment_order_path:
        for payload in _iter_json_lines(payment_order_path):
            try:
                event = PaymentOrderUpserted.from_dict(payload)
                if not _within_range(event.created_at):
                    continue
                correlation_id = extract_correlation_id_from_payload(payload)
                with correlation_context(correlation_id):
                    with session_scope() as session:
                        missing = upsert_payment_order(session, event)
                    record_event_lag(
                        config.payment_order_topic, event.updated_at or event.created_at
                    )
                    freshness.record(
                        config.payment_order_topic, event.updated_at or event.created_at
                    )
                    record_version_missing(config.payment_order_topic, missing)
                    record_consumer_message(config.payment_order_topic, "success")
                    if counter.record(missing):
                        logger.info(counter.summary())
            except Exception as exc:
                record_consumer_message("backfill-payment-order", "error")
                record_dlq("backfill-payment-order")
                _write_dlq_payload(
                    {
                        "topic": "backfill-payment-order",
                        "payload": payload,
                        "error": str(exc),
                        "correlation_id": extract_correlation_id_from_payload(payload),
                        "ingested_at": datetime.now(timezone.utc).isoformat(),
                    }
                )
                logger.exception("Backfill payment order failed; sent to DLQ")


def main() -> None:
    configure_logging()

    parser = argparse.ArgumentParser(description="Backoffice sync consumer")
    subparsers = parser.add_subparsers(dest="command")

    consume_parser = subparsers.add_parser("consume", help="Run Kafka consumer")
    consume_parser.add_argument("--max-messages", type=int)
    consume_parser.add_argument("--max-idle-seconds", type=int)
    consume_parser.set_defaults(
        func=lambda args: run_consumer(args.max_messages, args.max_idle_seconds)
    )

    backfill_parser = subparsers.add_parser("backfill", help="Run backfill from JSONL")
    backfill_parser.add_argument("--ledger-file", type=Path)
    backfill_parser.add_argument("--payment-order-file", type=Path)
    backfill_parser.add_argument("--since", type=str)
    backfill_parser.add_argument("--until", type=str)

    args = parser.parse_args()

    if args.command == "backfill":
        since = _parse_iso_datetime(args.since) if args.since else None
        until = _parse_iso_datetime(args.until) if args.until else None
        run_backfill(args.ledger_file, args.payment_order_file, since, until)
        return

    if args.command == "consume":
        run_consumer(args.max_messages, args.max_idle_seconds)
        return

    parser.print_help()


if __name__ == "__main__":
    main()
