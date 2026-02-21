from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Sequence

from opentelemetry import metrics
from opentelemetry.metrics import CallbackOptions, Observation

_meter = metrics.get_meter("tx-lookup-service.consumer")

CONSUMER_MESSAGES_TOTAL = _meter.create_counter(
    name="consumer_messages_total",
    description="Total consumer messages",
)

CONSUMER_EVENT_LAG_SECONDS = _meter.create_histogram(
    name="consumer_event_lag_seconds",
    description="Event time to processing lag in seconds",
    unit="s",
)

CONSUMER_DLQ_TOTAL = _meter.create_counter(
    name="consumer_dlq_total",
    description="Total DLQ writes",
)

VERSION_MISSING_TOTAL = _meter.create_counter(
    name="consumer_version_missing_total",
    description="Events missing updated_at/source_version",
)

PAIR_TOTAL = _meter.create_counter(
    name="pairing_total",
    description="Total pairing calculations",
)

PAIR_INCOMPLETE_TOTAL = _meter.create_counter(
    name="pairing_incomplete_total",
    description="Incomplete pairing calculations",
)

PAIR_INCOMPLETE_AGE_SECONDS = _meter.create_histogram(
    name="pairing_incomplete_age_seconds",
    description="Age of incomplete pairs in seconds",
    unit="s",
)

PAIRING_SKIPPED_TOTAL = _meter.create_counter(
    name="pairing_skipped_non_payment_order_total",
    description="Events skipped for pairing due to non-PAYMENT_ORDER related_type",
)

# --- Observable gauges (replaces Prometheus Gauge.set()) ---

_kafka_lag_values: dict[str, int] = {}
_freshness_values: dict[str, float] = {}


def _kafka_lag_callback(
    options: CallbackOptions,
) -> Sequence[Observation]:
    observations: list[Observation] = []
    for key, value in _kafka_lag_values.items():
        topic, partition = key.rsplit("|", 1)
        observations.append(
            Observation(value, attributes={"topic": topic, "partition": partition})
        )
    return observations


def _freshness_callback(
    options: CallbackOptions,
) -> Sequence[Observation]:
    return [
        Observation(value, attributes={"topic": topic})
        for topic, value in _freshness_values.items()
    ]


CONSUMER_KAFKA_LAG = _meter.create_observable_gauge(
    name="consumer_kafka_lag",
    description="Kafka partition lag",
    callbacks=[_kafka_lag_callback],
)

CONSUMER_FRESHNESS_SECONDS = _meter.create_observable_gauge(
    name="consumer_freshness_seconds",
    description="End-to-end data freshness in seconds (now - max event_time)",
    callbacks=[_freshness_callback],
    unit="s",
)


# --- Wrapper functions (signatures unchanged) ---


def record_consumer_message(topic: str, status: str) -> None:
    CONSUMER_MESSAGES_TOTAL.add(1, attributes={"topic": topic, "status": status})


def record_event_lag(topic: str, event_time: datetime | None) -> None:
    if event_time is None:
        return
    now = datetime.now(timezone.utc)
    lag = (now - event_time).total_seconds()
    CONSUMER_EVENT_LAG_SECONDS.record(max(0.0, lag), attributes={"topic": topic})


def record_kafka_lag(topic: str, partition: int, lag: int) -> None:
    _kafka_lag_values[f"{topic}|{partition}"] = max(0, lag)


def record_dlq(topic: str) -> None:
    CONSUMER_DLQ_TOTAL.add(1, attributes={"topic": topic})


def record_version_missing(topic: str, missing: bool) -> None:
    if missing:
        VERSION_MISSING_TOTAL.add(1, attributes={"topic": topic})


@dataclass
class FreshnessTracker:
    last_event_time: dict[str, datetime] = field(default_factory=dict)

    def record(self, topic: str, event_time: datetime | None) -> None:
        if event_time is None:
            return
        last = self.last_event_time.get(topic)
        if last is None or event_time > last:
            self.last_event_time[topic] = event_time
        latest = self.last_event_time.get(topic)
        if latest is None:
            return
        now = datetime.now(timezone.utc)
        freshness = (now - latest).total_seconds()
        _freshness_values[topic] = max(0.0, freshness)


@dataclass
class VersionMissingCounter:
    log_every: int = 100
    processed: int = 0
    missing_version: int = 0

    def record(self, missing: bool) -> bool:
        self.processed += 1
        if missing:
            self.missing_version += 1
        return self.processed % self.log_every == 0

    def summary(self) -> str:
        return (
            f"processed={self.processed} " f"version_missing_cnt={self.missing_version}"
        )


@dataclass
class PairingMetrics:
    log_every: int = 100
    total: int = 0
    incomplete: int = 0
    incomplete_age_sum: float = 0.0
    incomplete_age_max: float = 0.0

    def record(self, complete: bool, incomplete_age_sec: float | None) -> bool:
        self.total += 1
        PAIR_TOTAL.add(1)
        if not complete:
            self.incomplete += 1
            PAIR_INCOMPLETE_TOTAL.add(1)
            if incomplete_age_sec is not None:
                self.incomplete_age_sum += incomplete_age_sec
                self.incomplete_age_max = max(
                    self.incomplete_age_max, incomplete_age_sec
                )
                PAIR_INCOMPLETE_AGE_SECONDS.record(incomplete_age_sec)
        return self.total % self.log_every == 0

    def summary(self) -> str:
        ratio = (self.incomplete / self.total) if self.total else 0.0
        avg_age = self.incomplete_age_sum / self.incomplete if self.incomplete else 0.0
        return (
            f"pair_total={self.total} "
            f"pair_incomplete={self.incomplete} "
            f"pair_incomplete_ratio={ratio:.4f} "
            f"pair_incomplete_avg_age_sec={avg_age:.2f} "
            f"pair_incomplete_max_age_sec={self.incomplete_age_max:.2f}"
        )
