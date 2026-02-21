from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterable, Mapping

from src.common.time_utils import UTC, ensure_tzaware

DEFAULT_EFFECTIVE_START = datetime(1970, 1, 1, tzinfo=UTC)


@dataclass(frozen=True)
class RuleDefinition:
    rule_id: str
    domain: str | None
    metric: str | None
    threshold: float | int | None
    severity_map: Mapping[str, float] | None
    allowed_values: tuple[str, ...] | None
    comment: str | None
    effective_start_ts: datetime
    effective_end_ts: datetime | None
    is_current: bool

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "RuleDefinition":
        rule_id = payload.get("rule_id")
        if not rule_id:
            raise ValueError("rule_id is required for rule definitions")

        domain = payload.get("domain")
        metric = payload.get("metric")
        threshold = payload.get("threshold")
        severity_map = payload.get("severity_map")
        allowed_values = _parse_allowed_values(payload.get("allowed_values"))
        comment = payload.get("comment")

        effective_start = _parse_datetime(payload.get("effective_start_ts"))
        if effective_start is None:
            effective_start = DEFAULT_EFFECTIVE_START
        effective_end = _parse_datetime(payload.get("effective_end_ts"))

        is_current = payload.get("is_current")
        if is_current is None:
            is_current = effective_end is None

        return cls(
            rule_id=str(rule_id),
            domain=domain,
            metric=metric,
            threshold=threshold,
            severity_map=severity_map,
            allowed_values=allowed_values,
            comment=comment,
            effective_start_ts=effective_start,
            effective_end_ts=effective_end,
            is_current=bool(is_current),
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "rule_id": self.rule_id,
            "domain": self.domain,
            "metric": self.metric,
            "threshold": self.threshold,
            "severity_map": self.severity_map,
            "allowed_values": self.allowed_values,
            "comment": self.comment,
            "effective_start_ts": self.effective_start_ts.isoformat(),
            "effective_end_ts": self.effective_end_ts.isoformat()
            if self.effective_end_ts
            else None,
            "is_current": self.is_current,
        }

    def as_storage_dict(self) -> dict[str, Any]:
        """Return Spark/Delta write-friendly payload with typed timestamps."""
        threshold = float(self.threshold) if self.threshold is not None else None
        severity_map = (
            {
                str(key): (float(value) if value is not None else None)
                for key, value in self.severity_map.items()
            }
            if self.severity_map is not None
            else None
        )
        return {
            "rule_id": self.rule_id,
            "domain": self.domain,
            "metric": self.metric,
            "threshold": threshold,
            "severity_map": severity_map,
            "allowed_values": list(self.allowed_values)
            if self.allowed_values
            else None,
            "comment": self.comment,
            "effective_start_ts": self.effective_start_ts,
            "effective_end_ts": self.effective_end_ts,
            "is_current": self.is_current,
        }


def _parse_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return ensure_tzaware(value)
    if isinstance(value, str):
        normalized = value.replace("Z", "+00:00")
        parsed = datetime.fromisoformat(normalized)
        return ensure_tzaware(parsed)
    raise TypeError("Unsupported datetime value")


def _parse_allowed_values(value: Any) -> tuple[str, ...] | None:
    if value is None:
        return None
    if isinstance(value, (list, tuple, set)):
        return tuple(str(item) for item in value)
    raise ValueError("allowed_values must be a list of strings")


def select_rule(
    rules: Iterable[RuleDefinition],
    *,
    domain: str,
    metric: str,
) -> RuleDefinition | None:
    candidates = [
        rule for rule in rules if rule.domain == domain and rule.metric == metric
    ]
    if not candidates:
        return None
    current = [rule for rule in candidates if rule.is_current]
    if current:
        return max(current, key=lambda rule: rule.effective_start_ts)
    return max(candidates, key=lambda rule: rule.effective_start_ts)
