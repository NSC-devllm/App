from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable, Mapping

from src.common.rules import RuleDefinition, select_rule
from src.common.time_utils import date_kst, now_utc, to_utc

SEVERITY_WARN = "WARN"
SEVERITY_CRITICAL = "CRITICAL"

TAG_SOURCE_STALE = "SOURCE_STALE"
TAG_EVENT_DROP = "EVENT_DROP_SUSPECTED"

EXCEPTION_RECON = "RECON_DRIFT_HIGH"
EXCEPTION_SUPPLY = "SUPPLY_MISMATCH"

DEFAULT_FAILED_STATUSES = {"FAILED", "CANCELLED"}
DEFAULT_REFUNDED_STATUSES = {"REFUNDED"}

SUPPLY_ENTRY_SIGNS: dict[str, int] = {
    "MINT": 1,
    "CHARGE": 1,
    "BURN": -1,
    "WITHDRAW": -1,
}


@dataclass(frozen=True)
class ReconOutput:
    rows: list[dict[str, Any]]
    exceptions: list[dict[str, Any]]


@dataclass(frozen=True)
class SupplyOutput:
    row: dict[str, Any]
    exceptions: list[dict[str, Any]]


def _parse_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return to_utc(value)
    if isinstance(value, str):
        normalized = value.replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(normalized)
        except ValueError:
            return None
        return to_utc(parsed)
    return None


def _parse_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _resolve_dq_tag(dq_tags: Iterable[str] | None) -> str | None:
    if not dq_tags:
        return None
    tags = {tag for tag in dq_tags if tag}
    if TAG_SOURCE_STALE in tags:
        return TAG_SOURCE_STALE
    if TAG_EVENT_DROP in tags:
        return TAG_EVENT_DROP
    return next(iter(tags)) if tags else None


def _gating_active(dq_tag: str | None) -> bool:
    return dq_tag in {TAG_SOURCE_STALE, TAG_EVENT_DROP}


def _to_decimal(value: float | int | Decimal) -> Decimal:
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _evaluate_severity(
    value: Decimal | float | int | None,
    rule: RuleDefinition | None,
) -> str | None:
    if rule is None or value is None:
        return None

    metric_value = _to_decimal(value)
    if rule.severity_map:
        if "crit" in rule.severity_map:
            if metric_value > _to_decimal(rule.severity_map["crit"]):
                return SEVERITY_CRITICAL
        if "warn" in rule.severity_map:
            if metric_value > _to_decimal(rule.severity_map["warn"]):
                return SEVERITY_WARN

    if rule.threshold is not None and metric_value > _to_decimal(rule.threshold):
        return SEVERITY_CRITICAL
    return None


def _resolve_threshold(rule: RuleDefinition | None) -> Decimal | None:
    if rule is None:
        return None
    if rule.threshold is not None:
        return _to_decimal(rule.threshold)
    if rule.severity_map and "crit" in rule.severity_map:
        return _to_decimal(rule.severity_map["crit"])
    return None


def _build_exception(
    *,
    target_date: date,
    exception_type: str,
    severity: str,
    metric: str,
    metric_value: Decimal | None,
    run_id: str,
    rule_id: str | None,
    message: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "date_kst": target_date,
        "domain": "ledger",
        "exception_type": exception_type,
        "severity": severity,
        "source_table": None,
        "window_start_ts": None,
        "window_end_ts": None,
        "metric": metric,
        "metric_value": metric_value,
        "message": json.dumps(message or {}, default=str, ensure_ascii=True),
        "run_id": run_id,
        "rule_id": rule_id,
        "generated_at": now_utc(),
    }


def _select_snapshot_bounds(
    wallet_snapshots: Iterable[Mapping[str, Any]],
    *,
    target_date: date,
) -> dict[str, dict[str, Any]]:
    bounds: dict[str, dict[str, Any]] = {}
    for record in wallet_snapshots:
        snapshot_ts = _parse_datetime(record.get("snapshot_ts"))
        if snapshot_ts is None:
            continue
        if date_kst(snapshot_ts) != target_date:
            continue
        user_id = record.get("user_id")
        if not user_id:
            continue
        balance_total = _parse_decimal(record.get("balance_total"))
        if balance_total is None:
            continue

        key = str(user_id)
        entry = bounds.get(key)
        if entry is None:
            bounds[key] = {
                "min_ts": snapshot_ts,
                "max_ts": snapshot_ts,
                "start_balance": balance_total,
                "end_balance": balance_total,
            }
            continue

        if snapshot_ts < entry["min_ts"]:
            entry["min_ts"] = snapshot_ts
            entry["start_balance"] = balance_total
        if snapshot_ts > entry["max_ts"]:
            entry["max_ts"] = snapshot_ts
            entry["end_balance"] = balance_total

    return bounds


def _aggregate_net_flow(
    ledger_entries: Iterable[Mapping[str, Any]],
    *,
    target_date: date,
) -> dict[str, Decimal]:
    totals: dict[str, Decimal] = {}
    for record in ledger_entries:
        event_date = record.get("event_date_kst")
        if isinstance(event_date, date):
            event_kst = event_date
        else:
            event_time = _parse_datetime(record.get("event_time"))
            if event_time is None:
                continue
            event_kst = date_kst(event_time)

        if event_kst != target_date:
            continue

        wallet_id = record.get("wallet_id")
        if not wallet_id:
            continue
        amount_signed = _parse_decimal(record.get("amount_signed"))
        if amount_signed is None:
            continue

        key = str(wallet_id)
        totals[key] = totals.get(key, Decimal("0")) + amount_signed

    return totals


def _aggregate_supply_entries(
    ledger_entries: Iterable[Mapping[str, Any]],
    *,
    target_date: date,
) -> Decimal:
    total = Decimal("0")
    for record in ledger_entries:
        event_date = record.get("event_date_kst")
        if isinstance(event_date, date):
            event_kst = event_date
        else:
            event_time = _parse_datetime(record.get("event_time"))
            if event_time is None:
                continue
            event_kst = date_kst(event_time)

        if event_kst > target_date:
            continue

        entry_type = record.get("entry_type") or record.get("type")
        if not entry_type:
            continue
        sign = SUPPLY_ENTRY_SIGNS.get(str(entry_type))
        if sign is None:
            continue

        amount = _parse_decimal(record.get("amount"))
        if amount is None:
            continue
        total += amount * Decimal(sign)

    return total


def build_recon_snapshot_flow(
    wallet_snapshots: Iterable[Mapping[str, Any]],
    ledger_entries: Iterable[Mapping[str, Any]],
    *,
    target_date: date,
    run_id: str,
    rules: Iterable[RuleDefinition],
    dq_tags: Iterable[str] | None = None,
) -> ReconOutput:
    dq_tag = _resolve_dq_tag(dq_tags)
    recon_rule = select_rule(rules, domain="ledger", metric="drift_abs")

    snapshot_bounds = _select_snapshot_bounds(wallet_snapshots, target_date=target_date)
    net_flows = _aggregate_net_flow(ledger_entries, target_date=target_date)

    rows: list[dict[str, Any]] = []
    exceptions: list[dict[str, Any]] = []

    for user_id, bounds in snapshot_bounds.items():
        start_balance = bounds["start_balance"]
        end_balance = bounds["end_balance"]
        delta = end_balance - start_balance
        net_flow = net_flows.get(user_id, Decimal("0"))
        drift = delta - net_flow
        drift_abs = abs(drift)
        drift_pct = None
        if net_flow != 0:
            drift_pct = drift_abs / abs(net_flow)

        row = {
            "date_kst": target_date,
            "user_id": user_id,
            "delta_balance_total": delta,
            "net_flow_total": net_flow,
            "drift_abs": drift_abs,
            "drift_pct": drift_pct,
            "dq_tag": dq_tag,
            "run_id": run_id,
            "rule_id": recon_rule.rule_id if recon_rule else None,
        }
        rows.append(row)

        severity = _evaluate_severity(drift_abs, recon_rule)
        if severity:
            exceptions.append(
                _build_exception(
                    target_date=target_date,
                    exception_type=EXCEPTION_RECON,
                    severity=severity,
                    metric="drift_abs",
                    metric_value=drift_abs,
                    run_id=run_id,
                    rule_id=recon_rule.rule_id if recon_rule else None,
                    message={
                        "user_id": user_id,
                        "delta_balance_total": str(delta),
                        "net_flow_total": str(net_flow),
                        "drift_abs": str(drift_abs),
                    },
                )
            )

    return ReconOutput(rows=rows, exceptions=exceptions)


def build_supply_balance_daily(
    wallet_snapshots: Iterable[Mapping[str, Any]],
    ledger_entries: Iterable[Mapping[str, Any]],
    *,
    target_date: date,
    run_id: str,
    rules: Iterable[RuleDefinition],
    dq_tags: Iterable[str] | None = None,
) -> SupplyOutput:
    supply_rule = select_rule(rules, domain="ledger", metric="supply_diff_abs")
    threshold = _resolve_threshold(supply_rule)

    snapshot_bounds = _select_snapshot_bounds(wallet_snapshots, target_date=target_date)
    wallet_total = sum(
        (entry["end_balance"] for entry in snapshot_bounds.values()),
        Decimal("0"),
    )
    issued_supply = _aggregate_supply_entries(ledger_entries, target_date=target_date)

    diff_amount = issued_supply - wallet_total
    diff_abs = abs(diff_amount)
    if threshold is not None:
        is_ok = diff_abs <= threshold
    else:
        is_ok = diff_abs == 0

    row = {
        "date_kst": target_date,
        "issued_supply": issued_supply,
        "wallet_total_balance": wallet_total,
        "diff_amount": diff_amount,
        "is_ok": is_ok,
        "run_id": run_id,
        "rule_id": supply_rule.rule_id if supply_rule else None,
    }

    exceptions: list[dict[str, Any]] = []
    severity = _evaluate_severity(diff_abs, supply_rule)
    if severity:
        exceptions.append(
            _build_exception(
                target_date=target_date,
                exception_type=EXCEPTION_SUPPLY,
                severity=severity,
                metric="supply_diff_abs",
                metric_value=diff_abs,
                run_id=run_id,
                rule_id=supply_rule.rule_id if supply_rule else None,
                message={
                    "issued_supply": str(issued_supply),
                    "wallet_total_balance": str(wallet_total),
                    "diff_amount": str(diff_amount),
                },
            )
        )

    return SupplyOutput(row=row, exceptions=exceptions)


def build_ops_payment_failure_daily(
    payment_orders: Iterable[Mapping[str, Any]],
    *,
    target_date: date,
    run_id: str,
    failed_statuses: Iterable[str] | None = None,
    rule_id: str | None = None,
) -> list[dict[str, Any]]:
    failed_status_set = {
        status for status in (failed_statuses or DEFAULT_FAILED_STATUSES)
    }
    aggregates: dict[str | None, dict[str, int]] = {}

    for record in payment_orders:
        created_at = _parse_datetime(
            record.get("created_at") or record.get("event_time")
        )
        if created_at is None:
            continue
        if date_kst(created_at) != target_date:
            continue

        merchant_name = record.get("merchant_name")
        key = str(merchant_name) if merchant_name is not None else None
        entry = aggregates.setdefault(key, {"total": 0, "failed": 0})
        entry["total"] += 1

        status = record.get("status")
        if status is not None and str(status) in failed_status_set:
            entry["failed"] += 1

    rows: list[dict[str, Any]] = []
    for merchant_name, counts in aggregates.items():
        total = counts["total"]
        failed = counts["failed"]
        failure_rate = Decimal("0")
        if total > 0:
            failure_rate = Decimal(str(failed)) / Decimal(str(total))

        rows.append(
            {
                "date_kst": target_date,
                "merchant_name": merchant_name,
                "total_cnt": total,
                "failed_cnt": failed,
                "failure_rate": failure_rate,
                "run_id": run_id,
                "rule_id": rule_id,
            }
        )

    return rows


def build_ops_payment_refund_daily(
    payment_orders: Iterable[Mapping[str, Any]],
    *,
    target_date: date,
    run_id: str,
    refunded_statuses: Iterable[str] | None = None,
    rule_id: str | None = None,
) -> list[dict[str, Any]]:
    refunded_status_set = {
        status for status in (refunded_statuses or DEFAULT_REFUNDED_STATUSES)
    }
    aggregates: dict[str | None, dict[str, int]] = {}

    for record in payment_orders:
        created_at = _parse_datetime(
            record.get("created_at") or record.get("event_time")
        )
        if created_at is None:
            continue
        if date_kst(created_at) != target_date:
            continue

        merchant_name = record.get("merchant_name")
        key = str(merchant_name) if merchant_name is not None else None
        entry = aggregates.setdefault(key, {"total": 0, "refunded": 0})
        entry["total"] += 1

        status = record.get("status")
        if status is not None and str(status) in refunded_status_set:
            entry["refunded"] += 1

    rows: list[dict[str, Any]] = []
    for merchant_name, counts in aggregates.items():
        total = counts["total"]
        refunded = counts["refunded"]
        refund_rate = Decimal("0")
        if total > 0:
            refund_rate = Decimal(str(refunded)) / Decimal(str(total))

        rows.append(
            {
                "date_kst": target_date,
                "merchant_name": merchant_name,
                "total_cnt": total,
                "refunded_cnt": refunded,
                "refund_rate": refund_rate,
                "run_id": run_id,
                "rule_id": rule_id,
            }
        )

    return rows


def build_ops_ledger_pairing_quality_daily(
    ledger_entries: Iterable[Mapping[str, Any]],
    *,
    target_date: date,
    run_id: str,
    payment_orders: Iterable[Mapping[str, Any]] | None = None,
    rule_id: str | None = None,
) -> dict[str, Any]:
    entries: list[Mapping[str, Any]] = []
    for record in ledger_entries:
        event_date = record.get("event_date_kst")
        if isinstance(event_date, date):
            event_kst = event_date
        else:
            event_time = _parse_datetime(record.get("event_time"))
            if event_time is None:
                continue
            event_kst = date_kst(event_time)
        if event_kst != target_date:
            continue
        entries.append(record)

    entry_cnt = len(entries)
    null_related = sum(1 for record in entries if not record.get("related_id"))
    related_id_null_rate = Decimal("0")
    if entry_cnt:
        related_id_null_rate = Decimal(str(null_related)) / Decimal(str(entry_cnt))

    groups: dict[str, list[Mapping[str, Any]]] = {}
    for record in entries:
        related_id = record.get("related_id")
        if related_id is None:
            continue
        key = str(related_id)
        groups.setdefault(key, []).append(record)

    total_groups = len(groups)
    pair_candidate_groups = 0
    for records in groups.values():
        wallets = {
            record.get("wallet_id") for record in records if record.get("wallet_id")
        }
        if len(records) == 2 and len(wallets) == 2:
            pair_candidate_groups += 1

    pair_candidate_rate = Decimal("0")
    if total_groups:
        pair_candidate_rate = Decimal(str(pair_candidate_groups)) / Decimal(
            str(total_groups)
        )

    join_rate = Decimal("0")
    if payment_orders is not None and total_groups:
        order_ids = {
            str(record.get("order_id"))
            for record in payment_orders
            if record.get("order_id") is not None
        }
        joined = sum(1 for related_id in groups if related_id in order_ids)
        join_rate = Decimal(str(joined)) / Decimal(str(total_groups))

    return {
        "date_kst": target_date,
        "entry_cnt": entry_cnt,
        "related_id_null_rate": related_id_null_rate,
        "pair_candidate_rate": pair_candidate_rate,
        "join_payment_orders_rate": join_rate,
        "run_id": run_id,
        "rule_id": rule_id,
    }


def build_admin_tx_search(
    ledger_entries: Iterable[Mapping[str, Any]],
    *,
    target_date: date,
    run_id: str,
    rule_id: str | None = None,
) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    groups: dict[str, list[str]] = {}

    for record in ledger_entries:
        event_date = record.get("event_date_kst")
        if isinstance(event_date, date):
            event_kst = event_date
        else:
            event_time = _parse_datetime(record.get("event_time"))
            if event_time is None:
                continue
            event_kst = date_kst(event_time)
        if event_kst != target_date:
            continue

        tx_id = record.get("tx_id")
        wallet_id = record.get("wallet_id")
        if not tx_id or not wallet_id:
            continue

        related_id = record.get("related_id")
        related_key = str(related_id) if related_id is not None else None
        if related_key:
            groups.setdefault(related_key, []).append(str(tx_id))

        entries.append(
            {
                "event_date_kst": event_kst,
                "tx_id": str(tx_id),
                "wallet_id": str(wallet_id),
                "entry_type": record.get("entry_type") or record.get("type"),
                "amount": _parse_decimal(record.get("amount")),
                "amount_signed": _parse_decimal(record.get("amount_signed")),
                "event_time": _parse_datetime(record.get("event_time")),
                "related_id": related_key,
                "related_type": record.get("related_type"),
                "merchant_name": record.get("merchant_name"),
                "payment_status": record.get("status"),
                "paired_tx_id": None,
                "run_id": run_id,
                "rule_id": rule_id,
            }
        )

    paired_lookup: dict[str, str] = {}
    for related_id, tx_ids in groups.items():
        unique_ids = list(dict.fromkeys(tx_ids))
        if len(unique_ids) == 2:
            paired_lookup[unique_ids[0]] = unique_ids[1]
            paired_lookup[unique_ids[1]] = unique_ids[0]

    for entry in entries:
        tx_id = entry.get("tx_id")
        if tx_id in paired_lookup:
            entry["paired_tx_id"] = paired_lookup[tx_id]

    return entries
