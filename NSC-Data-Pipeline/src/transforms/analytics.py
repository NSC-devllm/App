from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable, Mapping

from src.common.time_utils import date_kst, now_utc, to_utc


@dataclass(frozen=True)
class TransformResult:
    records: list[dict[str, Any]]
    bad_records: list[dict[str, Any]]


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


def _parse_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return date_kst(value)
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value)
        except ValueError:
            return None
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


def _parse_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return None


def build_bad_record(
    record: Mapping[str, Any],
    *,
    reason: str,
    source_table: str,
    run_id: str,
) -> dict[str, Any]:
    detected_at = now_utc()
    return {
        "detected_date_kst": date_kst(detected_at),
        "source_table": source_table,
        "reason": reason,
        "record_json": json.dumps(record, default=str, ensure_ascii=True),
        "run_id": run_id,
        "rule_id": None,
        "detected_at": detected_at,
    }


def transform_order_events_records(
    orders: Iterable[Mapping[str, Any]],
    payment_orders: Iterable[Mapping[str, Any]],
    *,
    run_id: str,
) -> TransformResult:
    valid: list[dict[str, Any]] = []
    bad: list[dict[str, Any]] = []

    for record in orders:
        order_id = record.get("order_id")
        if order_id is None or order_id == "":
            bad.append(
                build_bad_record(
                    record,
                    reason="missing_order_id",
                    source_table="silver.order_events",
                    run_id=run_id,
                )
            )
            continue

        event_time = _parse_datetime(
            record.get("created_at") or record.get("event_time")
        )
        valid.append(
            {
                "order_ref": str(order_id),
                "order_source": "ORDERS",
                "user_id": record.get("user_id"),
                "merchant_name": None,
                "amount": _parse_decimal(record.get("total_amount")),
                "status": record.get("status"),
                "event_time": event_time,
                "event_date_kst": date_kst(event_time) if event_time else None,
                "run_id": run_id,
            }
        )

    for record in payment_orders:
        order_id = record.get("order_id")
        if order_id is None or order_id == "":
            bad.append(
                build_bad_record(
                    record,
                    reason="missing_order_id",
                    source_table="silver.order_events",
                    run_id=run_id,
                )
            )
            continue

        event_time = _parse_datetime(
            record.get("created_at") or record.get("event_time")
        )
        valid.append(
            {
                "order_ref": str(order_id),
                "order_source": "PAYMENT_ORDERS",
                "user_id": record.get("user_id"),
                "merchant_name": record.get("merchant_name"),
                "amount": _parse_decimal(record.get("amount")),
                "status": record.get("status"),
                "event_time": event_time,
                "event_date_kst": date_kst(event_time) if event_time else None,
                "run_id": run_id,
            }
        )

    return TransformResult(records=valid, bad_records=bad)


def transform_order_items_records(
    records: Iterable[Mapping[str, Any]],
    *,
    run_id: str,
) -> TransformResult:
    valid: list[dict[str, Any]] = []
    bad: list[dict[str, Any]] = []

    for record in records:
        item_id = _parse_int(record.get("item_id"))
        order_id = _parse_int(record.get("order_id"))
        product_id = _parse_int(record.get("product_id"))

        if item_id is None or order_id is None or product_id is None:
            bad.append(
                build_bad_record(
                    record,
                    reason="missing_required_keys",
                    source_table="silver.order_items",
                    run_id=run_id,
                )
            )
            continue

        valid.append(
            {
                "item_id": item_id,
                "order_id": order_id,
                "order_ref": str(order_id),
                "product_id": product_id,
                "quantity": _parse_int(record.get("quantity")),
                "price_at_purchase": _parse_decimal(record.get("price_at_purchase")),
                "run_id": run_id,
            }
        )

    return TransformResult(records=valid, bad_records=bad)


def transform_products_records(
    records: Iterable[Mapping[str, Any]],
    *,
    run_id: str,
) -> TransformResult:
    valid: list[dict[str, Any]] = []
    bad: list[dict[str, Any]] = []

    for record in records:
        product_id = _parse_int(record.get("product_id"))
        if product_id is None:
            bad.append(
                build_bad_record(
                    record,
                    reason="missing_product_id",
                    source_table="silver.products",
                    run_id=run_id,
                )
            )
            continue

        valid.append(
            {
                "product_id": product_id,
                "product_name": record.get("product_name"),
                "category": record.get("category"),
                "price_krw": _parse_decimal(record.get("price_krw")),
                "is_display": record.get("is_display"),
                "run_id": run_id,
            }
        )

    return TransformResult(records=valid, bad_records=bad)


def derive_user_key(user_id: str | None, *, salt: str) -> str | None:
    if user_id is None:
        return None
    if not salt:
        raise ValueError("salt is required for user_key anonymization")
    payload = f"{user_id}{salt}".encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _select_category_for_order(
    order_items: Iterable[Mapping[str, Any]],
    products: Mapping[str, str],
) -> dict[str, str]:
    best_by_order: dict[str, dict[str, Any]] = {}

    for item in order_items:
        order_ref = item.get("order_ref")
        if order_ref is None or order_ref == "":
            continue

        product_id = item.get("product_id")
        if product_id is None:
            continue
        category = products.get(str(product_id))
        if category is None:
            continue

        price = _parse_decimal(item.get("price_at_purchase")) or Decimal("0")
        quantity = _parse_int(item.get("quantity")) or 1
        line_amount = price * Decimal(quantity)
        item_id = _parse_int(item.get("item_id"))

        current = best_by_order.get(str(order_ref))
        if current is None:
            best_by_order[str(order_ref)] = {
                "category": category,
                "amount": line_amount,
                "item_id": item_id,
            }
            continue

        if line_amount > current["amount"]:
            best_by_order[str(order_ref)] = {
                "category": category,
                "amount": line_amount,
                "item_id": item_id,
            }
            continue

        if line_amount == current["amount"]:
            current_id = current.get("item_id")
            if item_id is not None and (current_id is None or item_id < current_id):
                best_by_order[str(order_ref)] = {
                    "category": category,
                    "amount": line_amount,
                    "item_id": item_id,
                }

    return {
        order_ref: payload["category"] for order_ref, payload in best_by_order.items()
    }


def build_fact_payment_anonymized(
    order_events: Iterable[Mapping[str, Any]],
    order_items: Iterable[Mapping[str, Any]],
    products: Iterable[Mapping[str, Any]],
    *,
    run_id: str,
    salt: str,
    order_source_filter: str | None = "PAYMENT_ORDERS",
) -> list[dict[str, Any]]:
    if not salt:
        raise ValueError("salt is required for user_key anonymization")

    product_categories: dict[str, str] = {}
    for product in products:
        product_id = product.get("product_id")
        category = product.get("category")
        if product_id is None or category is None:
            continue
        product_categories[str(product_id)] = str(category)

    categories_by_order = _select_category_for_order(order_items, product_categories)

    rows: list[dict[str, Any]] = []
    for record in order_events:
        if order_source_filter and record.get("order_source") != order_source_filter:
            continue

        order_ref = record.get("order_ref")
        if not order_ref:
            continue

        user_id = record.get("user_id")
        if not user_id:
            continue

        event_date = _parse_date(record.get("event_date_kst"))
        if event_date is None:
            event_time = _parse_datetime(record.get("event_time"))
            if event_time is not None:
                event_date = date_kst(event_time)

        if event_date is None:
            continue

        rows.append(
            {
                "date_kst": event_date,
                "user_key": derive_user_key(str(user_id), salt=salt),
                "merchant_name": record.get("merchant_name"),
                "amount": _parse_decimal(record.get("amount")),
                "status": record.get("status"),
                "category": categories_by_order.get(str(order_ref)),
                "run_id": run_id,
            }
        )

    return rows
