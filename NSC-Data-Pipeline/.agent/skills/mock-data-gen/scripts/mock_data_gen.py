#!/usr/bin/env python3
"""
Mock data generator for fixtures and mock_data scenarios.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass
from datetime import datetime, date, timezone, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Dict, List, Optional

BASE_TS = datetime(2026, 2, 1, 0, 0, 0, tzinfo=timezone.utc)
BASE_DATE = BASE_TS.date()

ENTRY_TYPE_SIGN = {
    "CHARGE": Decimal("1"),
    "WITHDRAW": Decimal("-1"),
    "PAYMENT": Decimal("-1"),
    "RECEIVE": Decimal("1"),
    "REFUND_OUT": Decimal("-1"),
    "REFUND_IN": Decimal("1"),
    "HOLD": Decimal("0"),
    "RELEASE": Decimal("0"),
}

SCENARIOS = {
    "one_day_normal",
    "stale_source",
    "duplicate_tx",
    "bad_amount",
    "multi_day_backfill",
}

SCENARIO_TABLES = [
    "user_wallets",
    "transaction_ledger",
    "payment_orders",
    "orders",
    "order_items",
    "products",
]


@dataclass
class ColumnSchema:
    name: str
    raw_type: str
    required: bool
    meaning: str
    quality: str


def _find_idx(headers: List[str], keyword: str) -> Optional[int]:
    for idx, header in enumerate(headers):
        if keyword in header:
            return idx
    return None


def parse_contract(path: Path) -> Dict[str, List[ColumnSchema]]:
    text = path.read_text(encoding="utf-8")
    lines = text.splitlines()
    tables: Dict[str, List[ColumnSchema]] = {}
    current_table: Optional[str] = None

    for i, line in enumerate(lines):
        names = re.findall(r"`([^`]+)`", line)
        if names and (line.lstrip().startswith("###") or line.strip().startswith("**") or "**`" in line):
            if len(names) == 1:
                current_table = names[0].strip()

        if line.strip().startswith("|") and "컬럼" in line and "타입" in line:
            header = [h.strip() for h in line.strip().strip("|").split("|")]
            col_idx = _find_idx(header, "컬럼")
            type_idx = _find_idx(header, "타입")
            req_idx = _find_idx(header, "필수")
            meaning_idx = _find_idx(header, "의미")
            quality_idx = _find_idx(header, "품질")

            rows: List[ColumnSchema] = []
            j = i + 2
            while j < len(lines) and lines[j].strip().startswith("|"):
                cells = [c.strip() for c in lines[j].strip().strip("|").split("|")]
                if col_idx is None or col_idx >= len(cells):
                    j += 1
                    continue
                col_name = cells[col_idx].strip().strip('`')
                if not col_name or col_name.startswith("---"):
                    j += 1
                    continue
                raw_type = cells[type_idx] if type_idx is not None and type_idx < len(cells) else ""
                required_cell = cells[req_idx] if req_idx is not None and req_idx < len(cells) else ""
                required = "✅" in required_cell
                meaning = cells[meaning_idx] if meaning_idx is not None and meaning_idx < len(cells) else ""
                quality = cells[quality_idx] if quality_idx is not None and quality_idx < len(cells) else ""
                rows.append(
                    ColumnSchema(
                        name=col_name,
                        raw_type=raw_type,
                        required=required,
                        meaning=meaning,
                        quality=quality,
                    )
                )
                j += 1

            if current_table:
                tables[current_table] = rows

    if not tables:
        raise ValueError(f"No schemas parsed from {path}")
    return tables


def normalize_type(raw_type: str) -> str:
    t = raw_type.lower()
    if "decimal" in t:
        return "decimal"
    if "timestamp" in t:
        return "timestamp"
    if t.startswith("date") or " date" in t:
        return "date"
    if "bigint" in t or "int" in t:
        return "int"
    if "boolean" in t or "bool" in t:
        return "bool"
    return "string"


def default_decimal_for_name(name: str) -> Decimal:
    name_l = name.lower()
    if "balance" in name_l:
        return Decimal("1000.00")
    if "amount" in name_l:
        return Decimal("100.00")
    return Decimal("1.00")


def default_value(col: ColumnSchema, seq: int) -> object:
    name_l = col.name.lower()
    kind = normalize_type(col.raw_type)

    if name_l in {"user_id", "wallet_id"}:
        return f"user_{seq}"
    if name_l == "tx_id":
        return f"tx_{seq}"
    if name_l == "order_id":
        return 1000 + seq if kind == "int" else f"order_{1000 + seq}"
    if name_l.endswith("_id") or name_l.endswith("id"):
        return 1000 + seq if kind == "int" else f"{col.name}_{seq}"

    if kind == "decimal":
        return default_decimal_for_name(col.name)
    if kind == "int":
        if "quantity" in name_l or "seq" in name_l:
            return 1
        return seq
    if kind == "bool":
        return True
    if kind == "timestamp":
        return BASE_TS
    if kind == "date":
        return BASE_DATE

    if "status" in name_l:
        return "OK"
    if "type" in name_l:
        return "CHARGE"
    if "merchant" in name_l:
        return "Shop A"
    if "product_name" in name_l:
        return "Widget"
    if "category" in name_l:
        return "general"

    return "sample"


def cast_value(value: object, col: ColumnSchema) -> object:
    kind = normalize_type(col.raw_type)
    if value is None:
        return None
    if kind == "decimal":
        return value if isinstance(value, Decimal) else Decimal(str(value))
    if kind == "timestamp":
        return value if isinstance(value, datetime) else BASE_TS
    if kind == "date":
        return value if isinstance(value, date) else BASE_DATE
    if kind == "int":
        return int(value)
    if kind == "bool":
        return bool(value)
    return value


def kst_date(dt_value: datetime) -> date:
    return (dt_value + timedelta(hours=9)).date()


def apply_derivations(row: Dict[str, object], schema: List[ColumnSchema]) -> None:
    cols = [c.name for c in schema]

    if "order_ref" in cols and not row.get("order_ref") and row.get("order_id") is not None:
        row["order_ref"] = str(row["order_id"])

    if "balance_total" in cols:
        bal_avail = row.get("balance_available")
        bal_frozen = row.get("balance_frozen")
        if isinstance(bal_avail, Decimal) and isinstance(bal_frozen, Decimal):
            row["balance_total"] = bal_avail + bal_frozen

    if "amount_signed" in cols:
        if row.get("amount_signed") is None:
            entry_type = row.get("entry_type") or row.get("type")
            amount = row.get("amount")
            if isinstance(amount, Decimal) and isinstance(entry_type, str):
                sign = ENTRY_TYPE_SIGN.get(entry_type, Decimal("1"))
                row["amount_signed"] = amount * sign

    for col in cols:
        if not col.endswith("_date_kst"):
            continue
        if col.startswith("snapshot"):
            source = row.get("snapshot_ts")
        elif col.startswith("event"):
            source = row.get("event_time") or row.get("created_at")
        else:
            source = row.get("event_time") or row.get("snapshot_ts") or row.get("created_at")
        if isinstance(source, datetime):
            row[col] = kst_date(source)
        else:
            row[col] = BASE_DATE


def make_row(schema: List[ColumnSchema], overrides: Optional[Dict[str, object]] = None, seq: int = 1) -> Dict[str, object]:
    row: Dict[str, object] = {}
    overrides = overrides or {}
    for col in schema:
        if col.name in overrides:
            row[col.name] = cast_value(overrides[col.name], col)
        else:
            row[col.name] = cast_value(default_value(col, seq), col)
    apply_derivations(row, schema)
    return row


def make_edge_row(schema: List[ColumnSchema], base_row: Dict[str, object]) -> Dict[str, object]:
    edge = dict(base_row)
    for col in schema:
        kind = normalize_type(col.raw_type)
        if kind == "decimal":
            edge[col.name] = Decimal("0.00")
        elif kind == "int":
            edge[col.name] = 0
        elif kind == "bool":
            edge[col.name] = False
        elif kind == "timestamp":
            edge[col.name] = BASE_TS + timedelta(hours=23, minutes=59, seconds=59)
        elif kind == "date":
            edge[col.name] = BASE_DATE
        elif kind == "string":
            edge[col.name] = ""
    apply_derivations(edge, schema)
    return edge


def make_error_rows(schema: List[ColumnSchema], base_row: Dict[str, object]) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    error = dict(base_row)
    required_cols = [c.name for c in schema if c.required]
    if required_cols:
        error[required_cols[0]] = None
    else:
        error[next(iter(base_row.keys()))] = None
    apply_derivations(error, schema)
    rows.append(error)

    for dup_col in ("tx_id", "order_id", "user_id"):
        if dup_col in base_row:
            rows.append(dict(base_row))
            break
    return rows


def generate_table_rows(schema: List[ColumnSchema]) -> Dict[str, List[Dict[str, object]]]:
    base = make_row(schema)
    return {
        "happy": [base],
        "edge": [make_edge_row(schema, base)],
        "error": make_error_rows(schema, base),
    }


def resolve_table_name(name: str, schemas: Dict[str, List[ColumnSchema]]) -> str:
    if name in schemas:
        return name
    if "." not in name:
        matches = [t for t in schemas if t.endswith(f".{name}")]
        if len(matches) == 1:
            return matches[0]
    raise KeyError(f"Table '{name}' not found in schemas")


def scenario_one_day_normal(schemas: Dict[str, List[ColumnSchema]]) -> Dict[str, List[Dict[str, object]]]:
    buyer = "user_1"
    seller = "user_2"
    order_id = 1001
    payment_order_id = "po_1001"
    amount = Decimal("100.00")

    rows: Dict[str, List[Dict[str, object]]] = {}
    rows["user_wallets"] = [
        make_row(schemas["user_wallets"], {
            "user_id": buyer,
            "balance": Decimal("1000.00"),
            "frozen_amount": Decimal("0.00"),
            "updated_at": BASE_TS,
        }, seq=1),
        make_row(schemas["user_wallets"], {
            "user_id": seller,
            "balance": Decimal("500.00"),
            "frozen_amount": Decimal("0.00"),
            "updated_at": BASE_TS,
        }, seq=2),
    ]

    rows["transaction_ledger"] = [
        make_row(schemas["transaction_ledger"], {
            "tx_id": "tx_pay_1",
            "wallet_id": buyer,
            "type": "PAYMENT",
            "amount": amount,
            "related_id": payment_order_id,
            "created_at": BASE_TS,
        }, seq=1),
        make_row(schemas["transaction_ledger"], {
            "tx_id": "tx_recv_1",
            "wallet_id": seller,
            "type": "RECEIVE",
            "amount": amount,
            "related_id": payment_order_id,
            "created_at": BASE_TS,
        }, seq=2),
    ]

    rows["payment_orders"] = [
        make_row(schemas["payment_orders"], {
            "order_id": payment_order_id,
            "user_id": buyer,
            "merchant_name": "Shop A",
            "amount": amount,
            "status": "PAID",
            "created_at": BASE_TS,
        }, seq=1)
    ]

    rows["orders"] = [
        make_row(schemas["orders"], {
            "order_id": order_id,
            "user_id": buyer,
            "total_amount": amount,
            "status": "PAID",
            "created_at": BASE_TS,
        }, seq=1)
    ]

    rows["order_items"] = [
        make_row(schemas["order_items"], {
            "item_id": 5001,
            "order_id": order_id,
            "product_id": 9001,
            "quantity": 1,
            "price_at_purchase": amount,
        }, seq=1)
    ]

    rows["products"] = [
        make_row(schemas["products"], {
            "product_id": 9001,
            "product_name": "Widget",
            "price_krw": amount,
            "stock_quantity": 10,
            "category": "general",
            "is_display": True,
        }, seq=1)
    ]

    return rows


def scenario_stale_source(schemas: Dict[str, List[ColumnSchema]]) -> Dict[str, List[Dict[str, object]]]:
    rows = scenario_one_day_normal(schemas)
    stale_ts = BASE_TS - timedelta(days=7)
    for table_rows in rows.values():
        for row in table_rows:
            for key in ("updated_at", "created_at"):
                if key in row:
                    row[key] = stale_ts
    return rows


def scenario_duplicate_tx(schemas: Dict[str, List[ColumnSchema]]) -> Dict[str, List[Dict[str, object]]]:
    rows = scenario_one_day_normal(schemas)
    if "transaction_ledger" in rows and rows["transaction_ledger"]:
        dup = dict(rows["transaction_ledger"][0])
        rows["transaction_ledger"].append(dup)
    return rows


def scenario_bad_amount(schemas: Dict[str, List[ColumnSchema]]) -> Dict[str, List[Dict[str, object]]]:
    rows = scenario_one_day_normal(schemas)
    bad_rows = [
        make_row(schemas["transaction_ledger"], {
            "tx_id": "tx_bad_1",
            "wallet_id": "user_1",
            "type": "PAYMENT",
            "amount": None,
            "related_id": "po_bad_1",
            "created_at": BASE_TS,
        }, seq=1),
        make_row(schemas["transaction_ledger"], {
            "tx_id": "tx_bad_2",
            "wallet_id": "user_1",
            "type": "PAYMENT",
            "amount": Decimal("0.00"),
            "related_id": "po_bad_2",
            "created_at": BASE_TS,
        }, seq=2),
        make_row(schemas["transaction_ledger"], {
            "tx_id": "tx_bad_3",
            "wallet_id": "user_1",
            "type": "PAYMENT",
            "amount": Decimal("-10.00"),
            "related_id": "po_bad_3",
            "created_at": BASE_TS,
        }, seq=3),
    ]
    rows["transaction_ledger"] = bad_rows

    rows["payment_orders"] = [
        make_row(schemas["payment_orders"], {
            "order_id": "po_bad_1",
            "user_id": "user_1",
            "merchant_name": "Shop A",
            "amount": None,
            "status": "FAILED",
            "created_at": BASE_TS,
        }, seq=1)
    ]
    return rows


def scenario_multi_day_backfill(schemas: Dict[str, List[ColumnSchema]]) -> Dict[str, List[Dict[str, object]]]:
    rows: Dict[str, List[Dict[str, object]]] = {name: [] for name in SCENARIO_TABLES}
    buyer = "user_1"
    seller = "user_2"
    amount = Decimal("100.00")

    for day in range(3):
        ts = BASE_TS + timedelta(days=day)
        order_id = 1001 + day
        payment_order_id = f"po_{1001 + day}"

        rows["user_wallets"].append(
            make_row(schemas["user_wallets"], {
                "user_id": buyer,
                "balance": Decimal("1000.00"),
                "frozen_amount": Decimal("0.00"),
                "updated_at": ts,
            }, seq=1)
        )

        rows["transaction_ledger"].append(
            make_row(schemas["transaction_ledger"], {
                "tx_id": f"tx_pay_{day}",
                "wallet_id": buyer,
                "type": "PAYMENT",
                "amount": amount,
                "related_id": payment_order_id,
                "created_at": ts,
            }, seq=1)
        )
        rows["transaction_ledger"].append(
            make_row(schemas["transaction_ledger"], {
                "tx_id": f"tx_recv_{day}",
                "wallet_id": seller,
                "type": "RECEIVE",
                "amount": amount,
                "related_id": payment_order_id,
                "created_at": ts,
            }, seq=2)
        )

        rows["payment_orders"].append(
            make_row(schemas["payment_orders"], {
                "order_id": payment_order_id,
                "user_id": buyer,
                "merchant_name": "Shop A",
                "amount": amount,
                "status": "PAID",
                "created_at": ts,
            }, seq=1)
        )

        rows["orders"].append(
            make_row(schemas["orders"], {
                "order_id": order_id,
                "user_id": buyer,
                "total_amount": amount,
                "status": "PAID",
                "created_at": ts,
            }, seq=1)
        )

        rows["order_items"].append(
            make_row(schemas["order_items"], {
                "item_id": 5001 + day,
                "order_id": order_id,
                "product_id": 9001,
                "quantity": 1,
                "price_at_purchase": amount,
            }, seq=1)
        )

    rows["products"].append(
        make_row(schemas["products"], {
            "product_id": 9001,
            "product_name": "Widget",
            "price_krw": amount,
            "stock_quantity": 10,
            "category": "general",
            "is_display": True,
        }, seq=1)
    )

    return rows


def generate_scenario(name: str, schemas: Dict[str, List[ColumnSchema]]) -> Dict[str, List[Dict[str, object]]]:
    if name == "one_day_normal":
        return scenario_one_day_normal(schemas)
    if name == "stale_source":
        return scenario_stale_source(schemas)
    if name == "duplicate_tx":
        return scenario_duplicate_tx(schemas)
    if name == "bad_amount":
        return scenario_bad_amount(schemas)
    if name == "multi_day_backfill":
        return scenario_multi_day_backfill(schemas)
    raise ValueError(f"Unknown scenario: {name}")


def sanitize_identifier(value: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_]", "_", value)


def to_py_literal(value: object) -> str:
    if value is None:
        return "None"
    if isinstance(value, str):
        return repr(value)
    if isinstance(value, bool):
        return "True" if value else "False"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, Decimal):
        return f"Decimal('{value}')"
    if isinstance(value, datetime):
        return (
            f"datetime({value.year}, {value.month}, {value.day}, "
            f"{value.hour}, {value.minute}, {value.second}, tzinfo=timezone.utc)"
        )
    if isinstance(value, date):
        return f"date({value.year}, {value.month}, {value.day})"
    return repr(value)


def render_rows_lines(rows: List[Dict[str, object]], indent: str = "    ") -> List[str]:
    lines = [f"{indent}return ["]
    inner = indent + "    "
    for row in rows:
        lines.append(f"{inner}{{")
        for key, val in row.items():
            lines.append(f"{inner}    {key!r}: {to_py_literal(val)},")
        lines.append(f"{inner}}},")
    lines.append(f"{indent}]")
    return lines


def render_table_fixture(table_name: str, table_rows: Dict[str, List[Dict[str, object]]]) -> str:
    prefix = sanitize_identifier(table_name)
    lines = [
        "import pytest",
        "from datetime import datetime, date, timezone",
        "from decimal import Decimal",
        "",
    ]

    for variant in ("happy", "edge", "error"):
        fixture_name = f"{prefix}_{variant}_rows"
        lines.append("@pytest.fixture")
        lines.append(f"def {fixture_name}():")
        lines.extend(render_rows_lines(table_rows[variant]))
        lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def render_scenario_fixture(name: str, scenario_rows: Dict[str, List[Dict[str, object]]]) -> str:
    fixture_name = f"scenario_{sanitize_identifier(name)}_tables"
    lines = [
        "import pytest",
        "from datetime import datetime, date, timezone",
        "from decimal import Decimal",
        "",
        "@pytest.fixture",
        f"def {fixture_name}():",
        "    return {",
    ]
    for table in SCENARIO_TABLES:
        rows = scenario_rows.get(table, [])
        lines.append(f"        {table!r}: [")
        for row in rows:
            lines.append("            {")
            for key, val in row.items():
                lines.append(f"                {key!r}: {to_py_literal(val)},")
            lines.append("            },")
        lines.append("        ],")
    lines.append("    }")
    return "\n".join(lines).rstrip() + "\n"


def json_safe(value: object) -> object:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat().replace("+00:00", "Z")
    if isinstance(value, date):
        return value.isoformat()
    return value


def write_mock_data_files(
    scenario: str,
    scenario_rows: Dict[str, List[Dict[str, object]]],
    mock_data_root: Path,
    output_format: str,
) -> None:
    for table, rows in scenario_rows.items():
        if table not in SCENARIO_TABLES:
            continue
        scenario_dir = mock_data_root / "scenarios" / scenario
        bronze_dir = mock_data_root / "bronze" / f"{table}_raw" / scenario
        scenario_dir.mkdir(parents=True, exist_ok=True)
        bronze_dir.mkdir(parents=True, exist_ok=True)

        filename = f"{table}.{output_format}" if output_format == "json" else f"{table}.jsonl"
        scenario_path = scenario_dir / filename
        bronze_path = bronze_dir / "data.jsonl"

        if output_format == "json":
            payload = [
                {k: json_safe(v) for k, v in row.items()}
                for row in rows
            ]
            scenario_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        else:
            with scenario_path.open("w", encoding="utf-8") as handle:
                for row in rows:
                    handle.write(json.dumps({k: json_safe(v) for k, v in row.items()}, ensure_ascii=False) + "\n")

        with bronze_path.open("w", encoding="utf-8") as handle:
            for row in rows:
                handle.write(json.dumps({k: json_safe(v) for k, v in row.items()}, ensure_ascii=False) + "\n")

    fixture_dir = mock_data_root / "fixtures"
    fixture_dir.mkdir(parents=True, exist_ok=True)
    dim_rule_path = fixture_dir / "dim_rule_scd2.json"
    if not dim_rule_path.exists():
        dim_rule_path.write_text(
            json.dumps([{"rule_id": "RULE_0001"}], indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )


def write_fixtures(output: str, content: str) -> None:
    if output == "-":
        sys.stdout.write(content)
        return
    output_path = Path(output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(content, encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate mock data fixtures and scenario files.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--table", help="Generate fixture rows for a single table.")
    group.add_argument("--scenario", help="Generate a multi-table scenario dataset.")

    parser.add_argument("--doc", default=".specs/data_contract.md", help="Path to data contract markdown.")
    parser.add_argument(
        "--fixtures-out",
        nargs="?",
        const="tests/unit/fixtures/mock_data_fixtures.py",
        default="-",
        help="Output path for fixture code. Use '-' for stdout.",
    )
    parser.add_argument("--mock-data-root", default="mock_data", help="Root path for mock_data outputs.")
    parser.add_argument("--format", choices=["json", "jsonl"], default="jsonl", help="mock_data file format.")

    args = parser.parse_args()

    schemas = parse_contract(Path(args.doc))

    if args.table:
        table_name = resolve_table_name(args.table, schemas)
        table_rows = generate_table_rows(schemas[table_name])
        content = render_table_fixture(table_name, table_rows)
        write_fixtures(args.fixtures_out, content)
        return

    scenario_name = args.scenario
    if scenario_name not in SCENARIOS:
        raise ValueError(f"Scenario must be one of: {', '.join(sorted(SCENARIOS))}")

    scenario_rows = generate_scenario(scenario_name, schemas)
    content = render_scenario_fixture(scenario_name, scenario_rows)
    write_fixtures(args.fixtures_out, content)
    write_mock_data_files(scenario_name, scenario_rows, Path(args.mock_data_root), args.format)


if __name__ == "__main__":
    main()
