from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from src.consumer.dlq import _to_text, prune_dlq_db, write_dlq_db, write_dlq_file


def test_write_dlq_file_creates_jsonl(tmp_path):
    path = str(tmp_path / "dlq" / "events.jsonl")
    payload = {"topic": "ledger.entry.upserted", "error": "bad field"}

    write_dlq_file(path, payload)

    lines = (tmp_path / "dlq" / "events.jsonl").read_text().strip().split("\n")
    assert len(lines) == 1
    assert json.loads(lines[0]) == payload


def test_write_dlq_file_appends(tmp_path):
    path = str(tmp_path / "events.jsonl")
    write_dlq_file(path, {"seq": 1})
    write_dlq_file(path, {"seq": 2})

    lines = (tmp_path / "events.jsonl").read_text().strip().split("\n")
    assert len(lines) == 2
    assert json.loads(lines[0])["seq"] == 1
    assert json.loads(lines[1])["seq"] == 2


def test_write_dlq_db_inserts_row():
    session = MagicMock()

    payload = {
        "topic": "ledger.entry.upserted",
        "error": "validation failed",
        "partition": 0,
        "offset": 42,
        "key": "tx-1",
        "payload": {"tx_id": "tx-1"},
        "correlation_id": "corr-1",
    }

    write_dlq_db(session, payload)

    session.add.assert_called_once()
    event = session.add.call_args[0][0]
    assert event.topic == "ledger.entry.upserted"
    assert event.error == "validation failed"
    assert event.partition == 0
    assert event.offset == 42


def test_prune_dlq_db_deletes_old():
    mock_result = SimpleNamespace(rowcount=5)
    session = MagicMock()
    session.execute.return_value = mock_result

    count = prune_dlq_db(session, retention_days=14)

    assert count == 5
    session.execute.assert_called_once()


def test_prune_dlq_db_zero_retention():
    session = MagicMock()

    count = prune_dlq_db(session, retention_days=0)

    assert count == 0
    session.execute.assert_not_called()


@pytest.mark.parametrize(
    "value,expected",
    [
        (None, None),
        ("hello", "hello"),
        ({"key": "val"}, '{"key": "val"}'),
        (12345, "12345"),
    ],
)
def test_to_text_handles_types(value, expected):
    assert _to_text(value) == expected
