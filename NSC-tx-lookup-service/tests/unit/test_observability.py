from __future__ import annotations

from src.common.observability import (
    correlation_context,
    extract_correlation_id_from_headers,
    extract_correlation_id_from_payload,
    get_correlation_id,
    normalize_correlation_id,
)


def test_correlation_context_sets_and_resets() -> None:
    before = get_correlation_id()

    with correlation_context("test-corr-123") as cid:
        assert cid == "test-corr-123"
        assert get_correlation_id() == "test-corr-123"

    assert get_correlation_id() == before


def test_correlation_context_generates_when_none() -> None:
    with correlation_context(None) as cid:
        assert cid is not None
        assert len(cid) > 0
        assert cid != "-"


def test_normalize_correlation_id_bytes() -> None:
    result = normalize_correlation_id(b"corr-bytes-123")
    assert result == "corr-bytes-123"


def test_normalize_correlation_id_none() -> None:
    assert normalize_correlation_id(None) is None


def test_normalize_correlation_id_empty_string() -> None:
    assert normalize_correlation_id("") is None
    assert normalize_correlation_id("  ") is None


def test_extract_from_headers() -> None:
    headers = [
        ("x-correlation-id", b"hdr-corr-456"),
        ("content-type", b"application/json"),
    ]
    result = extract_correlation_id_from_headers(headers)
    assert result == "hdr-corr-456"


def test_extract_from_headers_none() -> None:
    assert extract_correlation_id_from_headers(None) is None
    assert extract_correlation_id_from_headers([]) is None


def test_extract_from_payload() -> None:
    payload = {"correlation_id": "payload-corr-789", "tx_id": "tx-1"}
    result = extract_correlation_id_from_payload(payload)
    assert result == "payload-corr-789"


def test_extract_from_payload_missing() -> None:
    result = extract_correlation_id_from_payload({"tx_id": "tx-1"})
    assert result is None
