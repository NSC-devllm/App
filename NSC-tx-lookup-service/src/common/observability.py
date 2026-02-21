from __future__ import annotations

import contextvars
import uuid
from contextlib import contextmanager
from typing import Iterable, Mapping

CORRELATION_ID_HEADER = "X-Correlation-ID"

_CORRELATION_ID: contextvars.ContextVar[str] = contextvars.ContextVar(
    "correlation_id", default="-"
)


def normalize_correlation_id(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, bytes):
        try:
            value = value.decode("utf-8")
        except Exception:
            return None
    text = str(value).strip()
    return text or None


def generate_correlation_id() -> str:
    return str(uuid.uuid4())


def set_correlation_id(value: object | None) -> contextvars.Token[str]:
    normalized = normalize_correlation_id(value)
    return _CORRELATION_ID.set(normalized or generate_correlation_id())


def reset_correlation_id(token: contextvars.Token[str]) -> None:
    _CORRELATION_ID.reset(token)


def get_correlation_id() -> str:
    return _CORRELATION_ID.get()


@contextmanager
def correlation_context(value: object | None) -> Iterable[str]:
    token = set_correlation_id(value)
    try:
        yield get_correlation_id()
    finally:
        reset_correlation_id(token)


def extract_correlation_id_from_headers(
    headers: Iterable[tuple[str, bytes]] | None,
) -> str | None:
    if not headers:
        return None
    normalized = {key.lower(): value for key, value in headers if key}
    for key in ("x-correlation-id", "correlation-id", "correlation_id"):
        if key in normalized:
            return normalize_correlation_id(normalized[key])
    return None


def extract_correlation_id_from_payload(payload: Mapping[str, object]) -> str | None:
    for key in ("correlation_id", "correlationId", "x_correlation_id"):
        if key in payload:
            return normalize_correlation_id(payload.get(key))
    return None
