from __future__ import annotations

import logging
import re

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.api.observability import register_observability
from src.common.observability import CORRELATION_ID_HEADER, get_correlation_id


def test_logging_middleware_logs_request_and_response(caplog) -> None:
    app = FastAPI()
    register_observability(app)

    @app.get("/ping")
    async def ping() -> dict[str, bool]:
        return {"ok": True}

    caplog.set_level(logging.INFO, logger="src.api.observability")

    with TestClient(app) as client:
        response = client.get(
            "/ping?foo=bar",
            headers={"User-Agent": "pytest-agent"},
        )

    assert response.status_code == 200

    messages = [
        record.getMessage()
        for record in caplog.records
        if record.name == "src.api.observability"
    ]
    request_logs = [message for message in messages if message.startswith("api_request")]
    response_logs = [
        message for message in messages if message.startswith("api_response")
    ]

    assert len(request_logs) == 1
    assert "method=GET" in request_logs[0]
    assert "path=/ping" in request_logs[0]
    assert "query=foo=bar" in request_logs[0]
    assert "user_agent=pytest-agent" in request_logs[0]

    assert len(response_logs) == 1
    assert "method=GET" in response_logs[0]
    assert "path=/ping" in response_logs[0]
    assert "status_code=200" in response_logs[0]
    assert re.search(r"duration_ms=\d+", response_logs[0]) is not None


def test_logging_middleware_masks_sensitive_query_values(
    monkeypatch: pytest.MonkeyPatch, caplog
) -> None:
    monkeypatch.setenv("AUDIT_MASK_QUERY_KEYS", "access_token,token")

    app = FastAPI()
    register_observability(app)

    @app.get("/ping")
    async def ping() -> dict[str, bool]:
        return {"ok": True}

    caplog.set_level(logging.INFO, logger="src.api.observability")

    with TestClient(app) as client:
        response = client.get("/ping?access_token=secret&foo=bar")

    assert response.status_code == 200

    messages = [
        record.getMessage()
        for record in caplog.records
        if record.name == "src.api.observability"
    ]
    request_logs = [message for message in messages if message.startswith("api_request")]
    response_logs = [
        message for message in messages if message.startswith("api_response")
    ]

    assert len(request_logs) == 1
    assert "query=access_token=%2A%2A%2A&foo=bar" in request_logs[0]
    assert len(response_logs) == 1
    assert "query=access_token=%2A%2A%2A&foo=bar" in response_logs[0]


def test_logging_middleware_uses_default_masking_when_config_empty(
    monkeypatch: pytest.MonkeyPatch, caplog
) -> None:
    monkeypatch.setenv("AUDIT_MASK_QUERY_KEYS", "")

    app = FastAPI()
    register_observability(app)

    @app.get("/ping")
    async def ping() -> dict[str, bool]:
        return {"ok": True}

    caplog.set_level(logging.INFO, logger="src.api.observability")

    with TestClient(app) as client:
        response = client.get("/ping?token=secret&foo=bar")

    assert response.status_code == 200

    messages = [
        record.getMessage()
        for record in caplog.records
        if record.name == "src.api.observability"
    ]
    request_logs = [message for message in messages if message.startswith("api_request")]
    response_logs = [
        message for message in messages if message.startswith("api_response")
    ]

    assert len(request_logs) == 1
    assert "query=token=%2A%2A%2A&foo=bar" in request_logs[0]
    assert len(response_logs) == 1
    assert "query=token=%2A%2A%2A&foo=bar" in response_logs[0]


def test_logging_middleware_keeps_correlation_id_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = FastAPI()
    register_observability(app)

    @app.get("/ping")
    async def ping() -> dict[str, bool]:
        return {"ok": True}

    captured: list[tuple[str, str]] = []

    def fake_info(message: str, *args, **kwargs) -> None:
        rendered = message % args if args else message
        captured.append((rendered, get_correlation_id()))

    monkeypatch.setattr("src.api.observability.logger.info", fake_info)

    with TestClient(app) as client:
        response = client.get("/ping", headers={CORRELATION_ID_HEADER: "corr-123"})

    assert response.status_code == 200
    assert response.headers[CORRELATION_ID_HEADER] == "corr-123"
    assert len(captured) == 2
    assert captured[0][0].startswith("api_request")
    assert captured[1][0].startswith("api_response")
    assert captured[0][1] == "corr-123"
    assert captured[1][1] == "corr-123"


def test_logging_middleware_logs_failure_response(caplog) -> None:
    app = FastAPI()
    register_observability(app)

    @app.get("/boom")
    async def boom() -> dict[str, str]:
        raise RuntimeError("boom")

    caplog.set_level(logging.INFO, logger="src.api.observability")

    with TestClient(app, raise_server_exceptions=False) as client:
        response = client.get("/boom")

    assert response.status_code == 500

    messages = [
        record.getMessage()
        for record in caplog.records
        if record.name == "src.api.observability"
    ]
    response_logs = [
        message for message in messages if message.startswith("api_response")
    ]

    assert len(response_logs) == 1
    assert "method=GET" in response_logs[0]
    assert "path=/boom" in response_logs[0]
    assert "status_code=500" in response_logs[0]
    assert re.search(r"duration_ms=\d+", response_logs[0]) is not None
