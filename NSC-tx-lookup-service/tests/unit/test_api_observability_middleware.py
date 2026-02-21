from __future__ import annotations

import logging
import re

from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.api.observability import register_observability


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
