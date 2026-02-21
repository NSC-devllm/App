from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.api.health import router as health_router


def test_health_endpoint_returns_expected_payload() -> None:
    app = FastAPI()
    app.include_router(health_router)

    with TestClient(app) as client:
        response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {
        "status": "healthy",
        "service": "nsc-tx-lookup-service",
        "version": "0.1.0",
    }
