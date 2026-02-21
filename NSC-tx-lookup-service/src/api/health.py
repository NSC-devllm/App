from __future__ import annotations

from fastapi import APIRouter

router = APIRouter()


@router.get("/health", summary="Service health check")
def get_health() -> dict[str, str]:
    return {
        "status": "healthy",
        "service": "nsc-tx-lookup-service",
        "version": "0.1.0",
    }
