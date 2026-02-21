from __future__ import annotations

from fastapi import APIRouter

from src.api.constants import SERVICE_NAME, SERVICE_VERSION

router = APIRouter()


@router.get("/health", summary="Service health check")
def get_health() -> dict[str, str]:
    return {
        "status": "healthy",
        "service": SERVICE_NAME,
        "version": SERVICE_VERSION,
    }
