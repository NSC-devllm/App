from __future__ import annotations

from fastapi import APIRouter

from src.api.constants import SERVICE_VERSION
from src.common.config import load_config

router = APIRouter()


@router.get("/health", summary="Service health check")
def get_health() -> dict[str, str]:
    config = load_config()
    return {
        "status": "healthy",
        "service": config.service_name,
        "version": SERVICE_VERSION,
    }
