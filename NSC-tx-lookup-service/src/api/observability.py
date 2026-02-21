from __future__ import annotations

import logging
import time

from fastapi import FastAPI, Request

from src.common.metrics import (
    API_REQUESTS_INFLIGHT,
    observe_api_request,
)
from src.common.observability import (
    CORRELATION_ID_HEADER,
    correlation_context,
    get_correlation_id,
)

logger = logging.getLogger(__name__)


def _resolve_route_template(request: Request) -> str:
    route = request.scope.get("route")
    if route is not None and hasattr(route, "path"):
        return route.path
    return "unmatched"


def _request_context(request: Request) -> tuple[str, str, str, str, str]:
    method = request.method
    path = request.url.path
    query = request.url.query or "-"
    client_ip = request.client.host if request.client is not None else "-"
    user_agent = request.headers.get("user-agent", "-")
    return method, path, query, client_ip, user_agent


def register_observability(app: FastAPI) -> None:
    @app.middleware("http")
    async def correlation_middleware(request: Request, call_next):
        incoming = request.headers.get(CORRELATION_ID_HEADER)
        with correlation_context(incoming):
            response = await call_next(request)
            response.headers[CORRELATION_ID_HEADER] = get_correlation_id()
            return response

    @app.middleware("http")
    async def logging_middleware(request: Request, call_next):
        method, path, query, client_ip, user_agent = _request_context(request)
        logger.info(
            "api_request method=%s path=%s query=%s ip=%s user_agent=%s",
            method,
            path,
            query,
            client_ip,
            user_agent,
        )
        start = time.perf_counter()
        status_code = 500
        try:
            response = await call_next(request)
            status_code = response.status_code
            return response
        finally:
            duration_ms = int((time.perf_counter() - start) * 1000)
            logger.info(
                "api_response method=%s path=%s query=%s status_code=%s duration_ms=%s",
                method,
                path,
                query,
                status_code,
                duration_ms,
            )

    @app.middleware("http")
    async def metrics_middleware(request: Request, call_next):
        method = request.method
        route = _resolve_route_template(request)
        API_REQUESTS_INFLIGHT.add(1, attributes={"method": method, "route": route})
        start = time.perf_counter()
        status_code = 500
        try:
            response = await call_next(request)
            status_code = response.status_code
            return response
        finally:
            duration = time.perf_counter() - start
            observe_api_request(method, route, status_code, duration)
            API_REQUESTS_INFLIGHT.add(
                -1, attributes={"method": method, "route": route}
            )
