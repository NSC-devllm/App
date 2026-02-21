from __future__ import annotations

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


def _resolve_route_template(request: Request) -> str:
    route = request.scope.get("route")
    if route is not None and hasattr(route, "path"):
        return route.path
    return "unmatched"


def register_observability(app: FastAPI) -> None:
    @app.middleware("http")
    async def correlation_middleware(request: Request, call_next):
        incoming = request.headers.get(CORRELATION_ID_HEADER)
        with correlation_context(incoming):
            response = await call_next(request)
            response.headers[CORRELATION_ID_HEADER] = get_correlation_id()
            return response

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
