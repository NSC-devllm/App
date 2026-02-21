import time
from datetime import datetime
from typing import Annotated

from fastapi import Depends, FastAPI, HTTPException, Query
from starlette.requests import Request

from src.api.audit import build_audit_fields, extract_actor
from src.api.auth import require_admin_read
from src.api.constants import (
    ADMIN_ORDER_LOOKUP,
    ADMIN_TX_LOOKUP,
    ADMIN_WALLET_TX_LOOKUP,
)
from src.api.observability import register_observability
from src.api.schemas import AdminOrderResponse, AdminTxResponse, AdminWalletTxResponse
from src.api.service import (
    build_admin_order_response,
    build_admin_tx_response,
    build_admin_wallet_tx_response,
)
from src.common.logging import configure_logging
from src.common.otel import init_azure_monitor
from src.db.admin_tx import (
    fetch_admin_order_context,
    fetch_admin_tx_context,
    fetch_admin_wallet_tx,
)
from src.db.audit import record_admin_audit
from src.db.session import session_scope

configure_logging()
init_azure_monitor()
app = FastAPI(
    title="Backoffice Admin API",
    description=(
        "관리자용 거래 조회 API. "
        "원장 엔트리 기반 tx_id 조회와 PAYMENT/RECEIVE 페어링 정보를 제공합니다."
    ),
    version="0.1.0",
    openapi_tags=[
        {
            "name": "Admin Transaction",
            "description": "관리자 거래 조회 엔드포인트. ADMIN_READ 역할 필요.",
        },
    ],
)
register_observability(app)


@app.get(
    "/admin/tx/{tx_id}",
    response_model=AdminTxResponse,
    summary="거래 단건 조회",
    description=(
        "tx_id로 단일 원장 엔트리를 조회합니다. "
        "PAYMENT/RECEIVE 페어링이 완료된 경우 반대편 엔트리 정보도 포함됩니다. "
        "조회 시 감사 로그가 자동 기록됩니다."
    ),
    tags=["Admin Transaction"],
    responses={
        401: {"description": "인증 실패 — JWT 토큰 누락 또는 만료"},
        403: {"description": "권한 부족 — ADMIN_READ 역할 필요"},
        404: {"description": "해당 tx_id의 거래를 찾을 수 없음"},
    },
)
def get_admin_tx(
    tx_id: str,
    request: Request,
    _actor=Depends(require_admin_read),
) -> AdminTxResponse:
    start = time.monotonic()
    actor = extract_actor(request)
    result = "FOUND"
    status_code = 200
    response: AdminTxResponse | None = None

    with session_scope() as session:
        context = fetch_admin_tx_context(session, tx_id)
        if context is None:
            result = "NOT_FOUND"
            status_code = 404
        else:
            response = build_admin_tx_response(context)

        duration_ms = int((time.monotonic() - start) * 1000)
        audit_fields = build_audit_fields(
            actor=actor,
            action=ADMIN_TX_LOOKUP,
            resource_type="TX",
            resource_id=tx_id,
            result=result,
            status_code=status_code,
            request=request,
            duration_ms=duration_ms,
            result_count=1 if result == "FOUND" else 0,
        )
        record_admin_audit(session, audit_fields)

    if response is None:
        raise HTTPException(status_code=404, detail="tx_id not found")

    return response


@app.get(
    "/admin/payment-orders/{order_id}",
    response_model=AdminOrderResponse,
    summary="결제 주문 기준 조회",
    description=(
        "order_id로 결제 주문과 관련 원장 엔트리를 조회합니다. "
        "PAYMENT/RECEIVE 페어링 상태를 포함합니다. "
        "조회 시 감사 로그가 자동 기록됩니다."
    ),
    tags=["Admin Transaction"],
    responses={
        401: {"description": "인증 실패 — JWT 토큰 누락 또는 만료"},
        403: {"description": "권한 부족 — ADMIN_READ 역할 필요"},
        404: {"description": "해당 order_id의 결제 주문을 찾을 수 없음"},
    },
)
def get_admin_order(
    order_id: str,
    request: Request,
    _actor=Depends(require_admin_read),
) -> AdminOrderResponse:
    start = time.monotonic()
    actor = extract_actor(request)
    result = "FOUND"
    status_code = 200
    response: AdminOrderResponse | None = None
    result_count = 0

    with session_scope() as session:
        context = fetch_admin_order_context(session, order_id)
        if context is None:
            result = "NOT_FOUND"
            status_code = 404
        else:
            response = build_admin_order_response(context)
            result_count = len(context.ledger_entries)

        duration_ms = int((time.monotonic() - start) * 1000)
        audit_fields = build_audit_fields(
            actor=actor,
            action=ADMIN_ORDER_LOOKUP,
            resource_type="PAYMENT_ORDER",
            resource_id=order_id,
            result=result,
            status_code=status_code,
            request=request,
            duration_ms=duration_ms,
            result_count=result_count,
        )
        record_admin_audit(session, audit_fields)

    if response is None:
        raise HTTPException(status_code=404, detail="order_id not found")

    return response


@app.get(
    "/admin/wallets/{wallet_id}/tx",
    response_model=AdminWalletTxResponse,
    summary="지갑 기준 거래 조회",
    description=(
        "wallet_id로 해당 지갑의 최근 거래를 조회합니다. "
        "시간 범위(from/to)와 건수 제한(limit)을 지원합니다. "
        "조회 시 감사 로그가 자동 기록됩니다."
    ),
    tags=["Admin Transaction"],
    responses={
        401: {"description": "인증 실패 — JWT 토큰 누락 또는 만료"},
        403: {"description": "권한 부족 — ADMIN_READ 역할 필요"},
    },
)
def get_admin_wallet_tx(
    wallet_id: str,
    request: Request,
    _actor=Depends(require_admin_read),
    from_time: Annotated[
        datetime | None,
        Query(alias="from", description="조회 시작 시각 (ISO 8601)"),
    ] = None,
    to_time: Annotated[
        datetime | None,
        Query(alias="to", description="조회 종료 시각 (ISO 8601)"),
    ] = None,
    limit: Annotated[
        int,
        Query(ge=1, le=100, description="최대 조회 건수 (기본 20, 최대 100)"),
    ] = 20,
) -> AdminWalletTxResponse:
    start = time.monotonic()
    actor = extract_actor(request)

    with session_scope() as session:
        entries = fetch_admin_wallet_tx(
            session,
            wallet_id,
            from_time=from_time,
            to_time=to_time,
            limit=limit,
        )

        response = build_admin_wallet_tx_response(wallet_id, entries)
        result = "FOUND" if entries else "EMPTY"
        result_count = len(entries)

        duration_ms = int((time.monotonic() - start) * 1000)
        audit_fields = build_audit_fields(
            actor=actor,
            action=ADMIN_WALLET_TX_LOOKUP,
            resource_type="WALLET",
            resource_id=wallet_id,
            result=result,
            status_code=200,
            request=request,
            duration_ms=duration_ms,
            result_count=result_count,
        )
        record_admin_audit(session, audit_fields)

    return response
