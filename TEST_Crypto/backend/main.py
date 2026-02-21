from fastapi import FastAPI, HTTPException, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy import update
from pydantic import BaseModel
from decimal import Decimal
from typing import List, Optional
import uuid

from database import get_db, engine, Base
from models import UserWallet, PaymentOrder, TransactionLedger

# FastAPI 인스턴스 생성
app = FastAPI(title="NSC Payment System", description="Naver Stable Coin 결제 시스템 API")

# ==========================================
# Pydantic 모델 (데이터 검증 및 스키마 정의)
# ==========================================

class BuyRequest(BaseModel):
    """NSC 충전 요청 모델"""
    user_id: str
    amount: Decimal

class PayPrepareRequest(BaseModel):
    """결제 준비(Prepare) 요청 모델"""
    user_id: str
    merchant_name: str
    amount: Decimal

class PayConfirmRequest(BaseModel):
    """결제 확정(Confirm) 요청 모델"""
    order_id: str

class PayCancelRequest(BaseModel):
    """결제 취소(Cancel) 요청 모델"""
    order_id: str

class WalletResponse(BaseModel):
    """지갑 정보 응답 모델"""
    user_id: str
    balance: Decimal
    frozen_amount: Decimal

class OrderResponse(BaseModel):
    """주문 정보 응답 모델"""
    order_id: str
    status: str
    frozen_amount: Decimal

# ==========================================
# 핵심 로직 API (충전, 결제 흐름)
# ==========================================

@app.post("/api/buy")
async def buy_nsc(req: BuyRequest, db: AsyncSession = Depends(get_db)):
    """
    NSC 충전 API
    - 사용자의 지갑 잔액을 증가시킵니다.
    - 지갑이 없으면 새로 생성합니다.
    - 'BUY' 타입의 트랜잭션을 기록합니다.
    """
    if req.amount <= 0:
        raise HTTPException(status_code=400, detail="Amount must be positive")

    # 1. 지갑 조회 및 업데이트 (Update Wallet)
    result = await db.execute(select(UserWallet).where(UserWallet.user_id == req.user_id))
    wallet = result.scalars().first()

    if not wallet:
        # 지갑이 존재하지 않으면 신규 생성
        wallet = UserWallet(user_id=req.user_id, balance=0, frozen_amount=0)
        db.add(wallet)
    
    # 잔액 증가
    wallet.balance += req.amount
    
    # 2. 거래 장부 기록 (Add Ledger Entry)
    ledger = TransactionLedger(wallet_id=req.user_id, type="BUY", amount=req.amount)
    db.add(ledger)
    
    await db.commit() # 트랜잭션 커밋
    return {"message": "NSC Purchased", "new_balance": wallet.balance}

@app.post("/api/pay/prepare")
async def prepare_payment(req: PayPrepareRequest, db: AsyncSession = Depends(get_db)):
    """
    결제 준비 API (Freeze 단계)
    - 원자적 트랜잭션을 보장합니다.
    - 지갑 잔액을 차감하고 동결 금액(frozen_amount)을 증가시킵니다.
    - 'READY' 상태의 주문을 생성합니다.
    """
    async with db.begin(): # 트랜잭션 시작 (Start Transaction)
        # 1. 지갑 행 잠금 (Lock Wallet Row) - 동시성 제어를 위해 for_update 사용
        result = await db.execute(select(UserWallet).where(UserWallet.user_id == req.user_id).with_for_update())
        wallet = result.scalars().first()

        if not wallet:
             raise HTTPException(status_code=404, detail="Wallet not found")
        
        if wallet.balance < req.amount:
            raise HTTPException(status_code=400, detail="Insufficient Balance")

        # 2. 금액 동결 (Freeze Amount)
        # 사용 가능한 잔액에서 차감하고, 동결 금액으로 이동
        wallet.balance -= req.amount
        wallet.frozen_amount += req.amount
        
        # 3. 주문 생성 (Create Order)
        order_id = str(uuid.uuid4())
        order = PaymentOrder(
            order_id=order_id,
            user_id=req.user_id,
            merchant_name=req.merchant_name,
            amount=req.amount,
            status="READY"
        )
        db.add(order)
        
    # `async with db.begin()` 블록을 빠져나오면 자동으로 커밋됨
    return {"status": "READY", "order_id": order_id, "frozen_amount": req.amount}

@app.post("/api/pay/confirm")
async def confirm_payment(req: PayConfirmRequest, db: AsyncSession = Depends(get_db)):
    """
    결제 확정 API (Settle 단계)
    - 주문 상태를 'COMPLETED'로 변경합니다.
    - 동결된 금액을 실제로 차감하여 정산 처리를 완료합니다.
    - 'PAY' 타입의 트랜잭션을 기록합니다.
    """
    async with db.begin():
        # 1. 주문 조회 및 잠금 (Get Order)
        result = await db.execute(select(PaymentOrder).where(PaymentOrder.order_id == req.order_id).with_for_update())
        order = result.scalars().first()

        if not order or order.status != "READY":
            raise HTTPException(status_code=400, detail="Invalid Order")

        # 2. 지갑 조회 및 잠금 (Get Wallet)
        wallet_result = await db.execute(select(UserWallet).where(UserWallet.user_id == order.user_id).with_for_update())
        wallet = wallet_result.scalars().first()

        # 3. 동결 금액 영구 차감 (Deduct Frozen Amount - Permanent Settle)
        wallet.frozen_amount -= order.amount
        
        # 4. 주문 상태 업데이트 (Update Order Status)
        order.status = "COMPLETED"
        
        # 5. 거래 장부 기록 (Ledger Entry)
        ledger = TransactionLedger(
            wallet_id=order.user_id,
            type="PAY",
            amount=order.amount,
            related_id=order.order_id
        )
        db.add(ledger)

    return {"status": "COMPLETED", "order_id": order.order_id}

@app.post("/api/pay/cancel")
async def cancel_payment(req: PayCancelRequest, db: AsyncSession = Depends(get_db)):
    """
    결제 취소 API
    - 주문 상태를 'CANCELED'로 변경합니다.
    - 동결된 금액을 다시 지갑 잔액으로 환불합니다.
    - 'REFUND' 타입의 트랜잭션을 기록합니다.
    """
    async with db.begin():
        # 1. 주문 조회 및 잠금 (Get Order)
        result = await db.execute(select(PaymentOrder).where(PaymentOrder.order_id == req.order_id).with_for_update())
        order = result.scalars().first()

        if not order or order.status != "READY":
            raise HTTPException(status_code=400, detail="Invalid Order for Cancellation")

        # 2. 지갑 조회 및 잠금 (Get Wallet)
        wallet_result = await db.execute(select(UserWallet).where(UserWallet.user_id == order.user_id).with_for_update())
        wallet = wallet_result.scalars().first()

        # 3. 동결 금액 환불 (Refund Frozen Amount)
        wallet.frozen_amount -= order.amount
        wallet.balance += order.amount # 잔액 원상 복구
        
        # 4. 주문 상태 업데이트 (Update Order Status)
        order.status = "CANCELED"
        
        # 5. 거래 장부 기록 (Ledger Entry)
        ledger = TransactionLedger(
            wallet_id=order.user_id,
            type="REFUND",
            amount=order.amount,
            related_id=order.order_id
        )
        db.add(ledger)

    return {"status": "CANCELED", "order_id": order.order_id}

# ==========================================
# 조회용 View API
# ==========================================

@app.get("/api/wallet/{user_id}")
async def get_wallet(user_id: str, db: AsyncSession = Depends(get_db)):
    """
    지갑 정보 조회 API
    - 사용자 지갑의 잔액과 동결 금액을 조회합니다.
    - 최근 10건의 거래 내역을 함께 반환합니다.
    """
    result = await db.execute(select(UserWallet).where(UserWallet.user_id == user_id))
    wallet = result.scalars().first()
    
    if not wallet:
        # 데모 편의성을 위해 지갑이 없으면 자동 생성
        wallet = UserWallet(user_id=user_id, balance=0, frozen_amount=0)
        db.add(wallet)
        await db.commit()
    
    # 최근 10건의 거래 내역 조회 (Get last 10 transactions)
    ledger_res = await db.execute(select(TransactionLedger).where(TransactionLedger.wallet_id == user_id).order_by(TransactionLedger.created_at.desc()).limit(10))
    transactions = ledger_res.scalars().all()
    
    return {
        "user_id": wallet.user_id,
        "balance": wallet.balance,
        "frozen_amount": wallet.frozen_amount,
        "transactions": transactions
    }

@app.get("/api/admin/ledger")
async def get_all_ledger(db: AsyncSession = Depends(get_db)):
    """(관리자용) 전체 거래 장부 조회 - 최근 50건"""
    result = await db.execute(select(TransactionLedger).order_by(TransactionLedger.created_at.desc()).limit(50))
    return result.scalars().all()

@app.get("/api/admin/orders")
async def get_all_orders(db: AsyncSession = Depends(get_db)):
    """(관리자용) 전체 주문 내역 조회 - 최근 50건"""
    result = await db.execute(select(PaymentOrder).order_by(PaymentOrder.created_at.desc()).limit(50))
    return result.scalars().all()
