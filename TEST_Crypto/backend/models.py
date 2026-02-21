from sqlalchemy import Column, String, Numeric, TIMESTAMP, func
import uuid
from database import Base

class UserWallet(Base):
    """
    사용자 지갑 테이블
    - 사용자의 현재 잔액과 결제 진행 중인(동결된) 금액을 관리합니다.
    """
    __tablename__ = "user_wallets"

    user_id = Column(String(255), primary_key=True) # 사용자 ID (PK)
    balance = Column(Numeric(18, 2), default=0.00, nullable=False) # 사용 가능한 잔액
    frozen_amount = Column(Numeric(18, 2), default=0.00, nullable=False) # 결제 대기 중(가승인) 묶인 금액
    updated_at = Column(TIMESTAMP, server_default=func.now(), onupdate=func.now()) # 마지막 업데이트 시간

class PaymentOrder(Base):
    """
    결제 주문 테이블
    - 각 결제 요청의 상태를 추적합니다. (READY -> COMPLETED or CANCELED)
    """
    __tablename__ = "payment_orders"

    order_id = Column(String(255), primary_key=True) # 주문 고유 ID (PK)
    user_id = Column(String(255), nullable=True) # 결제자 ID
    merchant_name = Column(String(255), nullable=True) # 가맹점 이름
    amount = Column(Numeric(18, 2), nullable=False) # 결제 금액
    status = Column(String(50), default='READY') # 결제 상태: READY(준비), COMPLETED(완료), CANCELED(취소)
    created_at = Column(TIMESTAMP, server_default=func.now()) # 주문 생성 시간

class TransactionLedger(Base):
    """
    거래 장부 (Ledger) 테이블
    - 자금의 모든 이동 히스토리를 불변으로 기록합니다.
    """
    __tablename__ = "transaction_ledger"

    tx_id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4())) # 트랜잭션 고유 ID
    wallet_id = Column(String(255), nullable=True) # 대상 지갑 ID
    type = Column(String(50), nullable=False) # 거래 유형: BUY(충전), PAY(결제), REFUND(환불/취소)
    amount = Column(Numeric(18, 2), nullable=False) # 거래 금액
    related_id = Column(String(255), nullable=True) # 연관 주문 ID (충전 시에는 없을 수 있음)
    created_at = Column(TIMESTAMP, server_default=func.now()) # 거래 발생 시간
