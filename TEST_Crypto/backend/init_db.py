import asyncio
from database import engine, Base
from models import UserWallet, PaymentOrder, TransactionLedger

async def init_db():
    """
    데이터베이스 초기화 함수
    - 기존의 모든 테이블을 삭제하고 새로 생성합니다.
    - 개발/테스트 환경에서 스키마 변경 사항을 반영하기 위해 사용됩니다.
    """
    async with engine.begin() as conn:
        print("Dropping all tables... (기존 테이블 삭제)")
        await conn.run_sync(Base.metadata.drop_all)
        print("Creating all tables... (신규 테이블 생성)")
        await conn.run_sync(Base.metadata.create_all)
    print("Database initialized completely. (초기화 완료)")

if __name__ == "__main__":
    # 비동기 힘수인 init_db를 실행하기 위해 asyncio.run 사용
    asyncio.run(init_db())
