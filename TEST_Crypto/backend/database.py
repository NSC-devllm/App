from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base

# 데이터베이스 연결 URL (비동기 SQLite 사용)
DATABASE_URL = "sqlite+aiosqlite:///./naver_finance.db"

# 비동기 엔진 생성
# echo=True: 실행되는 SQL 쿼리를 로그로 출력
engine = create_async_engine(DATABASE_URL, echo=True)

# 비동기 세션 팩토리 설정
AsyncSessionLocal = sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False, # 커밋 후에도 객체 상태 유지
    autocommit=False,       # 자동 커밋 비활성화 (명시적 커밋 필요)
    autoflush=False,        # 자동 플러시 비활성화 (명시적 플러시 필요)
)

# SQLAlchemy 선언적 베이스 클래스 생성 (모든 모델이 상속받음)
Base = declarative_base()

async def get_db():
    """
    FastAPI 의존성 주입을 위한 DB 세션 생성기
    - 요청마다 새로운 세션을 생성하고, 요청 처리가 끝나면 닫습니다.
    """
    async with AsyncSessionLocal() as session:
        yield session
