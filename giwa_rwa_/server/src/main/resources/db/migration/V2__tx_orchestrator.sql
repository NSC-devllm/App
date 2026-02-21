-- outbox_tx: 온체인 트랜잭션 제출/추적/재시도를 위한 오케스트레이션 상태 테이블
CREATE TABLE IF NOT EXISTS outbox_tx (
  -- 트랜잭션 outbox 레코드 고유 ID.
  -- 예시: 'f6d84f75-f4b1-48a4-b7e6-7f8f3fcb4db2'
  outbox_id UUID PRIMARY KEY,
  -- 비즈니스 요청 단위 멱등 키(중복 제출 방지용).
  -- 예시: 'signup-k1:tokenize:reserve:0x7c0c8b...'
  request_id TEXT NOT NULL UNIQUE,
  -- 트랜잭션 발신 지갑 주소.
  -- 예시: '0x9f7c84f48a4df5f6cfe0f5ce1f5f11f6b9b38c58'
  from_address VARCHAR(42) NOT NULL,
  -- 트랜잭션 수신 컨트랙트 주소(필요 없는 경우 NULL 가능).
  -- 예시: '0x3C44CdDdB6a900fa2b585dd299e03d12FA4293BC'
  to_address VARCHAR(42),
  -- 체인 nonce(같은 from_address에서 순차 증가).
  -- 예시: 1024
  nonce BIGINT,
  -- 체인에 전파된 tx hash.
  -- 예시: '0x4b9f0b52c1f04b7f7f6f9c9f5e7c9f5e7c9f5e7c9f5e7c9f5e7c9f5e7c9f5e7c'
  tx_hash VARCHAR(66),
  -- 서명 완료된 raw transaction(직렬화 hex 문자열).
  -- 예시: '0x02f9012a...'
  raw_tx TEXT,
  -- 트랜잭션 처리 상태.
  -- 예시: 'CREATED' | 'SIGNED' | 'SENT' | 'MINED' | 'FAILED' | 'REPLACED'
  status TEXT NOT NULL,
  -- 트랜잭션 업무 유형(관측/분류용).
  -- 예시: 'TOKENIZE_RESERVE_REGISTER' | 'TRADE_LIST' | 'TRADE_BUY' | 'FAUCET_MUSD'
  tx_type TEXT NOT NULL,
  -- 트랜잭션에 함께 저장할 업무 컨텍스트(JSON).
  -- 예시: '{"classId":"0x7c0...","count":100}'
  payload JSONB,
  -- 실패 시 마지막 오류 메시지.
  -- 예시: 'replacement transaction underpriced'
  last_error TEXT,
  -- outbox 레코드 생성 시각.
  -- 예시: '2026-02-17T11:20:00Z'
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  -- 상태/오류/해시 변경 시각.
  -- 예시: '2026-02-17T11:20:02Z'
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
