-- api_idempotency: 동일 API 재요청 시 중복 실행을 막고 기존 결과를 재사용하기 위한 테이블
CREATE TABLE IF NOT EXISTS api_idempotency (
  -- HTTP 메서드 + 엔드포인트 템플릿 식별자.
  -- 예시: 'POST /admin/classes/{classId}/tokenize'
  endpoint TEXT NOT NULL,
  -- 클라이언트가 보낸 Idempotency-Key 헤더 값.
  -- 예시: 'signup-k1' 또는 'trade-buy-20260217-001'
  idempotency_key TEXT NOT NULL,
  -- 요청(path/query/body) canonical JSON의 SHA-256 해시(64 hex).
  -- 예시: 'a3f8b4d2f8e9c4a6b2d1e3f4a5c6d7e8f9a0b1c2d3e4f5a6b7c8d9e0f1a2b3c4'
  request_hash VARCHAR(64) NOT NULL,
  -- 처리 상태.
  -- 예시: 'IN_PROGRESS' | 'COMPLETED' | 'FAILED'
  status TEXT NOT NULL,
  -- 저장된 최종 HTTP 응답 코드.
  -- 예시: 200, 201, 409
  response_status INT,
  -- 저장된 최종 HTTP 응답 본문(JSON).
  -- 예시: '{"status":"IN_PROGRESS","statusUrl":"/idempotency/status?..."}'
  response_body JSONB,
  -- idempotency 레코드 생성 시각.
  -- 예시: '2026-02-17T11:40:00Z'
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  -- 상태/응답 정보 갱신 시각.
  -- 예시: '2026-02-17T11:40:02Z'
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  -- 같은 endpoint 내에서 같은 key는 1건만 허용.
  PRIMARY KEY (endpoint, idempotency_key)
);
