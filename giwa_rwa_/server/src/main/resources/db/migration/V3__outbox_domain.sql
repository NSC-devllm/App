-- outbox_event: 도메인 이벤트 원본(불변, INSERT 전용)을 저장하는 테이블
CREATE TABLE IF NOT EXISTS outbox_event (
  -- 이벤트 고유 ID.
  -- 예시: '01a0ecf5-f04c-4a43-9dc6-c727b97f3c58'
  event_id UUID PRIMARY KEY,
  -- 이벤트가 발생한 집합(애그리게이트) 종류.
  -- 예시: 'User' | 'Class' | 'Complex'
  aggregate_type TEXT NOT NULL,
  -- 애그리게이트 식별자.
  -- 예시: '7e0afdb6-6a88-4c28-89eb-9d4f4d0c79fd' 또는 '0x7c0c8b...'
  aggregate_id TEXT NOT NULL,
  -- 이벤트 타입명.
  -- 예시: 'UserSignedUp' | 'ClassTokenized' | 'ComplexImported'
  event_type TEXT NOT NULL,
  -- 이벤트 본문(JSON).
  -- 예시: '{"classId":"0x7c0...","status":"TOKENIZED"}'
  payload JSONB NOT NULL,
  -- Kafka 전송 대상 토픽(설정값 기반).
  -- 예시: 'rwa.domain.events'
  topic TEXT NOT NULL,
  -- Kafka 파티션 키(같은 키는 같은 파티션으로 전송).
  -- 예시: 'A10027831' 또는 '0x7c0c8b...'
  partition_key TEXT NOT NULL,
  -- 도메인 관점에서 이벤트가 발생한 시각.
  -- 예시: '2026-02-17T11:30:00Z'
  occurred_at TIMESTAMPTZ NOT NULL,
  -- outbox_event 레코드가 DB에 기록된 시각.
  -- 예시: '2026-02-17T11:30:00Z'
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- outbox_delivery: outbox_event 전송 상태(재시도/락 포함)를 관리하는 운영 테이블
CREATE TABLE IF NOT EXISTS outbox_delivery (
  -- outbox_event.event_id와 1:1 매핑되는 키(FK + PK).
  -- 예시: '01a0ecf5-f04c-4a43-9dc6-c727b97f3c58'
  event_id UUID PRIMARY KEY REFERENCES outbox_event(event_id),
  -- 전송 상태.
  -- 예시: 'INIT' | 'SEND_SUCCESS' | 'SEND_FAIL'
  status TEXT NOT NULL,
  -- 전송 시도 횟수(실패 시 증가).
  -- 예시: 0, 1, 2
  attempt_count INT NOT NULL DEFAULT 0,
  -- 마지막 실패 원인.
  -- 예시: 'Timeout while sending to Kafka broker'
  last_error TEXT,
  -- 다음 재시도 예정 시각.
  -- 예시: '2026-02-17T11:31:00Z'
  next_retry_at TIMESTAMPTZ,
  -- 현재 이벤트를 점유한 워커 인스턴스 ID.
  -- 예시: 'b8e8b8e1-0469-4c3e-8f4a-b24d4f7b84d2'
  locked_by TEXT,
  -- 락을 획득한 시각(락 TTL 판단에 사용).
  -- 예시: '2026-02-17T11:30:05Z'
  locked_at TIMESTAMPTZ,
  -- 실제 전송 성공 시각.
  -- 예시: '2026-02-17T11:30:06Z'
  sent_at TIMESTAMPTZ,
  -- 상태/재시도/락 정보 최종 갱신 시각.
  -- 예시: '2026-02-17T11:30:06Z'
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
