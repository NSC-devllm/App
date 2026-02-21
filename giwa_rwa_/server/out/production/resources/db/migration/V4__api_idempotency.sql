CREATE TABLE IF NOT EXISTS api_idempotency (
  endpoint TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  request_hash VARCHAR(64) NOT NULL,
  status TEXT NOT NULL,
  response_status INT,
  response_body JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (endpoint, idempotency_key)
);
