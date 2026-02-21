CREATE TABLE IF NOT EXISTS outbox_tx (
  outbox_id UUID PRIMARY KEY,
  request_id TEXT NOT NULL UNIQUE,
  from_address VARCHAR(42) NOT NULL,
  to_address VARCHAR(42),
  nonce BIGINT,
  tx_hash VARCHAR(66),
  raw_tx TEXT,
  status TEXT NOT NULL,
  tx_type TEXT NOT NULL,
  payload JSONB,
  last_error TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
