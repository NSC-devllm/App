CREATE TABLE IF NOT EXISTS processed_events (
  id BIGSERIAL PRIMARY KEY,
  event_key TEXT NOT NULL,
  block_number BIGINT NOT NULL,
  tx_hash TEXT NOT NULL,
  payload JSONB NOT NULL,
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS listings (
  id BIGSERIAL PRIMARY KEY,
  property_id TEXT NOT NULL,
  listing_status TEXT NOT NULL,
  price NUMERIC,
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS trades (
  id BIGSERIAL PRIMARY KEY,
  listing_id BIGINT NOT NULL,
  buyer TEXT NOT NULL,
  seller TEXT NOT NULL,
  tx_hash TEXT NOT NULL,
  traded_at TIMESTAMPTZ DEFAULT now(),
  amount NUMERIC
);

CREATE TABLE IF NOT EXISTS balances (
  owner TEXT PRIMARY KEY,
  token_id BIGINT NOT NULL,
  amount NUMERIC NOT NULL,
  updated_at TIMESTAMPTZ DEFAULT now()
);
