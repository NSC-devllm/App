CREATE TABLE IF NOT EXISTS users (
  user_id UUID PRIMARY KEY,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  compliance_status TEXT NOT NULL,
  compliance_updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS wallets (
  user_id UUID PRIMARY KEY REFERENCES users(user_id),
  address VARCHAR(42) NOT NULL UNIQUE,
  encrypted_privkey BYTEA NOT NULL,
  enc_version INT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS complexes (
  kapt_code TEXT PRIMARY KEY,
  kapt_name TEXT,
  kapt_addr TEXT,
  doro_juso TEXT,
  ho_cnt INT,
  raw_json JSONB NOT NULL,
  fetched_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS classes (
  class_id TEXT PRIMARY KEY,
  kapt_code TEXT NOT NULL REFERENCES complexes(kapt_code),
  class_key TEXT NOT NULL,
  unit_count INT NOT NULL,
  status TEXT NOT NULL,
  doc_hash TEXT,
  base_token_id NUMERIC(78,0),
  issued_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (kapt_code, class_key)
);

CREATE TABLE IF NOT EXISTS units (
  unit_id TEXT PRIMARY KEY,
  class_id TEXT NOT NULL REFERENCES classes(class_id),
  unit_no INT NOT NULL,
  token_id NUMERIC(78,0),
  status TEXT NOT NULL,
  UNIQUE (class_id, unit_no)
);
