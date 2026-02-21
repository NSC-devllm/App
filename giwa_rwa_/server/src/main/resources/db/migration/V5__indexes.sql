CREATE INDEX IF NOT EXISTS idx_api_idempotency_status_created_at
  ON api_idempotency (status, created_at);

CREATE INDEX IF NOT EXISTS idx_outbox_tx_status_updated_at
  ON outbox_tx (status, updated_at);

CREATE INDEX IF NOT EXISTS idx_outbox_delivery_retry
  ON outbox_delivery (status, next_retry_at, locked_at, updated_at);

CREATE INDEX IF NOT EXISTS idx_outbox_event_created_at
  ON outbox_event (created_at);

CREATE INDEX IF NOT EXISTS idx_classes_kapt_code_status
  ON classes (kapt_code, status);

CREATE INDEX IF NOT EXISTS idx_units_class_id_status
  ON units (class_id, status);
