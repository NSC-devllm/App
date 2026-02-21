package io.rwa.server.idempotency;

public enum IdempotencyRecordStatus {
    IN_PROGRESS,
    COMPLETED,
    FAILED
}
