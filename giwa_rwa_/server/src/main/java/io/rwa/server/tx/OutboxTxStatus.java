package io.rwa.server.tx;

public enum OutboxTxStatus {
    CREATED,
    SIGNED,
    SENT,
    MINED,
    FAILED,
    REPLACED
}
