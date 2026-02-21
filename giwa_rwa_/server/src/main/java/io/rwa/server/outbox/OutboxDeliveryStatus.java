package io.rwa.server.outbox;

public enum OutboxDeliveryStatus {
    INIT,
    SEND_SUCCESS,
    SEND_FAIL
}
