package io.rwa.server.idempotency;

public record IdempotencyContext(
    String endpoint,
    String idempotencyKey,
    String requestHash
) {
}
