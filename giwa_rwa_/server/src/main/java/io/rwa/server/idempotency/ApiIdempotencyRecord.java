package io.rwa.server.idempotency;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;

public record ApiIdempotencyRecord(
    String endpoint,
    String idempotencyKey,
    String requestHash,
    IdempotencyRecordStatus status,
    Integer responseStatus,
    JsonNode responseBody,
    Instant createdAt,
    Instant updatedAt
) {
}
