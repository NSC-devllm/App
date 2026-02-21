package io.rwa.server.outbox;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import java.util.UUID;

public record OutboxEventRow(
    UUID eventId,
    String topic,
    String partitionKey,
    JsonNode payload,
    Instant createdAt
) {
}
