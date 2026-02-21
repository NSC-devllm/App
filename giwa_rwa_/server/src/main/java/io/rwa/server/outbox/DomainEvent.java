package io.rwa.server.outbox;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import java.util.UUID;

public record DomainEvent(
    UUID eventId,
    String aggregateType,
    String aggregateId,
    String eventType,
    Instant occurredAt,
    String topic,
    String partitionKey,
    JsonNode payload
) {
}
