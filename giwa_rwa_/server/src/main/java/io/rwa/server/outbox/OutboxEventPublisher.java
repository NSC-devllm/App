package io.rwa.server.outbox;

import com.fasterxml.jackson.databind.JsonNode;
import io.rwa.server.config.RwaProperties;
import java.time.Instant;
import java.util.UUID;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;

@Component
public class OutboxEventPublisher {

    private final ApplicationEventPublisher eventPublisher;
    private final RwaProperties properties;

    public OutboxEventPublisher(ApplicationEventPublisher eventPublisher, RwaProperties properties) {
        this.eventPublisher = eventPublisher;
        this.properties = properties;
    }

    public void publish(String aggregateType, String aggregateId, String eventType, String partitionKey, JsonNode payload) {
        DomainEvent event = new DomainEvent(
            UUID.randomUUID(),
            aggregateType,
            aggregateId,
            eventType,
            Instant.now(),
            properties.getOutbox().getDefaultTopic(),
            partitionKey,
            payload
        );
        eventPublisher.publishEvent(event);
    }
}
