package io.rwa.server.outbox;

import io.rwa.server.config.RwaProperties;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

@Component
public class OutboxKafkaPublishListener {

    private static final Logger log = LoggerFactory.getLogger(OutboxKafkaPublishListener.class);

    private final RwaProperties properties;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final OutboxRepository outboxRepository;

    public OutboxKafkaPublishListener(
        RwaProperties properties,
        KafkaTemplate<String, String> kafkaTemplate,
        OutboxRepository outboxRepository
    ) {
        this.properties = properties;
        this.kafkaTemplate = kafkaTemplate;
        this.outboxRepository = outboxRepository;
    }

    @Async("outboxPublisherExecutor")
    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void publishAfterCommit(DomainEvent event) {
        if (!properties.getOutbox().isKafkaEnabled()) {
            return;
        }

        try {
            kafkaTemplate.send(event.topic(), event.partitionKey(), event.payload().toString()).get();
            outboxRepository.markSendSuccess(event.eventId());
        } catch (Exception e) {
            log.warn("Failed to publish outbox event {}", event.eventId(), e);
            outboxRepository.markSendFail(
                event.eventId(),
                e.getMessage(),
                Duration.ofSeconds(properties.getOutbox().getBackoffSeconds())
            );
        }
    }
}
