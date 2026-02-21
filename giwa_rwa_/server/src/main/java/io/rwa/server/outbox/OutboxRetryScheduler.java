package io.rwa.server.outbox;

import io.rwa.server.config.RwaProperties;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class OutboxRetryScheduler {

    private static final Logger log = LoggerFactory.getLogger(OutboxRetryScheduler.class);

    private final RwaProperties properties;
    private final OutboxRepository outboxRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final String instanceId = UUID.randomUUID().toString();

    public OutboxRetryScheduler(
        RwaProperties properties,
        OutboxRepository outboxRepository,
        KafkaTemplate<String, String> kafkaTemplate
    ) {
        this.properties = properties;
        this.outboxRepository = outboxRepository;
        this.kafkaTemplate = kafkaTemplate;
    }

    @Scheduled(fixedDelayString = "${rwa.outbox.retry-interval-ms:60000}")
    public void retryFailedDeliveries() {
        if (!properties.getOutbox().isKafkaEnabled()) {
            return;
        }

        List<UUID> claimed = outboxRepository.claimBatch(instanceId);
        for (UUID eventId : claimed) {
            Optional<OutboxEventRow> eventOpt = outboxRepository.findEvent(eventId);
            if (eventOpt.isEmpty()) {
                outboxRepository.unlock(eventId);
                continue;
            }

            OutboxEventRow event = eventOpt.get();
            try {
                // Kafka send is intentionally outside DB lock/transaction window.
                kafkaTemplate.send(event.topic(), event.partitionKey(), event.payload().toString()).get();
                outboxRepository.markSendSuccess(eventId);
            } catch (Exception e) {
                log.warn("Retry publish failed for event {}", eventId, e);
                outboxRepository.markSendFail(
                    eventId,
                    e.getMessage(),
                    Duration.ofSeconds(properties.getOutbox().getBackoffSeconds())
                );
            }
        }
    }
}
