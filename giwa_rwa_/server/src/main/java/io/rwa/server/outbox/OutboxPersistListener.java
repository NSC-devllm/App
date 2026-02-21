package io.rwa.server.outbox;

import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

@Component
public class OutboxPersistListener {

    private final OutboxRepository outboxRepository;

    public OutboxPersistListener(OutboxRepository outboxRepository) {
        this.outboxRepository = outboxRepository;
    }

    @TransactionalEventListener(phase = TransactionPhase.BEFORE_COMMIT)
    public void persist(DomainEvent event) {
        outboxRepository.insertEventAndInitDelivery(event);
    }
}
