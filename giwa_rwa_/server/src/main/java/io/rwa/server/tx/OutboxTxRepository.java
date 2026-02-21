package io.rwa.server.tx;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

public interface OutboxTxRepository extends JpaRepository<OutboxTxEntity, UUID> {
    Optional<OutboxTxEntity> findByRequestId(String requestId);
    Optional<OutboxTxEntity> findByTxHash(String txHash);
    List<OutboxTxEntity> findByStatus(String status);
}
