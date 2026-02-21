package io.rwa.server.compliance;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.rwa.server.outbox.OutboxEventPublisher;
import io.rwa.server.common.ApiException;
import io.rwa.server.wallet.ComplianceStatus;
import io.rwa.server.wallet.UserEntity;
import io.rwa.server.wallet.UserRepository;
import jakarta.transaction.Transactional;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

@Service
public class ComplianceService {

    private final UserRepository userRepository;
    private final OutboxEventPublisher outboxEventPublisher;
    private final ObjectMapper objectMapper;

    public ComplianceService(UserRepository userRepository, OutboxEventPublisher outboxEventPublisher, ObjectMapper objectMapper) {
        this.userRepository = userRepository;
        this.outboxEventPublisher = outboxEventPublisher;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public UserEntity approve(UUID userId) {
        UserEntity user = userRepository.findById(userId)
            .orElseThrow(() -> new ApiException(HttpStatus.NOT_FOUND, "User not found: " + userId));
        user.setComplianceStatus(ComplianceStatus.APPROVED);
        user.setComplianceUpdatedAt(Instant.now());
        UserEntity saved = userRepository.save(user);

        outboxEventPublisher.publish(
            "User",
            userId.toString(),
            "ComplianceApproved",
            userId.toString(),
            objectMapper.createObjectNode().put("userId", userId.toString()).put("status", "APPROVED")
        );
        return saved;
    }

    @Transactional
    public UserEntity revoke(UUID userId) {
        UserEntity user = userRepository.findById(userId)
            .orElseThrow(() -> new ApiException(HttpStatus.NOT_FOUND, "User not found: " + userId));
        user.setComplianceStatus(ComplianceStatus.REVOKED);
        user.setComplianceUpdatedAt(Instant.now());
        UserEntity saved = userRepository.save(user);

        outboxEventPublisher.publish(
            "User",
            userId.toString(),
            "ComplianceRevoked",
            userId.toString(),
            objectMapper.createObjectNode().put("userId", userId.toString()).put("status", "REVOKED")
        );
        return saved;
    }

    public List<UserEntity> findByStatus(ComplianceStatus status) {
        return userRepository.findByComplianceStatus(status);
    }
}
