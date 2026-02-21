package io.rwa.server.wallet;

import java.util.List;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

public interface UserRepository extends JpaRepository<UserEntity, UUID> {
    List<UserEntity> findByComplianceStatus(ComplianceStatus status);
}
