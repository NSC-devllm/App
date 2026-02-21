package io.rwa.server.wallet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.test.autoconfigure.orm.jpa.TestEntityManager;
import org.hibernate.exception.ConstraintViolationException;
import org.springframework.transaction.annotation.Transactional;

@DataJpaTest(properties = {
    "spring.flyway.enabled=false",
    "spring.datasource.url=jdbc:h2:mem:rwa-jpa-user-wallet;DB_CLOSE_DELAY=-1;INIT=CREATE DOMAIN IF NOT EXISTS JSONB AS JSON",
    "spring.datasource.driverClassName=org.h2.Driver",
    "spring.jpa.hibernate.ddl-auto=create-drop"
})
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Transactional
class UserWalletRepositoryJpaIntegrationTest {

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private WalletRepository walletRepository;

    @Autowired
    private TestEntityManager entityManager;

    @Test
    @DisplayName("UserRepository는 compliance 상태로 사용자 목록을 조회한다")
    void userRepositoryShouldFindByComplianceStatus() {
        // given: APPROVED/PENDING 사용자를 각각 저장한다.
        UserEntity approved = new UserEntity();
        approved.setUserId(UUID.fromString("8fdbebf4-f297-47e3-8ecf-2d2d914d205e"));
        approved.setCreatedAt(Instant.parse("2026-02-17T00:00:00Z"));
        approved.setComplianceStatus(ComplianceStatus.APPROVED);
        approved.setComplianceUpdatedAt(Instant.parse("2026-02-17T00:00:00Z"));
        userRepository.save(approved);

        UserEntity pending = new UserEntity();
        pending.setUserId(UUID.fromString("0d4c3337-1921-49b8-b8ad-90f93711b168"));
        pending.setCreatedAt(Instant.parse("2026-02-17T00:00:00Z"));
        pending.setComplianceStatus(ComplianceStatus.PENDING);
        pending.setComplianceUpdatedAt(Instant.parse("2026-02-17T00:00:00Z"));
        userRepository.save(pending);
        entityManager.flush();
        entityManager.clear();

        // when: APPROVED 상태 조건으로 조회한다.
        List<UserEntity> approvedUsers = userRepository.findByComplianceStatus(ComplianceStatus.APPROVED);

        // then: APPROVED 사용자만 반환된다.
        assertThat(approvedUsers).hasSize(1);
        assertThat(approvedUsers.get(0).getUserId()).isEqualTo(approved.getUserId());
    }

    @Test
    @DisplayName("WalletRepository는 address로 지갑을 조회한다")
    void walletRepositoryShouldFindByAddress() {
        // given: 조회 대상 주소를 가진 지갑을 저장한다.
        WalletEntity wallet = new WalletEntity();
        wallet.setUserId(UUID.fromString("5e858f16-2d86-4f9f-b9b3-68c3fce13dc7"));
        wallet.setAddress("0x1234567890abcdef1234567890abcdef12345678");
        wallet.setEncryptedPrivkey(new byte[] { 9, 8, 7, 6 });
        wallet.setEncVersion(1);
        wallet.setCreatedAt(Instant.parse("2026-02-17T01:00:00Z"));
        walletRepository.save(wallet);
        entityManager.flush();
        entityManager.clear();

        // when: 주소 조건으로 지갑을 조회한다.
        WalletEntity found = walletRepository.findByAddress(wallet.getAddress()).orElseThrow();

        // then: 저장한 userId/address가 그대로 조회된다.
        assertThat(found.getUserId()).isEqualTo(wallet.getUserId());
        assertThat(found.getAddress()).isEqualTo(wallet.getAddress());
        assertThat(found.getEncryptedPrivkey()).containsExactly((byte) 9, (byte) 8, (byte) 7, (byte) 6);
    }

    @Test
    @DisplayName("WalletRepository는 동일 address 중복 저장 시 제약조건 위반이 발생한다")
    void walletRepositoryShouldFailOnDuplicateAddress() {
        // given: 이미 사용 중인 address를 가진 지갑을 먼저 저장한다.
        WalletEntity first = new WalletEntity();
        first.setUserId(UUID.fromString("d2095fc1-2679-4215-b179-68e0aa5694d0"));
        first.setAddress("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        first.setEncryptedPrivkey(new byte[] { 1 });
        first.setEncVersion(1);
        first.setCreatedAt(Instant.parse("2026-02-17T02:00:00Z"));
        walletRepository.save(first);
        entityManager.flush();

        WalletEntity duplicate = new WalletEntity();
        duplicate.setUserId(UUID.fromString("ec7786d0-ab8c-40f4-baa3-e4c356af7b95"));
        duplicate.setAddress(first.getAddress());
        duplicate.setEncryptedPrivkey(new byte[] { 2 });
        duplicate.setEncVersion(1);
        duplicate.setCreatedAt(Instant.parse("2026-02-17T02:01:00Z"));

        // when: 동일 address로 두 번째 지갑을 저장한다.
        // then: unique 제약조건 위반 예외가 발생한다.
        assertThatThrownBy(() -> {
            walletRepository.save(duplicate);
            entityManager.flush();
        }).isInstanceOf(ConstraintViolationException.class);
    }
}
