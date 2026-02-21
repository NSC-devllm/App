package io.rwa.server.wallet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.rwa.server.common.ApiException;
import io.rwa.server.outbox.OutboxEventPublisher;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;

@ExtendWith(MockitoExtension.class)
class WalletServiceTest {

    @Mock
    private UserRepository userRepository;

    @Mock
    private WalletRepository walletRepository;

    @Mock
    private WalletCryptoService walletCryptoService;

    @Mock
    private OutboxEventPublisher outboxEventPublisher;

    private WalletService walletService;

    @BeforeEach
    void setUp() {
        walletService = new WalletService(
            userRepository,
            walletRepository,
            walletCryptoService,
            outboxEventPublisher,
            new ObjectMapper()
        );
    }

    @Test
    @DisplayName("signup은 사용자/지갑을 저장하고 UserSignedUp 이벤트를 발행한다")
    void signupShouldPersistUserAndWalletAndPublishEvent() {
        // given: signup 수행에 필요한 암호화/저장 목 동작을 준비한다.
        UUID userId = UUID.fromString("0199587d-78f9-7000-8000-000000000001");
        when(userRepository.save(any(UserEntity.class))).thenAnswer(invocation -> {
            UserEntity entity = invocation.getArgument(0);
            entity.setUserId(userId);
            return entity;
        });
        when(walletCryptoService.encryptPrivateKey(any())).thenReturn(new byte[] { 1, 2, 3 });
        when(walletRepository.save(any(WalletEntity.class))).thenAnswer(invocation -> invocation.getArgument(0));

        // when: 신규 가입을 실행한다.
        SignupResult result = walletService.signup();

        // then: 사용자/지갑 저장과 outbox 이벤트 발행이 기대한 값으로 수행된다.
        ArgumentCaptor<String> privateKeyCaptor = ArgumentCaptor.forClass(String.class);
        verify(walletCryptoService).encryptPrivateKey(privateKeyCaptor.capture());
        assertThat(privateKeyCaptor.getValue()).matches("^[0-9a-fA-F]{64}$");
        ArgumentCaptor<UserEntity> userCaptor = ArgumentCaptor.forClass(UserEntity.class);
        verify(userRepository).save(userCaptor.capture());
        UserEntity savedUser = userCaptor.getValue();
        assertThat(result.userId()).isEqualTo(userId);
        assertThat(savedUser.getCreatedAt()).isNotNull();
        assertThat(savedUser.getComplianceStatus()).isEqualTo(ComplianceStatus.PENDING);
        assertThat(savedUser.getComplianceUpdatedAt()).isNotNull();

        ArgumentCaptor<WalletEntity> walletCaptor = ArgumentCaptor.forClass(WalletEntity.class);
        verify(walletRepository).save(walletCaptor.capture());
        WalletEntity savedWallet = walletCaptor.getValue();
        assertThat(savedWallet.getUserId()).isEqualTo(result.userId());
        assertThat(savedWallet.getAddress()).isEqualTo(result.address());
        assertThat(savedWallet.getAddress()).isEqualTo(savedWallet.getAddress().toLowerCase());
        assertThat(savedWallet.getEncryptedPrivkey()).containsExactly((byte) 1, (byte) 2, (byte) 3);
        assertThat(savedWallet.getEncVersion()).isEqualTo(1);
        assertThat(savedWallet.getCreatedAt()).isNotNull();

        ArgumentCaptor<JsonNode> payloadCaptor = ArgumentCaptor.forClass(JsonNode.class);
        verify(outboxEventPublisher).publish(
            eq("User"),
            eq(result.userId().toString()),
            eq("UserSignedUp"),
            eq(result.userId().toString()),
            payloadCaptor.capture()
        );
        JsonNode payload = payloadCaptor.getValue();
        assertThat(payload.path("userId").asText()).isEqualTo(result.userId().toString());
        assertThat(payload.path("address").asText()).isEqualTo(result.address());
    }

    @Test
    @DisplayName("assertApproved는 사용자가 APPROVED가 아니면 403 예외를 던진다")
    void assertApprovedShouldThrowWhenUserIsNotApproved() {
        // given: 사용자의 compliance 상태가 APPROVED가 아니도록 준비한다.
        UUID userId = UUID.randomUUID();
        UserEntity user = new UserEntity();
        user.setUserId(userId);
        user.setComplianceStatus(ComplianceStatus.PENDING);
        when(userRepository.findById(userId)).thenReturn(Optional.of(user));

        // when: 승인 상태 검증을 수행한다.
        // then: FORBIDDEN 예외가 발생하고 메시지에 APPROVED 요구사항이 포함된다.
        assertThatThrownBy(() -> walletService.assertApproved(userId))
            .isInstanceOfSatisfying(ApiException.class, ex -> {
                assertThat(ex.getStatus()).isEqualTo(HttpStatus.FORBIDDEN);
                assertThat(ex.getMessage()).contains("APPROVED");
            });
    }

    @Test
    @DisplayName("getWallet은 지갑이 없으면 404 예외를 던진다")
    void getWalletShouldThrowNotFoundWhenWalletIsMissing() {
        // given: 조회 대상 사용자의 지갑이 저장소에 없도록 준비한다.
        UUID userId = UUID.randomUUID();
        when(walletRepository.findById(userId)).thenReturn(Optional.empty());

        // when: 지갑 조회를 수행한다.
        // then: NOT_FOUND 예외가 발생한다.
        assertThatThrownBy(() -> walletService.getWallet(userId))
            .isInstanceOfSatisfying(ApiException.class, ex -> {
                assertThat(ex.getStatus()).isEqualTo(HttpStatus.NOT_FOUND);
                assertThat(ex.getMessage()).contains("Wallet not found");
            });
    }
}
