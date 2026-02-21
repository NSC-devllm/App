package io.rwa.server.wallet;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.rwa.server.common.ApiException;
import io.rwa.server.outbox.OutboxEventPublisher;
import jakarta.transaction.Transactional;
import java.time.Instant;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.web3j.crypto.ECKeyPair;
import org.web3j.crypto.Keys;
import org.web3j.utils.Numeric;

@Service
public class WalletService {

    private final UserRepository userRepository;
    private final WalletRepository walletRepository;
    private final WalletCryptoService walletCryptoService;
    private final OutboxEventPublisher outboxEventPublisher;
    private final ObjectMapper objectMapper;

    public WalletService(
        UserRepository userRepository,
        WalletRepository walletRepository,
        WalletCryptoService walletCryptoService,
        OutboxEventPublisher outboxEventPublisher,
        ObjectMapper objectMapper
    ) {
        this.userRepository = userRepository;
        this.walletRepository = walletRepository;
        this.walletCryptoService = walletCryptoService;
        this.outboxEventPublisher = outboxEventPublisher;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public SignupResult signup() {
        try {
            ECKeyPair keyPair = Keys.createEcKeyPair();
            String address = "0x" + Keys.getAddress(keyPair.getPublicKey());
            String privateKeyHex = Numeric.toHexStringNoPrefixZeroPadded(keyPair.getPrivateKey(), 64);

            UserEntity user = new UserEntity();
            user.setCreatedAt(Instant.now());
            user.setComplianceStatus(ComplianceStatus.PENDING);
            user.setComplianceUpdatedAt(Instant.now());
            UserEntity savedUser = userRepository.save(user);
            UUID userId = savedUser.getUserId();
            if (userId == null) {
                throw new ApiException(HttpStatus.INTERNAL_SERVER_ERROR, "Failed to get generated user_id from database");
            }

            WalletEntity wallet = new WalletEntity();
            wallet.setUserId(userId);
            wallet.setAddress(address.toLowerCase());
            wallet.setEncryptedPrivkey(walletCryptoService.encryptPrivateKey(privateKeyHex));
            wallet.setEncVersion(1);
            wallet.setCreatedAt(Instant.now());
            walletRepository.save(wallet);

            outboxEventPublisher.publish(
                "User",
                userId.toString(),
                "UserSignedUp",
                userId.toString(),
                objectMapper.createObjectNode()
                    .put("userId", userId.toString())
                    .put("address", address.toLowerCase())
            );

            return new SignupResult(userId, address.toLowerCase());
        } catch (ApiException e) {
            throw e;
        } catch (Exception e) {
            throw new ApiException(HttpStatus.INTERNAL_SERVER_ERROR, "Failed to create custodial wallet: " + e.getMessage());
        }
    }

    public UserEntity getUser(UUID userId) {
        return userRepository.findById(userId)
            .orElseThrow(() -> new ApiException(HttpStatus.NOT_FOUND, "User not found: " + userId));
    }

    public WalletEntity getWallet(UUID userId) {
        return walletRepository.findById(userId)
            .orElseThrow(() -> new ApiException(HttpStatus.NOT_FOUND, "Wallet not found for user: " + userId));
    }

    public String getAddress(UUID userId) {
        return getWallet(userId).getAddress();
    }

    public String decryptUserPrivateKey(UUID userId) {
        return walletCryptoService.decryptPrivateKey(getWallet(userId).getEncryptedPrivkey());
    }

    public void assertApproved(UUID userId) {
        UserEntity user = getUser(userId);
        if (user.getComplianceStatus() != ComplianceStatus.APPROVED) {
            throw new ApiException(HttpStatus.FORBIDDEN, "User compliance_status must be APPROVED");
        }
    }
}
