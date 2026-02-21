package io.rwa.server.wallet;

import io.rwa.server.common.ApiException;
import io.rwa.server.config.RwaProperties;
import jakarta.annotation.PostConstruct;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Base64;
import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

@Service
public class WalletCryptoService {

    private static final int IV_LENGTH = 12;
    private static final int TAG_LENGTH = 128;

    private final RwaProperties properties;
    private final SecureRandom secureRandom = new SecureRandom();
    private SecretKeySpec masterKey;

    public WalletCryptoService(RwaProperties properties) {
        this.properties = properties;
    }

    @PostConstruct
    public void init() {
        String base64 = properties.getMasterKeyBase64();
        if (base64 == null || base64.isBlank()) {
            throw new ApiException(HttpStatus.INTERNAL_SERVER_ERROR, "MASTER_KEY_BASE64 is required");
        }
        byte[] keyBytes = Base64.getDecoder().decode(base64);
        if (keyBytes.length != 16 && keyBytes.length != 24 && keyBytes.length != 32) {
            throw new ApiException(HttpStatus.INTERNAL_SERVER_ERROR, "MASTER_KEY_BASE64 must decode to 16/24/32 bytes");
        }
        this.masterKey = new SecretKeySpec(keyBytes, "AES");
    }

    public byte[] encryptPrivateKey(String privateKeyHex) {
        try {
            byte[] iv = new byte[IV_LENGTH];
            secureRandom.nextBytes(iv);

            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            cipher.init(Cipher.ENCRYPT_MODE, masterKey, new GCMParameterSpec(TAG_LENGTH, iv));
            byte[] encrypted = cipher.doFinal(privateKeyHex.getBytes(StandardCharsets.UTF_8));

            byte[] out = new byte[IV_LENGTH + encrypted.length];
            System.arraycopy(iv, 0, out, 0, IV_LENGTH);
            System.arraycopy(encrypted, 0, out, IV_LENGTH, encrypted.length);
            return out;
        } catch (Exception e) {
            throw new ApiException(HttpStatus.INTERNAL_SERVER_ERROR, "Failed to encrypt private key");
        }
    }

    public String decryptPrivateKey(byte[] encrypted) {
        try {
            byte[] iv = Arrays.copyOfRange(encrypted, 0, IV_LENGTH);
            byte[] ciphertext = Arrays.copyOfRange(encrypted, IV_LENGTH, encrypted.length);

            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            cipher.init(Cipher.DECRYPT_MODE, masterKey, new GCMParameterSpec(TAG_LENGTH, iv));
            byte[] plain = cipher.doFinal(ciphertext);
            return new String(plain, StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new ApiException(HttpStatus.INTERNAL_SERVER_ERROR, "Failed to decrypt private key");
        }
    }
}
