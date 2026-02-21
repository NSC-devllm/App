package io.rwa.server.config;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.rwa.server.common.ApiException;
import jakarta.annotation.PostConstruct;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

@Component
public class SharedConstantsLoader {

    private final RwaProperties properties;
    private final ObjectMapper objectMapper;

    private BigInteger shareScale = BigInteger.TEN.pow(18);
    private int confirmations = 12;

    public SharedConstantsLoader(RwaProperties properties, ObjectMapper objectMapper) {
        this.properties = properties;
        this.objectMapper = objectMapper;
    }

    @PostConstruct
    public void load() {
        Path constantsPath = Path.of(properties.getSharedDirPath(), "config", "constants.json");
        if (!Files.exists(constantsPath)) {
            throw new ApiException(HttpStatus.INTERNAL_SERVER_ERROR, "Missing constants file: " + constantsPath);
        }
        try {
            JsonNode root = objectMapper.readTree(Files.readString(constantsPath));
            this.shareScale = root.path("SHARE_SCALE").bigIntegerValue();
            this.confirmations = root.path("CONFIRMATIONS").asInt(12);
        } catch (IOException e) {
            throw new ApiException(HttpStatus.INTERNAL_SERVER_ERROR, "Failed to load constants: " + e.getMessage());
        }
    }

    public BigInteger getShareScale() {
        return shareScale;
    }

    public int getConfirmations() {
        return confirmations;
    }
}
