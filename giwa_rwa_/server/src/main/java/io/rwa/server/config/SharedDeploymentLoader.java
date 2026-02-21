package io.rwa.server.config;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.rwa.server.common.ApiException;
import jakarta.annotation.PostConstruct;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

@Component
public class SharedDeploymentLoader {

    private static final Set<String> REQUIRED_CONTRACTS = Set.of(
        "MockUSD",
        "PropertyRegistry",
        "PropertyShare1155",
        "PropertyTokenizer",
        "FixedPriceMarketDvP"
    );

    private final RwaProperties properties;
    private final ObjectMapper objectMapper;
    private final Map<String, String> addresses = new ConcurrentHashMap<>();

    public SharedDeploymentLoader(RwaProperties properties, ObjectMapper objectMapper) {
        this.properties = properties;
        this.objectMapper = objectMapper;
    }

    @PostConstruct
    public void load() {
        Path deploymentPath = Path.of(properties.getSharedDirPath(), "deployments", "giwa-sepolia.json");
        if (!Files.exists(deploymentPath)) {
            throw new ApiException(HttpStatus.INTERNAL_SERVER_ERROR, "Missing deployment file: " + deploymentPath);
        }
        try {
            JsonNode root = objectMapper.readTree(Files.readString(deploymentPath));
            JsonNode contracts = root.path("contracts");
            if (!contracts.isObject()) {
                throw new ApiException(HttpStatus.INTERNAL_SERVER_ERROR, "Invalid deployment file: contracts missing");
            }

            for (String contractName : REQUIRED_CONTRACTS) {
                String address = contracts.path(contractName).asText();
                if (address == null || address.isBlank()) {
                    throw new ApiException(HttpStatus.INTERNAL_SERVER_ERROR, "Missing address for " + contractName);
                }
                addresses.put(contractName, normalizeAddress(address));
            }

            if (contracts.has("KYCRegistry")) {
                throw new ApiException(HttpStatus.INTERNAL_SERVER_ERROR, "KYCRegistry must not be referenced");
            }
        } catch (IOException e) {
            throw new ApiException(HttpStatus.INTERNAL_SERVER_ERROR, "Failed to load deployment file: " + e.getMessage());
        }
    }

    public String addressOf(String contractName) {
        String address = addresses.get(contractName);
        if (address == null) {
            throw new ApiException(HttpStatus.INTERNAL_SERVER_ERROR, "Contract address not loaded: " + contractName);
        }
        return address;
    }

    private String normalizeAddress(String address) {
        return address.toLowerCase();
    }
}
