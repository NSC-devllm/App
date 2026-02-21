package io.rwa.server.config;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.rwa.server.common.ApiException;
import jakarta.annotation.PostConstruct;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

@Component
public class SharedAbiLoader {

    private final RwaProperties properties;
    private final ObjectMapper objectMapper;
    private final Map<String, JsonNode> abiByContract = new ConcurrentHashMap<>();

    public SharedAbiLoader(RwaProperties properties, ObjectMapper objectMapper) {
        this.properties = properties;
        this.objectMapper = objectMapper;
    }

    @PostConstruct
    public void load() {
        Path abiDir = Path.of(properties.getSharedDirPath(), "abi");
        String[] names = new String[] {
            "MockUSD",
            "PropertyRegistry",
            "PropertyShare1155",
            "PropertyTokenizer",
            "FixedPriceMarketDvP"
        };
        for (String name : names) {
            Path abiPath = abiDir.resolve(name + ".json");
            if (!Files.exists(abiPath)) {
                throw new ApiException(HttpStatus.INTERNAL_SERVER_ERROR, "Missing ABI file: " + abiPath);
            }
            try {
                JsonNode root = objectMapper.readTree(Files.readString(abiPath));
                abiByContract.put(name, root.path("abi"));
            } catch (IOException e) {
                throw new ApiException(HttpStatus.INTERNAL_SERVER_ERROR, "Failed to load ABI " + name + ": " + e.getMessage());
            }
        }
    }

    public JsonNode abiOf(String contractName) {
        JsonNode abi = abiByContract.get(contractName);
        if (abi == null) {
            throw new ApiException(HttpStatus.INTERNAL_SERVER_ERROR, "ABI not loaded: " + contractName);
        }
        return abi;
    }
}
