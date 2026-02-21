package io.rwa.server.publicdata;

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.validation.constraints.NotNull;

public record ComplexImportRequest(
    @NotNull JsonNode rawItemJson
) {
}
