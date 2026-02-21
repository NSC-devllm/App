package io.rwa.server.compliance;

import jakarta.validation.constraints.NotNull;
import java.util.UUID;

public record ComplianceRequest(
    @NotNull UUID userId
) {
}
