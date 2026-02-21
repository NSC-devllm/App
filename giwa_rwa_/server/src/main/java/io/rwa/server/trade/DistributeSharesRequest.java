package io.rwa.server.trade;

import jakarta.validation.constraints.NotNull;
import java.math.BigInteger;
import java.util.UUID;

public record DistributeSharesRequest(
    @NotNull UUID toUserId,
    String tokenId,
    String unitId,
    @NotNull BigInteger amount
) {
}
