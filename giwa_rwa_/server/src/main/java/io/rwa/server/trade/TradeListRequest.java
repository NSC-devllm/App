package io.rwa.server.trade;

import jakarta.validation.constraints.NotNull;
import java.math.BigInteger;
import java.util.UUID;

public record TradeListRequest(
    @NotNull UUID sellerUserId,
    String tokenId,
    String unitId,
    @NotNull BigInteger amount,
    @NotNull BigInteger unitPrice
) {
}
