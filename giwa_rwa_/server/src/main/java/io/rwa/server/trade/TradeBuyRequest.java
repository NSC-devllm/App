package io.rwa.server.trade;

import jakarta.validation.constraints.NotNull;
import java.math.BigInteger;
import java.util.UUID;

public record TradeBuyRequest(
    @NotNull UUID buyerUserId,
    @NotNull BigInteger listingId,
    @NotNull BigInteger amount
) {
}
