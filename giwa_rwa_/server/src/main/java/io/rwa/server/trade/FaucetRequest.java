package io.rwa.server.trade;

import jakarta.validation.constraints.NotNull;
import java.math.BigInteger;
import java.util.UUID;

public record FaucetRequest(
    @NotNull UUID toUserId,
    @NotNull BigInteger amount
) {
}
