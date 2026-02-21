package io.rwa.server.trade;

import java.math.BigInteger;
import java.util.List;
import java.util.UUID;

public record TradeResult(
    List<UUID> outboxIds,
    BigInteger listingId,
    BigInteger tokenId,
    BigInteger amount,
    BigInteger unitPrice,
    BigInteger cost
) {
}
