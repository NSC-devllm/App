package io.rwa.server.trade;

import java.util.UUID;

public record AdminAssetResult(
    UUID outboxId,
    String txType,
    String txStatus,
    String txHash
) {
}
