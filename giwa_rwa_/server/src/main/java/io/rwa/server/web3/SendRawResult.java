package io.rwa.server.web3;

public record SendRawResult(
    String rawTransaction,
    String txHash
) {
}
