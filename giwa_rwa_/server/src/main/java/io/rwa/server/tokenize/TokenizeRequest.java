package io.rwa.server.tokenize;

public record TokenizeRequest(
    Integer chunkSize,
    Long issuedAt
) {
}
