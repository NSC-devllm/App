package io.rwa.server.common;

import java.time.Instant;

public record ErrorResponse(
    String message,
    int status,
    Instant timestamp
) {
}
