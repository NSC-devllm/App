package io.rwa.server.wallet;

import java.util.UUID;

public record SignupResult(
    UUID userId,
    String address
) {
}
