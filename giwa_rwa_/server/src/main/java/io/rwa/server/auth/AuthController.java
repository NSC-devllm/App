package io.rwa.server.auth;

import io.rwa.server.wallet.SignupResult;
import io.rwa.server.wallet.WalletService;
import java.util.Map;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class AuthController {

    private final WalletService walletService;

    public AuthController(WalletService walletService) {
        this.walletService = walletService;
    }

    @PostMapping("/auth/signup")
    public Map<String, Object> signup() {
        SignupResult result = walletService.signup();
        return Map.of(
            "userId", result.userId(),
            "address", result.address()
        );
    }
}
