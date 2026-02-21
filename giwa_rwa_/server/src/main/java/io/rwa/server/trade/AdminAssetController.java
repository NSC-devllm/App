package io.rwa.server.trade;

import jakarta.validation.Valid;
import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/admin")
public class AdminAssetController {

    private final AdminAssetService adminAssetService;

    public AdminAssetController(AdminAssetService adminAssetService) {
        this.adminAssetService = adminAssetService;
    }

    @PostMapping("/faucet/musd")
    public Map<String, Object> faucet(
        @Valid @RequestBody FaucetRequest request,
        @RequestHeader("Idempotency-Key") String idempotencyKey
    ) {
        AdminAssetResult result = adminAssetService.faucetMockUsd(request, idempotencyKey);
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("outboxId", result.outboxId());
        data.put("txType", result.txType());
        data.put("txStatus", result.txStatus());
        data.put("txHash", result.txHash());
        return data;
    }

    @PostMapping("/distribute/shares")
    public Map<String, Object> distributeShares(
        @Valid @RequestBody DistributeSharesRequest request,
        @RequestHeader("Idempotency-Key") String idempotencyKey
    ) {
        AdminAssetResult result = adminAssetService.distributeShares(request, idempotencyKey);
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("outboxId", result.outboxId());
        data.put("txType", result.txType());
        data.put("txStatus", result.txStatus());
        data.put("txHash", result.txHash());
        return data;
    }
}
