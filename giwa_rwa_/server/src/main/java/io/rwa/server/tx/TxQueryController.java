package io.rwa.server.tx;

import io.rwa.server.common.ApiException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/tx")
public class TxQueryController {

    private final TxOrchestratorService txOrchestratorService;

    public TxQueryController(TxOrchestratorService txOrchestratorService) {
        this.txOrchestratorService = txOrchestratorService;
    }

    @GetMapping("/outbox/{outboxId}")
    public Map<String, Object> getOutbox(@PathVariable UUID outboxId) {
        OutboxTxEntity tx = txOrchestratorService.findByOutboxId(outboxId)
            .orElseThrow(() -> new ApiException(HttpStatus.NOT_FOUND, "Outbox tx not found"));
        return toMap(tx);
    }

    @GetMapping("/by-hash/{txHash}")
    public Map<String, Object> getByHash(@PathVariable String txHash) {
        OutboxTxEntity tx = txOrchestratorService.findByHash(txHash)
            .orElseThrow(() -> new ApiException(HttpStatus.NOT_FOUND, "Tx not found"));
        return toMap(tx);
    }

    private Map<String, Object> toMap(OutboxTxEntity tx) {
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("outboxId", tx.getOutboxId());
        data.put("requestId", tx.getRequestId());
        data.put("fromAddress", tx.getFromAddress());
        data.put("toAddress", tx.getToAddress());
        data.put("nonce", tx.getNonce());
        data.put("txHash", tx.getTxHash());
        data.put("status", tx.getStatus());
        data.put("txType", tx.getTxType());
        data.put("lastError", tx.getLastError());
        data.put("createdAt", tx.getCreatedAt());
        data.put("updatedAt", tx.getUpdatedAt());
        return data;
    }
}
