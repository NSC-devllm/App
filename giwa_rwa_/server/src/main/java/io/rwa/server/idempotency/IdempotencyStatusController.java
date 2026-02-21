package io.rwa.server.idempotency;

import io.rwa.server.common.ApiException;
import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class IdempotencyStatusController {

    private final IdempotencyService idempotencyService;

    public IdempotencyStatusController(IdempotencyService idempotencyService) {
        this.idempotencyService = idempotencyService;
    }

    @GetMapping("/idempotency/status")
    public Map<String, Object> getStatus(@RequestParam("endpoint") String endpoint, @RequestParam("key") String key) {
        ApiIdempotencyRecord record = idempotencyService.find(endpoint, key)
            .orElseThrow(() -> new ApiException(HttpStatus.NOT_FOUND, "Idempotency record not found"));

        Map<String, Object> data = new LinkedHashMap<>();
        data.put("endpoint", record.endpoint());
        data.put("idempotencyKey", record.idempotencyKey());
        data.put("status", record.status().name());
        data.put("responseStatus", record.responseStatus());
        data.put("responseBody", record.responseBody());
        data.put("createdAt", record.createdAt());
        data.put("updatedAt", record.updatedAt());
        return data;
    }
}
