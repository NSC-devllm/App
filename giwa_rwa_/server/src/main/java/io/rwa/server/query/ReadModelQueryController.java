package io.rwa.server.query;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping
public class ReadModelQueryController {

    private final ReadModelQueryService readModelQueryService;

    public ReadModelQueryController(ReadModelQueryService readModelQueryService) {
        this.readModelQueryService = readModelQueryService;
    }

    @GetMapping("/market/listings")
    public List<Map<String, Object>> listings(
        @RequestParam(value = "classId", required = false) String classId,
        @RequestParam(value = "status", required = false) String status
    ) {
        return readModelQueryService.marketListings(classId, status);
    }

    @GetMapping("/users/{userId}/holdings")
    public List<Map<String, Object>> holdings(@PathVariable UUID userId) {
        return readModelQueryService.holdings(userId);
    }

    @GetMapping("/tokens/{tokenId}/holders")
    public List<Map<String, Object>> holders(
        @PathVariable String tokenId,
        @RequestParam(value = "limit", defaultValue = "100") int limit
    ) {
        return readModelQueryService.tokenHolders(tokenId, limit);
    }
}
