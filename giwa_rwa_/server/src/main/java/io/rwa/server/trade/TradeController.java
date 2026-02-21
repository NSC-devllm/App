package io.rwa.server.trade;

import jakarta.validation.Valid;
import java.util.Map;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/trade")
public class TradeController {

    private final TradeService tradeService;

    public TradeController(TradeService tradeService) {
        this.tradeService = tradeService;
    }

    @PostMapping("/list")
    public Map<String, Object> list(
        @Valid @RequestBody TradeListRequest request,
        @RequestHeader("Idempotency-Key") String idempotencyKey
    ) {
        TradeResult result = tradeService.list(request, idempotencyKey);
        return Map.of(
            "outboxIds", result.outboxIds(),
            "tokenId", result.tokenId(),
            "amount", result.amount(),
            "unitPrice", result.unitPrice()
        );
    }

    @PostMapping("/buy")
    public Map<String, Object> buy(
        @Valid @RequestBody TradeBuyRequest request,
        @RequestHeader("Idempotency-Key") String idempotencyKey
    ) {
        TradeResult result = tradeService.buy(request, idempotencyKey);
        return Map.of(
            "outboxIds", result.outboxIds(),
            "listingId", result.listingId(),
            "tokenId", result.tokenId(),
            "amount", result.amount(),
            "unitPrice", result.unitPrice(),
            "cost", result.cost()
        );
    }
}
