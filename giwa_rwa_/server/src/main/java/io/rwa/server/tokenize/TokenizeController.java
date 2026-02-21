package io.rwa.server.tokenize;

import io.rwa.server.tokenize.TokenizeService.TokenizeResult;
import java.util.Map;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/admin/classes")
public class TokenizeController {

    private final TokenizeService tokenizeService;

    public TokenizeController(TokenizeService tokenizeService) {
        this.tokenizeService = tokenizeService;
    }

    @PostMapping("/{classId}/tokenize")
    public Map<String, Object> tokenize(
        @PathVariable String classId,
        @RequestHeader("Idempotency-Key") String idempotencyKey,
        @RequestBody(required = false) TokenizeRequest request
    ) {
        TokenizeResult result = tokenizeService.tokenizeClass(classId, request, idempotencyKey);
        return Map.of(
            "classId", result.classId(),
            "docHash", result.docHash(),
            "status", result.status(),
            "outboxIds", result.outboxIds()
        );
    }
}
