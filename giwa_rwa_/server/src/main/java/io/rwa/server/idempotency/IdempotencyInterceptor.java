package io.rwa.server.idempotency;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.rwa.server.common.ApiException;
import io.rwa.server.common.CanonicalJsonService;
import io.rwa.server.common.HashUtils;
import io.rwa.server.security.CachedBodyHttpServletRequest;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;
import org.springframework.web.servlet.HandlerMapping;

@Component
public class IdempotencyInterceptor implements HandlerInterceptor {

    private static final String IDEMPOTENCY_KEY_HEADER = "Idempotency-Key";
    private static final String CONTEXT_ATTR = "idempotency.context";

    private final IdempotencyService idempotencyService;
    private final ObjectMapper objectMapper;
    private final CanonicalJsonService canonicalJsonService;

    public IdempotencyInterceptor(
        IdempotencyService idempotencyService,
        ObjectMapper objectMapper,
        CanonicalJsonService canonicalJsonService
    ) {
        this.idempotencyService = idempotencyService;
        this.objectMapper = objectMapper;
        this.canonicalJsonService = canonicalJsonService;
    }

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        if (!isMutating(request)) {
            return true;
        }

        String idempotencyKey = request.getHeader(IDEMPOTENCY_KEY_HEADER);
        if (idempotencyKey == null || idempotencyKey.isBlank()) {
            throw new ApiException(HttpStatus.BAD_REQUEST, "Idempotency-Key required");
        }

        String endpoint = normalizeEndpointId(request);
        String requestHash = computeRequestHash(request);

        boolean inserted = idempotencyService.tryInsertInProgress(endpoint, idempotencyKey, requestHash);
        if (inserted) {
            request.setAttribute(CONTEXT_ATTR, new IdempotencyContext(endpoint, idempotencyKey, requestHash));
            return true;
        }

        Optional<ApiIdempotencyRecord> existingOpt = idempotencyService.find(endpoint, idempotencyKey);
        if (existingOpt.isEmpty()) {
            throw new ApiException(HttpStatus.CONFLICT, "Idempotency conflict");
        }

        ApiIdempotencyRecord existing = existingOpt.get();
        if (!requestHash.equals(existing.requestHash())) {
            throw new ApiException(HttpStatus.CONFLICT, "Idempotency-Key reuse with different payload");
        }

        if (existing.status() == IdempotencyRecordStatus.COMPLETED) {
            writeStoredResponse(response, existing);
            return false;
        }

        if (existing.status() == IdempotencyRecordStatus.IN_PROGRESS) {
            response.setStatus(HttpStatus.ACCEPTED.value());
            response.setContentType("application/json");
            ObjectNode body = objectMapper.createObjectNode();
            body.put("status", "IN_PROGRESS");
            body.put("endpoint", endpoint);
            body.put("idempotencyKey", idempotencyKey);
            body.put("statusUrl", "/idempotency/status?endpoint=" + endpoint + "&key=" + idempotencyKey);
            response.getWriter().write(body.toString());
            return false;
        }

        int status = existing.responseStatus() == null ? 409 : existing.responseStatus();
        response.setStatus(status);
        response.setContentType("application/json");
        response.getWriter().write(existing.responseBody() == null
            ? objectMapper.createObjectNode().put("status", "FAILED").toString()
            : existing.responseBody().toString());
        return false;
    }

    @Override
    public void afterCompletion(HttpServletRequest request, HttpServletResponse response, Object handler, Exception ex) {
        Object contextObj = request.getAttribute(CONTEXT_ATTR);
        if (!(contextObj instanceof IdempotencyContext context)) {
            return;
        }

        JsonNode responseBody = (JsonNode) request.getAttribute(IdempotencyResponseAdvice.RESPONSE_BODY_ATTR);
        int statusCode = response.getStatus();

        if (ex != null || statusCode >= 500) {
            ObjectNode error = objectMapper.createObjectNode();
            error.put("message", ex == null ? "request failed" : ex.getMessage());
            idempotencyService.markFailed(context.endpoint(), context.idempotencyKey(), statusCode == 0 ? 500 : statusCode, error);
            return;
        }

        if (responseBody == null) {
            responseBody = objectMapper.createObjectNode();
        }
        idempotencyService.markCompleted(context.endpoint(), context.idempotencyKey(), statusCode, responseBody);
    }

    private boolean isMutating(HttpServletRequest request) {
        String method = request.getMethod();
        return HttpMethod.POST.matches(method)
            || HttpMethod.PUT.matches(method)
            || HttpMethod.PATCH.matches(method)
            || HttpMethod.DELETE.matches(method);
    }

    private String normalizeEndpointId(HttpServletRequest request) {
        String template = (String) request.getAttribute(HandlerMapping.BEST_MATCHING_PATTERN_ATTRIBUTE);
        if (template == null || template.isBlank()) {
            template = request.getRequestURI();
        }
        return request.getMethod() + " " + template;
    }

    private String computeRequestHash(HttpServletRequest request) {
        Map<String, Object> canonical = new LinkedHashMap<>();
        canonical.put("path", request.getRequestURI());

        Map<String, Object> query = new LinkedHashMap<>();
        request.getParameterMap().entrySet().stream()
            .sorted(Map.Entry.comparingByKey())
            .forEach(entry -> query.put(entry.getKey(), entry.getValue()));
        canonical.put("query", query);

        JsonNode bodyNode = objectMapper.createObjectNode();
        byte[] body = extractBody(request);
        if (body.length > 0) {
            try {
                bodyNode = objectMapper.readTree(body);
            } catch (Exception ignored) {
                ObjectNode raw = objectMapper.createObjectNode();
                raw.put("raw", new String(body, StandardCharsets.UTF_8));
                bodyNode = raw;
            }
        }
        canonical.put("body", bodyNode);

        String canonicalJson = canonicalJsonService.canonicalString(objectMapper.valueToTree(canonical));
        return HashUtils.sha256Hex(canonicalJson);
    }

    private byte[] extractBody(HttpServletRequest request) {
        if (request instanceof CachedBodyHttpServletRequest wrapped) {
            return wrapped.getCachedBody();
        }
        return new byte[0];
    }

    private void writeStoredResponse(HttpServletResponse response, ApiIdempotencyRecord existing) throws Exception {
        int status = existing.responseStatus() == null ? 200 : existing.responseStatus();
        response.setStatus(status);
        response.setContentType("application/json");
        if (existing.responseBody() == null) {
            response.getWriter().write("{}");
            return;
        }
        response.getWriter().write(existing.responseBody().toString());
    }
}
