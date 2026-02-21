package io.rwa.server.idempotency;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Service;

@Service
public class IdempotencyService {

    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;

    public IdempotencyService(NamedParameterJdbcTemplate jdbcTemplate, ObjectMapper objectMapper) {
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
    }

    public boolean tryInsertInProgress(String endpoint, String idempotencyKey, String requestHash) {
        String sql = """
            INSERT INTO api_idempotency(endpoint, idempotency_key, request_hash, status, created_at, updated_at)
            VALUES (:endpoint, :key, :hash, :status, now(), now())
            ON CONFLICT(endpoint, idempotency_key) DO NOTHING
            """;
        int updated = jdbcTemplate.update(sql, new MapSqlParameterSource()
            .addValue("endpoint", endpoint)
            .addValue("key", idempotencyKey)
            .addValue("hash", requestHash)
            .addValue("status", IdempotencyRecordStatus.IN_PROGRESS.name()));
        return updated == 1;
    }

    public Optional<ApiIdempotencyRecord> find(String endpoint, String key) {
        String sql = """
            SELECT endpoint, idempotency_key, request_hash, status, response_status, response_body, created_at, updated_at
            FROM api_idempotency
            WHERE endpoint = :endpoint AND idempotency_key = :key
            """;
        List<ApiIdempotencyRecord> rows = jdbcTemplate.query(sql,
            Map.of("endpoint", endpoint, "key", key),
            (rs, rowNum) -> toRecord(rs));
        return rows.stream().findFirst();
    }

    public void markCompleted(String endpoint, String key, int responseStatus, JsonNode responseBody) {
        String sql = """
            UPDATE api_idempotency
            SET status = :status,
                response_status = :responseStatus,
                response_body = CAST(:responseBody AS jsonb),
                updated_at = now()
            WHERE endpoint = :endpoint AND idempotency_key = :key
            """;
        jdbcTemplate.update(sql, new MapSqlParameterSource()
            .addValue("status", IdempotencyRecordStatus.COMPLETED.name())
            .addValue("responseStatus", responseStatus)
            .addValue("responseBody", responseBody == null ? "null" : responseBody.toString())
            .addValue("endpoint", endpoint)
            .addValue("key", key));
    }

    public void markFailed(String endpoint, String key, int responseStatus, JsonNode responseBody) {
        String sql = """
            UPDATE api_idempotency
            SET status = :status,
                response_status = :responseStatus,
                response_body = CAST(:responseBody AS jsonb),
                updated_at = now()
            WHERE endpoint = :endpoint AND idempotency_key = :key
            """;
        jdbcTemplate.update(sql, new MapSqlParameterSource()
            .addValue("status", IdempotencyRecordStatus.FAILED.name())
            .addValue("responseStatus", responseStatus)
            .addValue("responseBody", responseBody == null ? "null" : responseBody.toString())
            .addValue("endpoint", endpoint)
            .addValue("key", key));
    }

    private ApiIdempotencyRecord toRecord(ResultSet rs) throws SQLException {
        JsonNode body = null;
        String bodyString = rs.getString("response_body");
        if (bodyString != null) {
            try {
                body = objectMapper.readTree(bodyString);
            } catch (Exception ignored) {
                body = objectMapper.createObjectNode().put("raw", bodyString);
            }
        }
        return new ApiIdempotencyRecord(
            rs.getString("endpoint"),
            rs.getString("idempotency_key"),
            rs.getString("request_hash"),
            IdempotencyRecordStatus.valueOf(rs.getString("status")),
            rs.getObject("response_status") == null ? null : rs.getInt("response_status"),
            body,
            rs.getObject("created_at", Instant.class),
            rs.getObject("updated_at", Instant.class)
        );
    }
}
