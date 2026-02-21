package io.rwa.server.query;

import io.rwa.server.common.ApiException;
import io.rwa.server.wallet.WalletService;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Service;

@Service
public class ReadModelQueryService {

    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final WalletService walletService;

    public ReadModelQueryService(NamedParameterJdbcTemplate jdbcTemplate, WalletService walletService) {
        this.jdbcTemplate = jdbcTemplate;
        this.walletService = walletService;
    }

    public List<Map<String, Object>> marketListings(String classId, String status) {
        StringBuilder sql = new StringBuilder("""
            SELECT id, property_id, listing_status, price, created_at, updated_at
            FROM listings
            WHERE 1 = 1
            """);
        MapSqlParameterSource params = new MapSqlParameterSource();
        if (classId != null && !classId.isBlank()) {
            sql.append(" AND property_id = :classId");
            params.addValue("classId", classId);
        }
        if (status != null && !status.isBlank()) {
            sql.append(" AND listing_status = :status");
            params.addValue("status", status);
        }
        sql.append(" ORDER BY updated_at DESC");

        return jdbcTemplate.queryForList(sql.toString(), params);
    }

    public List<Map<String, Object>> holdings(UUID userId) {
        String address = walletService.getAddress(userId);
        String sql = """
            SELECT owner, token_id, amount, updated_at
            FROM balances
            WHERE lower(owner) = lower(:owner)
            ORDER BY token_id ASC
            """;
        return jdbcTemplate.queryForList(sql, Map.of("owner", address));
    }

    public List<Map<String, Object>> tokenHolders(String tokenId, int limit) {
        String sql = """
            SELECT owner, token_id, amount, updated_at
            FROM balances
            WHERE CAST(token_id AS TEXT) = :tokenId
            ORDER BY amount DESC
            LIMIT :limit
            """;
        return jdbcTemplate.queryForList(sql, new MapSqlParameterSource()
            .addValue("tokenId", tokenId)
            .addValue("limit", limit));
    }

    public Map<String, Object> listingById(String listingId) {
        String sql = """
            SELECT id, property_id, listing_status, price, created_at, updated_at
            FROM listings
            WHERE CAST(id AS TEXT) = :listingId
            """;
        List<Map<String, Object>> rows = jdbcTemplate.queryForList(sql, Map.of("listingId", listingId));
        if (rows.isEmpty()) {
            throw new ApiException(HttpStatus.NOT_FOUND, "Listing not found in read model");
        }
        return new HashMap<>(rows.get(0));
    }
}
