package io.rwa.server.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.rwa.server.wallet.ComplianceStatus;
import io.rwa.server.wallet.UserEntity;
import io.rwa.server.wallet.UserRepository;
import io.rwa.server.wallet.WalletEntity;
import io.rwa.server.wallet.WalletRepository;
import io.rwa.server.outbox.OutboxEventPublisher;
import io.rwa.server.outbox.OutboxRetryScheduler;
import io.rwa.server.tx.TxReceiptScheduler;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

@SpringBootTest(properties = {
    "rwa.master-key-base64=MDEyMzQ1Njc4OWFiY2RlZg==",
    "rwa.outbox.kafka-enabled=false",
    "spring.task.scheduling.enabled=false",
    "spring.flyway.enabled=false",
    "spring.jpa.hibernate.ddl-auto=none"
})
@AutoConfigureMockMvc
@EnabledIfSystemProperty(named = "runPg18Integration", matches = "true")
class AuthSignupDbDefaultIntegrationTest {

    private static final String TEST_SCHEMA = "it_uuidv7";
    private static final String BASE_DB_URL = "jdbc:postgresql://localhost:5432/rwa";
    private static final String DB_USER = "rwa";
    private static final String DB_PASSWORD = "rwa_password";

    @DynamicPropertySource
    static void dataSourceProps(DynamicPropertyRegistry registry) {
        try (Connection conn = DriverManager.getConnection(BASE_DB_URL, DB_USER, DB_PASSWORD);
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE SCHEMA IF NOT EXISTS " + TEST_SCHEMA);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to prepare test schema: " + TEST_SCHEMA, e);
        }

        registry.add("spring.datasource.url", () -> BASE_DB_URL + "?currentSchema=" + TEST_SCHEMA);
        registry.add("spring.datasource.username", () -> DB_USER);
        registry.add("spring.datasource.password", () -> DB_PASSWORD);
    }

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private WalletRepository walletRepository;

    @Autowired
    private NamedParameterJdbcTemplate jdbcTemplate;

    @MockBean
    private OutboxEventPublisher outboxEventPublisher;

    @MockBean
    private TxReceiptScheduler txReceiptScheduler;

    @MockBean
    private OutboxRetryScheduler outboxRetryScheduler;

    private UUID createdUserId;
    private String usedIdempotencyKey;

    @BeforeEach
    void ensureSchema() {
        jdbcTemplate.getJdbcTemplate().execute("DROP TABLE IF EXISTS wallets");
        jdbcTemplate.getJdbcTemplate().execute("DROP TABLE IF EXISTS users");
        jdbcTemplate.getJdbcTemplate().execute("DROP TABLE IF EXISTS api_idempotency");
        jdbcTemplate.getJdbcTemplate().execute("DROP TABLE IF EXISTS outbox_tx");

        jdbcTemplate.getJdbcTemplate().execute("""
            CREATE TABLE users (
              user_id UUID PRIMARY KEY DEFAULT uuidv7(),
              created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
              compliance_status TEXT NOT NULL,
              compliance_updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            """);
        jdbcTemplate.getJdbcTemplate().execute("""
            CREATE TABLE wallets (
              user_id UUID PRIMARY KEY REFERENCES users(user_id),
              address VARCHAR(42) NOT NULL UNIQUE,
              encrypted_privkey BYTEA NOT NULL,
              enc_version INT NOT NULL,
              created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            """);
        jdbcTemplate.getJdbcTemplate().execute("""
            CREATE TABLE outbox_tx (
              outbox_id UUID PRIMARY KEY,
              request_id TEXT NOT NULL UNIQUE,
              from_address VARCHAR(42) NOT NULL,
              to_address VARCHAR(42),
              nonce BIGINT,
              tx_hash VARCHAR(66),
              raw_tx TEXT,
              status TEXT NOT NULL,
              tx_type TEXT NOT NULL,
              payload JSONB,
              last_error TEXT,
              created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
              updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            """);
        jdbcTemplate.getJdbcTemplate().execute("""
            CREATE TABLE api_idempotency (
              endpoint TEXT NOT NULL,
              idempotency_key TEXT NOT NULL,
              request_hash VARCHAR(64) NOT NULL,
              status TEXT NOT NULL,
              response_status INT,
              response_body JSONB,
              created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
              updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
              PRIMARY KEY (endpoint, idempotency_key)
            )
            """);
    }

    @AfterEach
    void cleanup() {
        if (createdUserId == null) {
            return;
        }

        jdbcTemplate.update(
            "DELETE FROM wallets WHERE user_id = :userId",
            Map.of("userId", createdUserId)
        );
        jdbcTemplate.update(
            "DELETE FROM users WHERE user_id = :userId",
            Map.of("userId", createdUserId)
        );
        if (usedIdempotencyKey != null) {
            jdbcTemplate.update(
                "DELETE FROM api_idempotency WHERE endpoint = :endpoint AND idempotency_key = :key",
                Map.of(
                    "endpoint", "POST /auth/signup",
                    "key", usedIdempotencyKey
                )
            );
        }
    }

    @Test
    @DisplayName("signup은 DB uuidv7 기본값으로 user_id를 생성하고 users/wallets를 저장한다")
    void signupShouldCreateUserIdViaDatabaseDefaultUuidV7() throws Exception {
        Instant before = Instant.now();
        usedIdempotencyKey = "it-signup-db-default-" + UUID.randomUUID();

        MvcResult result = mockMvc.perform(
                post("/auth/signup").header("Idempotency-Key", usedIdempotencyKey)
            )
            .andExpect(status().isOk())
            .andReturn();

        JsonNode body = objectMapper.readTree(result.getResponse().getContentAsString());
        createdUserId = UUID.fromString(body.path("userId").asText());
        String address = body.path("address").asText();
        Instant after = Instant.now();

        assertThat(createdUserId.version()).isEqualTo(7);
        assertThat(createdUserId.variant()).isEqualTo(2);
        assertThat(address).matches("^0x[0-9a-f]{40}$");

        Optional<UserEntity> userOpt = userRepository.findById(createdUserId);
        Optional<WalletEntity> walletOpt = walletRepository.findById(createdUserId);

        assertThat(userOpt).isPresent();
        assertThat(walletOpt).isPresent();

        UserEntity user = userOpt.orElseThrow();
        WalletEntity wallet = walletOpt.orElseThrow();

        assertThat(user.getComplianceStatus()).isEqualTo(ComplianceStatus.PENDING);
        assertThat(user.getCreatedAt()).isNotNull();
        assertThat(user.getComplianceUpdatedAt()).isNotNull();
        assertThat(user.getCreatedAt()).isEqualTo(user.getComplianceUpdatedAt());
        assertThat(user.getCreatedAt()).isBetween(before.minusSeconds(5), after.plusSeconds(5));

        assertThat(wallet.getAddress()).isEqualTo(address);
        assertThat(wallet.getAddress()).isEqualTo(wallet.getAddress().toLowerCase());
        assertThat(wallet.getEncryptedPrivkey()).isNotNull();
        assertThat(wallet.getEncVersion()).isEqualTo(1);
        assertThat(wallet.getCreatedAt()).isNotNull();
    }
}
