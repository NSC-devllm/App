package io.rwa.server.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.rwa.server.common.CanonicalJsonService;
import io.rwa.server.config.RwaProperties;
import io.rwa.server.config.WebMvcConfig;
import io.rwa.server.idempotency.ApiIdempotencyRecord;
import io.rwa.server.idempotency.IdempotencyInterceptor;
import io.rwa.server.idempotency.IdempotencyRecordStatus;
import io.rwa.server.idempotency.IdempotencyService;
import io.rwa.server.wallet.SignupResult;
import io.rwa.server.wallet.WalletService;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.test.web.servlet.MockMvc;

@WebMvcTest(controllers = AuthController.class)
@Import({ WebMvcConfig.class, IdempotencyInterceptor.class, CanonicalJsonService.class })
class AuthIdempotencyIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @MockBean
    private WalletService walletService;

    @MockBean
    private IdempotencyService idempotencyService;

    @MockBean
    private RwaProperties properties;

    @Test
    @DisplayName("signup은 Idempotency-Key 헤더가 없으면 400을 반환한다")
    void shouldReturnBadRequestWhenIdempotencyHeaderIsMissing() throws Exception {
        // given: Idempotency-Key 헤더 없이 signup 요청을 보낸다.
        // when: /auth/signup 엔드포인트를 호출한다.
        // then: 필수 키 누락으로 400 응답을 반환한다.
        mockMvc.perform(post("/auth/signup"))
            .andExpect(status().isBadRequest())
            .andExpect(jsonPath("$.status").value(400))
            .andExpect(jsonPath("$.message").value("Idempotency-Key required"));
    }

    @Test
    @DisplayName("signup 첫 요청은 정상 응답 후 idempotency COMPLETED 레코드를 저장한다")
    void shouldStoreCompletedResponseOnFirstRequest() throws Exception {
        // given: 신규 idempotency key로 첫 signup 요청이 성공하도록 목 동작을 준비한다.
        UUID userId = UUID.fromString("44389f5e-40f0-48e7-a6fb-1987d7e27d37");
        when(idempotencyService.tryInsertInProgress(anyString(), eq("signup-k1"), anyString())).thenReturn(true);
        when(walletService.signup()).thenReturn(new SignupResult(userId, "0xabc"));

        // when: /auth/signup 요청을 전송한다.
        mockMvc.perform(post("/auth/signup").header("Idempotency-Key", "signup-k1"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.userId").value(userId.toString()))
            .andExpect(jsonPath("$.address").value("0xabc"));

        // then: 응답 본문이 idempotency completed 레코드로 저장된다.
        ArgumentCaptor<JsonNode> responseBodyCaptor = ArgumentCaptor.forClass(JsonNode.class);
        verify(idempotencyService).markCompleted(anyString(), eq("signup-k1"), eq(200), responseBodyCaptor.capture());
        JsonNode responseBody = responseBodyCaptor.getValue();
        assertThat(responseBody.path("userId").asText()).isEqualTo(userId.toString());
        assertThat(responseBody.path("address").asText()).isEqualTo("0xabc");
    }

    @Test
    @DisplayName("signup 재요청은 COMPLETED idempotency 응답을 재사용하고 비즈니스 로직을 재실행하지 않는다")
    void shouldReturnStoredResponseWhenIdempotencyRecordIsAlreadyCompleted() throws Exception {
        // given: 동일 key에 대한 COMPLETED idempotency 레코드(캐시 응답)가 존재하도록 준비한다.
        JsonNode cachedBody = objectMapper.createObjectNode().put("userId", "cached-user").put("address", "0xcached");
        when(idempotencyService.tryInsertInProgress(anyString(), eq("signup-k2"), anyString())).thenAnswer(invocation -> {
            String requestHash = invocation.getArgument(2, String.class);
            when(idempotencyService.find(anyString(), eq("signup-k2"))).thenReturn(Optional.of(
                new ApiIdempotencyRecord(
                    "POST /auth/signup",
                    "signup-k2",
                    requestHash,
                    IdempotencyRecordStatus.COMPLETED,
                    201,
                    cachedBody,
                    Instant.now(),
                    Instant.now()
                )
            ));
            return false;
        });

        // when: 동일 key로 /auth/signup을 다시 호출한다.
        mockMvc.perform(post("/auth/signup").header("Idempotency-Key", "signup-k2"))
            .andExpect(status().isCreated())
            .andExpect(jsonPath("$.userId").value("cached-user"))
            .andExpect(jsonPath("$.address").value("0xcached"));

        // then: 저장된 응답을 재사용하고 signup 비즈니스 로직은 재실행하지 않는다.
        verify(walletService, never()).signup();
        verify(idempotencyService, never()).markCompleted(anyString(), anyString(), anyInt(), any(JsonNode.class));
    }
}
