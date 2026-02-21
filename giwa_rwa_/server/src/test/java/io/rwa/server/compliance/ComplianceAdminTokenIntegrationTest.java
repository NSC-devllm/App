package io.rwa.server.compliance;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import io.rwa.server.config.RwaProperties;
import io.rwa.server.idempotency.IdempotencyInterceptor;
import io.rwa.server.wallet.ComplianceStatus;
import io.rwa.server.wallet.UserEntity;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.web.servlet.MockMvc;

@WebMvcTest(controllers = ComplianceController.class)
class ComplianceAdminTokenIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private ComplianceService complianceService;

    @MockBean
    private IdempotencyInterceptor idempotencyInterceptor;

    @MockBean
    private RwaProperties properties;

    @BeforeEach
    void setUp() throws Exception {
        doReturn(true).when(idempotencyInterceptor).preHandle(any(), any(), any());
        when(properties.getAdminApiToken()).thenReturn("admin-secret-token");
    }

    @Test
    @DisplayName("관리자 토큰이 없으면 /admin/compliance/users 요청은 401을 반환한다")
    void shouldRejectAdminRequestWhenTokenHeaderIsMissing() throws Exception {
        // given: 관리자 토큰 헤더가 없는 /admin 요청을 준비한다.
        // when: /admin/compliance/users를 호출한다.
        // then: AdminTokenFilter가 401을 반환하고 서비스 로직은 호출되지 않는다.
        mockMvc.perform(get("/admin/compliance/users").param("status", "APPROVED"))
            .andExpect(status().isUnauthorized())
            .andExpect(jsonPath("$.status").value(401))
            .andExpect(jsonPath("$.message").value("Invalid admin token"));

        verify(complianceService, never()).findByStatus(any());
    }

    @Test
    @DisplayName("유효한 관리자 토큰이면 /admin/compliance/users 요청은 200과 사용자 목록을 반환한다")
    void shouldAllowAdminRequestWithValidToken() throws Exception {
        // given: 유효한 관리자 토큰과 APPROVED 사용자 조회 결과를 준비한다.
        UUID userId = UUID.fromString("7f2edf86-4c41-4104-ab4b-469f2d16f83e");
        UserEntity user = new UserEntity();
        user.setUserId(userId);
        user.setComplianceStatus(ComplianceStatus.APPROVED);
        user.setComplianceUpdatedAt(Instant.parse("2026-02-17T10:00:00Z"));
        when(complianceService.findByStatus(ComplianceStatus.APPROVED)).thenReturn(List.of(user));

        // when: 유효한 X-Admin-Token으로 /admin/compliance/users를 호출한다.
        mockMvc.perform(
            get("/admin/compliance/users")
                .param("status", "APPROVED")
                .header("X-Admin-Token", "admin-secret-token")
        )
            .andExpect(status().isOk())
            .andExpect(jsonPath("$[0].userId").value(userId.toString()))
            .andExpect(jsonPath("$[0].status").value("APPROVED"));

        // then: complianceService가 APPROVED 상태로 조회된다.
        verify(complianceService).findByStatus(eq(ComplianceStatus.APPROVED));
    }
}
