package com.example.cryptoorder.Account.controller;

import com.example.cryptoorder.Account.dto.RegisterRequestDto;
import com.example.cryptoorder.Account.entity.User;
import com.example.cryptoorder.Account.service.UserAccountService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.time.LocalDate;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
class AuthControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @MockitoBean
    private UserAccountService userAccountService;

    @Test
    @DisplayName("회원가입 시 필수 입력값이 누락되면 400 에러를 반환해야 한다")
    void signup_Fail_Validation() throws Exception {
        // given
        // 전화번호 형식이 틀리고 이름이 비어있는 잘못된 요청
        RegisterRequestDto badRequest = new RegisterRequestDto(
                "", // name blank
                "01012345678", // invalid phone pattern
                LocalDate.now(),
                "testId",
                "pw"
        );

        // when & then
        mockMvc.perform(post("/api/auth/signup")
                                .contentType(MediaType.APPLICATION_JSON)
                                .content(objectMapper.writeValueAsString(badRequest))
                        // SecurityConfig에서 /api/auth/** 는 permitAll 이므로 csrf 등 고려 불필요
                )
                .andExpect(status().isBadRequest()) // 400 Bad Request
                .andExpect(jsonPath("$.errorCode").value("VALIDATION_ERROR"));
    }

    @Test
    @DisplayName("정상적인 회원가입 요청은 200 OK를 반환해야 한다")
    void signup_Success() throws Exception {
        // given
        RegisterRequestDto request = new RegisterRequestDto(
                "홍길동",
                "010-1234-5678",
                LocalDate.of(1990, 1, 1),
                "hong",
                "password"
        );

        // Mocking 서비스 응답
        User mockUser = User.builder()
                .userName("홍길동")
                .phoneNumber("010-1234-5678")
                .isActive(true)
                .build();

        given(userAccountService.createFullAccount(any(), any(), any(), any(), any()))
                .willReturn(mockUser);

        // when & then
        mockMvc.perform(post("/api/auth/signup")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.userName").value("홍길동"));
    }
}