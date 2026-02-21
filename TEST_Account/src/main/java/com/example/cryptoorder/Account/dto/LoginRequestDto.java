package com.example.cryptoorder.Account.dto;

import jakarta.validation.constraints.NotBlank;

/**
 * 로그인 요청 DTO
 */
public record LoginRequestDto(
        @NotBlank(message = "아이디는 필수입니다.") String loginId,

        @NotBlank(message = "비밀번호는 필수입니다.") String password) {
}
