package com.example.cryptoorder.Account.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Past;
import java.time.LocalDate;

/**
 * 회원가입 요청 DTO
 */
public record RegisterRequestDto(
        @NotBlank(message = "이름은 필수입니다.") String name,

        @NotBlank(message = "전화번호는 필수입니다.") @Pattern(regexp = "^010-\\d{4}-\\d{4}$", message = "전화번호 형식이 올바르지 않습니다. (예: 010-1234-5678)") String phoneNumber,

        @Past(message = "생년월일은 과거 날짜여야 합니다.") LocalDate birthDate,

        @NotBlank(message = "아이디는 필수입니다.") String loginId,

        @NotBlank(message = "비밀번호는 필수입니다.") String password) {
}
