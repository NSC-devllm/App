package com.example.cryptoorder.Account.dto;

import java.time.LocalDateTime;

/**
 * 에러 응답 DTO
 */
public record ErrorResponseDto(
        String errorCode,
        String message,
        LocalDateTime timestamp) {
    public ErrorResponseDto(String errorCode, String message) {
        this(errorCode, message, LocalDateTime.now());
    }
}
