package com.example.cryptoorder.Account.dto;

import java.time.LocalDateTime;

/**
 * 사용자 응답 DTO (Entity 대신 반환)
 */
public record UserResponseDto(
        String userHexId,
        String userName,
        String phoneNumber,
        LocalDateTime createdAt,
        boolean isActive) {
}
