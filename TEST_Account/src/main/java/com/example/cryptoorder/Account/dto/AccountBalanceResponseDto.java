package com.example.cryptoorder.Account.dto;

/**
 * 계좌 잔액 조회 응답 DTO
 */
public record AccountBalanceResponseDto(
        String accountNumber,
        Long balance,
        boolean isActive) {
}
