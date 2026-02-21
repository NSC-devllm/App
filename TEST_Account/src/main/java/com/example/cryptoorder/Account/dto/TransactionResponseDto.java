package com.example.cryptoorder.Account.dto;

import com.example.cryptoorder.Account.constant.TransactionStatus;
import com.example.cryptoorder.Account.constant.TransactionType;
import java.time.LocalDateTime;

/**
 * 거래 내역 응답 DTO
 */
public record TransactionResponseDto(
        String transactionId,
        TransactionType transactionType,
        TransactionStatus status,
        Long amount,
        Long balanceAfterTransaction,
        LocalDateTime transactionTime) {
}
