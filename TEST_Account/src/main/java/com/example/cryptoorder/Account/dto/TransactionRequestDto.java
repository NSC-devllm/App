package com.example.cryptoorder.Account.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import java.util.UUID;

/**
 * 입출금 요청 DTO
 */
public record TransactionRequestDto(
        @NotNull(message = "계좌 ID는 필수입니다.") UUID accountId,

        @NotNull(message = "금액은 필수입니다.") @Positive(message = "금액은 0보다 커야 합니다.") Long amount,

        @NotBlank(message = "상대방 정보는 필수입니다.") String counterparty // 입금 시 sender, 출금 시 receiver
) {
}
