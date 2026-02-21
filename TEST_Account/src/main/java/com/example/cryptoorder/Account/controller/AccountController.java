package com.example.cryptoorder.Account.controller;

import com.example.cryptoorder.Account.dto.AccountBalanceResponseDto;
import com.example.cryptoorder.Account.entity.KRWAccount;
import com.example.cryptoorder.Account.repository.KRWAccountBalanceRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

/**
 * 계좌 관련 API 컨트롤러
 * - 계좌 잔액 조회
 */
@RestController
@RequestMapping("/api/account")
@RequiredArgsConstructor
public class AccountController {

    private final KRWAccountBalanceRepository krwAccountRepository;

    /**
     * 계좌 잔액 조회 API
     * GET /api/account/{accountId}/balance
     */
    @GetMapping("/{accountId}/balance")
    public ResponseEntity<AccountBalanceResponseDto> getBalance(@PathVariable UUID accountId) {
        KRWAccount account = krwAccountRepository.findById(accountId)
                .orElseThrow(() -> new IllegalArgumentException("계좌를 찾을 수 없습니다."));

        AccountBalanceResponseDto response = new AccountBalanceResponseDto(
                account.getAccountNumber(),
                account.getBalance(),
                account.isActive());

        return ResponseEntity.ok(response);
    }
}
