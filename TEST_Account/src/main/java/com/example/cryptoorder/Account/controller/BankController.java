package com.example.cryptoorder.Account.controller;

import com.example.cryptoorder.Account.dto.TransactionRequestDto;
import com.example.cryptoorder.Account.dto.TransactionResponseDto;
import com.example.cryptoorder.Account.entity.KRWAccountHistory;
import com.example.cryptoorder.Account.service.BankAccountService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 은행 거래 관련 API 컨트롤러
 * - 입금
 * - 출금
 */
@RestController
@RequestMapping("/api/bank")
@RequiredArgsConstructor
public class BankController {

    private final BankAccountService bankAccountService;

    /**
     * 입금 API
     * POST /api/bank/deposit
     */
    @PostMapping("/deposit")
    public ResponseEntity<TransactionResponseDto> deposit(@Valid @RequestBody TransactionRequestDto request) {
        KRWAccountHistory history = bankAccountService.deposit(
                request.accountId(),
                request.amount(),
                request.counterparty() // sender
        );

        TransactionResponseDto response = new TransactionResponseDto(
                history.getTransaction().getTransactionId().toString(),
                history.getTransactionType(),
                history.getTransaction().getStatus(),
                history.getAmount(),
                history.getBalanceAfterTransaction(),
                history.getTransaction().getTransactionDate());

        return ResponseEntity.ok(response);
    }

    /**
     * 출금 API
     * POST /api/bank/withdraw
     */
    @PostMapping("/withdraw")
    public ResponseEntity<TransactionResponseDto> withdraw(@Valid @RequestBody TransactionRequestDto request) {
        KRWAccountHistory history = bankAccountService.withdraw(
                request.accountId(),
                request.amount(),
                request.counterparty() // receiver
        );

        TransactionResponseDto response = new TransactionResponseDto(
                history.getTransaction().getTransactionId().toString(),
                history.getTransactionType(),
                history.getTransaction().getStatus(),
                history.getAmount(),
                history.getBalanceAfterTransaction(),
                history.getTransaction().getTransactionDate());

        return ResponseEntity.ok(response);
    }
}
