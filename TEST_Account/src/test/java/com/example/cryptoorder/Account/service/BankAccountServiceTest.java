package com.example.cryptoorder.Account.service;

import com.example.cryptoorder.Account.entity.KRWAccount;
import com.example.cryptoorder.Account.repository.KRWAccountBalanceRepository;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class BankAccountServiceTest {

    @Mock // 1. Repository는 가짜 (Mock)
    private KRWAccountBalanceRepository accountRepository;

    @InjectMocks // 2. Service는 진짜 (Mock Repository가 주입된 형태)
    private BankAccountService bankAccountService;

    @Test
    @DisplayName("잔액보다 큰 금액을 출금하려 하면 예외가 발생해야 한다")
    void withdraw_Fail_InsufficientBalance() {
        // given
        UUID accountId = UUID.randomUUID();
        KRWAccount account = KRWAccount.builder()
                .id(accountId)
                .balance(5000L) // 잔액 5천원
                .build();

        // Lock 조회 시 잔액 5천원인 계좌 반환 모킹
        when(accountRepository.findByIdWithLock(accountId)).thenReturn(Optional.of(account));

        // when & then
        // 10,000원 출금 시도 -> IllegalStateException 발생 검증
        assertThatThrownBy(() -> bankAccountService.withdraw(accountId, 10000L, "Receiver"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("잔액이 부족합니다.");
    }
}
