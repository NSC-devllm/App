package com.example.cryptoorder.Account.service;

import com.example.cryptoorder.Account.entity.KRWAccount;
import com.example.cryptoorder.Account.repository.KRWAccountBalanceRepository;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
class BankAccountServiceConcurrencyTest {

    @Autowired
    private BankAccountService bankAccountService;

    @Autowired
    private KRWAccountBalanceRepository accountRepository;

    @Test
    @DisplayName("동시에 100번 입금 요청 시 잔액 정합성이 보장되어야 한다 (비관적 락 검증)")
    void depositConcurrencyTest() throws InterruptedException {
        // given
        // 테스트용 계좌 생성 및 저장 (초기 잔액 0원)
        KRWAccount account = KRWAccount.builder()
                .accountNumber("123-456")
                .balance(0L)
                .isActive(true)
                .build();
        KRWAccount savedAccount = accountRepository.save(account);
        UUID accountId = savedAccount.getId();

        int numberOfThreads = 100;
        long depositAmount = 1000L;
        ExecutorService executorService = Executors.newFixedThreadPool(32);
        CountDownLatch latch = new CountDownLatch(numberOfThreads);

        // when
        for (int i = 0; i < numberOfThreads; i++) {
            executorService.submit(() -> {
                try {
                    // 입금 서비스 호출
                    bankAccountService.deposit(accountId, depositAmount, "Tester");
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await(); // 모든 스레드가 끝날 때까지 대기

        // then
        KRWAccount resultAccount = accountRepository.findById(accountId).orElseThrow();

        // 1000원 * 100번 = 100,000원이 되어야 함
        // 락이 동작하지 않으면 Race Condition으로 인해 이 금액보다 적게 나옴
        assertThat(resultAccount.getBalance()).isEqualTo(numberOfThreads * depositAmount);
    }

    @Test
    @DisplayName("동시에 출금 요청 시 잔액 부족으로 일부 요청만 성공해야 한다 (동시성 제어 + 예외 처리)")
    void withdrawConcurrencyTest() throws InterruptedException {
        // given
        // 1. 초기 잔액 10,000원 설정
        KRWAccount account = KRWAccount.builder()
                .accountNumber("999-999")
                .balance(10_000L) // 1만원
                .isActive(true)
                .build();
        KRWAccount savedAccount = accountRepository.save(account);
        UUID accountId = savedAccount.getId();

        int numberOfThreads = 10;
        long withdrawAmount = 2_000L; // 2천원씩 10번 인출 시도 (총 2만원 시도) -> 5번만 성공해야 함

        ExecutorService executorService = Executors.newFixedThreadPool(10);
        CountDownLatch latch = new CountDownLatch(numberOfThreads);

        // 성공 횟수와 실패 횟수를 세기 위한 카운터 (스레드 안전)
        AtomicInteger successCount = new AtomicInteger();
        AtomicInteger failCount = new AtomicInteger();

        // when
        for (int i = 0; i < numberOfThreads; i++) {
            executorService.submit(() -> {
                try {
                    bankAccountService.withdraw(accountId, withdrawAmount, "Receiver");
                    successCount.getAndIncrement();
                } catch (Exception e) {
                    // 잔액 부족 예외 발생 시 실패 카운트 증가
                    failCount.getAndIncrement();
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();

        // then
        KRWAccount resultAccount = accountRepository.findById(accountId).orElseThrow();

        // 1. 잔액 검증: 10,000원에서 2,000원씩 5번 빠져나갔으니 0원이 되어야 함
        assertThat(resultAccount.getBalance()).isEqualTo(0L);

        // 2. 횟수 검증: 5번 성공, 5번 실패(잔액부족)
        assertThat(successCount.get()).isEqualTo(5);
        assertThat(failCount.get()).isEqualTo(5);
    }
}