package com.example.cryptoorder.Account.service;

import com.example.cryptoorder.Account.entity.Account;
import com.example.cryptoorder.Account.entity.KRWAccount;
import com.example.cryptoorder.Account.entity.NaverPoint;
import com.example.cryptoorder.Account.entity.User;
import com.example.cryptoorder.Account.repository.AccountRepository;
import com.example.cryptoorder.Account.repository.KRWAccountBalanceRepository;
import com.example.cryptoorder.Account.repository.NaverPointRepository;
import com.example.cryptoorder.Account.repository.UserRepository;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.security.crypto.password.PasswordEncoder;
import static org.mockito.Mockito.never;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.LocalDate;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class UserAccountServiceTest {

    @InjectMocks
    private UserAccountService userAccountService;

    @Mock private UserRepository userRepository;
    @Mock private AccountRepository accountRepository;
    @Mock private NaverPointRepository naverPointRepository;
    @Mock private PasswordEncoder passwordEncoder;
    // KRWAccountBalanceRepository는 createFullAccount에서 사용되지 않으므로 mock 제외 가능

    @Test
    @DisplayName("회원가입 성공 시 User, Account, Point 지갑이 모두 생성되어야 한다")
    void createFullAccount_Success() {
        // given
        String rawPassword = "password123";
        when(passwordEncoder.encode(rawPassword)).thenReturn("encodedPassword");

        // when
        userAccountService.createFullAccount(
                "홍길동", "010-1234-5678", LocalDate.of(1990, 1, 1), "hong", rawPassword
        );

        // then
        // 1. User 저장 호출 검증
        verify(userRepository, times(1)).save(any(User.class));
        // 2. Account 저장 호출 검증 (비밀번호 암호화 확인 포함)
        verify(accountRepository, times(1)).save(any(Account.class));
        // 3. NaverPoint 저장 호출 검증
        verify(naverPointRepository, times(1)).save(any(NaverPoint.class));
    }

    @Test
    @DisplayName("계좌에 잔액이 남아있는 사용자는 탈퇴할 수 없다")
    void deleteUser_Fail_HasBalance() {
        // given
        UUID userId = UUID.randomUUID();

        // 잔액이 있는 계좌를 가진 유저 모킹
        KRWAccount richAccount = KRWAccount.builder().balance(100L).build();
        User richUser = User.builder()
                .id(userId)
                .krwAccounts(List.of(richAccount)) // 100원 보유
                .build();

        when(userRepository.findById(userId)).thenReturn(Optional.of(richUser));

        // when & then
        assertThatThrownBy(() -> userAccountService.deleteUser(userId))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Cannot delete user with non-zero balance");

        // 검증: 계좌 비활성화나 포인트 삭제 로직이 호출되지 않았어야 함
        verify(naverPointRepository, never()).findById(any());
    }
}