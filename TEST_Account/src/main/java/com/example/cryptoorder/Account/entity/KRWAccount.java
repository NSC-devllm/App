package com.example.cryptoorder.Account.entity;

import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.UpdateTimestamp;

import java.time.LocalDateTime;
import java.util.UUID;

@Entity
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Builder
@Table(name="krw_accounts")
public class KRWAccount{

    @Id
    @GeneratedValue(strategy= GenerationType.UUID)
    //공간 절약과 인덱스 성능 향상을 위해서 BINARY(16)으로 컬럼 타입 설정
    @Column(name="acount_id",columnDefinition = "BINARY(16)")
    //내부 계좌 관리 id (외부에 노출되지 않음)
    private UUID id;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name="user_id")
    private User user;

    //계좌 번호 (실제 입출금에 사용되는 번호)
    //계좌 번호 규정에 따른 검증 로직 추가 필요
    private String accountNumber;

    private String ownerName;

    private boolean isActive;

    private Long balance;

    @UpdateTimestamp
    private LocalDateTime updateTime;

    /**
     * 계좌 해지 처리
     * 잔액이 남아있다면 예외를 발생시키거나, 별도 처리를 강제할 수 있음
     */
    public void closeAccount() {
        if (this.balance > 0) {
            throw new IllegalStateException("잔액이 남아있는 계좌는 해지할 수 없습니다.");
        }
        this.isActive = false;
        // 필요하다면 해지 시점 기록 등을 여기서 추가 수행
    }

    /**
     * 계좌 재활성화
     */
    public void reactivateAccount() {
        this.isActive = true;
    }

    /**
     * 입금 처리
     */
    public void deposit(Long amount) {
        if (amount <= 0) {
            throw new IllegalArgumentException("입금액은 0보다 커야 합니다.");
        }
        this.balance += amount;
    }

    /**
     * 출금 처리
     */
    public void withdraw(Long amount) {
        if (amount <= 0) {
            throw new IllegalArgumentException("출금액은 0보다 커야 합니다.");
        }
        if (this.balance < amount) {
            throw new IllegalStateException("잔액이 부족합니다.");
        }
        this.balance -= amount;
    }

}
