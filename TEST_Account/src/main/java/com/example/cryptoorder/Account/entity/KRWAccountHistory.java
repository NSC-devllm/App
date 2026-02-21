package com.example.cryptoorder.Account.entity;

import com.example.cryptoorder.Account.constant.TransactionType;
import jakarta.persistence.*;
import jakarta.validation.constraints.PositiveOrZero;
import lombok.*;
import org.hibernate.annotations.Check;
import org.hibernate.annotations.CreationTimestamp;
import org.springframework.data.annotation.CreatedDate;

import java.time.LocalDateTime;

@Entity
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Builder
@Table(name = "krw_account_histories")
public class KRWAccountHistory {

    // 추후 수평 확장시 계정별로 거래기록 관련 프로세스가 특정 DB로 가도록 보장하는 로직 추가 필요
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "transaction_id")
    private KRWTransaction transaction;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name= "account_id")
    private KRWAccount bankAccount;

    // String 대신 Enum 사용 (오타 방지, 타입 안전성)
    @Enumerated(EnumType.STRING)
    @Column(name = "transaction_type", nullable = false)
    private TransactionType transactionType;

    // 금액은 null일 수 없음
    @Column(nullable = false)
    private Long amount;

    // 거래 후 잔액 - 정합성 검증용
    // 서버 단에서 0미만으로 안가게 체크
    @PositiveOrZero
    // DB레벨에서 음수로 저장되는 것 방지
    // 잔고 부족 메세지 전파되도록 추후에 예외처리 로직 추가 필요
    @Check(constraints = "amount >= 0")
    @Column(name = "balance_after_transaction", nullable = false)
    private Long balanceAfterTransaction;

    // 거래 발생 시간
    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt;

    // 생성자 (검토 필요)
    public KRWAccountHistory(KRWAccount bankAccount, KRWTransaction transaction,
                             TransactionType type, Long amount, Long balanceAfter) {
        this.bankAccount = bankAccount;
        this.transaction = transaction;
        this.transactionType = type;
        this.amount = amount;
        this.balanceAfterTransaction = balanceAfter;
    }


}
