package com.example.cryptoorder.Account.entity;

import com.example.cryptoorder.Account.constant.TransactionStatus;
import com.example.cryptoorder.Account.constant.TransactionType;
import io.hypersistence.utils.hibernate.id.Tsid;
import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.GenericGenerator;
import org.hibernate.annotations.UpdateTimestamp;

import java.time.LocalDateTime;

@Entity
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Builder
@Table(name="krw_transactions")
public class KRWTransaction {
    @Id
    @Tsid
    private Long transactionId;

    @ManyToOne(fetch = FetchType.LAZY)
    private User relatedUserId;

    private String sender;

    private String receiver;

    private Long amount;

    private TransactionStatus status;

    private TransactionType transactionType;

    @CreationTimestamp
    private LocalDateTime transactionDate;


}
