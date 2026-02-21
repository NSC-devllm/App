package com.example.cryptoorder.Account.entity;

import jakarta.persistence.*;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import org.hibernate.annotations.CreationTimestamp;

import java.time.LocalDateTime;
import java.util.UUID;

@Entity
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Builder
@Table(name="naver_point_histories")
public class NaverPointHistory {

    @Id
    @GeneratedValue(strategy= GenerationType.UUID)
    //공간 절약과 인덱스 성능 향상을 위해서 BINARY(16)으로 컬럼 타입 설정
    @Column(columnDefinition = "BINARY(16)")
    private UUID pointHistoryId;

    @ManyToOne(fetch = FetchType.LAZY)
    // 식별관계인 계정의 PK를 외래키로 참조
    private Account account;

    //포인트 적립 일시
    @CreationTimestamp
    private LocalDateTime accrualDate;

    @NotNull
    private Long amount;

    @NotNull
    //포인트 적립 사유
    private String reason;



}
