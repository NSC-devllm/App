package com.example.cryptoorder.Account.entity;

import jakarta.persistence.*;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import org.hibernate.annotations.UpdateTimestamp;

import java.time.LocalDateTime;
import java.util.UUID;

@Entity
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Builder
@Table(name="naver_points")
public class NaverPoint {

    @Id
    @Column(name= "account_id")
    private UUID id;

    @OneToOne(fetch = FetchType.LAZY)
    @MapsId
    @JoinColumn(name = "account_id")
    @NotNull
    // 식별관계인 계정의 PK를 외래키로 참조
    private Account account;

    @NotNull
    private Long balance;

    @NotNull
    private boolean isActive;

    @UpdateTimestamp
    private LocalDateTime updateTime;

    /**
     * 포인트 해지 처리
     * 잔액이 남아있다면 예외를 발생시키거나, 별도 처리를 강제할 수 있음
     */
    public void closeAccount() {
        if (this.balance > 0) {
            throw new IllegalStateException("포인트 잔액이 남아있는 계좌는 해지할 수 없습니다.");
        }
        this.isActive = false;
        // 필요하다면 해지 시점 기록 등을 여기서 추가 수행
    }


    /**
     * 포인트 재활성화
     */
    public void reactivateAccount() {
        this.isActive = true;
    }


}
