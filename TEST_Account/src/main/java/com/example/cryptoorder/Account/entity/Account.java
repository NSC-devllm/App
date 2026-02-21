package com.example.cryptoorder.Account.entity;

import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.UuidGenerator;

import java.util.UUID;

@Entity
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Builder
@Table(name="accounts")
@ToString(exclude = "userLoginPw")
public class Account {

    @Id
    @Column(name="account_id", columnDefinition = "BINARY(16)")
    private UUID id;

    //User 클래스의 사용자 내부식별ID를 외래키로 참조
    //외래키를 포함하고 있기때문에 주인
    //1대1 관계이므로 Unique 제약조건 설정
    //유저를 부모로 두고 계정을 자식으로 두는 식별관계
    @OneToOne(fetch = FetchType.LAZY)
    @JoinColumn(unique = true)
    @MapsId
    private User user;

    private String userLoginId;

    //추후에 비밀번호 관련 검증 로직 추가 필요
    @Column(nullable = false)
    private String userLoginPw;


}
