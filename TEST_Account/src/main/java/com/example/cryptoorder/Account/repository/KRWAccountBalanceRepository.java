package com.example.cryptoorder.Account.repository;

import com.example.cryptoorder.Account.entity.KRWAccount;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.Optional;
import java.util.UUID;

public interface KRWAccountBalanceRepository extends JpaRepository<KRWAccount, UUID> {
    // 비관적 락을 적용하여 계좌 조회 (쓰기 잠금)
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select k from KRWAccount k where k.id = :id")
    Optional<KRWAccount> findByIdWithLock(@Param("id") UUID id);
}