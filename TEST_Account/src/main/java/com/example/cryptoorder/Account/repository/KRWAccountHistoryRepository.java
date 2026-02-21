package com.example.cryptoorder.Account.repository;

import com.example.cryptoorder.Account.entity.KRWAccountHistory;
import org.springframework.data.jpa.repository.JpaRepository;

public interface KRWAccountHistoryRepository extends JpaRepository<KRWAccountHistory, Long> {
}