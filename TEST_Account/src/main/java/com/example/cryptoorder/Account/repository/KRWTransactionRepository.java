package com.example.cryptoorder.Account.repository;

import com.example.cryptoorder.Account.entity.KRWTransaction;
import org.springframework.data.jpa.repository.JpaRepository;

public interface KRWTransactionRepository extends JpaRepository<KRWTransaction, Long> {
}