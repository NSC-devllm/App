package com.example.cryptoorder.Account.repository;

import com.example.cryptoorder.Account.entity.NaverPointHistory;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

public interface NaverPointHistoryRepository extends JpaRepository<NaverPointHistory, UUID> {
}