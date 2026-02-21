package com.example.cryptoorder.Account.repository;

import com.example.cryptoorder.Account.entity.Account;
import com.example.cryptoorder.Account.entity.NaverPoint;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;
import java.util.UUID;

public interface NaverPointRepository extends JpaRepository<NaverPoint, UUID> {

    Optional<NaverPoint> findByAccount_Id(UUID accountId);

    Optional<NaverPoint> findByAccount(Account account);

}