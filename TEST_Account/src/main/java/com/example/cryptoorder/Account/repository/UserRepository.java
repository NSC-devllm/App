package com.example.cryptoorder.Account.repository;

import com.example.cryptoorder.Account.entity.User;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

public interface UserRepository extends JpaRepository<User, UUID> {
}