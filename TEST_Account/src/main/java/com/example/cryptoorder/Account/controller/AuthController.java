package com.example.cryptoorder.Account.controller;

import com.example.cryptoorder.Account.dto.LoginRequestDto;
import com.example.cryptoorder.Account.dto.RegisterRequestDto;
import com.example.cryptoorder.Account.dto.UserResponseDto;
import com.example.cryptoorder.Account.entity.User;
import com.example.cryptoorder.Account.service.UserAccountService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 인증 관련 API 컨트롤러
 * - 회원가입
 * - 로그인
 */
@RestController
@RequestMapping("/api/auth")
@RequiredArgsConstructor
public class AuthController {

    private final UserAccountService userAccountService;

    /**
     * 회원가입 API
     * POST /api/auth/signup
     */
    @PostMapping("/signup")
    public ResponseEntity<UserResponseDto> signup(@Valid @RequestBody RegisterRequestDto request) {
        User user = userAccountService.createFullAccount(
                request.name(),
                request.phoneNumber(),
                request.birthDate(),
                request.loginId(),
                request.password());

        UserResponseDto response = new UserResponseDto(
                user.getUserHexId(),
                user.getUserName(),
                user.getPhoneNumber(),
                user.getCreatedAt(),
                user.isActive());

        return ResponseEntity.ok(response);
    }

    /**
     * 로그인 API
     * POST /api/auth/login
     */
    @PostMapping("/login")
    public ResponseEntity<UserResponseDto> login(@Valid @RequestBody LoginRequestDto request) {
        User user = userAccountService.login(request.loginId(), request.password());

        UserResponseDto response = new UserResponseDto(
                user.getUserHexId(),
                user.getUserName(),
                user.getPhoneNumber(),
                user.getCreatedAt(),
                user.isActive());

        return ResponseEntity.ok(response);
    }
}
