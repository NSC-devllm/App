package com.example.cryptoorder.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.DelegatingPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.web.cors.CorsConfiguration;
import org.springframework.web.cors.CorsConfigurationSource;
import org.springframework.web.cors.UrlBasedCorsConfigurationSource;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Configuration
@EnableWebSecurity
public class SecurityConfig {

    /**
     * Security Filter Chain 설정
     * - CORS 활성화
     * - CSRF 비활성화 (REST API용)
     * - 세션 Stateless 설정
     * - 인증 경로 설정
     */
    @Bean
    public SecurityFilterChain securityFilterChain(HttpSecurity http) throws Exception {
        http
                // CORS 설정 적용
                .cors(cors -> cors.configurationSource(corsConfigurationSource()))
                // CSRF 비활성화 (REST API이므로)
                .csrf(AbstractHttpConfigurer::disable)
                // 세션을 사용하지 않음 (Stateless)
                .sessionManagement(session -> session
                        .sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                // 요청별 권한 설정
                .authorizeHttpRequests(auth -> auth
                        // 인증 없이 접근 가능한 경로
                        .requestMatchers("/api/auth/**").permitAll()
                        .requestMatchers("/actuator/**").permitAll()
                        .requestMatchers("/h2-console/**").permitAll()
                        // 그 외 모든 요청은 인증 필요
                        .anyRequest().authenticated())
                // H2 콘솔을 위한 Frame 옵션 비활성화 (개발용)
                .headers(headers -> headers
                        .frameOptions(frame -> frame.sameOrigin()));

        return http.build();
    }

    /**
     * CORS 설정
     */
    @Bean
    public CorsConfigurationSource corsConfigurationSource() {
        CorsConfiguration configuration = new CorsConfiguration();
        // 허용할 Origin (개발 환경)
        configuration.setAllowedOrigins(Arrays.asList(
                "http://localhost:3000",
                "http://localhost:5173",
                "http://localhost:8080"));
        // 허용할 HTTP 메서드
        configuration.setAllowedMethods(Arrays.asList("GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"));
        // 허용할 헤더
        configuration.setAllowedHeaders(List.of("*"));
        // 자격 증명 허용
        configuration.setAllowCredentials(true);
        // Preflight 캐시 시간 (1시간)
        configuration.setMaxAge(3600L);

        UrlBasedCorsConfigurationSource source = new UrlBasedCorsConfigurationSource();
        source.registerCorsConfiguration("/**", configuration);
        return source;
    }

    /**
     * PasswordEncoder 설정
     * DelegatingPasswordEncoder를 사용하여 향후 암호화 알고리즘 변경에 유연하게 대응
     */
    @Bean
    public PasswordEncoder passwordEncoder() {
        // 1. 기본으로 사용할 암호화 ID 지정 (현재 가장 무난한 bcrypt 사용)
        String idForEncode = "bcrypt";

        // 2. 지원할 암호화 인코더 목록 생성
        Map<String, PasswordEncoder> encoders = new HashMap<>();

        // Bcrypt 설정 (Strength 10~12 권장)
        encoders.put("bcrypt", new BCryptPasswordEncoder());

        // 추후 컴플라이언스 기준에 맞춰서 암호화 강화 필요 시 전환 가능

        return new DelegatingPasswordEncoder(idForEncode, encoders);
    }
}
