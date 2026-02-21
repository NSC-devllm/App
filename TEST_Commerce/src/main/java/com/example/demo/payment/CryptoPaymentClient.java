package com.example.demo.payment;

import com.example.demo.payment.dto.PayConfirmRequest;
import com.example.demo.payment.dto.PayPrepareRequest;
import com.example.demo.payment.dto.PayPrepareResponse;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;

@Component
public class CryptoPaymentClient {

    private final WebClient webClient;

    public CryptoPaymentClient(WebClient.Builder webClientBuilder) {
        // CryptoTeam Python Server Address
        this.webClient = webClientBuilder.baseUrl("http://localhost:8000").build();
    }

    // 1. Prepare Payment (Freeze Funds)
    public PayPrepareResponse preparePayment(String userId, String merchantName, BigDecimal amount) {
        return webClient.post()
                .uri("/api/pay/prepare")
                .bodyValue(new PayPrepareRequest(userId, merchantName, amount))
                .retrieve()
                .bodyToMono(PayPrepareResponse.class)
                .block(); // Synchronous call for simplicity
    }

    // 2. Confirm Payment (Deduct Funds)
    public String confirmPayment(String orderId) {
        return webClient.post()
                .uri("/api/pay/confirm")
                .bodyValue(new PayConfirmRequest(orderId))
                .retrieve()
                .bodyToMono(String.class) // Response is JSON, but we just need success status
                .block();
    }
}
