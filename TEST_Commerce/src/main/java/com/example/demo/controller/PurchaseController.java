package com.example.demo.controller;

import com.example.demo.payment.PaymentService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;

@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
public class PurchaseController {

    private final PaymentService paymentService;

    @PostMapping("/purchase")
    public String purchaseNikeShoes() {
        // Hardcoded scenario for demo
        String userId = "test_user_01";
        String product = "Nike Air Force";
        BigDecimal price = BigDecimal.valueOf(50000);

        try {
            paymentService.processPayment(userId, product, price);
            return "Purchase Successful!";
        } catch (Exception e) {
            e.printStackTrace();
            return "Purchase Failed: " + e.getMessage();
        }
    }
}
