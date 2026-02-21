package com.example.demo.payment;

import com.example.demo.payment.dto.PayPrepareResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;

@Service
@RequiredArgsConstructor
public class PaymentService {

    private final CryptoPaymentClient cryptoPaymentClient;

    public void processPayment(String userId, String productName, BigDecimal amount) {
        System.out.println("Processing payment for: " + productName + " (" + amount + " KRW)");

        // 1. Prepare (Freeze)
        PayPrepareResponse prepareRes = cryptoPaymentClient.preparePayment(userId, "Naver Shopping", amount);
        System.out.println("Payment Prepared: " + prepareRes.getOrder_id());

        // 2. Confirm (Settle)
        // In a real app, we would save the order here before confirming
        String confirmRes = cryptoPaymentClient.confirmPayment(prepareRes.getOrder_id());
        System.out.println("Payment Confirmed: " + confirmRes);
    }
}
