package com.example.demo.payment.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class PayPrepareRequest {
    private String user_id;
    private String merchant_name;
    private BigDecimal amount;
}
