package com.example.demo.payment.dto;

import lombok.Data;

@Data
public class PayPrepareResponse {
    private String status;
    private String order_id;
    private String frozen_amount;
}
