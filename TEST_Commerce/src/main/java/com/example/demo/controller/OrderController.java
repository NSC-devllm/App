package com.example.demo.controller;

import com.example.demo.domain.Order;
import com.example.demo.service.OrderService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

/**
 * [주문 컨트롤러]
 * 역할: 클라이언트의 HTTP 요청을 받고 서비스 계층으로 전달
 */
@RestController
@RequestMapping("/api/orders")
@RequiredArgsConstructor
public class OrderController {
    private final OrderService orderService;

    // 주문 생성 API
    @PostMapping
    public ResponseEntity<Order> placeOrder(
            @RequestParam String userId,
            @RequestParam Long productId,
            @RequestParam int quantity) {
        return ResponseEntity.ok(orderService.createOrder(userId, productId, quantity));
    }

    /**
     * 결제 확정 API
     * 예시: POST /api/orders/1/confirm
     */
    @PostMapping("/{orderId}/confirm")
    public ResponseEntity<Order> confirmPayment(@PathVariable Long orderId) {
        Order updatedOrder = orderService.confirmPayment(orderId);
        return ResponseEntity.ok(updatedOrder);
    }
}