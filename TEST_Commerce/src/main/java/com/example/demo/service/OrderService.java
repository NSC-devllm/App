package com.example.demo.service;

import com.example.demo.domain.*;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.math.BigDecimal;

/**
 * [주문 서비스]
 * 역할: 주문 생성, 재고 차감, 결제 상태 변경 등 핵심 비즈니스 로직 처리
 * 특징: @Transactional을 통해 모든 DB 작업의 원자성을 보장함 (하나라도 실패하면 롤백)
 */
@Service
@RequiredArgsConstructor
public class OrderService {
    private final OrderRepository orderRepository;
    private final ProductRepository productRepository;

    /**
     * [주문 생성 로직]
     * 1. 상품 존재 여부 확인
     * 2. 재고 확인 및 가예약(차감)
     * 3. 주문 마스터 및 상세 정보 생성 (기본 상태: PENDING)
     */
    @Transactional
    public Order createOrder(String userId, Long productId, int quantity) {
        Product product = productRepository.findById(productId)
                .orElseThrow(() -> new RuntimeException("상품을 찾을 수 없습니다."));

        if (product.getStockQuantity() < quantity) {
            throw new RuntimeException("재고가 부족합니다.");
        }
        product.setStockQuantity(product.getStockQuantity() - quantity);

        Order order = new Order();
        order.setUserId(userId);
        order.setTotalAmount(product.getPriceKrw().multiply(BigDecimal.valueOf(quantity)));
        order.setStatus(OrderStatus.PENDING); // 초기 보류 상태

        OrderItem orderItem = new OrderItem();
        orderItem.setOrder(order);
        orderItem.setProductId(productId);
        orderItem.setQuantity(quantity);
        orderItem.setPriceAtPurchase(product.getPriceKrw());
        order.getOrderItems().add(orderItem);

        return orderRepository.save(order);
    }

    /**
     * [결제 확정 로직]
     * 역할: 보류(PENDING) 상태의 주문을 결제 완료(PAID) 상태로 전환
     * 확장성: 향후 토큰 결제 완료 이벤트나 사빈님용 분석 로그 발행 기능을 여기에 추가
     */
    @Transactional
    public Order confirmPayment(Long orderId) {
        Order order = orderRepository.findById(orderId)
                .orElseThrow(() -> new RuntimeException("주문 내역을 찾을 수 없습니다."));

        if (order.getStatus() != OrderStatus.PENDING) {
            throw new RuntimeException("결제 대기 중인 주문이 아닙니다. 현재 상태: " + order.getStatus());
        }

        order.setStatus(OrderStatus.PAID); // 상태 전환
        return order;
    }
}