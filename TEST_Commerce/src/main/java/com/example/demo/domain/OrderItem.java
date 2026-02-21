package com.example.demo.domain;

import jakarta.persistence.*;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.math.BigDecimal;

@Entity
@Table(name = "order_items")
@Data
@NoArgsConstructor
public class OrderItem {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long itemId;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "order_id")
    private Order order; // 어떤 주문에 속하는지

    private Long productId; // 상품 ID

    private Integer quantity; // 구매 수량

    private BigDecimal priceAtPurchase; // 구매 당시 가격 (금융 기록의 핵심!)
}