package com.example.demo.domain;

import jakarta.persistence.*;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

@Entity
@Table(name = "orders") // DB 테이블명
@Data
@NoArgsConstructor
public class Order {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long orderId;

    private String userId; // 주문자 ID

    private BigDecimal totalAmount; // 총 주문 금액

    @Enumerated(EnumType.STRING)
    private OrderStatus status; // 주문 상태 (PENDING, PAID 등)

    private LocalDateTime createdAt; // 주문 일시

    // 주문 상세와의 일대다 관계 (Cascade를 걸어야 주문 저장 시 상세도 같이 저장돼)
    @OneToMany(mappedBy = "order", cascade = CascadeType.ALL, orphanRemoval = true)
    private List<OrderItem> orderItems = new ArrayList<>();

    @PrePersist
    protected void onCreate() {
        this.createdAt = LocalDateTime.now();
        if (this.status == null) this.status = OrderStatus.PENDING;
    }
}