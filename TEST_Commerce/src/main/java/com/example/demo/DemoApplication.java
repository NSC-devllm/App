package com.example.demo;

import com.example.demo.domain.Product;
import com.example.demo.domain.ProductRepository;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import java.math.BigDecimal;

@SpringBootApplication
public class DemoApplication {
    public static void main(String[] args) {
        SpringApplication.run(DemoApplication.class, args);
    }

    @Bean
    public CommandLineRunner init(ProductRepository repo) {
        return args -> {
            // STO 상품 생성
            Product stoProduct = new Product();
            stoProduct.setProductName("네이버 STO 주식");
            stoProduct.setPriceKrw(BigDecimal.valueOf(50000));
            stoProduct.setStockQuantity(100);
            stoProduct.setCategory("STO");

            repo.save(stoProduct);

            System.out.println("✅ STO 상품 저장 완료!");
            repo.findAll().forEach(p ->
                    System.out.println("상품: " + p.getProductName() +
                            ", 가격: " + p.getPriceKrw()));
        };
    }
}
