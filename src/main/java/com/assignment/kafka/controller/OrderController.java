package com.assignment.kafka.controller;

import com.assignment.kafka.avro.Order;
import com.assignment.kafka.producer.OrderProducer;
import com.assignment.kafka.service.OrderAggregationService;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Random;

@RestController
@RequestMapping("/api/orders")
@RequiredArgsConstructor
public class OrderController {

    private final OrderProducer orderProducer;
    private final OrderAggregationService aggregationService;
    private final Random random = new Random();

    @PostMapping("/send")
    public ResponseEntity<OrderResponse> sendOrder(@RequestBody OrderRequest request) {
        Order order = Order.newBuilder()
                .setOrderId(request.getOrderId())
                .setProduct(request.getProduct())
                .setPrice(request.getPrice())
                .build();

        orderProducer.sendOrder(order);

        return ResponseEntity.ok(new OrderResponse(
                "Order sent successfully",
                order.getOrderId().toString(),
                order.getProduct().toString(),
                order.getPrice()
        ));
    }

    @PostMapping("/send-random")
    public ResponseEntity<OrderResponse> sendRandomOrder() {
        String orderId = "API-" + System.currentTimeMillis();
        String[] products = {"Item1", "Item2", "Item3", "Laptop", "Phone"};
        String product = products[random.nextInt(products.length)];
        float price = 10.0f + random.nextFloat() * 990.0f;

        Order order = Order.newBuilder()
                .setOrderId(orderId)
                .setProduct(product)
                .setPrice(price)
                .build();

        orderProducer.sendOrder(order);

        return ResponseEntity.ok(new OrderResponse(
                "Random order sent successfully",
                order.getOrderId().toString(),
                order.getProduct().toString(),
                order.getPrice()
        ));
    }

    @GetMapping("/stats")
    public ResponseEntity<StatsResponse> getStats() {
        return ResponseEntity.ok(new StatsResponse(
                aggregationService.getTotalOrders().get(),
                aggregationService.getRunningAverage(),
                aggregationService.getTotalPriceInCents().get() / 100.0
        ));
    }

    @PostMapping("/stats/reset")
    public ResponseEntity<String> resetStats() {
        aggregationService.reset();
        return ResponseEntity.ok("Statistics reset successfully");
    }

    @GetMapping("/health")
    public ResponseEntity<String> health() {
        return ResponseEntity.ok("Kafka Order System is running!");
    }

    @Data
    @AllArgsConstructor
    public static class OrderRequest {
        private String orderId;
        private String product;
        private float price;
    }

    @Data
    @AllArgsConstructor
    public static class OrderResponse {
        private String message;
        private String orderId;
        private String product;
        private float price;
    }

    @Data
    @AllArgsConstructor
    public static class StatsResponse {
        private int totalOrders;
        private double runningAverage;
        private double totalRevenue;
    }
}