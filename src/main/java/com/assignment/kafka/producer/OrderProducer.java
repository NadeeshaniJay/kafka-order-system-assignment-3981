package com.assignment.kafka.producer;

import com.assignment.kafka.avro.Order;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Component
public class OrderProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String topic;
    private final boolean producerEnabled;
    private final int maxOrders;
    private final AtomicInteger orderCounter = new AtomicInteger(1001);
    private final Random random = new Random();

    private static final String[] PRODUCTS = {
            "Item1", "Item2", "Item3", "Item4", "Item5",
            "Laptop", "Phone", "Tablet", "Headphones", "Monitor"
    };

    public OrderProducer(
            KafkaTemplate<String, Object> kafkaTemplate,
            @Value("${kafka.topic.orders}") String topic,
            @Value("${order.producer.enabled}") boolean producerEnabled,
            @Value("${order.producer.max-orders}") int maxOrders) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
        this.producerEnabled = producerEnabled;
        this.maxOrders = maxOrders;
    }

    @Scheduled(fixedDelayString = "${order.producer.interval-ms}")
    public void produceOrders() {
        if (!producerEnabled) {
            return;
        }

        int currentCount = orderCounter.get();
        if (currentCount > maxOrders + 1000) {
            log.info("Maximum orders reached. Stopping producer.");
            return;
        }

        Order order = createRandomOrder();
        sendOrder(order);
        orderCounter.incrementAndGet();
    }

    private Order createRandomOrder() {
        String orderId = String.valueOf(orderCounter.get());
        String product = PRODUCTS[random.nextInt(PRODUCTS.length)];
        float price = 10.0f + random.nextFloat() * 990.0f;

        return Order.newBuilder()
                .setOrderId(orderId)
                .setProduct(product)
                .setPrice(price)
                .build();
    }

    public void sendOrder(Order order) {
        CompletableFuture<SendResult<String, Object>> future =
                kafkaTemplate.send(topic, order.getOrderId().toString(), order);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("Sent order: {} | Product: {} | Price: ${} | Offset: {}",
                        order.getOrderId(),
                        order.getProduct(),
                        String.format("%.2f", order.getPrice()),
                        result.getRecordMetadata().offset());
            } else {
                log.error("Failed to send order: {} | Error: {}",
                        order.getOrderId(), ex.getMessage());
            }
        });
    }
}