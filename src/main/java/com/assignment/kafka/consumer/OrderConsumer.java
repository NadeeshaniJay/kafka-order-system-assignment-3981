package com.assignment.kafka.consumer;

import com.assignment.kafka.avro.Order;
import com.assignment.kafka.service.OrderAggregationService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.Random;

@Slf4j
@Component
public class OrderConsumer {

    private final OrderAggregationService aggregationService;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String dlqTopic;
    private final Random random = new Random();

    private static final int MAX_RETRIES = 3;

    public OrderConsumer(
            OrderAggregationService aggregationService,
            KafkaTemplate<String, Object> kafkaTemplate,
            @Value("${kafka.topic.orders-dlq}") String dlqTopic) {
        this.aggregationService = aggregationService;
        this.kafkaTemplate = kafkaTemplate;
        this.dlqTopic = dlqTopic;
    }

    @KafkaListener(topics = "${kafka.topic.orders}", groupId = "${spring.kafka.consumer.group-id}")
    public void consumeOrder(
            @Payload Order order,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.OFFSET) Long offset,
            Acknowledgment acknowledgment) {

        boolean processed = false;
        int retryCount = 0;
        Exception lastException = null;

        while (!processed && retryCount <= MAX_RETRIES) {
            try {
                log.info("Processing order: {} | Product: {} | Price: ${} | Attempt: {}/{}",
                        order.getOrderId(),
                        order.getProduct(),
                        String.format("%.2f", order.getPrice()),
                        retryCount + 1,
                        MAX_RETRIES + 1);

                // Simulate random temporary failures (10% chance)
                if (random.nextInt(10) == 0) {
                    throw new RuntimeException("Temporary processing failure");
                }

                processOrder(order);

                aggregationService.addOrder(order);

                processed = true;
                log.info("Successfully processed order: {}", order.getOrderId());

            } catch (Exception e) {
                lastException = e;
                retryCount++;

                if (retryCount <= MAX_RETRIES) {
                    log.warn("Retry {}/{} for order: {} | Error: {}",
                            retryCount, MAX_RETRIES, order.getOrderId(), e.getMessage());

                    try {
                        long backoffMs = (long) Math.pow(2, retryCount - 1) * 1000;
                        log.info("Waiting {}ms before retry...", backoffMs);
                        Thread.sleep(backoffMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        log.error("Retry interrupted for order: {}", order.getOrderId());
                        break;
                    }
                } else {
                    log.error("Max retries exceeded for order: {} | Sending to DLQ",
                            order.getOrderId());
                }
            }
        }

        if (!processed) {
            sendToDLQ(order, lastException);
        }

        acknowledgment.acknowledge();
    }

    private void processOrder(Order order) {
        try {
            Thread.sleep(100); // Simulate processing time
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Processing interrupted", e);
        }

        log.debug("Order {} processed successfully", order.getOrderId());
    }

    private void sendToDLQ(Order order, Exception exception) {
        log.error("Sending order to DLQ: {} | Reason: {}",
                order.getOrderId(), exception != null ? exception.getMessage() : "Unknown error");

        try {
            kafkaTemplate.send(dlqTopic, order.getOrderId().toString(), order)
                    .whenComplete((result, ex) -> {
                        if (ex == null) {
                            log.info("Order sent to DLQ: {} | Offset: {}",
                                    order.getOrderId(),
                                    result.getRecordMetadata().offset());
                        } else {
                            log.error("Failed to send order to DLQ: {} | Error: {}",
                                    order.getOrderId(), ex.getMessage());
                        }
                    });
        } catch (Exception e) {
            log.error("Critical error sending to DLQ: {}", e.getMessage());
        }
    }

    @KafkaListener(topics = "${kafka.topic.orders-dlq}", groupId = "dlq-consumer-group")
    public void consumeDLQ(
            @Payload Order order,
            @Header(KafkaHeaders.OFFSET) Long offset,
            Acknowledgment acknowledgment) {

        log.error("DLQ Message received - Order: {} | Product: {} | Price: ${} | Offset: {}",
                order.getOrderId(),
                order.getProduct(),
                String.format("%.2f", order.getPrice()),
                offset);

        acknowledgment.acknowledge();
    }
}