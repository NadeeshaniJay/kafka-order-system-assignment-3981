package com.assignment.kafka.service;

import com.assignment.kafka.avro.Order;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Service
public class OrderAggregationService {

    @Getter
    private final AtomicInteger totalOrders = new AtomicInteger(0);

    @Getter
    private final AtomicLong totalPriceInCents = new AtomicLong(0);

    private static final int LOG_INTERVAL = 10;

    public void addOrder(Order order) {
        int count = totalOrders.incrementAndGet();

        long priceInCents = (long) (order.getPrice() * 100);
        totalPriceInCents.addAndGet(priceInCents);

        if (count % LOG_INTERVAL == 0) {
            logAggregation();
        }
    }

    public double getRunningAverage() {
        int count = totalOrders.get();
        if (count == 0) {
            return 0.0;
        }

        long totalCents = totalPriceInCents.get();
        return (double) totalCents / (count * 100);
    }

    public void logAggregation() {
        double average = getRunningAverage();
        log.info("=".repeat(70));
        log.info("AGGREGATION STATS:");
        log.info("  Total Orders Processed: {}", totalOrders.get());
        log.info("  Running Average Price: ${}", String.format("%.2f", average));
        log.info("  Total Revenue: ${}", String.format("%.2f", totalPriceInCents.get() / 100.0));
        log.info("=".repeat(70));
    }

    public void reset() {
        totalOrders.set(0);
        totalPriceInCents.set(0);
        log.info("Aggregation statistics reset");
    }
}