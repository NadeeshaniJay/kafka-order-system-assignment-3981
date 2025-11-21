# Kafka Order System - Assignment - EG/2020/3981

A complete Spring Boot application demonstrating Kafka-based order processing with Avro serialization, real-time aggregation, retry logic, and Dead Letter Queue (DLQ).

## Features

* **Avro Serialization**: Schema-based message serialization using Apache Avro  
* **Real-time Aggregation**: Running average calculation of order prices  
* **Retry Logic**: Automatic retry with exponential backoff for temporary failures  
* **Dead Letter Queue**: Failed messages after max retries sent to DLQ  
* **REST API**: Endpoints for manual order submission and statistics  
* **Auto Producer**: Scheduled automatic order generation  
* **Kafka UI**: Web interface for monitoring topics and messages

## Project Structure

```
kafka-order-system/
├── src/main/
│   ├── java/com/assignment/kafka/
│   │   ├── KafkaOrderSystemApplication.java
│   │   ├── config/
│   │   │   └── KafkaConfig.java
│   │   ├── producer/
│   │   │   └── OrderProducer.java
│   │   ├── consumer/
│   │   │   └── OrderConsumer.java
│   │   ├── service/
│   │   │   └── OrderAggregationService.java
│   │   └── controller/
│   │       └── OrderController.java
│   └── resources/
│       ├── avro/
│       │   └── order.avsc
│       └── application.properties
├── docker-compose.yml
├── pom.xml
└── README.md
```

### Prerequisites

- Java 17 or higher
- Maven 3.6+
- Docker and Docker Compose

### Start Kafka Infrastructure

```bash
docker-compose up -d
```

### Build the Application

```bash
mvn clean package
```

### Run the Application

```bash
mvn spring-boot:run
```

## Testing the System

### Access Kafka UI
Open the browser and navigate to:
```
http://localhost:8090
```

monitor:
- Topics: `orders` and `orders-dlq`
- Messages in real-time
- Consumer groups
- Schema registry

### View Application Logs

The application automatically produces orders every 2 seconds. Watch the console for:

```
INFO - Sent order: 1009 | Product: Laptop | Price: $599.99 | Offset: 0
INFO - Processing order: 1009 | Product: Laptop | Price: $599.99 | Attempt: 0/4
INFO - Successfully processed order: 1009
INFO - ======================================================================
INFO - AGGREGATION STATS:
INFO -   Total Orders Processed: 10
INFO -   Running Average Price: $512.45
INFO -   Total Revenue: $5124.50
INFO - ======================================================================
```

### Test REST API Endpoints

#### 1. Send a Custom Order
```bash
curl -X POST http://localhost:8080/api/orders/send \
  -H "Content-Type: application/json" \
  -d '{
    "orderId": "CUSTOM-001",
    "product": "Gaming Console",
    "price": 499.99
  }'
```

#### 2. Send a Random Order
```bash
curl -X POST http://localhost:8080/api/orders/send-random
```

#### 3. Get Statistics
```bash
curl http://localhost:8080/api/orders/stats
```

Response:
```json
{
  "totalOrders": 50,
  "runningAverage": 487.32,
  "totalRevenue": 24366.00
}
```

#### 4. Reset Statistics
```bash
curl -X POST http://localhost:8080/api/orders/stats/reset
```

#### 5. Health Check
```bash
curl http://localhost:8080/api/orders/health
```

## Retry Logic & DLQ Demonstration

The consumer simulates a 10% failure rate to demonstrate retry and DLQ functionality.

### Expected Behavior:

1. **Successful Processing** (90% of messages):
   ```
   Processing order: 1005 | Product: Phone | Price: $799.99 | Retry: 0/4
   Successfully processed order: 1005
   ```

2. **Temporary Failure with Retry**:
   ```
   Processing order: 1006 | Product: Tablet | Price: $399.99 | Retry: 0/4
   Error processing order: 1006 | Error: Temporary processing failure
   Retrying order: 1006 | Attempt: 1/3
   ```

3. **DLQ After Max Retries**:
   ```
   Processing order: 1007 | Product: Monitor | Price: $299.99 | Retry: 4/4
   Sending order to DLQ: 1007 | Reason: Temporary processing failure
   DLQ Message received - Order: 1007 | Product: Monitor | Price: $299.99
   ```

