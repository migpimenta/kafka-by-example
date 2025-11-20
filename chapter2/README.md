# Chapter 2: Building Production-Grade Clients

## Summary

**Goal:** Build robust Kafka producers and consumers that handle real-world scenarios including error handling, retries, serialization, monitoring, and graceful shutdown patterns.

**Use Case:** You'll implement an e-commerce order processing pipeline that simulates realistic message flows:
- **Orders Topic**: New customer orders with order details, items, and totals
- **Payments Topic**: Payment processing events (authorized, captured, failed)
- **Shipments Topic**: Shipping and fulfillment updates

This mirrors real production systems where orders flow through multiple stages (placement → payment → fulfillment → shipping), with each stage consuming from one topic and potentially producing to another.

**What you'll do:**
- Build producers that generate realistic order events with proper serialization (JSON, then Avro)
- Configure producer reliability settings: `acks`, idempotence, retries, batching, compression
- Implement consumers with proper error handling, retry logic, and dead letter queues (DLQ)
- Replace auto-commit with manual offset management for precise control
- Build multi-stage processing: consumers that read from one topic and produce to another
- Implement graceful shutdown handling and proper resource cleanup
- Add structured logging, correlation IDs, and monitoring to track message flow

**Learn:** Kafka client APIs (Java and Python), serialization formats (JSON, Avro, Protobuf), producer durability vs throughput trade-offs, consumer commit semantics (auto vs manual), idempotent producers, transactional producers, error handling patterns, retry strategies, dead letter queues, application lifecycle management, structured logging, distributed tracing concepts.

---

## Use Case: E-Commerce Order Processing Pipeline

In a modern e-commerce platform, orders go through several stages, with each stage handled by different microservices communicating via Kafka:

```
Customer Places Order
       ↓
   [orders topic] ← Order Service produces
       ↓
Payment Service consumes → Processes payment
       ↓
   [payments topic] ← Payment Service produces
       ↓
Fulfillment Service consumes → Picks & packs items
       ↓
   [shipments topic] ← Fulfillment Service produces
       ↓
Shipping Service consumes → Delivers to customer
```

**Why this architecture?**
- **Decoupling**: Services don't call each other directly; they communicate via events
- **Scalability**: Each service can scale independently based on load
- **Resilience**: If one service is down, others continue processing their queues
- **Audit trail**: All events are persisted in Kafka for replay and debugging
- **Multiple consumers**: Analytics, notifications, and inventory services can also consume these events

**Message characteristics:**
- **Order messages**: Keyed by `order_id` to maintain ordering per order
- **Payment messages**: Keyed by `order_id` to group payment events with orders
- **Shipment messages**: Keyed by `order_id` for tracking per-order fulfillment

---

## Prerequisites

- Completed Chapter 1 (Kafka Fundamentals)
- Java 11+ or Python 3.8+ installed
- Maven or Gradle (for Java) or pip (for Python)
- Running 3-broker Kafka cluster (use `common/docker-compose.yml`)

---

## Project Structure

```
chapter2/
├── README.md
├── java/
│   ├── pom.xml
│   └── src/main/java/com/example/kafka/
│       ├── producer/
│       │   ├── SimpleOrderProducer.java
│       │   ├── RobustOrderProducer.java
│       │   └── AvroOrderProducer.java
│       ├── consumer/
│       │   ├── SimpleOrderConsumer.java
│       │   ├── RobustOrderConsumer.java
│       │   ├── PaymentProcessor.java
│       │   └── ShipmentProcessor.java
│       ├── model/
│       │   ├── Order.java
│       │   ├── Payment.java
│       │   └── Shipment.java
│       └── util/
│           └── JsonSerializer.java
└── python/
    ├── requirements.txt
    ├── producer/
    │   ├── simple_order_producer.py
    │   ├── robust_order_producer.py
    │   └── avro_order_producer.py
    ├── consumer/
    │   ├── simple_order_consumer.py
    │   ├── robust_order_consumer.py
    │   ├── payment_processor.py
    │   └── shipment_processor.py
    ├── model/
    │   ├── order.py
    │   ├── payment.py
    │   └── shipment.py
    └── util/
        └── json_serializer.py
```

---

## Instructions

### Part 1: Setting Up Topics and Basic Producers

#### Exercise 1: Create Topics for the Order Pipeline

**Objective:** Set up the three core topics with appropriate configurations.

**Tasks:**
- Create `orders` topic with 3 partitions, replication factor 3
- Create `payments` topic with 3 partitions, replication factor 3
- Create `shipments` topic with 3 partitions, replication factor 3
- Set retention to 7 days for all topics
- Configure `min.insync.replicas=2` for durability

**Commands:** 
```bash
# TODO: Add topic creation commands
```

---

#### Exercise 2: Build a Simple Order Producer (Java or Python)

**Objective:** Create a basic producer that generates random order events.

**Order Message Structure (JSON):**
```json
{
  "order_id": "ORD-12345",
  "customer_id": "CUST-789",
  "timestamp": "2025-11-15T10:30:00Z",
  "items": [
    {"product_id": "PROD-001", "name": "Laptop", "quantity": 1, "price": 999.99},
    {"product_id": "PROD-002", "name": "Mouse", "quantity": 2, "price": 25.50}
  ],
  "total_amount": 1050.99,
  "status": "PENDING"
}
```

**Tasks:**
- Use `order_id` as the message key to ensure ordering per order
- Generate random orders with 1-5 items
- Use basic producer configuration (default settings)
- Send 10 orders and verify they're in the topic

**Key concepts:** Basic producer API, message keys, JSON serialization

---

#### Exercise 3: Configure Producer for Reliability

**Objective:** Enhance the producer with production-grade reliability settings.

**Tasks:**
- Set `acks=all` to ensure all ISR replicas acknowledge
- Enable `enable.idempotence=true` to prevent duplicates
- Configure `retries` and `retry.backoff.ms`
- Set `max.in.flight.requests.per.connection=5`
- Add callback handlers to log success/failure
- Test by producing 100 orders and monitoring for errors

**Key concepts:** Producer acknowledgments, idempotence, retries, callbacks

---

#### Exercise 4: Optimize Producer Performance

**Objective:** Tune batching and compression for higher throughput.

**Tasks:**
- Configure `batch.size` (default 16KB, try 32KB)
- Set `linger.ms` to allow batching (try 10ms, 100ms)
- Enable compression: test `gzip`, `snappy`, `lz4`, `zstd`
- Measure throughput: orders/second and MB/second
- Compare trade-offs: latency vs throughput

**Key concepts:** Batching, compression algorithms, throughput optimization

---

### Part 2: Building Robust Consumers

#### Exercise 5: Build a Simple Order Consumer

**Objective:** Create a basic consumer that reads and logs orders.

**Tasks:**
- Use consumer group `order-processors`
- Subscribe to `orders` topic
- Deserialize JSON messages
- Log each order (order_id, customer_id, total_amount)
- Use auto-commit (default settings)
- Run multiple instances to observe partition assignment

**Key concepts:** Basic consumer API, consumer groups, auto-commit

---

#### Exercise 6: Implement Manual Offset Management

**Objective:** Replace auto-commit with manual offset commits for precise control.

**Tasks:**
- Disable auto-commit: `enable.auto.commit=false`
- Process messages in batches
- Commit offsets after successful processing
- Handle commit failures with retry logic
- Test by killing consumer mid-processing and verifying no message loss

**Key concepts:** Manual commits, at-least-once delivery, offset management

---

#### Exercise 7: Add Error Handling and Retry Logic

**Objective:** Handle transient and permanent failures gracefully.

**Tasks:**
- Wrap message processing in try-catch
- Implement exponential backoff for transient errors (network timeouts)
- Create `orders-dlq` (dead letter queue) topic for poison pills
- Send permanently failed messages to DLQ with error metadata
- Add structured logging with correlation IDs
- Simulate failures: throw exceptions for specific order IDs

**Key concepts:** Error handling, retry strategies, dead letter queues, poison pill messages

---

#### Exercise 8: Implement Graceful Shutdown

**Objective:** Handle application termination properly.

**Tasks:**
- Register shutdown hooks (Java) or signal handlers (Python)
- Stop consuming new messages on SIGTERM
- Finish processing current batch
- Commit final offsets
- Close consumer properly
- Test with `docker stop` and verify clean shutdown in logs

**Key concepts:** Graceful shutdown, resource cleanup, signal handling

---

### Part 3: Multi-Stage Processing Pipeline

#### Exercise 9: Build Payment Processor (Consumer + Producer)

**Objective:** Create a service that consumes orders and produces payment events.

**Payment Message Structure:**
```json
{
  "payment_id": "PAY-67890",
  "order_id": "ORD-12345",
  "amount": 1050.99,
  "timestamp": "2025-11-15T10:30:05Z",
  "status": "AUTHORIZED",
  "payment_method": "CREDIT_CARD",
  "last_four": "4242"
}
```

**Tasks:**
- Consume from `orders` topic
- Simulate payment processing (90% success, 10% failure)
- Produce payment result to `payments` topic
- Use transactional producer-consumer pattern for exactly-once semantics
- Key payment messages by `order_id`
- Commit consumer offset only after payment event is produced

**Key concepts:** Producer-consumer chains, transactional processing, exactly-once semantics

---

#### Exercise 10: Build Shipment Processor

**Objective:** Create a service that consumes payments and produces shipment events.

**Shipment Message Structure:**
```json
{
  "shipment_id": "SHIP-11111",
  "order_id": "ORD-12345",
  "timestamp": "2025-11-15T10:30:10Z",
  "status": "DISPATCHED",
  "carrier": "FedEx",
  "tracking_number": "1Z999AA10123456784",
  "estimated_delivery": "2025-11-18T18:00:00Z"
}
```

**Tasks:**
- Consume from `payments` topic, filter for `status=AUTHORIZED`
- Simulate shipment creation (assign carrier, generate tracking number)
- Produce shipment event to `shipments` topic
- Handle failures: payment declined, out of stock
- Add end-to-end correlation ID tracking across all three topics

**Key concepts:** Event filtering, stateful processing, correlation IDs

---

### Part 4: Schema Evolution and Serialization

#### Exercise 11: Migrate from JSON to Avro

**Objective:** Use Avro for schema evolution and better performance.

**Tasks:**
- Set up Schema Registry (Docker container)
- Define Avro schemas for Order, Payment, Shipment
- Refactor producers to use AvroSerializer
- Refactor consumers to use AvroDeserializer
- Test backward compatibility: add optional field to schema
- Measure serialization size: JSON vs Avro

**Key concepts:** Schema Registry, Avro serialization, schema evolution, backward compatibility

---

#### Exercise 12: Handle Schema Evolution

**Objective:** Safely evolve schemas without breaking consumers.

**Tasks:**
- Add new field `discount_code` to Order schema (optional)
- Deploy new producer version that includes discount codes
- Verify old consumers still work (backward compatibility)
- Update consumers to handle new field
- Test forward compatibility: old producer, new consumer

**Key concepts:** Schema compatibility modes (backward, forward, full), schema versioning

---

### Part 5: Monitoring and Observability

#### Exercise 13: Add Structured Logging

**Objective:** Implement comprehensive logging for debugging and monitoring.

**Tasks:**
- Use structured logging format (JSON)
- Include: timestamp, correlation_id, order_id, event_type, status
- Log key events: message received, processing started, processing completed, error occurred
- Add execution time metrics
- Use log levels appropriately (INFO, WARN, ERROR)

**Key concepts:** Structured logging, correlation IDs, observability

---

#### Exercise 14: Track Consumer Lag

**Objective:** Monitor how far behind consumers are.

**Tasks:**
- Use `kafka-consumer-groups` CLI to check lag
- Simulate backlog: produce 1000 orders, slow down consumer
- Monitor lag per partition
- Set up alerts for lag > threshold (e.g., 100 messages)
- Implement lag tracking in application code using consumer metrics

**Key concepts:** Consumer lag, monitoring, performance metrics

---

#### Exercise 15: End-to-End Latency Tracking

**Objective:** Measure how long orders take to flow through the pipeline.

**Tasks:**
- Add timestamp to each message at production
- Calculate processing latency at each stage
- Track end-to-end latency: order created → shipment dispatched
- Identify bottlenecks (which stage is slowest?)
- Set up percentile metrics (p50, p95, p99)
- Visualize latency distribution

**Key concepts:** Latency tracking, performance profiling, distributed tracing

---

## Verification

You've successfully completed Chapter 2 when you can:

- ✅ Build producers with proper reliability settings (acks, idempotence, retries)
- ✅ Implement consumers with manual offset management and error handling
- ✅ Create multi-stage processing pipelines (consumer → process → producer)
- ✅ Use Avro with Schema Registry for serialization
- ✅ Handle schema evolution without breaking compatibility
- ✅ Implement graceful shutdown and resource cleanup
- ✅ Add structured logging and monitoring
- ✅ Track and optimize consumer lag and end-to-end latency
- ✅ Explain the trade-offs between throughput, latency, and durability

---

## Solutions

_Solutions for each exercise will be provided in the `java/` and `python/` directories with complete working code examples._

---

## Exploration Questions

After completing the exercises, reflect on these questions:

- What happens if you lose a message during payment processing? How would you detect and handle it?
- How does idempotence prevent duplicate orders from being charged twice?
- What are the trade-offs between auto-commit and manual offset management?
- When should you use a dead letter queue vs retrying indefinitely?
- How does batching affect end-to-end latency in your pipeline?
- What happens if the Schema Registry is unavailable? How does this affect producers and consumers?
- How would you implement exactly-once semantics across the entire pipeline (orders → payments → shipments)?

---

## Next Steps

Once you've mastered production-grade clients, proceed to:
- **Chapter 3**: Monitoring and Observability - Deep dive into metrics, dashboards, and alerting
- **Chapter 4**: Consumer Groups and Rebalancing - Advanced partition assignment and state management
- **Chapter 5**: Fault Tolerance and Recovery - Simulate failures and implement recovery strategies

