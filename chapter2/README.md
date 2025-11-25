# Chapter 2: Building Production-Grade Clients

## Summary

**Goal:** Build robust Kafka producers and consumers that handle real-world scenarios including error handling, retries, serialization, and graceful shutdown patterns.

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

**Learn:** Kafka client APIs (Java and Python), serialization formats (JSON, Avro, Protobuf), producer durability vs throughput trade-offs, consumer commit semantics (auto vs manual), idempotent producers, transactional producers, error handling patterns, retry strategies, dead letter queues, application lifecycle management.

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

## Usage

From the `chapter2` directory, run:

```bash
./start.sh
```

This will start the 3-broker Kafka cluster plus Schema Registry (needed for Avro exercises).

To stop and clean up:
```bash
./stop.sh
```

---

## Prerequisites

- Completed Chapter 1 (Kafka Fundamentals)
- Java 11+ or Python 3.8+ installed
- Maven or Gradle (for Java) or pip (for Python)

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

Try completing the instructions below before viewing the solution section to maximize learning.

**Note:** Your Kafka cluster has 3 brokers running on ports 9092, 9094, and 9096, plus Schema Registry on port 8081. You can connect to any broker via `--bootstrap-server localhost:9092` (or 9094, 9096).

### Part 1: Setting Up Topics and Basic Producers

1. **Create Topics for the Order Pipeline** to set up the three core topics with appropriate configurations.
   - Create `orders` topic with 3 partitions, replication factor 3
   - Create `payments` topic with 3 partitions, replication factor 3
   - Create `shipments` topic with 3 partitions, replication factor 3
   - Set retention to 7 days for all topics using the `--config retention.ms` flag
   - Configure `min.insync.replicas=2` for durability using the `--config` flag
   - Verify all topics were created successfully with `kafka-topics --list`

---

2. **Build a Simple Order Producer** (Java or Python) that generates random order events.

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

   - Use `order_id` as the message key to ensure ordering per order
   - Generate random orders with 1-5 items per order
   - Use basic producer configuration with default settings
   - Send 10 orders to the `orders` topic
   - Verify messages are in the topic using `kafka-console-consumer`
   - Research how to serialize Java/Python objects to JSON

---

3. **Configure Producer for Reliability** to enhance your producer with production-grade settings.
   - Set `acks=all` to ensure all ISR replicas acknowledge writes
   - Enable `enable.idempotence=true` to prevent duplicate messages
   - Configure `retries` (try a high value like 10) and `retry.backoff.ms` (try 100ms)
   - Set `max.in.flight.requests.per.connection=5` for optimal idempotent producer performance
   - Add callback handlers to log success/failure for each message sent
   - Test by producing 100 orders and verify no errors occur
   - Research what happens when a broker goes down during message production

---

4. **Optimize Producer Performance** by tuning batching and compression for higher throughput.
   - Configure `batch.size` - start with default 16KB, then try 32KB and 64KB
   - Set `linger.ms` to allow batching - try 0ms (default), 10ms, and 100ms
   - Enable compression and test different algorithms: `gzip`, `snappy`, `lz4`, `zstd`
   - Measure throughput by producing 1000 orders and recording orders/second and MB/second
   - Compare trade-offs: observe how `linger.ms` affects latency vs throughput
   - Research which compression algorithm gives the best balance of speed and compression ratio

---

### Part 2: Building Robust Consumers

5. **Build a Simple Order Consumer** that reads and logs orders from the topic.
   - Use consumer group `order-processors` to enable parallel processing
   - Subscribe to the `orders` topic
   - Deserialize JSON messages back to Order objects
   - Log each order showing order_id, customer_id, and total_amount
   - Use auto-commit with default settings (`enable.auto.commit=true`)
   - Run multiple instances (2-3) and observe how partitions are assigned
   - Research how consumer groups enable scaling and fault tolerance

---

6. **Implement Manual Offset Management** to replace auto-commit with precise control over when offsets are committed.
   - Disable auto-commit by setting `enable.auto.commit=false`
   - Process messages in batches (e.g., 10 messages at a time)
   - Commit offsets manually after successfully processing each batch
   - Handle commit failures with retry logic (catch `CommitFailedException`)
   - Test by killing the consumer mid-processing and restarting it
   - Verify no message loss occurs and no duplicates are processed
   - Research the difference between `commitSync()` and `commitAsync()`

---

7. **Add Error Handling and Retry Logic** to handle transient and permanent failures gracefully.
   - Wrap message processing logic in try-catch blocks
   - Implement exponential backoff for transient errors (e.g., network timeouts, temporary database issues)
   - Create an `orders-dlq` (dead letter queue) topic for poison pill messages
   - Send permanently failed messages to the DLQ with error metadata (original message, error reason, timestamp)
   - Add structured logging with correlation IDs to track messages across processing stages
   - Simulate failures by throwing exceptions for specific order IDs (e.g., orders with ID containing "ERROR")
   - Research the difference between transient vs permanent errors and appropriate handling strategies

---

8. **Implement Graceful Shutdown** to handle application termination properly without losing data.
   - Register shutdown hooks (Java: `Runtime.addShutdownHook()`) or signal handlers (Python: `signal.signal()`)
   - Stop consuming new messages when receiving SIGTERM or SIGINT
   - Finish processing the current batch of messages before shutting down
   - Commit final offsets to ensure progress is saved
   - Close the consumer properly to trigger partition rebalancing
   - Test with `docker stop` (SIGTERM) and verify clean shutdown in logs
   - Research the difference between SIGTERM and SIGKILL and why graceful shutdown matters

---

### Part 3: Multi-Stage Processing Pipeline

9. **Build Payment Processor** that acts as both a consumer and producer (Consumer + Producer pattern).

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

   - Consume messages from the `orders` topic using consumer group `payment-processors`
   - Simulate payment processing (90% success rate, 10% failure - random)
   - Produce payment results to the `payments` topic
   - Use the transactional producer-consumer pattern for exactly-once semantics
   - Key payment messages by `order_id` to maintain ordering with original orders
   - Commit consumer offset only after the payment event is successfully produced
   - Research how Kafka transactions provide exactly-once processing guarantees

---

10. **Build Shipment Processor** that consumes payment events and produces shipment events.

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

   - Consume messages from the `payments` topic using consumer group `shipment-processors`
   - Filter for messages with `status=AUTHORIZED` (ignore declined payments)
   - Simulate shipment creation by assigning a random carrier (FedEx, UPS, DHL) and generating tracking numbers
   - Produce shipment events to the `shipments` topic
   - Handle failures such as payment declined or out of stock scenarios
   - Add end-to-end correlation ID tracking across all three topics (orders → payments → shipments)
   - Research how event filtering and stateful processing work in stream processing

---

### Part 4: Schema Evolution and Serialization

11. **Migrate from JSON to Avro** to use schema-based serialization for better performance and evolution.
   - Verify Schema Registry is running on `http://localhost:8081` (started by `./start.sh`)
   - Define Avro schemas for Order, Payment, and Shipment models (`.avsc` files)
   - Refactor producers to use `KafkaAvroSerializer` (Confluent) or equivalent library
   - Refactor consumers to use `KafkaAvroDeserializer` to automatically fetch schemas
   - Test backward compatibility by adding an optional field to a schema
   - Measure serialization size by comparing JSON vs Avro message sizes in Kafka
   - Research why Avro is more efficient than JSON for high-throughput systems

---

12. **Handle Schema Evolution** to safely evolve schemas without breaking existing consumers.
   - Add a new optional field `discount_code` (string, nullable) to the Order Avro schema
   - Deploy a new producer version that includes discount codes in 50% of orders
   - Verify old consumers (without the discount_code field) still work (backward compatibility)
   - Update consumers to handle and log the new `discount_code` field
   - Test forward compatibility: run old producer with new consumer and verify it works
   - Experiment with Schema Registry compatibility modes: BACKWARD, FORWARD, FULL, NONE
   - Research when to use each compatibility mode and the trade-offs involved

---


## Verification

You've successfully completed Chapter 2 when you can:

- ✅ Build producers with proper reliability settings (acks, idempotence, retries)
- ✅ Implement consumers with manual offset management and error handling
- ✅ Create multi-stage processing pipelines (consumer → process → producer)
- ✅ Use Avro with Schema Registry for serialization
- ✅ Handle schema evolution without breaking compatibility
- ✅ Implement graceful shutdown and resource cleanup
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

