# Kafka by Example

Learning Kafka through practical exercises gives you a deep understanding of how it behaves in real‑world scenarios. Below are 6 progressively challenging exercises that build your skills—from basic operations to production‑ready configurations and failure scenarios.

> Each exercise has:
> - a `starter/` folder: implement your solution here following that exercise's `README.md` instructions.
> - a `solution/` folder: one possible reference solution (there can be others).
>
> Try to complete the starter before viewing the solution to maximize learning.

---
## Exercise Roadmap

### 1. Kafka Fundamentals: Topics, Producers, and Consumers
**Goal:** Master the basics by creating topics, producing messages via CLI, and consuming them with different configurations.

**What you'll do:**
- Create topics with varying partition counts and inspect their metadata
- Use `kafka-console-producer` to send messages with and without keys
- Consume messages from different offsets (beginning, latest, specific offset)
- Experiment with consumer groups to understand partition assignment

**Learn:** Topic architecture, partitions, replication factor, message keys, offsets, consumer group coordination, partition-to-consumer mapping.

---

### 2. Building Production-Grade Clients
**Goal:** Implement robust producer and consumer applications with proper configuration, error handling, and monitoring.

**What you'll do:**
- Write a producer application (Java/Python/Go) that sends structured data with configurable serialization (JSON, Avro, or Protobuf)
- Configure producer reliability settings: `acks`, `retries`, `idempotence`, batching, and compression
- Build a consumer application with proper poll loops, commit strategies (auto vs manual), and graceful shutdown
- Implement structured logging to track message processing and errors

**Learn:** Client APIs, serialization formats, producer durability vs throughput trade-offs, consumer commit semantics, application lifecycle management.

---

### 3. Monitoring and Observability
**Goal:** Gain visibility into Kafka cluster and application health using CLI tools, logs, and key metrics.

**What you'll do:**
- Use `kafka-consumer-groups` to monitor consumer lag across partitions
- Track producer and consumer metrics (throughput, latency, error rates)
- Analyze broker logs to understand topic operations and client connections
- Simulate consumer lag by adding processing delays and observe how it affects the system
- Create a monitoring dashboard or script that reports cluster health

**Learn:** Lag tracking, offset management, consumer group state, broker health indicators, performance bottlenecks identification.

---

### 4. Consumer Groups and Rebalancing
**Goal:** Understand how Kafka coordinates multiple consumers and handles dynamic scaling through partition rebalancing.

**What you'll do:**
- Deploy multiple consumer instances in the same consumer group reading from a multi-partition topic
- Observe partition assignment and workload distribution
- Trigger rebalancing by adding/removing consumer instances
- Compare eager vs cooperative rebalancing protocols
- Implement rebalance listeners to handle partition assignment changes gracefully
- Test scenarios: scaling up during high load, scaling down during low load

**Learn:** Consumer group coordination, partition assignment strategies (range, round-robin, sticky), rebalance protocols, state management during rebalancing, minimizing rebalance downtime.

---

### 5. Fault Tolerance and Recovery
**Goal:** Simulate real-world failures and implement strategies to maintain system reliability.

**What you'll do:**
- Stop the Kafka broker mid-operation and observe producer/consumer behavior
- Configure producer timeouts and retry policies to handle transient failures
- Test consumer recovery: interrupt processing mid-batch and verify offset handling
- Implement idempotent producers to prevent duplicate messages
- Experiment with `enable.auto.commit` vs manual offset commits to understand message delivery guarantees
- Simulate message loss and duplication scenarios, then implement safeguards

**Learn:** At-least-once vs at-most-once vs exactly-once semantics, producer retries and timeouts, consumer failure recovery, offset commit strategies, data loss prevention, handling duplicate messages.

---

### 6. Performance Tuning and High Throughput
**Goal:** Optimize Kafka for high-volume workloads and understand the trade-offs between throughput, latency, and reliability.

**What you'll do:**
- Generate high message volume using a load testing tool or custom producer
- Tune producer configs: batch size, linger time, compression (gzip, snappy, lz4, zstd)
- Optimize consumer configs: fetch sizes, max poll records, processing parallelism
- Monitor system behavior under load: CPU, memory, network, disk I/O
- Test with different message sizes and identify throughput limits
- Experiment with partitioning strategies to balance load
- Document the performance characteristics and optimal configurations for different scenarios

**Learn:** Throughput optimization, latency vs reliability trade-offs, compression effectiveness, batching strategies, backpressure handling, capacity planning, when to add more partitions or brokers.

---
## Project Structure
This repository uses a shared Kafka environment plus isolated folders per exercise for focused learning.

```
kafka-by-example/
├── common/                # Shared Docker/Kafka setup (KRaft single broker for now)
├── exercise1/
│   ├── README.md          # Instructions & goals
│   ├── starter/           # Where you build your solution
│   └── solution/          # One possible implementation
├── exercise2/
│   ├── README.md
│   ├── starter/
│   └── solution/
└── ... (future exercises)
```

---
## Using the Shared Kafka Environment
Start from `common/` (currently a single KRaft broker: replication factor must be 1 until more brokers are added in later exercises).

```bash
cd common
docker-compose up --build
```

Stop & clean:
```bash
cd common
docker-compose down -v
```

### Basic CLI Examples (from host with Kafka CLI installed)
Create a topic:
```bash
kafka-topics --bootstrap-server localhost:9092 \
  --create --topic demo-topic \
  --partitions 3 --replication-factor 1
```
List topics:
```bash
kafka-topics --bootstrap-server localhost:9092 --list
```
Describe a topic:
```bash
kafka-topics --bootstrap-server localhost:9092 --describe --topic demo-topic
```
Produce (console):
```bash
echo "hello" | kafka-console-producer --bootstrap-server localhost:9092 --topic demo-topic
```
Consume (console):
```bash
kafka-console-consumer --bootstrap-server localhost:9092 --topic demo-topic --from-beginning
```

---
## Workflow Per Exercise
1. Read the exercise's `README.md`.
2. Work in that exercise's `starter/` folder.
3. Use or extend the shared environment under `common/` as needed.
4. Test your approach (CLI, code, logs, metrics).
5. Compare with `solution/`—note design choices and trade‑offs.
6. Iterate or refactor for clarity, resilience, performance.

---
## Future Enhancements
Later exercises may add more brokers for replication, controller quorum configuration, and failure simulations. Expect to adjust:
- `node.id` per broker
- `controller.quorum.voters`
- Replication factors and ISR behavior

---
## Next Step
Start with `exercise1/`—open its `README.md`, spin up Kafka from `common/`, and follow the instructions.

Happy Kafka exploring!
