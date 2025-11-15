#  Kafka Hands-On: Learning by Doing

Learning Kafka through practical chapters gives you a deep understanding of how it behaves in real‑world scenarios. Below are 6 progressively challenging chapters that build your skills—from basic operations to production‑ready configurations and failure scenarios.

> Each chapter folder contains a `README.md` with detailed instructions, concepts to learn, and solutions. Work through the instructions step-by-step to build your understanding before reviewing the provided solutions.

---
## Chapter Roadmap

### 1. Kafka Fundamentals: Topics, Producers, and Consumers
**Goal:** Build a solid foundation in Kafka fundamentals through hands-on exploration using a 3-broker cluster.

**What you'll do:**
- Create and inspect topics with different partition counts and replication factors
- Produce messages with and without keys to understand partitioning strategies
- Consume messages from various positions and observe ordering guarantees
- Work with consumer groups to understand partition assignment and rebalancing
- Test key-based routing and monitor consumer lag using CLI tools

**Learn:** Topic architecture, partitions, replication factor, ISR (In-Sync Replicas), message keys, hashing-based partitioning, offsets, consumer groups, partition assignment strategies, rebalancing, fault tolerance, and the trade-offs between throughput and durability.

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
This repository uses a shared Kafka environment plus isolated folders per chapter for focused learning.

```
kafka-by-example/
├── common/                # Shared Docker/Kafka setup (3-broker KRaft cluster)
├── chapter1/
│   └── README.md          # Instructions, concepts, and solutions
├── chapter2/
│   └── README.md          # (future chapter)
└── ... (future chapters)
```

---
## Using the Shared Kafka Environment
Start from `common/` (3-broker KRaft cluster on ports 9092, 9094, 9096).

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
  --partitions 3 --replication-factor 3
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
## Workflow Per Chapter
1. Read the chapter's `README.md` for instructions and concepts.
2. Work in that chapter's folder.
3. Use the shared Kafka environment under `common/` or extend it as needed.
4. Test your approach (CLI, code, logs, metrics).
5. Review the solutions section in the README to compare approaches and learn trade‑offs.
6. Iterate or refactor for clarity, resilience, and performance.

---
## Future Enhancements
The current setup uses a 3-broker KRaft cluster with combined broker and controller roles. Future chapters may explore:
- Separating controller and broker roles for production-like setups
- Adding more brokers for horizontal scaling experiments
- Implementing security (SSL/SASL authentication)
- Multi-datacenter replication scenarios

---
## Next Step
Start with `chapter1/`—open its `README.md`, spin up Kafka from `common/`, and follow the instructions.

Happy Kafka exploring!
