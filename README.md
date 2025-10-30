# Kafka by Example

Learning Kafka through practical exercises gives you a deep understanding of how it behaves in real‑world scenarios. Below are 10 progressively challenging exercises that build your skills—from basic setup to simulating outages and tuning performance.

> Each exercise has:
> - a `starter/` folder: implement your solution here following that exercise's `README.md` instructions.
> - a `solution/` folder: one possible reference solution (there can be others).
>
> Try to complete the starter before viewing the solution to maximize learning.

---
## Exercise Roadmap

### 1. Create and Inspect a Topic
Goal: Use CLI tools to create a topic and inspect its configuration.  
Learn: Partitions, replication factor, topic metadata.

### 2. Produce and Consume Messages
Goal: Use `kafka-console-producer` and `kafka-console-consumer` to send and read messages.  
Learn: Message flow, offsets, consumer groups.

### 3. Write a Simple Producer and Consumer in Code
Goal: Build a basic Kafka producer and consumer (Java, Python, etc.).  
Learn: Client APIs, serialization, poll loop, configuration basics.

### 4. Simulate Consumer Lag
Goal: Artificially slow down processing (sleep/delay) to observe lag.  
Learn: Lag metrics, offset tracking, group behavior.

### 5. Simulate Broker Unavailability
Goal: Stop the Kafka container temporarily and observe producer/consumer reactions.  
Learn: Retries, timeouts, error handling strategies.

### 6. Tune Producer Configs for Reliability
Goal: Experiment with `acks`, `retries`, `delivery.timeout.ms`, batching, compression.  
Learn: Durability vs latency trade‑offs, idempotence.

### 7. Explore Consumer Rebalancing
Goal: Run multiple consumers in the same group and watch partition assignment changes.  
Learn: Rebalancing, cooperative vs eager protocols, group coordination.

### 8. Monitor Kafka with CLI and Logs
Goal: Use `kafka-consumer-groups`, `kafka-topics`, and broker logs to monitor activity.  
Learn: Lag tracking, group membership, topic health.

### 9. Simulate Message Loss or Duplication
Goal: Interrupt consumers mid‑processing or disable `enable.auto.commit`.  
Learn: At‑least‑once vs effectively‑once semantics, manual offsets.

### 10. Stress Test with High Throughput
Goal: Push high message volume and observe system behavior.  
Learn: Throughput limits, batching efficiency, compression, backpressure.

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
