# Chapter 1: Kafka Fundamentals: Topics, Producers, and Consumers

## Summary

**Goal:** Build a solid foundation in Kafka fundamentals through hands-on exploration of topics, partitions, replication, producers, and consumers using a 3-broker cluster.

**What you'll do:**
- Create and inspect topics with different partition counts and replication factors
- Produce messages with and without keys to understand partitioning strategies
- Consume messages from various offsets and partitions to observe ordering guarantees
- Work with consumer groups to understand partition assignment, rebalancing, and parallel processing
- Test key-based routing and verify deterministic partition assignment
- Monitor consumer lag and offset tracking using CLI tools

**Learn:** Topic architecture, partitions, replication factor, ISR (In-Sync Replicas), message keys, hashing-based partitioning, offsets, consumer groups, partition assignment strategies, rebalancing, fault tolerance, leader election, producer acknowledgments, and the trade-offs between throughput, latency, and durability.

---

## Usage

From the `chapter1` directory, run:

```bash
cd chapter1
docker-compose up --build --force-recreate
```

This will start the 3-broker Kafka cluster.

To stop and clean up:
```bash
docker rm -f $(docker ps -aq --filter "name=kafka-")
```

## Instructions

Try completing the instructions below before viewing the solution section to maximize learning.

### Part 1: Creating and Inspecting Topics

> **Note:** Your Kafka cluster has 3 brokers running on ports 9092, 9094, and 9096. You can connect to any of them via `--bootstrap-server localhost:9092` (or 9094, 9096).

1. **Create your first topic** called `demo-topic` with 3 partitions and a replication factor of 3.
   - Use the `kafka-topics` CLI tool with the `--create` option
   - Specify `--bootstrap-server localhost:9092`
   - Research the flags for setting partitions and replication factor
   - With 3 brokers, you can now use replication factor 3 for better fault tolerance

2. **List all topics** in your Kafka cluster to verify your topic was created.
   - Use `kafka-topics` with the appropriate flag to list topics

3. **Describe the topic** to see detailed information about its configuration.
   - Use the `--describe` option to inspect partition assignments, leaders, and replicas
   - Pay attention to the ISR (In-Sync Replicas) column—with replication factor 3, you should see all 3 brokers listed
   - Notice how each partition has a leader and 2 replicas distributed across the 3 brokers

4. **Create additional topics** to experiment with different configurations:
   - A topic named `single-partition-topic` with only 1 partition and replication factor 3
   - A topic named `many-partitions-topic` with 6 partitions and replication factor 2
   - Describe each and observe how Kafka distributes partition leaders and replicas across the 3 brokers

### Part 2: Producing Messages

5. **Send messages without keys** using the console producer.
   - Start `kafka-console-producer` targeting your `demo-topic`
   - Type several messages (one per line) and observe that they're sent
   - Research how to gracefully exit the producer (hint: Ctrl+D or Ctrl+C)

6. **Send messages with keys** to understand how keys affect partition assignment.
   - Use the console producer with the `--property` flag to enable key parsing
   - Set `parse.key=true` and `key.separator=:` properties
   - Send messages in format `key:value` (e.g., `user1:hello`, `user2:world`, `user1:again`)
   - Send at least 5-10 messages with different keys

### Part 3: Consuming Messages

7. **Consume from the beginning** to read all messages in the topic.
   - Use `kafka-console-consumer` with the `--from-beginning` flag
   - Target your `demo-topic` and observe all messages you produced
   - Notice that messages might not be in the exact order you sent them—why might this be?

8. **Consume with key display** to see how messages were keyed.
   - Use the console consumer with properties to print keys
   - Set `print.key=true` and `key.separator=:` properties
   - Observe which keys were assigned to your messages

9. **Consume from a specific offset** in a particular partition.
   - Use the `--partition` flag to target a specific partition (try partition 0)
   - Use the `--offset` flag to start reading from a specific position (try offset 0, then try offset 2)
   - Research the difference between numeric offsets and special values like "earliest" and "latest"

### Part 4: Consumer Groups and Partition Assignment

10. **Create your first consumer group** by assigning a group ID.
    - Start a console consumer with the `--group` flag and name it `demo-group`
    - Read from `demo-topic` (don't use `--from-beginning`)
    - Keep this consumer running in one terminal window

11. **Produce new messages** while your consumer is running.
    - In a separate terminal, start a producer for `demo-topic`
    - Send several new messages
    - Observe that your consumer receives them in real-time

12. **Check consumer group status** to see offset information.
    - Use the `kafka-consumer-groups` CLI tool with the `--describe` option
    - Specify your group name `demo-group`
    - Examine the output: current offset, log end offset, and lag for each partition

13. **Add a second consumer to the same group** to observe partition rebalancing.
    - Start another console consumer with the same group ID (`demo-group`)
    - Watch the console output—both consumers should announce a rebalance
    - Send more messages and observe that they're distributed between the two consumers
    - Note: With 3 partitions and 2 consumers, how are partitions assigned?

14. **Add a third consumer** to fully distribute the workload.
    - Start a third console consumer in the same group
    - Observe the rebalance again
    - Produce messages and verify each consumer handles roughly equal traffic
    - What happens if you add a 4th consumer when you only have 3 partitions?

15. **Experiment with multiple consumer groups** to understand independence.
    - Create a second consumer group called `demo-group-2`
    - Start a consumer in this new group with `--from-beginning`
    - Notice that this group has its own offset tracking, independent of `demo-group`
    - Use `kafka-consumer-groups --describe` to compare the offsets between groups

### Part 5: Understanding Partition Assignment with Keys

16. **Test key-based partition assignment**.
    - Produce 20-30 messages to `demo-topic` with keys (use 4-5 distinct key values)
    - Consume these messages with key printing enabled
    - Stop the consumer and restart it with the `--partition` flag to read each partition individually
    - Document which keys ended up in which partitions
    - Send more messages with the same keys and verify they go to the same partitions

### Exploration Questions

After completing the steps above, take some time to reflect on these questions:

- How does Kafka determine which partition a message goes to when you don't specify a key?
- How does Kafka determine which partition a message goes to when you DO specify a key?
- What happens to consumer group offsets when all consumers in a group shut down?
- Why might you want more partitions than consumers in a group?
- What are the trade-offs between having many small partitions vs few large partitions?

### Verification

You've successfully completed Chapter 1 when you can:
- ✅ Create topics with different partition counts and describe their configuration
- ✅ Produce messages both with and without keys using the console producer
- ✅ Consume messages from different starting points (beginning, latest, specific offset)
- ✅ Run multiple consumers in the same group and observe partition assignment
- ✅ Monitor consumer group lag using CLI tools
- ✅ Explain how message keys affect partition assignment 

## Solutions

1. **Create `demo-topic` with 3 partitions and replication factor 3**

```
kafka-topics --create \
  --topic my-topic \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 3
```

2. **List all topics**

```
kafka-topics --list --bootstrap-server localhost:9092
```

3. **Describe the topics**

```
kafka-topics --describe \
  --topic my-topic \
  --bootstrap-server localhost:9092
```

You should have the following result:
```
Topic: my-topic TopicId: PK1MmXu8SJu51lAwefYBRQ PartitionCount: 3       ReplicationFactor: 3    Configs: min.insync.replicas=2
        Topic: my-topic Partition: 0    Leader: 1       Replicas: 1,2,3 Isr: 1,2,3      Elr: N/A        LastKnownElr: N/A
        Topic: my-topic Partition: 1    Leader: 2       Replicas: 2,3,1 Isr: 2,3,1      Elr: N/A        LastKnownElr: N/A
        Topic: my-topic Partition: 2    Leader: 3       Replicas: 3,1,2 Isr: 3,1,2      Elr: N/A        LastKnownElr: N/A
```

**Understanding the output:**

The describe command shows critical information about your topic's architecture:

- **PartitionCount: 3** - The topic is divided into 3 partitions (0, 1, and 2), allowing parallel processing and horizontal scaling.
- **ReplicationFactor: 3** - Each partition has 3 copies (one leader and 2 replicas) distributed across the brokers for fault tolerance.
- **Leader** - The broker ID responsible for handling all reads and writes for that partition. Notice how leadership is distributed (broker 1 leads partition 0, broker 2 leads partition 1, broker 3 leads partition 2).
- **Replicas** - Lists all broker IDs that should have a copy of this partition's data. For partition 0, replicas are on brokers 1, 2, and 3.
- **Isr (In-Sync Replicas)** - The set of replicas that are fully caught up with the leader. All replicas being in-sync (1,2,3) indicates a healthy partition. If a broker falls behind or fails, it will be removed from the ISR.
- **min.insync.replicas=2** - A safety configuration requiring at least 2 replicas (leader + 1 follower) to acknowledge writes. This prevents data loss if one broker goes down, while still allowing writes to succeed.

The replica distribution (e.g., `1,2,3` for partition 0, `2,3,1` for partition 1) shows how Kafka strategically spreads data across brokers. This ensures that if any single broker fails, the other two can continue serving all partitions without data loss.

4. **Create additional topics**

Create a topic with a single partition:
```bash
kafka-topics --create \
  --topic single-partition-topic \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 3
```

Create a topic with many partitions:
```bash
kafka-topics --create \
  --topic many-partitions-topic \
  --bootstrap-server localhost:9092 \
  --partitions 6 \
  --replication-factor 2
```

Describe both topics to see the distribution:
```bash
kafka-topics --describe \
  --topic single-partition-topic \
  --bootstrap-server localhost:9092

kafka-topics --describe \
  --topic many-partitions-topic \
  --bootstrap-server localhost:9092
```
**Example output for `single-partition-topic`:**
```
Topic: single-partition-topic   TopicId: ABC123...  PartitionCount: 1   ReplicationFactor: 3    Configs: min.insync.replicas=2
        Topic: single-partition-topic   Partition: 0    Leader: 2       Replicas: 2,3,1 Isr: 2,3,1
```

With only 1 partition, you'll notice:
- Only one leader is assigned (in this example, broker 2)
- All 3 brokers still hold replicas (2, 3, 1) for fault tolerance
- This configuration limits parallelism—only one consumer in a group can process messages at a time
- However, it guarantees total ordering of all messages in the topic

**Example output for `many-partitions-topic`:**
```
Topic: many-partitions-topic    TopicId: DEF456...  PartitionCount: 6   ReplicationFactor: 2    Configs: min.insync.replicas=2
        Topic: many-partitions-topic    Partition: 0    Leader: 1       Replicas: 1,2   Isr: 1,2
        Topic: many-partitions-topic    Partition: 1    Leader: 2       Replicas: 2,3   Isr: 2,3
        Topic: many-partitions-topic    Partition: 2    Leader: 3       Replicas: 3,1   Isr: 3,1
        Topic: many-partitions-topic    Partition: 3    Leader: 1       Replicas: 1,3   Isr: 1,3
        Topic: many-partitions-topic    Partition: 4    Leader: 2       Replicas: 2,1   Isr: 2,1
        Topic: many-partitions-topic    Partition: 5    Leader: 3       Replicas: 3,2   Isr: 3,2
```

With 6 partitions and replication factor 2, observe:
- **Leadership distribution**: Each broker leads 2 partitions (broker 1 leads 0 and 3, broker 2 leads 1 and 4, broker 3 leads 2 and 5)
- **Replica placement**: Kafka evenly distributes replicas across all brokers, ensuring no single broker becomes a bottleneck
- **Fault tolerance**: With replication factor 2, each partition can survive one broker failure
- **Parallelism**: Up to 6 consumers in a group can process messages simultaneously, one per partition
- **Trade-off**: Lower replication factor (2 vs 3) means less durability but potentially better write performance

This demonstrates Kafka's intelligent partition assignment algorithm, which aims to:
1. Distribute leadership evenly across brokers to balance load
2. Place replicas on different brokers than the leader for fault tolerance
3. Spread replicas evenly to avoid overloading any single broker

### Part 2: Producing Messages

5. **Send messages without keys** using the console producer.

Start the console producer:
```bash
kafka-console-producer \
  --topic demo-topic \
  --bootstrap-server localhost:9092
```

Once the producer starts, you'll see a prompt (`>`). Type messages, one per line:
```
>Hello Kafka!
>This is my first message
>Messages without keys are distributed round-robin
>across all partitions
>You can type as many messages as you want
```

Each time you press Enter, the message is sent to Kafka immediately. To exit gracefully:
- **On Linux/macOS**: Press `Ctrl+D` (sends EOF) or `Ctrl+C` (interrupt)
- **On Windows**: Press `Ctrl+Z` then Enter, or `Ctrl+C`

**What happens to messages without keys:**

When you don't specify a key, Kafka uses a **round-robin** or **sticky partitioning** strategy (depending on your Kafka version and producer configuration):

- **Sticky partitioning (default in newer versions)**: Messages are batched and sent to one partition until the batch is full or a timeout occurs, then switches to another partition. This improves throughput by reducing the number of requests.
- **Round-robin (older versions)**: Each message alternates between partitions sequentially (0 → 1 → 2 → 0 → 1 → 2...).

Since messages are distributed across multiple partitions without keys:
- ✅ **Good for**: High throughput and parallel processing when message order doesn't matter globally
- ❌ **Not ideal for**: Scenarios where you need messages to be processed in the exact order they were sent
- 📝 **Note**: Order is still guaranteed *within* each partition, but not across partitions

Example distribution (with 3 partitions):
```
Message "Hello Kafka!" → Partition 1
Message "This is my first message" → Partition 1 (sticky batch)
Message "Messages without keys..." → Partition 1 (sticky batch)
Message "across all partitions" → Partition 2 (new batch)
Message "You can type as many..." → Partition 2 (sticky batch)
```

The exact partition assignment is handled by the producer's partitioner logic and isn't visible in the console producer output, but you can verify it later by consuming from specific partitions.

6. **Send messages with keys** to understand how keys affect partition assignment.

Start the console producer with key parsing enabled:
```bash
kafka-console-producer \
  --topic demo-topic \
  --bootstrap-server localhost:9092 \
  --property parse.key=true \
  --property key.separator=:
```

Once the producer starts, send messages in `key:value` format:
```
>user1:Hello from user1
>user2:First message from user2
>user3:User3 checking in
>user1:Second message from user1
>user2:Another message from user2
>user1:Third message from user1
>user4:New user joining
>user3:User3 again
>user2:User2 final message
>user4:User4 second message
```

Press `Ctrl+D` (or `Ctrl+C`) to exit when done.

**What happens with message keys:**

Unlike keyless messages, messages with keys are **deterministically assigned** to partitions using a hashing algorithm:

- Kafka computes a hash of the key using MurmurHash2: `hash = murmur2(key)`
- The hash is mapped to a partition: `partition = hash(key) % partition_count`
- **Critical guarantee**: All messages with the same key always go to the same partition

This ensures:
- ✅ **Ordering per key**: Messages with `user1` will always be processed in order
- ✅ **Logical grouping**: Related messages stay together on the same partition
- ✅ **Stateful processing**: Consumers can maintain state per key

Example distribution with 3 partitions:
```
user1:Hello from user1           → Partition 2 (hash(user1) % 3 = 2)
user2:First message from user2   → Partition 0 (hash(user2) % 3 = 0)
user3:User3 checking in          → Partition 1 (hash(user3) % 3 = 1)
user1:Second message from user1  → Partition 2 (same key → same partition)
user2:Another message from user2 → Partition 0 (same key → same partition)
```

**Important notes:**
- The partition assignment for a key remains consistent **only if the partition count stays the same**
- Adding partitions later breaks the key-to-partition mapping for existing data
- Keys can be strings, numbers, or any serializable data

### Part 3: Consuming Messages

7. **Consume from the beginning** to read all messages in the topic.

Start the console consumer with the `--from-beginning` flag:
```bash
kafka-console-consumer \
  --topic demo-topic \
  --bootstrap-server localhost:9092 \
  --from-beginning
```

You'll see all messages that were previously produced to the topic, including both the keyless messages from step 5 and the keyed messages from step 6:
```
Hello Kafka!
This is my first message
Messages without keys are distributed round-robin
user1:Hello from user1
across all partitions
user2:First message from user2
You can type as many messages as you want
user3:User3 checking in
user1:Second message from user1
user2:Another message from user2
...
```

Press `Ctrl+C` to stop the consumer.

**Why messages appear out of order:**

You'll notice that messages don't appear in the exact order you sent them. This happens because:

1. **Multiple partitions**: Your `demo-topic` has 3 partitions, and messages are distributed across all of them
2. **Consumer reads in partition order**: The consumer reads from all partitions but processes them independently
3. **No global ordering guarantee**: Kafka only guarantees ordering **within a single partition**, not across partitions

The consumer interleaves messages from different partitions as it polls them. For example:
```
Partition 0: [msg1, msg4, msg7]
Partition 1: [msg2, msg5, msg8]  →  Consumer sees: msg1, msg2, msg3, msg4, msg5, msg6, msg7, msg8, msg9
Partition 2: [msg3, msg6, msg9]
```

**When order matters:**
- If you need **global ordering** across all messages, use a single partition (but this limits parallelism)
- If you need **ordering per entity** (user, session, device), use message keys to ensure related messages go to the same partition
- The consumer will process messages from each partition in order, maintaining per-key ordering

**The `--from-beginning` flag:**
- Without this flag, the consumer starts from the **latest** offset (only new messages)
- With this flag, it starts from **offset 0** in each partition (all existing messages)
- This is useful for replaying data, debugging, or initial data loads

8. **Consume with key display** to see how messages were keyed.

Start the console consumer with key printing enabled:
```bash
kafka-console-consumer \
  --topic demo-topic \
  --bootstrap-server localhost:9092 \
  --from-beginning \
  --property print.key=true \
  --property key.separator=:
```

Output will show keys for keyed messages and `null` for keyless messages:
```
null:Hello Kafka!
null:This is my first message
user1:Hello from user1
user2:First message from user2
user3:User3 checking in
user1:Second message from user1
null:Messages without keys are distributed round-robin
user2:Another message from user2
...
```

The key-value separator (`:`) makes it easy to distinguish between keys and values. Messages sent without keys show `null` as the key, confirming they were sent without a key and were distributed using the default partitioning strategy.

9. **Consume from a specific offset** in a particular partition.

Read partition 0 starting from offset 0:
```bash
kafka-console-consumer \
  --topic demo-topic \
  --bootstrap-server localhost:9092 \
  --partition 0 \
  --offset 0
```

Read partition 0 starting from offset 2:
```bash
kafka-console-consumer \
  --topic demo-topic \
  --bootstrap-server localhost:9092 \
  --partition 0 \
  --offset 2
```

**Understanding offsets:**

- **Numeric offset (e.g., 0, 2, 10)**: Starts reading from that exact position in the partition
- **`earliest`**: Equivalent to offset 0, reads from the beginning of the partition
- **`latest`**: Starts from the end, only reads new messages that arrive after the consumer starts

When targeting a specific partition, you only see messages from that partition in the exact order they were written. This is useful for:
- Debugging specific partition behavior
- Replaying messages from a known point
- Investigating message ordering within a partition

### Part 4: Consumer Groups and Partition Assignment

10. **Create your first consumer group** by assigning a group ID.

Start a console consumer with a group ID:
```bash
kafka-console-consumer \
  --topic demo-topic \
  --bootstrap-server localhost:9092 \
  --group demo-group
```

Keep this consumer running. It will:
- Start consuming from the **latest** offset (not from beginning)
- Track its progress using the group ID
- Only receive new messages produced after it starts

The consumer is now part of the `demo-group` consumer group, and Kafka will track which messages this group has consumed.

11. **Produce new messages** while your consumer is running.

Open a second terminal and start a producer:
```bash
kafka-console-producer \
  --topic demo-topic \
  --bootstrap-server localhost:9092
```

Send messages:
```
>Real-time message 1
>Real-time message 2
>Real-time message 3
```

Switch to the consumer terminal—you should see these messages appear immediately. This demonstrates Kafka's real-time streaming capability.

12. **Check consumer group status** to see offset information.

```bash
kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe \
  --group demo-group
```

Example output:
```
GROUP           TOPIC       PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG  CONSUMER-ID                                     HOST            CLIENT-ID
demo-group      demo-topic  0          15              15              0    consumer-demo-group-1-abc123                    /172.17.0.1     consumer-demo-group-1
demo-group      demo-topic  1          12              12              0    consumer-demo-group-1-abc123                    /172.17.0.1     consumer-demo-group-1
demo-group      demo-topic  2          18              18              0    consumer-demo-group-1-abc123                    /172.17.0.1     consumer-demo-group-1
```

**Understanding the output:**
- **CURRENT-OFFSET**: The last offset this group has successfully processed
- **LOG-END-OFFSET**: The latest offset available in the partition
- **LAG**: The difference (LOG-END-OFFSET - CURRENT-OFFSET). Zero lag means the consumer is caught up
- **CONSUMER-ID**: Unique identifier for each consumer instance
- Notice one consumer is assigned all 3 partitions

13. **Add a second consumer to the same group** to observe partition rebalancing.

In a new terminal, start a second consumer:
```bash
kafka-console-consumer \
  --topic demo-topic \
  --bootstrap-server localhost:9092 \
  --group demo-group
```

Both consumers will log rebalancing messages. After rebalancing:
- Consumer 1 might handle partitions 0 and 1
- Consumer 2 might handle partition 2

Produce new messages and observe they're distributed between the two consumers. Check the group status again:
```bash
kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe \
  --group demo-group
```

You'll see each consumer assigned to different partitions, demonstrating Kafka's automatic load balancing within consumer groups.

14. **Add a third consumer** to fully distribute the workload.

Start a third consumer:
```bash
kafka-console-consumer \
  --topic demo-topic \
  --bootstrap-server localhost:9092 \
  --group demo-group
```

After rebalancing, each of the 3 consumers will handle exactly 1 partition. Produce messages and verify each consumer processes roughly equal traffic.

**What happens with a 4th consumer?**

If you start a 4th consumer with only 3 partitions:
```bash
kafka-console-consumer \
  --topic demo-topic \
  --bootstrap-server localhost:9092 \
  --group demo-group
```

The 4th consumer will be idle—it won't receive any messages because all 3 partitions are already assigned. Kafka cannot split a partition across multiple consumers. This demonstrates the relationship between partition count and maximum parallelism within a consumer group.

15. **Experiment with multiple consumer groups** to understand independence.

Create a second consumer group:
```bash
kafka-console-consumer \
  --topic demo-topic \
  --bootstrap-server localhost:9092 \
  --group demo-group-2 \
  --from-beginning
```

This consumer reads from the beginning because `demo-group-2` has never consumed from this topic before. Each consumer group maintains its own offset tracking independently.

Compare offsets between groups:
```bash
kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe \
  --group demo-group

kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe \
  --group demo-group-2
```

`demo-group` will show offsets at the end (caught up), while `demo-group-2` might show different offsets depending on how much it has consumed. This independence allows:
- Multiple applications to consume the same data
- Different processing speeds per application
- Replay capability per group without affecting others

### Part 5: Understanding Partition Assignment with Keys

16. **Test key-based partition assignment**.

Produce messages with distinct keys:
```bash
kafka-console-producer \
  --topic demo-topic \
  --bootstrap-server localhost:9092 \
  --property parse.key=true \
  --property key.separator=:
```

Send 20-30 messages with 4-5 distinct keys:
```
>apple:Message 1 from apple
>banana:Message 1 from banana
>cherry:Message 1 from cherry
>date:Message 1 from date
>apple:Message 2 from apple
>banana:Message 2 from banana
>elderberry:Message 1 from elderberry
>apple:Message 3 from apple
>cherry:Message 2 from cherry
>date:Message 2 from date
>banana:Message 3 from banana
>elderberry:Message 2 from elderberry
>apple:Message 4 from apple
>cherry:Message 3 from cherry
>date:Message 3 from date
>banana:Message 4 from banana
>apple:Message 5 from apple
>elderberry:Message 3 from elderberry
```

Consume with key printing:
```bash
kafka-console-consumer \
  --topic demo-topic \
  --bootstrap-server localhost:9092 \
  --from-beginning \
  --property print.key=true \
  --property key.separator=:
```

Now read each partition individually to see which keys went where:

**Partition 0:**
```bash
kafka-console-consumer \
  --topic demo-topic \
  --bootstrap-server localhost:9092 \
  --partition 0 \
  --offset earliest \
  --property print.key=true \
  --property key.separator=:
```

**Partition 1:**
```bash
kafka-console-consumer \
  --topic demo-topic \
  --bootstrap-server localhost:9092 \
  --partition 1 \
  --offset earliest \
  --property print.key=true \
  --property key.separator=:
```

**Partition 2:**
```bash
kafka-console-consumer \
  --topic demo-topic \
  --bootstrap-server localhost:9092 \
  --partition 2 \
  --offset earliest \
  --property print.key=true \
  --property key.separator=:
```

**Document your findings:**

You might observe something like:
```
Partition 0: banana (all 4 messages in order)
Partition 1: cherry, elderberry (all messages for each key in order)
Partition 2: apple, date (all messages for each key in order)
```

**Verify consistency:**

Produce additional messages with the same keys:
```
>apple:Message 6 from apple
>banana:Message 5 from banana
>cherry:Message 4 from cherry
```

Consume from specific partitions again—you'll confirm that:
- `apple` messages still go to partition 2
- `banana` messages still go to partition 0
- `cherry` messages still go to partition 1

This demonstrates the **deterministic nature** of key-based partitioning: as long as the partition count doesn't change, a given key will always map to the same partition, guaranteeing ordering for all messages with that key.

---

## Concepts Deep Dive

This section provides detailed explanations of the core Kafka concepts covered in this chapter.

### Topics and Partitions

**Topics** are logical channels or categories where messages are published. Think of them as message queues or database tables, but designed for streaming data.

**Partitions** are the fundamental unit of parallelism in Kafka. Each topic is split into one or more partitions, which are ordered, immutable sequences of messages. Key characteristics:

- **Ordering guarantee**: Messages within a single partition are strictly ordered by their offset (position). However, there's no ordering guarantee across partitions.
- **Parallelism**: Each partition can be consumed independently, allowing multiple consumers to process data in parallel.
- **Scalability**: Partitions can be distributed across different brokers, enabling horizontal scaling of both storage and throughput.
- **Immutability**: Once written to a partition, messages cannot be modified. They can only be appended or deleted after retention policies expire.

**Choosing partition count:**
- More partitions = more parallelism but also more overhead (file handles, memory, network connections)
- A common starting point: number of expected consumers or 2-3x the number of brokers
- Increasing partition count later is possible but breaks key-based routing guarantees

### Brokers and Cluster Architecture

A **Kafka broker** is a single Kafka server that stores data and serves client requests. Multiple brokers form a **Kafka cluster**.

**Broker responsibilities:**
- **Storage**: Each broker stores partition replicas on its local disk
- **Serving requests**: Handles produce requests (writes) and fetch requests (reads) from clients
- **Replication**: Followers fetch data from leaders to stay in sync
- **Metadata management**: Participates in cluster coordination and leadership elections

**Cluster architecture:**

```
Kafka Cluster
├── Broker 1 (localhost:9092)
│   ├── Partition 0 (leader)
│   ├── Partition 1 (follower)
│   └── Partition 2 (follower)
├── Broker 2 (localhost:9094)
│   ├── Partition 0 (follower)
│   ├── Partition 1 (leader)
│   └── Partition 2 (follower)
└── Broker 3 (localhost:9096)
    ├── Partition 0 (follower)
    ├── Partition 1 (follower)
    └── Partition 2 (leader)
```

**Key concepts:**

- **Cluster**: A group of brokers working together, appearing as a single system to clients
- **Broker ID**: Each broker has a unique integer identifier (1, 2, 3, etc.)
- **Bootstrap servers**: Initial broker addresses clients use to discover the full cluster
  - Clients connect to any broker and automatically discover all others
  - Convention: provide multiple brokers for redundancy (e.g., `localhost:9092,localhost:9094,localhost:9096`)
- **Controller**: One broker is elected as the controller, responsible for:
  - Managing partition leader elections
  - Handling broker joins/failures
  - Coordinating metadata changes across the cluster
  - In KRaft mode, controllers form a separate quorum for consensus

**Why multiple brokers?**

1. **Fault tolerance**: If one broker fails, others continue serving data
2. **Scalability**: Distribute storage and load across multiple machines
3. **Parallelism**: Multiple brokers can serve different partitions simultaneously
4. **High availability**: No single point of failure

**Production considerations:**
- Minimum 3 brokers recommended for production (allows RF=3)
- Brokers should be on separate physical/virtual machines
- Consider separate disks for data and logs
- Network bandwidth often the limiting factor, not CPU or disk

### Replication and Fault Tolerance

Kafka provides durability through **replication**. Each partition has multiple copies (replicas) distributed across different brokers.

**Key components:**

- **Leader**: One replica serves as the leader for each partition, handling all reads and writes
- **Followers**: Other replicas passively replicate data from the leader
- **In-Sync Replicas (ISR)**: The set of replicas that are fully caught up with the leader
  - A follower is in the ISR if it has fetched messages up to the leader's high watermark within `replica.lag.time.max.ms` (default: 30 seconds)
  - If a follower falls too far behind, it's removed from the ISR
  - Only ISR members are eligible to become the new leader if the current leader fails

**Replication factor** determines how many copies exist:
- **RF=1**: No redundancy, data loss if broker fails (not recommended for production)
- **RF=2**: Survives one broker failure
- **RF=3**: Industry standard, survives two broker failures (recommended for production)

**min.insync.replicas (min.isr)**: A critical safety setting
- With `min.isr=2` and `RF=3`, at least 2 replicas must acknowledge writes
- If only 1 replica remains available, producers with `acks=all` will fail
- This prevents accepting writes that might be lost if the last broker fails
- Trade-off: availability vs. durability

**Eligible Leader Replicas (ELR)** in KRaft mode:
- Replicas that have fallen out of ISR but are still viable leader candidates
- Provides more graceful degradation than traditional "unclean leader election"
- Helps avoid data loss during recovery scenarios

### Message Keys and Partitioning

Messages in Kafka consist of:
- **Key** (optional): Used to determine the target partition
- **Value**: The actual message payload
- **Headers** (optional): Key-value metadata
- **Timestamp**: When the message was produced

**Partitioning strategies:**

**1. Key-based partitioning (with key):**
```
partition = hash(key) % number_of_partitions
```
- Uses MurmurHash2 algorithm by default
- Same key always goes to same partition (deterministic)
- Guarantees ordering for all messages with the same key
- Use cases: user events, device telemetry, session data

**2. Round-robin/Sticky partitioning (without key):**
- **Sticky partitioning** (Kafka 2.4+): Batch messages to one partition until batch is full, then switch
  - Reduces requests and improves throughput
  - Messages are distributed evenly across partitions over time
- **Round-robin** (older versions): Alternates partitions for each message
- No ordering guarantees across the topic
- Use cases: logs, metrics, events where order doesn't matter

**3. Custom partitioner:**
- You can implement custom partitioning logic in producer code
- Example: route premium users to specific partitions, geographic partitioning

**Important caveat**: If you change the partition count, the hash mapping changes:
```
Before (3 partitions): hash(key) % 3 = 2
After (4 partitions):  hash(key) % 4 = 1  // Different partition!
```
This breaks the key-to-partition guarantee for existing data.

### Offsets and Consumer Position

An **offset** is a unique, sequential integer that identifies each message within a partition. It's the position of the message in the partition's log.

**Offset characteristics:**
- Starts at 0 for each partition
- Increments by 1 for each new message in that partition
- Never reused, even if messages are deleted
- Each partition maintains its own offset sequence independently

**Consumer offset tracking:**

Kafka tracks two types of offsets:
1. **Current offset**: The next message the consumer will read
2. **Committed offset**: The last offset the consumer has successfully processed and acknowledged

When a consumer reads messages:
```
1. Fetch messages starting from current offset
2. Process messages
3. Commit offset (acknowledging successful processing)
4. Current offset advances
```

**Offset commit strategies:**

- **Automatic commits** (`enable.auto.commit=true`):
  - Kafka commits offsets periodically (default: every 5 seconds)
  - Simple but can lead to duplicate processing if consumer crashes between auto-commits
  
- **Manual commits**:
  - Application explicitly commits after processing
  - More control, can commit after database writes, etc.
  - Prevents message loss but may cause duplicates on failure (at-least-once delivery)

**Special offset positions:**

- **`earliest`/`beginning`**: Start from offset 0 (or earliest available after retention)
- **`latest`/`end`**: Start from current end offset (only new messages)
- **Specific offset**: Start from exact position (e.g., offset 100)

**Consumer group offset storage:**

- Offsets are stored in an internal Kafka topic: `__consumer_offsets`
- Each consumer group maintains its own offset for each partition
- Offsets persist even if all consumers shut down
- When a consumer restarts, it resumes from the last committed offset

### Consumer Groups and Partition Assignment

A **consumer group** is a set of consumers that cooperatively consume a topic. Each message is delivered to only one consumer within the group.

**How partition assignment works:**

```
Topic with 6 partitions: [P0, P1, P2, P3, P4, P5]

1 consumer in group:  C1 → [P0, P1, P2, P3, P4, P5]  (all partitions)
2 consumers in group: C1 → [P0, P1, P2]
                      C2 → [P3, P4, P5]
3 consumers in group: C1 → [P0, P1]
                      C2 → [P2, P3]
                      C3 → [P4, P5]
6 consumers in group: C1 → [P0]
                      C2 → [P1]
                      ... (one partition each)
7+ consumers:         C7 → [] (idle, no partitions available)
```

**Key principle**: A partition can only be assigned to one consumer within a group, but a consumer can handle multiple partitions.

**Rebalancing** occurs when:
- A consumer joins or leaves the group
- A consumer crashes or becomes unresponsive
- New partitions are added to the topic
- Consumer group coordinator fails over

**Rebalancing process:**
1. All consumers stop consuming
2. Partition assignment is recalculated using the assignment strategy
3. Consumers resume with their new partition assignments
4. Brief unavailability during rebalancing (typically seconds)

**Assignment strategies:**

- **RangeAssignor** (default): Assigns consecutive partitions to each consumer
  - Can lead to uneven distribution across multiple topics
  
- **RoundRobinAssignor**: Distributes partitions evenly in round-robin fashion
  - Better balance but can cause unnecessary rebalancing
  
- **StickyAssignor**: Minimizes partition movement during rebalancing
  - Maintains as many existing assignments as possible
  - Best for reducing rebalancing overhead
  
- **CooperativeStickyAssignor** (Kafka 2.4+): Allows incremental rebalancing
  - Only affected partitions stop processing
  - No "stop-the-world" pause for entire consumer group

**Multiple consumer groups:**

Different groups reading the same topic operate independently:
```
Topic: orders
├── Group: order-processing-service (commits at offset 1000)
├── Group: analytics-service (commits at offset 500)
└── Group: audit-service (commits at offset 1200)
```

Each group:
- Maintains its own offsets
- Can consume at different rates
- Can use different processing logic
- Receives all messages independently

This enables the **publish-subscribe pattern**: multiple applications can consume the same data stream independently.

### Leaders, Replicas, and ISR

**Partition leadership** is central to Kafka's architecture. For each partition:

**Leader replica:**
- Handles all produce and consume requests
- Maintains the authoritative copy of the partition
- Writes are first committed to the leader's log
- Tracks which followers are keeping up (ISR)

**Follower replicas:**
- Passively replicate data by fetching from the leader
- Do not serve client requests (in standard Kafka)
- Continuously send fetch requests to stay in sync
- Can become leader if current leader fails

**In-Sync Replica (ISR) criteria:**

A follower stays in the ISR if:
1. It has caught up to the leader's log end offset (LEO)
2. It has fetched data within `replica.lag.time.max.ms` (default: 30s)

A follower is removed from ISR if:
- It falls behind the leader's high watermark
- It hasn't sent a fetch request within the lag time threshold
- It's offline or unreachable

**High watermark (HW):**
- The offset up to which all ISR members have replicated
- Only messages below the HW are visible to consumers
- Ensures consumers only see messages that won't be lost on leader failure

**Leader election:**

When a leader fails:
1. Controller detects the failure (via ZooKeeper/KRaft)
2. Selects a new leader from the ISR (typically the first replica in the ISR)
3. Updates metadata and notifies all brokers
4. Clients automatically reconnect to the new leader

**Unclean leader election** (`unclean.leader.election.enable`):
- If enabled and no ISR replicas are available, allows out-of-sync replicas to become leader
- **Risk**: Potential data loss (messages not replicated to the new leader are lost)
- **Benefit**: Higher availability (partition comes back online faster)
- Default: `false` (prefer consistency over availability)

**Preferred leader election:**
- Each partition has a "preferred" leader (first replica in the list)
- Kafka can automatically rebalance leadership after failures are resolved
- Controlled by `auto.leader.rebalance.enable` (default: true)
- Ensures even distribution of leadership across brokers

### Producer Acknowledgments (acks)

The `acks` setting controls durability guarantees:

**`acks=0` (fire and forget):**
- Producer doesn't wait for any acknowledgment
- Highest throughput, lowest latency
- No durability guarantee—data can be lost if broker fails
- Use case: Metrics, logs where some loss is acceptable

**`acks=1` (leader acknowledgment):**
- Producer waits for leader to write to its log
- Balanced throughput and durability
- Risk: Data loss if leader fails before followers replicate
- Use case: Non-critical events, acceptable rare data loss

**`acks=all` or `acks=-1` (full ISR acknowledgment):**
- Producer waits for leader and all ISR members to acknowledge
- Strongest durability guarantee
- Lower throughput due to waiting for replication
- Combined with `min.insync.replicas=2`, ensures data is on at least 2 brokers
- Use case: Financial transactions, critical business events

**Example scenario** (RF=3, min.isr=2, acks=all):
```
1. Producer sends message to leader (broker 1)
2. Leader writes to its log
3. Followers (brokers 2 and 3) fetch and write to their logs
4. Leader waits for at least 1 follower (min.isr=2 total including leader)
5. Leader acknowledges to producer
6. If broker 1 fails, data is safe on broker 2 or 3
```

### Message Retention and Compaction

Kafka stores messages for a configurable retention period:

**Time-based retention** (`retention.ms`):
- Default: 7 days
- Messages older than retention period are deleted
- Applies per partition
- Example: `retention.ms=86400000` (1 day)

**Size-based retention** (`retention.bytes`):
- Maximum size of partition's log
- Oldest messages deleted when limit exceeded
- Default: unlimited
- Example: `retention.bytes=1073741824` (1 GB)

**Log compaction** (`cleanup.policy=compact`):
- Keeps only the latest value for each key
- Useful for changelog-style topics (database CDC, user profiles)
- Guarantees at least the last value for each key is retained
- Background process, not immediate

**Segment files:**
- Partitions are divided into segment files (default: 1 GB or 7 days)
- Only closed segments are eligible for deletion/compaction
- Active segment is never deleted

### Performance Considerations

**Batching:**
- Producers batch messages for efficiency
- Controlled by `linger.ms` (wait time) and `batch.size` (bytes)
- Larger batches = better throughput but higher latency
- Sticky partitioning optimizes batching for keyless messages

**Compression:**
- Supports gzip, snappy, lz4, zstd
- Reduces network bandwidth and storage
- Trade-off: CPU overhead for compression/decompression
- Applied per batch, not per message

**Page cache:**
- Kafka relies heavily on OS page cache
- Sequential disk I/O is extremely fast when cached
- Avoid using too much heap memory; let OS cache data
- Zero-copy transfer from page cache to network socket

**Network and disk:**
- Sequential writes are fast (hundreds of MB/s)
- Followers use sequential reads (also cached)
- Network typically the bottleneck, not disk
- Multiple disks (JBOD) for higher throughput

**Partition count impact:**
- More partitions = more parallelism
- But: more file handles, memory overhead, leader election time
- Each partition has its own log files and memory buffers
- Typical range: 100s to low 1000s of partitions per broker

### Use Cases and Patterns

**Event streaming:**
- Capture real-time events (clicks, transactions, sensor data)
- Multiple consumers process events independently
- Example: User activity → [Analytics, Recommendations, Audit]

**Message queue replacement:**
- Like traditional MQ but with persistence and replay
- Consumer groups provide queue semantics
- Durability and ordering guarantees

**Log aggregation:**
- Collect logs from distributed services
- Central pipeline for processing and storage
- Better than file-based log shipping

**Stream processing:**
- Kafka Streams or other frameworks consume and produce to topics
- Stateful processing with local state stores
- Join, aggregate, window operations on streams

**Change Data Capture (CDC):**
- Capture database changes as events
- Log compaction maintains latest state per key
- Sync databases, build materialized views

**Microservices communication:**
- Asynchronous communication between services
- Event-driven architecture
- Decoupling and buffering

---

## Summary

You've learned the foundational concepts of Kafka through hands-on practice:

✅ **Topics and partitions**: How Kafka organizes and scales data streams  
✅ **Replication**: How Kafka provides fault tolerance and durability  
✅ **Message keys**: How to control partitioning and guarantee ordering  
✅ **Offsets**: How Kafka tracks position in the stream  
✅ **Consumer groups**: How Kafka enables parallel processing and load balancing  
✅ **Leaders and ISR**: How Kafka manages partition replicas and handles failures  

These fundamentals are the building blocks for all advanced Kafka features. Understanding them deeply will help you design robust, scalable streaming applications.

**Next steps**: Proceed to Chapter 2 to explore more advanced topics like handling broker failures, tuning performance, and understanding behavior under load.


