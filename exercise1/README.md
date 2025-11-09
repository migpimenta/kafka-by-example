# Exercise 1: Kafka Fundamentals: Topics, Producers, and Consumers

## Summary

**Goal:** Master the basics by creating topics, producing messages via CLI, and consuming them with different configurations.

**What you'll do:**
- Create topics with varying partition counts and inspect their metadata
- Use `kafka-console-producer` to send messages with and without keys
- Consume messages from different offsets (beginning, latest, specific offset)
- Experiment with consumer groups to understand partition assignment

**Learn:** Topic architecture, partitions, replication factor, message keys, offsets, consumer group coordination, partition-to-consumer mapping.

---

## Usage

From the `exercise1` directory, run:

```bash
cd exercise1
docker-compose up --build --force-recreate
```

This will start the 3-broker Kafka cluster.

To stop and clean up:
```bash
docker-compose down -v
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

You've successfully completed Exercise 1 when you can:
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


