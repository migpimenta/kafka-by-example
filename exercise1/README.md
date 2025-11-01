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

From `starter` directory, run:

```
cd starter
docker-compose up --build --force-recreate
```

This will start the Kafka.


## Instructions

Try completing the instructions using the starter before viewing the solution section below to maximize learning.

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
