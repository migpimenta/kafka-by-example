# Exercise 1

This exercise demonstrates how to spin up a Kafka cluster using the shared configuration in `common/docker-compose.yml`.

- The `starter` and `solution` folders each contain a `docker-compose.yml` that extends the base compose file from `../common/docker-compose.yml`.
- For now, both simply start the same Kafka service as defined in the common configuration.

## Usage

From either the `starter` or `solution` directory, run:

```
docker-compose up --build --force-recreate
```

This will start the Kafka cluster as defined in the shared configuration.

## Questions / Steps (to refine later)
1. Create the topic `exercise1.topic` with 6 partitions and replication factor 3; verify its existence via a topic listing.
2. Describe the topic to inspect partition leaders, replica assignments, and the ISR set for each partition.
3. Stop one broker and observe the ISR shrink; relate the impact to the configured `min.insync.replicas` and produce acknowledgment behavior.
4. Produce messages without a key and then with a key; compare how partition selection differs and inspect per-partition log growth.
5. Restart the stopped broker and confirm ISR recovery and any leader changes upon rejoining the cluster.
