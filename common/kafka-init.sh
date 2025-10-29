#!/bin/bash

KAFKA_LOG_DIRS=${KAFKA_LOG_DIRS:-/tmp/kraft-combined-logs}
KAFKA_CLUSTER_ID_FILE="$KAFKA_LOG_DIRS/meta.properties"

if [ ! -f "$KAFKA_CLUSTER_ID_FILE" ]; then
  # Generate a new cluster ID
  CLUSTER_ID=$(/usr/bin/kafka-storage random-uuid)
  echo "Formatting storage with CLUSTER_ID: $CLUSTER_ID"
  /usr/bin/kafka-storage format \
    --ignore-formatted \
    --cluster-id $CLUSTER_ID \
    --config /etc/kafka/kafka.properties
else
  echo "Storage already formatted, skipping format step."
fi

# Start Kafka
exec /etc/confluent/docker/run
