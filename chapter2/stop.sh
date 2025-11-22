#!/bin/bash

# Stop and clean up Kafka cluster and Schema Registry
echo "Stopping and removing Kafka cluster and Schema Registry..."
docker-compose down
docker rm -f $(docker ps -aq --filter "name=kafka-") 2>/dev/null
docker rm -f $(docker ps -aq --filter "name=schema-registry") 2>/dev/null
echo "Kafka cluster and Schema Registry stopped and cleaned up."

