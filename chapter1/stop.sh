#!/bin/bash

# Stop and clean up all Kafka containers
echo "Stopping and removing all Kafka containers..."
docker rm -f $(docker ps -aq --filter "name=kafka-")
echo "Kafka cluster stopped and cleaned up."

