#!/bin/bash

# Start the 3-broker Kafka cluster + Schema Registry for Chapter 2
echo "Starting 3-broker Kafka cluster with Schema Registry..."
docker-compose up -d

echo ""
echo "Kafka cluster and Schema Registry are starting up..."
echo ""
echo "Services:"
echo "  - Kafka brokers: localhost:9092, localhost:9094, localhost:9096"
echo "  - Schema Registry: http://localhost:8081"
echo ""
echo "To view logs, run: docker-compose logs -f"

