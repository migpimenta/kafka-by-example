#!/bin/bash

# Start the 3-broker Kafka cluster for Chapter 1
# Logs will stream to this terminal. Open another terminal to run commands.
echo "Starting 3-broker Kafka cluster..."
echo ""
echo "Services:"
echo "  - Kafka brokers: localhost:9092, localhost:9094, localhost:9096"
echo ""
echo "Logs will stream below. Press Ctrl+C to stop all services."
echo "=================================================="
echo ""
docker-compose up --build --force-recreate

