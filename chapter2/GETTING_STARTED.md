# Chapter 2 - Getting Started

## Quick Start

### 1. Start Kafka Cluster with Schema Registry

From the `chapter2` directory:

```bash
# Start the 3-broker Kafka cluster + Schema Registry
docker-compose up -d

# Verify all services are running
docker ps
```

You should see:
- `kafka-1` on port 9092
- `kafka-2` on port 9094
- `kafka-3` on port 9096
- `schema-registry` on port 8081

### 2. Setup for Java Development

```bash
cd java

# Build the project
mvn clean package

# Run a specific producer (example)
mvn exec:java -Dexec.mainClass="com.example.kafka.producer.SimpleOrderProducer"

# Run a specific consumer (example)
mvn exec:java -Dexec.mainClass="com.example.kafka.consumer.SimpleOrderConsumer"
```

### 3. Setup for Python Development

```bash
cd python

# Create virtual environment (recommended)
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Run a specific producer (example)
python producer/simple_order_producer.py

# Run a specific consumer (example)
python consumer/simple_order_consumer.py
```

### 4. Verify Setup

```bash
# List topics
kafka-topics --bootstrap-server localhost:9092 --list

# Check Schema Registry
curl http://localhost:8081/subjects

# Monitor consumer groups
kafka-consumer-groups --bootstrap-server localhost:9092 --list
```

### 5. Cleanup

```bash
# Stop all services
docker-compose down

# Remove volumes (clean slate)
docker-compose down -v

# Stop and remove all kafka containers
docker rm -f $(docker ps -aq --filter "name=kafka-")
```

## Troubleshooting

**Issue: Cannot connect to Kafka**
- Verify containers are running: `docker ps`
- Check logs: `docker logs kafka-1`
- Ensure ports 9092, 9094, 9096 are not in use

**Issue: Schema Registry not accessible**
- Check if running: `docker ps | grep schema-registry`
- Test connectivity: `curl http://localhost:8081`
- View logs: `docker logs schema-registry`

**Issue: Maven build fails**
- Ensure Java 11+ is installed: `java -version`
- Clean and rebuild: `mvn clean install -U`

**Issue: Python dependencies fail**
- Upgrade pip: `pip install --upgrade pip`
- Install wheel: `pip install wheel`
- Retry: `pip install -r requirements.txt`

## Next Steps

Open the [README.md](./README.md) and start with Exercise 1!

