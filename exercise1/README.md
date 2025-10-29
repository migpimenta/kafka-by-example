# Exercise 1

This exercise demonstrates how to spin up a Kafka cluster using the shared configuration in `common/docker-compose.yml`.

- The `starter` and `solution` folders each contain a `docker-compose.yml` that extends the base compose file from `../common/docker-compose.yml`.
- For now, both simply start the same Kafka service as defined in the common configuration.

## Usage

From either the `starter` or `solution` directory, run:

```
docker-compose up --build
```

This will start the Kafka cluster as defined in the shared configuration.