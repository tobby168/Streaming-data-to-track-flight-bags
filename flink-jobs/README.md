# Flink Jobs

This directory implements the three Flink pipelines defined in `DESIGN.md`:

- **BagLatestJob** (`bag-latest-job/`): deduplicates baggage events and keeps the latest state per bag while mirroring it to Kafka and ClickHouse.
- **FlightKpiAlertJob** (`flight-kpi-alert-job/`): aggregates 10-second KPIs per flight and emits alerts when data is stale or missing.
- **ItineraryStatusJob** (`itinerary-status-job/`): tracks the latest state per itinerary and produces notifications for at-risk journeys.

Each job is written with the PyFlink Table API and expects Kafka and ClickHouse endpoints provided via environment variables. Default values align with the local Kubernetes deployment described in the design doc.

## Shared conventions
- Kafka topics and ClickHouse tables follow the fixed names from `DESIGN.md`.
- Watermarks are applied to event-time fields to handle out-of-order baggage events (design doc specifies a 5% out-of-order rate).
- All sinks use upsert semantics for compacted Kafka topics and JDBC for ClickHouse tables.

## Running locally
Use `flink run -py <path>` for each job after ensuring the Kafka and JDBC connectors are on the classpath. Example:

```bash
export PYTHONPATH=flink-jobs  # makes the shared flink_jobs package available
flink run -py flink-jobs/bag-latest-job/main.py
flink run -py flink-jobs/flight-kpi-alert-job/main.py
flink run -py flink-jobs/itinerary-status-job/main.py
```

Set the following environment variables as needed:

- `KAFKA_BOOTSTRAP` (default `kafka:9092`)
- `KAFKA_START_MODE` (default `earliest-offset`)
- `CLICKHOUSE_JDBC_URL` (default `jdbc:clickhouse://clickhouse:9000/default`)
- `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD`
- Overrides for topic names: `TOPIC_BAGGAGE_EVENTS`, `TOPIC_BAG_LATEST`, `TOPIC_ALERTS`, `TOPIC_NOTIFICATIONS`, `TOPIC_FLIGHT_SCHEDULE`
