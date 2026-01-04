"""ItineraryStatusJob tracks transfer journeys and emits notifications."""

import sys
from pathlib import Path

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, DataTypes, Schema
from pyflink.table import expressions as expr

sys.path.append(str(Path(__file__).resolve().parents[1]))

from flink_jobs.common import (
    load_configs,
    register_kafka_json_source,
    register_clickhouse_sink,
    baggage_event_schema,
    json_options,
)


def run():
    env = StreamExecutionEnvironment.get_execution_environment()
    table_env = StreamTableEnvironment.create(env)

    kafka_cfg, clickhouse_cfg, topics = load_configs()

    register_kafka_json_source(
        table_env,
        name="baggage_events",
        topic=topics.baggage_events,
        bootstrap_servers=kafka_cfg.bootstrap_servers,
        schema=baggage_event_schema,
        start_mode=kafka_cfg.start_mode,
        json_options=json_options,
        watermark_field="event_time",
    )

    status_schema = (
        Schema.new_builder()
        .column("itinerary_id", DataTypes.STRING())
        .column("journey_state", DataTypes.STRING())
        .column("last_event_type", DataTypes.STRING())
        .column("last_airport", DataTypes.STRING())
        .column("last_event_time", DataTypes.TIMESTAMP_LTZ(3))
        .column("updated_at", DataTypes.TIMESTAMP_LTZ(3))
        .build()
    )

    register_clickhouse_sink(
        table_env,
        name="itinerary_status_clickhouse",
        table_name="itinerary_status",
        jdbc_url=clickhouse_cfg.jdbc_url,
        username=clickhouse_cfg.username,
        password=clickhouse_cfg.password,
        schema=status_schema,
    )

    events = table_env.from_path("baggage_events")
    table_env.create_temporary_view("events", events)

    ranked = table_env.sql_query(
        """
        SELECT *, ROW_NUMBER() OVER (PARTITION BY itinerary_id ORDER BY event_time DESC, ingest_time DESC) AS rn
        FROM events
        """
    )

    latest = ranked.filter(expr.col("rn") == 1).select(
        expr.col("itinerary_id"),
        expr.col("event_type").alias("last_event_type"),
        expr.col("airport").alias("last_airport"),
        expr.col("event_time").alias("last_event_time"),
        expr.col("ingest_time").alias("updated_at"),
        _journey_state(expr.col("event_type")).alias("journey_state"),
    )

    table_env.create_temporary_view("itinerary_status", latest)

    table_env.execute_sql(
        "INSERT INTO itinerary_status_clickhouse SELECT itinerary_id, journey_state, last_event_type, last_airport, last_event_time, updated_at FROM itinerary_status"
    )

    table_env.execute_sql(
        f"""
        CREATE TABLE notifications_kafka_sink (
            itinerary_id STRING,
            journey_state STRING,
            last_event_type STRING,
            last_airport STRING,
            last_event_time TIMESTAMP_LTZ(3),
            updated_at TIMESTAMP_LTZ(3),
            PRIMARY KEY (itinerary_id) NOT ENFORCED
        ) WITH (
            'connector' = 'upsert-kafka',
            'topic' = '{topics.notifications}',
            'properties.bootstrap.servers' = '{kafka_cfg.bootstrap_servers}',
            'key.format' = 'json',
            'value.format' = 'json'
        )
        """
    )

    table_env.execute_sql(
        "INSERT INTO notifications_kafka_sink SELECT * FROM itinerary_status WHERE journey_state IN ('AT_RISK_FOR_CONNECTION', 'DELAYED_OR_REROUTED', 'EXCEPTION')"
    ).wait()


def _journey_state(event_type):
    return (
        expr.case_()
        .when(event_type == "TransferIn", "AT_RISK_FOR_CONNECTION")
        .when(event_type == "ReassociatedToNextLeg", "RECOVERING")
        .when(event_type == "LoadedOnAircraft", "ON_TIME")
        .when(event_type == "Offloaded", "DELAYED_OR_REROUTED")
        .otherwise("EXCEPTION")
    )


if __name__ == "__main__":
    run()
