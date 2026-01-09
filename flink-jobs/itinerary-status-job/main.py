"""ItineraryStatusJob tracks transfer journeys and emits notifications."""

import os
import sys
from pathlib import Path

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment

sys.path.append(str(Path(__file__).resolve().parents[1]))

DETACHED = os.getenv("FLINK_DETACHED", "0").lower() in ("1", "true", "yes")

from flink_jobs.common import (
    load_configs,
    register_kafka_json_source,
    baggage_event_schema,
    json_options,
)


def run():
    env = StreamExecutionEnvironment.get_execution_environment()
    table_env = StreamTableEnvironment.create(env)

    kafka_cfg, topics = load_configs()

    register_kafka_json_source(
        table_env,
        name="baggage_events",
        topic=topics.baggage_events,
        bootstrap_servers=kafka_cfg.bootstrap_servers,
        schema=baggage_event_schema,
        start_mode=kafka_cfg.start_mode,
        json_options=json_options,
        watermark_field="event_time_ts",
    )

    events = table_env.from_path("baggage_events")
    table_env.create_temporary_view("events", events)

    ranked = table_env.sql_query(
        """
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY itinerary_id
                ORDER BY event_time_ts DESC, ingest_time_ts DESC
            ) AS rn
        FROM events
        """
    )
    table_env.create_temporary_view("ranked_events", ranked)

    latest = table_env.sql_query(
        """
        SELECT
            itinerary_id,
            CASE
                WHEN event_type = 'TransferIn' THEN 'AT_RISK_FOR_CONNECTION'
                WHEN event_type = 'ReassociatedToNextLeg' THEN 'RECOVERING'
                WHEN event_type = 'LoadedOnAircraft' THEN 'ON_TIME'
                WHEN event_type = 'Offloaded' THEN 'DELAYED_OR_REROUTED'
                ELSE 'EXCEPTION'
            END AS journey_state,
            event_type AS last_event_type,
            airport AS last_airport,
            event_time_ts AS last_event_time,
            ingest_time_ts AS updated_at
        FROM ranked_events
        WHERE rn = 1
        """
    )

    table_env.create_temporary_view("itinerary_status", latest)

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

    notifications_result = table_env.execute_sql(
        "INSERT INTO notifications_kafka_sink SELECT * FROM itinerary_status WHERE journey_state IN ('AT_RISK_FOR_CONNECTION', 'DELAYED_OR_REROUTED', 'EXCEPTION')"
    )
    if not DETACHED:
        notifications_result.wait()


if __name__ == "__main__":
    run()
