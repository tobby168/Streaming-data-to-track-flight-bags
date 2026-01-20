"""BagLatestJob processes baggage events and maintains the latest state per bag."""

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


def build_tables(table_env, kafka_cfg, topics):
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

def run():
    env = StreamExecutionEnvironment.get_execution_environment()
    table_env = StreamTableEnvironment.create(env)

    kafka_cfg, topics = load_configs()
    build_tables(table_env, kafka_cfg, topics)

    events = table_env.from_path("baggage_events")
    table_env.create_temporary_view("baggage_events_view", events)

    latest = table_env.sql_query(
        """
        SELECT bag_id, itinerary_id, customer_id, flight_id, leg_index, airport, event_type, event_time_ts AS event_time, ingest_time_ts AS ingest_time, attributes
        FROM (
            SELECT e.*, ROW_NUMBER() OVER(PARTITION BY bag_id ORDER BY event_time_ts DESC, ingest_time_ts DESC) as rownum
            FROM baggage_events_view e
        )
        WHERE rownum = 1
        """
    )

    table_env.create_temporary_view("bag_latest", latest)

    table_env.execute_sql(
        f"""
        CREATE TABLE bag_latest_kafka_sink (
            bag_id STRING,
            itinerary_id STRING,
            customer_id STRING,
            flight_id STRING,
            leg_index INT,
            event_type STRING,
            airport STRING,
            event_time TIMESTAMP_LTZ(3),
            ingest_time TIMESTAMP_LTZ(3),
            attributes MAP<STRING, STRING>,
            PRIMARY KEY (bag_id) NOT ENFORCED
        ) WITH (
            'connector' = 'upsert-kafka',
            'topic' = '{topics.bag_latest}',
            'properties.bootstrap.servers' = '{kafka_cfg.bootstrap_servers}',
            'key.format' = 'json',
            'value.format' = 'json'
        )
        """
    )

    results = [
        table_env.execute_sql(
            "INSERT INTO bag_latest_kafka_sink SELECT bag_id, itinerary_id, customer_id, flight_id, leg_index, event_type, airport, event_time, ingest_time, attributes FROM bag_latest"
        )
    ]

    if not DETACHED:
        results[-1].wait()


if __name__ == "__main__":
    run()
