"""BagLatestJob processes baggage events and maintains the latest state per bag."""

import sys
from pathlib import Path

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, Schema, DataTypes

sys.path.append(str(Path(__file__).resolve().parents[1]))

from flink_jobs.common import (
    load_configs,
    register_kafka_json_source,
    register_clickhouse_sink,
    baggage_event_schema,
    json_options,
)


def build_tables(table_env, kafka_cfg, clickhouse_cfg, topics):
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

    bag_latest_schema = Schema.new_builder() \
        .column("bag_id", DataTypes.STRING()) \
        .column("itinerary_id", DataTypes.STRING()) \
        .column("flight_id", DataTypes.STRING()) \
        .column("event_type", DataTypes.STRING()) \
        .column("airport", DataTypes.STRING()) \
        .column("event_time", DataTypes.TIMESTAMP_LTZ(3)) \
        .column("ingest_time", DataTypes.TIMESTAMP_LTZ(3)) \
        .column("attributes", DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING())) \
        .build()

    register_clickhouse_sink(
        table_env,
        name="bag_latest_clickhouse",
        table_name="bag_latest",
        jdbc_url=clickhouse_cfg.jdbc_url,
        username=clickhouse_cfg.username,
        password=clickhouse_cfg.password,
        schema=bag_latest_schema,
    )


# pylint: disable=too-many-statements

def run():
    env = StreamExecutionEnvironment.get_execution_environment()
    table_env = StreamTableEnvironment.create(env)

    kafka_cfg, clickhouse_cfg, topics = load_configs()
    build_tables(table_env, kafka_cfg, clickhouse_cfg, topics)

    events = table_env.from_path("baggage_events")
    table_env.create_temporary_view("baggage_events_view", events)

    latest = table_env.sql_query(
        """
        SELECT bag_id, itinerary_id, flight_id, event_type, airport, event_time, ingest_time, attributes
        FROM (
            SELECT e.*, ROW_NUMBER() OVER(PARTITION BY bag_id ORDER BY event_time DESC, ingest_time DESC) as rownum
            FROM baggage_events_view e
        )
        WHERE rownum = 1
        """
    )

    table_env.create_temporary_view("bag_latest", latest)

    table_env.execute_sql(
        "INSERT INTO bag_latest_clickhouse SELECT * FROM bag_latest"
    )

    table_env.execute_sql(
        f"""
        CREATE TABLE bag_latest_kafka_sink (
            bag_id STRING,
            itinerary_id STRING,
            flight_id STRING,
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

    table_env.execute_sql(
        "INSERT INTO bag_latest_kafka_sink SELECT * FROM bag_latest"
    ).wait()


if __name__ == "__main__":
    run()
