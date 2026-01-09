"""FlightKpiAlertJob computes 10s KPIs and produces alert records."""

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
    flight_schedule_schema,
    json_options,
)


# pylint: disable=too-many-locals

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

    register_kafka_json_source(
        table_env,
        name="flight_schedule",
        topic=topics.flight_schedule,
        bootstrap_servers=kafka_cfg.bootstrap_servers,
        schema=flight_schedule_schema,
        start_mode=kafka_cfg.start_mode,
        json_options=json_options,
        watermark_field="dep_time_ts",
    )

    events = table_env.from_path("baggage_events")
    table_env.create_temporary_view("events", events)

    windowed = table_env.sql_query(
        """
        SELECT
            flight_id,
            window_start,
            window_end,
            COUNT(*) AS events,
            SUM(CASE WHEN event_type = 'LoadedOnAircraft' THEN 1 ELSE 0 END) AS loaded,
            SUM(CASE WHEN event_type = 'LoadedOnAircraft' THEN 0 ELSE 1 END) AS missing,
            TIMESTAMPDIFF(SECOND, MAX(event_time_ts), CURRENT_TIMESTAMP) AS freshness_sec
        FROM TABLE(TUMBLE(TABLE events, DESCRIPTOR(event_time_ts), INTERVAL '10' SECOND))
        GROUP BY flight_id, window_start, window_end
        """
    )

    table_env.create_temporary_view("windowed_kpis", windowed)

    scored = table_env.sql_query(
        """
        SELECT
            flight_id,
            window_start,
            window_end,
            events,
            loaded,
            missing,
            freshness_sec,
            CASE
                WHEN missing > 0 AND freshness_sec > 120 THEN 'CRITICAL'
                WHEN missing > 0 THEN 'WARN'
                ELSE 'OK'
            END AS severity
        FROM windowed_kpis
        """
    )

    table_env.create_temporary_view("flight_kpis", scored)

    table_env.execute_sql(
        f"""
        CREATE TABLE flight_kpis_kafka_sink (
            flight_id STRING,
            window_start TIMESTAMP_LTZ(3),
            window_end TIMESTAMP_LTZ(3),
            events BIGINT,
            loaded BIGINT,
            missing BIGINT,
            freshness_sec DOUBLE,
            severity STRING,
            PRIMARY KEY (flight_id, window_start) NOT ENFORCED
        ) WITH (
            'connector' = 'upsert-kafka',
            'topic' = '{topics.flight_kpis}',
            'properties.bootstrap.servers' = '{kafka_cfg.bootstrap_servers}',
            'key.format' = 'json',
            'value.format' = 'json'
        )
        """
    )

    kpi_result = table_env.execute_sql(
        "INSERT INTO flight_kpis_kafka_sink SELECT * FROM flight_kpis"
    )
    if not DETACHED:
        kpi_result.wait()

    table_env.execute_sql(
        f"""
        CREATE TABLE alerts_kafka_sink (
            flight_id STRING,
            window_start TIMESTAMP_LTZ(3),
            window_end TIMESTAMP_LTZ(3),
            severity STRING,
            missing BIGINT,
            PRIMARY KEY (flight_id, window_start) NOT ENFORCED
        ) WITH (
            'connector' = 'upsert-kafka',
            'topic' = '{topics.alerts}',
            'properties.bootstrap.servers' = '{kafka_cfg.bootstrap_servers}',
            'key.format' = 'json',
            'value.format' = 'json'
        )
        """
    )

    alerts_result = table_env.execute_sql(
        "INSERT INTO alerts_kafka_sink SELECT flight_id, window_start, window_end, severity, missing FROM flight_kpis WHERE severity <> 'OK'"
    )
    if not DETACHED:
        alerts_result.wait()


if __name__ == "__main__":
    run()
