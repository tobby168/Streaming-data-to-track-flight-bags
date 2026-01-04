"""FlightKpiAlertJob computes 10s KPIs and produces alert records."""

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
    flight_schedule_schema,
    json_options,
)


# pylint: disable=too-many-locals

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

    register_kafka_json_source(
        table_env,
        name="flight_schedule",
        topic=topics.flight_schedule,
        bootstrap_servers=kafka_cfg.bootstrap_servers,
        schema=flight_schedule_schema,
        start_mode=kafka_cfg.start_mode,
        json_options=json_options,
        watermark_field="dep_time",
    )

    kpi_schema = (
        Schema.new_builder()
        .column("flight_id", DataTypes.STRING())
        .column("window_start", DataTypes.TIMESTAMP_LTZ(3))
        .column("window_end", DataTypes.TIMESTAMP_LTZ(3))
        .column("events", DataTypes.BIGINT())
        .column("loaded", DataTypes.BIGINT())
        .column("missing", DataTypes.BIGINT())
        .column("freshness_sec", DataTypes.DOUBLE())
        .column("severity", DataTypes.STRING())
        .build()
    )

    register_clickhouse_sink(
        table_env,
        name="flight_kpis_clickhouse",
        table_name="flight_kpis",
        jdbc_url=clickhouse_cfg.jdbc_url,
        username=clickhouse_cfg.username,
        password=clickhouse_cfg.password,
        schema=kpi_schema,
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
            EXTRACT(EPOCH FROM CURRENT_TIMESTAMP - max_event_time) AS freshness_sec
        FROM (
            SELECT *, MAX(event_time) OVER w AS max_event_time
            FROM TABLE(TUMBLE(TABLE events, DESCRIPTOR(event_time), INTERVAL '10' SECOND))
            WINDOW w AS (PARTITION BY flight_id ORDER BY window_start RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
        )
        GROUP BY flight_id, window_start, window_end, max_event_time
        """
    )

    scored = windowed.select(
        expr.col("flight_id"),
        expr.col("window_start"),
        expr.col("window_end"),
        expr.col("events"),
        expr.col("loaded"),
        expr.col("missing"),
        expr.col("freshness_sec"),
        expr.case_()
        .when((expr.col("missing") > 0) & (expr.col("freshness_sec") > 120), "CRITICAL")
        .when(expr.col("missing") > 0, "WARN")
        .otherwise("OK")
        .alias("severity"),
    )

    table_env.create_temporary_view("flight_kpis", scored)

    table_env.execute_sql(
        "INSERT INTO flight_kpis_clickhouse SELECT * FROM flight_kpis"
    )

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

    table_env.execute_sql(
        "INSERT INTO alerts_kafka_sink SELECT flight_id, window_start, window_end, severity, missing FROM flight_kpis WHERE severity <> 'OK'"
    ).wait()


if __name__ == "__main__":
    run()
