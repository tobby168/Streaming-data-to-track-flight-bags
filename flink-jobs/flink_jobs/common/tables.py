"""Table registration helpers for Kafka sources and ClickHouse sinks."""

from pyflink.table import Schema, TableDescriptor


def register_kafka_json_source(
    table_env,
    name: str,
    topic: str,
    bootstrap_servers: str,
    schema: Schema,
    start_mode: str,
    json_options: dict,
    watermark_field: str | None = None,
    watermark_lag_seconds: int = 5,
):
    descriptor = (
        TableDescriptor.for_connector("kafka")
        .schema(_schema_with_watermark(schema, watermark_field, watermark_lag_seconds))
        .option("topic", topic)
        .option("properties.bootstrap.servers", bootstrap_servers)
        .option("properties.group.id", name)
        .option("scan.startup.mode", start_mode)
    )

    for key, value in json_options.items():
        descriptor = descriptor.option(key, value)

    table_env.create_temporary_table(name, descriptor)


def register_clickhouse_sink(
    table_env,
    name: str,
    table_name: str,
    jdbc_url: str,
    username: str,
    password: str,
    schema: Schema,
):
    descriptor = (
        TableDescriptor.for_connector("jdbc")
        .schema(schema)
        .option("url", jdbc_url)
        .option("table-name", table_name)
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
    )
    if username:
        descriptor = descriptor.option("username", username)
    if password:
        descriptor = descriptor.option("password", password)

    table_env.create_temporary_table(name, descriptor)


def _schema_with_watermark(schema: Schema, watermark_field, watermark_lag_seconds):
    builder = Schema.new_builder().from_schema(schema)
    if watermark_field:
        builder.watermark(
            watermark_field,
            f"{watermark_field} - INTERVAL '{watermark_lag_seconds}' SECOND",
        )
    return builder.build()
