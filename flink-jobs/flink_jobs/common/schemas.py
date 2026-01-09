"""Reusable schema fragments for the Kafka inputs defined in DESIGN.md."""

from pyflink.table import DataTypes, Schema


baggage_event_schema = (
    Schema.new_builder()
    .column("event_id", DataTypes.STRING())
    .column("bag_id", DataTypes.STRING())
    .column("itinerary_id", DataTypes.STRING())
    .column("customer_id", DataTypes.STRING())
    .column("flight_id", DataTypes.STRING())
    .column("leg_index", DataTypes.INT())
    .column("airport", DataTypes.STRING())
    .column("event_type", DataTypes.STRING())
    .column("event_time", DataTypes.STRING())
    .column("ingest_time", DataTypes.STRING())
    .column("scan_point_id", DataTypes.STRING())
    .column("reader_type", DataTypes.STRING())
    .column("confidence", DataTypes.DOUBLE())
    .column("attributes", DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()))
    .column("payload", DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()))
    .column_by_expression(
        "event_time_ts",
        "CAST(REPLACE(REPLACE(REPLACE(event_time, 'T', ' '), 'Z', ''), '+00:00', '') AS TIMESTAMP_LTZ(3))",
    )
    .column_by_expression(
        "ingest_time_ts",
        "CAST(REPLACE(REPLACE(REPLACE(ingest_time, 'T', ' '), 'Z', ''), '+00:00', '') AS TIMESTAMP_LTZ(3))",
    )
    .build()
)


flight_schedule_schema = (
    Schema.new_builder()
    .column("flight_id", DataTypes.STRING())
    .column("airport_origin", DataTypes.STRING())
    .column("airport_dest", DataTypes.STRING())
    .column("dep_time", DataTypes.STRING())
    .column("arr_time", DataTypes.STRING())
    .column("bag_cutoff_time", DataTypes.STRING())
    .column("expected_bags", DataTypes.INT())
    .column_by_expression(
        "dep_time_ts",
        "CAST(REPLACE(REPLACE(REPLACE(dep_time, 'T', ' '), 'Z', ''), '+00:00', '') AS TIMESTAMP_LTZ(3))",
    )
    .column_by_expression(
        "arr_time_ts",
        "CAST(REPLACE(REPLACE(REPLACE(arr_time, 'T', ' '), 'Z', ''), '+00:00', '') AS TIMESTAMP_LTZ(3))",
    )
    .column_by_expression(
        "bag_cutoff_time_ts",
        "CAST(REPLACE(REPLACE(REPLACE(bag_cutoff_time, 'T', ' '), 'Z', ''), '+00:00', '') AS TIMESTAMP_LTZ(3))",
    )
    .build()
)


json_options = {
    "format": "json",
    "json.ignore-parse-errors": "true",
    "json.timestamp-format.standard": "ISO-8601",
}
