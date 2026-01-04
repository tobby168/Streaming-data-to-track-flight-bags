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
    .column("event_time", DataTypes.TIMESTAMP_LTZ(3))
    .column("ingest_time", DataTypes.TIMESTAMP_LTZ(3))
    .column("scan_point_id", DataTypes.STRING())
    .column("reader_type", DataTypes.STRING())
    .column("confidence", DataTypes.DOUBLE())
    .column("attributes", DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()))
    .column("payload", DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()))
    .build()
)


flight_schedule_schema = (
    Schema.new_builder()
    .column("flight_id", DataTypes.STRING())
    .column("airport_origin", DataTypes.STRING())
    .column("airport_dest", DataTypes.STRING())
    .column("dep_time", DataTypes.TIMESTAMP_LTZ(3))
    .column("arr_time", DataTypes.TIMESTAMP_LTZ(3))
    .column("bag_cutoff_time", DataTypes.TIMESTAMP_LTZ(3))
    .column("expected_bags", DataTypes.INT())
    .build()
)


json_options = {
    "format": "json",
    "json.ignore-parse-errors": "true",
}
