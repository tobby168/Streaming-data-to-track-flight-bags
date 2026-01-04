CREATE DATABASE IF NOT EXISTS baggage;

CREATE TABLE IF NOT EXISTS baggage.flight_kpis
(
    flight_id String,
    airport_origin String,
    airport_dest String,
    dep_time DateTime,
    arr_time DateTime,
    loaded_pct Float64,
    missing_bags UInt32,
    freshness_sec UInt32,
    updated_at DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (flight_id, dep_time);

CREATE TABLE IF NOT EXISTS baggage.bag_latest
(
    bag_id String,
    itinerary_id String,
    customer_id String,
    flight_id String,
    leg_index UInt8,
    airport String,
    event_type String,
    event_time DateTime,
    ingest_time DateTime,
    attributes Map(String, String),
    updated_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY bag_id;

CREATE TABLE IF NOT EXISTS baggage.alerts
(
    flight_id String,
    rule_id String,
    window_start DateTime,
    severity String,
    payload String,
    created_at DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (flight_id, rule_id, window_start);

CREATE TABLE IF NOT EXISTS baggage.itinerary_status
(
    itinerary_id String,
    journey_state String,
    latest_event String,
    latest_event_time DateTime,
    connection_airport String,
    risk_reason String,
    updated_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY itinerary_id;
