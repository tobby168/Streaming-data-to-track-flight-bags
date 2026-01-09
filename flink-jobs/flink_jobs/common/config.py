"""Configuration helpers for Flink jobs.

The design doc fixes Kafka topics and the ClickHouse sink names. These helpers
provide a single place to manage defaults while allowing overrides via
environment variables. Values match the local kind-based deployment from the
design.
"""
import os
from dataclasses import dataclass


@dataclass
class KafkaConfig:
    bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
    security_protocol: str = os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
    start_mode: str = os.getenv("KAFKA_START_MODE", "earliest-offset")




@dataclass
class TopicConfig:
    baggage_events: str = os.getenv("TOPIC_BAGGAGE_EVENTS", "baggage.events.v1")
    bag_latest: str = os.getenv("TOPIC_BAG_LATEST", "baggage.bag.latest.v1")
    alerts: str = os.getenv("TOPIC_ALERTS", "baggage.alerts.v1")
    notifications: str = os.getenv(
        "TOPIC_NOTIFICATIONS", "baggage.notifications.v1"
    )
    flight_kpis: str = os.getenv(
        "TOPIC_FLIGHT_KPIS", "baggage.flight.kpis.v1"
    )
    flight_schedule: str = os.getenv(
        "TOPIC_FLIGHT_SCHEDULE", "flight.schedule.v1"
    )


def load_configs() -> tuple[KafkaConfig, TopicConfig]:
    """Helper that loads all config dataclasses at once."""
    return KafkaConfig(), TopicConfig()
