from __future__ import annotations

import json
from datetime import datetime
from typing import Iterable

try:
    from kafka import KafkaProducer
except ImportError:  # pragma: no cover - optional dependency for tests
    KafkaProducer = None  # type: ignore

from .models import BagAssignment, BaggageEvent, FlightSchedule


class ProducerClient:
    def send(self, topic: str, key: str, value: str) -> None:  # pragma: no cover - interface
        raise NotImplementedError

    def flush(self) -> None:  # pragma: no cover - interface
        raise NotImplementedError


class KafkaProducerClient(ProducerClient):
    def __init__(self, bootstrap_servers: str) -> None:
        if KafkaProducer is None:
            raise RuntimeError("kafka-python is required to run KafkaProducerClient")
        self._producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            key_serializer=lambda v: v.encode("utf-8"),
            value_serializer=lambda v: v.encode("utf-8"),
        )

    def send(self, topic: str, key: str, value: str) -> None:
        self._producer.send(topic, key=key, value=value)

    def flush(self) -> None:
        self._producer.flush()


class InMemoryProducer(ProducerClient):
    def __init__(self) -> None:
        self.messages: list[tuple[str, str, str]] = []

    def send(self, topic: str, key: str, value: str) -> None:
        self.messages.append((topic, key, value))

    def flush(self) -> None:
        return


class BaggageKafkaPublisher:
    def __init__(self, client: ProducerClient) -> None:
        self.client = client

    @staticmethod
    def _fmt(dt: datetime) -> str:
        return dt.isoformat(timespec="milliseconds")

    def publish(
        self,
        flights: Iterable[FlightSchedule],
        assignments: Iterable[BagAssignment],
        events: Iterable[BaggageEvent],
    ) -> None:
        for flight in flights:
            payload = json.dumps(
                {
                    "flight_id": flight.flight_id,
                    "airport_origin": flight.airport_origin,
                    "airport_dest": flight.airport_dest,
                    "dep_time": self._fmt(flight.dep_time),
                    "arr_time": self._fmt(flight.arr_time),
                    "bag_cutoff_time": self._fmt(flight.bag_cutoff_time),
                    "expected_bags": flight.expected_bags,
                }
            )
            self.client.send("flight.schedule.v1", key=flight.flight_id, value=payload)

        for assignment in assignments:
            payload = json.dumps(
                {
                    "bag_id": assignment.bag_id,
                    "itinerary_id": assignment.itinerary_id,
                    "customer_id": assignment.customer_id,
                    "bags_in_itinerary": assignment.bags_in_itinerary,
                    "legs": [leg.__dict__ for leg in assignment.legs],
                }
            )
            self.client.send("baggage.assignment.v1", key=assignment.bag_id, value=payload)

        for event in events:
            self.client.send("baggage.events.v1", key=event.bag_id, value=json.dumps(event.to_json_payload()))

        self.client.flush()
