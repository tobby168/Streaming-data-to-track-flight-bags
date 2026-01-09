from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional


@dataclass
class FlightSchedule:
    flight_id: str
    airport_origin: str
    airport_dest: str
    dep_time: datetime
    arr_time: datetime
    bag_cutoff_time: datetime
    expected_bags: int


@dataclass
class Leg:
    leg_index: int
    flight_id: str
    origin: str
    dest: str


@dataclass
class BagAssignment:
    bag_id: str
    itinerary_id: str
    customer_id: str
    bags_in_itinerary: int
    legs: List[Leg]


@dataclass
class BaggageEvent:
    event_id: str
    bag_id: str
    itinerary_id: str
    customer_id: str
    flight_id: str
    leg_index: int
    airport: str
    event_type: str
    event_time: datetime
    ingest_time: datetime
    scan_point_id: str
    reader_type: str
    confidence: float
    attributes: Dict[str, str] = field(default_factory=dict)

    def to_json_payload(self) -> Dict[str, object]:
        """Return a serializable dict for Kafka production."""
        return {
            "event_id": self.event_id,
            "bag_id": self.bag_id,
            "itinerary_id": self.itinerary_id,
            "customer_id": self.customer_id,
            "flight_id": self.flight_id,
            "leg_index": self.leg_index,
            "airport": self.airport,
            "event_type": self.event_type,
            "event_time": self.event_time.isoformat(timespec="milliseconds"),
            "ingest_time": self.ingest_time.isoformat(timespec="milliseconds"),
            "scan_point_id": self.scan_point_id,
            "reader_type": self.reader_type,
            "confidence": round(self.confidence, 4),
            "attributes": self.attributes,
        }
