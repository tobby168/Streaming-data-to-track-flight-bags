from __future__ import annotations

import json
import math
import random
import uuid
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, List, Tuple

from . import config
from .models import BagAssignment, BaggageEvent, FlightSchedule, Leg


def _truncated_gauss(mean: float, stddev: float, minimum: float, maximum: float) -> float:
    value = random.gauss(mean, stddev)
    return min(max(value, minimum), maximum)


def _lognormal_from_p(percentile_50: float, percentile_90: float) -> Tuple[float, float]:
    """Derive log-normal parameters from the 50th and 90th percentiles.

    For a log-normal distribution, quantiles follow:
    q(p) = exp(mu + sigma * Phi^-1(p))
    We use the analytical inverse CDF value for p=0.9.
    """

    z_p90 = 1.2815515655446004  # Phi^-1(0.9)
    mu = math.log(percentile_50)
    sigma = (math.log(percentile_90) - mu) / z_p90
    return mu, sigma


def _lognormal_minutes(p50: float, p90: float) -> float:
    mu, sigma = _lognormal_from_p(p50, p90)
    return random.lognormvariate(mu, sigma)


class MockDataGenerator:
    """Generates flights, assignments, and baggage events following the design doc."""

    def __init__(self, seed: int | None = None) -> None:
        self.random = random.Random(seed)
        random.seed(seed)

    def generate(self) -> Tuple[List[FlightSchedule], List[BagAssignment], List[BaggageEvent]]:
        flights = self._generate_flights()
        assignments, bag_counts = self._generate_assignments(flights)
        events = self._generate_events(assignments, bag_counts, flights)
        return flights, assignments, events

    def _generate_flights(self) -> List[FlightSchedule]:
        today = datetime.now(timezone.utc).date()
        flights: List[FlightSchedule] = []
        start_time = datetime(today.year, today.month, today.day, 5, 0, tzinfo=timezone.utc)
        interval_minutes = int(24 * 60 / config.FLIGHTS_PER_DAY)
        for i in range(config.FLIGHTS_PER_DAY):
            origin = config.AIRPORT if i % 4 != 0 else config.HUB
            dest_pool = (
                config.PRIMARY_DESTINATIONS if origin == config.AIRPORT else config.CONNECTION_DESTINATIONS
            )
            dest = dest_pool[i % len(dest_pool)]
            dep_time = start_time + timedelta(minutes=i * interval_minutes)
            flight_duration = timedelta(minutes=80 if dest == config.HUB else 120)
            arr_time = dep_time + flight_duration
            bag_cutoff_time = dep_time - config.DEFAULT_BAG_CUTOFF_OFFSET
            flight_id = f"BR{900 + i:03d}|{today}|{origin}->{dest}"
            flights.append(
                FlightSchedule(
                    flight_id=flight_id,
                    airport_origin=origin,
                    airport_dest=dest,
                    dep_time=dep_time,
                    arr_time=arr_time,
                    bag_cutoff_time=bag_cutoff_time,
                    expected_bags=0,
                )
            )
        return flights

    def _sample_passenger_count(self) -> int:
        count = _truncated_gauss(
            config.PASSENGERS_MEAN,
            config.PASSENGERS_STD,
            config.PASSENGERS_MIN,
            config.PASSENGERS_MAX,
        )
        return int(round(count))

    def _sample_bag_count(self) -> int:
        return self.random.choices([0, 1, 2, 3], weights=config.BAG_COUNT_WEIGHTS, k=1)[0]

    def _select_flight(self, flights: List[FlightSchedule], origin: str, dest: str | None = None) -> FlightSchedule:
        candidates = [f for f in flights if f.airport_origin == origin and (dest is None or f.airport_dest == dest)]
        if not candidates:
            candidates = [f for f in flights if f.airport_origin == origin]
        return self.random.choice(candidates)

    def _generate_assignments(self, flights: List[FlightSchedule]) -> Tuple[List[BagAssignment], Dict[str, int]]:
        assignments: List[BagAssignment] = []
        bag_counts: Dict[str, int] = {}
        itinerary_counter = 0
        bag_counter = 0
        customer_counter = 0

        for flight in flights:
            passengers = self._sample_passenger_count()
            for _ in range(passengers):
                bags = self._sample_bag_count()
                if bags == 0:
                    continue
                itinerary_id = f"ITI{itinerary_counter:06d}"
                itinerary_counter += 1
                customer_id = f"CUST{customer_counter:06d}"
                customer_counter += 1
                legs: List[Leg] = []

                is_transfer = self.random.random() < config.TRANSFER_RATIO and flight.airport_dest == config.HUB
                if is_transfer:
                    second_leg = self._select_flight(flights, origin=config.HUB)
                    legs = [
                        Leg(leg_index=0, flight_id=flight.flight_id, origin=flight.airport_origin, dest=flight.airport_dest),
                        Leg(
                            leg_index=1,
                            flight_id=second_leg.flight_id,
                            origin=second_leg.airport_origin,
                            dest=second_leg.airport_dest,
                        ),
                    ]
                else:
                    legs = [Leg(leg_index=0, flight_id=flight.flight_id, origin=flight.airport_origin, dest=flight.airport_dest)]

                for bag_idx in range(bags):
                    bag_id = f"BAG{bag_counter:06d}"
                    bag_counter += 1
                    assignment = BagAssignment(
                        bag_id=bag_id,
                        itinerary_id=itinerary_id,
                        customer_id=customer_id,
                        bags_in_itinerary=bags,
                        legs=legs,
                    )
                    assignments.append(assignment)
                    for leg in legs:
                        bag_counts.setdefault(leg.flight_id, 0)
                        bag_counts[leg.flight_id] += 1

        for flight in flights:
            flight.expected_bags = bag_counts.get(flight.flight_id, 0)
        return assignments, bag_counts

    def _event_times_for_leg(self, schedule: FlightSchedule) -> Dict[str, datetime]:
        dep_time = schedule.dep_time
        checked_in_at = dep_time - timedelta(
            minutes=_truncated_gauss(
                config.CHECKED_IN_MEAN,
                config.CHECKED_IN_STD,
                config.CHECKED_IN_MIN,
                config.CHECKED_IN_MAX,
            )
        )
        inducted_at = checked_in_at + timedelta(minutes=_lognormal_minutes(config.INDUCTED_P50, config.INDUCTED_P90))
        screened_at = inducted_at + timedelta(minutes=_lognormal_minutes(config.SCREENED_P50, config.SCREENED_P90))
        makeup_at = screened_at + timedelta(minutes=_lognormal_minutes(config.MAKEUP_P50, config.MAKEUP_P90))
        loaded_at = schedule.bag_cutoff_time - timedelta(minutes=config.LOADED_BUFFER_MINUTES)
        offloaded_at = schedule.arr_time + timedelta(minutes=5)
        carousel_at = offloaded_at + timedelta(minutes=10)
        return {
            "CheckedIn": checked_in_at,
            "Inducted": inducted_at,
            "ScreenedCleared": screened_at,
            "MakeupArrived": makeup_at,
            "LoadedOnAircraft": loaded_at,
            "Offloaded": offloaded_at,
            "CarouselArrived": carousel_at,
        }

    def _generate_events(
        self,
        assignments: List[BagAssignment],
        bag_counts: Dict[str, int],
        flights: List[FlightSchedule],
    ) -> List[BaggageEvent]:
        events: List[BaggageEvent] = []
        flight_map = {f.flight_id: f for f in flights}
        for assignment in assignments:
            for leg in assignment.legs:
                schedule = flight_map[leg.flight_id]
                times = self._event_times_for_leg(schedule)
                for event_type, event_time in times.items():
                    ingest_time = self._ingest_time(event_time)
                    event = BaggageEvent(
                        event_id=str(uuid.uuid4()),
                        bag_id=assignment.bag_id,
                        itinerary_id=assignment.itinerary_id,
                        customer_id=assignment.customer_id,
                        flight_id=leg.flight_id,
                        leg_index=leg.leg_index,
                        airport=leg.origin,
                        event_type=event_type,
                        event_time=event_time,
                        ingest_time=ingest_time,
                        scan_point_id=f"{leg.origin}_{event_type.upper()}_{self.random.randint(1, 20)}",
                        reader_type=self.random.choice(["RFID", "Barcode", "Manual"]),
                        confidence=self.random.uniform(0.93, 1.0),
                        attributes=self._attributes(event_type, leg, schedule),
                    )
                    events.append(event)
        events.sort(key=lambda e: e.event_time)
        return events

    def _ingest_time(self, event_time: datetime) -> datetime:
        if self.random.random() < config.OUT_OF_ORDER_RATE:
            delay = self.random.uniform(
                config.OUT_OF_ORDER_MIN_DELAY.total_seconds(),
                config.OUT_OF_ORDER_MAX_DELAY.total_seconds(),
            )
            return event_time + timedelta(seconds=delay)
        return event_time + timedelta(seconds=self.random.uniform(5, 45))

    def _attributes(self, event_type: str, leg: Leg, schedule: FlightSchedule) -> Dict[str, str]:
        attributes: Dict[str, str] = {}
        if event_type == "ScreenedCleared":
            attributes["security_result"] = "CLEARED"
        if event_type == "MakeupArrived":
            attributes["container_id"] = f"AKE{self.random.randint(10000, 99999)}{leg.origin[:2]}"
        if event_type == "LoadedOnAircraft":
            attributes["load_seq"] = str(self.random.randint(1, 50))
        if event_type == "Offloaded":
            attributes["belt"] = f"B{self.random.randint(1, 12)}"
        if event_type == "CarouselArrived":
            attributes["carousel"] = f"C{self.random.randint(1, 6)}"
        attributes["dest"] = schedule.airport_dest
        return attributes


def events_to_json(events: Iterable[BaggageEvent]) -> str:
    return "\n".join(json.dumps(event.to_json_payload()) for event in events)
