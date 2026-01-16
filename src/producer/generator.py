from __future__ import annotations

import json
import math
import random
import uuid
from collections import deque
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Deque, Dict, Iterable, List, Tuple

from . import config
from .models import BagAssignment, BaggageEvent, FlightSchedule, Leg


def _truncated_gauss(rnd: random.Random, mean: float, stddev: float, minimum: float, maximum: float) -> float:
    value = rnd.gauss(mean, stddev)
    return min(max(value, minimum), maximum)


def _lognormal_from_p(percentile_50: float, percentile_90: float) -> Tuple[float, float]:
    """Derive log-normal parameters from the 50th and 90th percentiles."""
    z_p90 = 1.2815515655446004  # Phi^-1(0.9)
    mu = math.log(percentile_50)
    sigma = (math.log(percentile_90) - mu) / z_p90
    return mu, sigma


def _lognormal_minutes(rnd: random.Random, p50: float, p90: float) -> float:
    mu, sigma = _lognormal_from_p(p50, p90)
    return rnd.lognormvariate(mu, sigma)


class MockDataGenerator:
    """Generates flights, assignments, and baggage events using a rolled TPE schedule."""

    def __init__(self, seed: int | None = None) -> None:
        self.random = random.Random(seed)
        random.seed(seed)
        self._base_schedule = self._load_schedule()
        self._rolled_schedule = self._roll_schedule(self._base_schedule)
        self._all_flights: List[FlightSchedule] = []
        self._all_meta: Dict[str, Dict[str, object]] = {}
        self._active: Dict[str, Dict[str, object]] = {}
        self._current_window_ids: set[str] = set()
        self._current_flights: List[FlightSchedule] = []
        self._current_meta: Dict[str, Dict[str, object]] = {}
        self._bag_counter = 0
        self._itinerary_counter = 0
        self._customer_counter = 0
        self._init_schedule_state()

    def generate(self) -> Tuple[List[FlightSchedule], List[BagAssignment], Iterable[BaggageEvent]]:
        self._ensure_window_state()
        emit_cutoff = datetime.now(timezone.utc) + timedelta(seconds=config.EMIT_LOOKAHEAD_SECONDS)

        events: List[BaggageEvent] = []
        flights_in_batch: set[str] = set()
        bag_ids_in_batch: set[str] = set()

        for flight_id, state in self._active.items():
            ev_queue: Deque[BaggageEvent] = state["events"]  # type: ignore[assignment]
            while ev_queue and ev_queue[0].event_time <= emit_cutoff:
                event = ev_queue.popleft()
                events.append(event)
                flights_in_batch.add(flight_id)
                bag_ids_in_batch.add(event.bag_id)

        events.sort(key=lambda e: e.event_time)

        if not events:
            return [], [], []

        assignments: List[BagAssignment] = []
        flights: List[FlightSchedule] = []
        for flight_id in flights_in_batch:
            state = self._active.get(flight_id)
            if not state:
                continue
            flights.append(state["flight"])  # type: ignore[arg-type]
            for bid, assignment in state["assignments"].items():  # type: ignore[assignment]
                if bid in bag_ids_in_batch:
                    assignments.append(assignment)

        return flights, assignments, events

    def _init_schedule_state(self) -> None:
        flights, meta = self._instantiate_flights()
        self._all_flights = flights
        self._all_meta = meta
        self._current_window_ids = set()
        self._current_window_ids = set()

    def _ensure_window_state(self) -> None:
        flights, meta = self._window_flights(self._all_flights, self._all_meta)
        window_ids = {f.flight_id for f in flights}
        now = datetime.now(timezone.utc)

        # Add newly active flights
        for flight in flights:
            if flight.flight_id in self._active:
                continue
            state = self._build_flight_state(flight, meta[flight.flight_id])
            self._active[flight.flight_id] = state

        # Drop completed flights outside the window
        grace = timedelta(minutes=30)
        to_remove: List[str] = []
        for flight_id, state in self._active.items():
            flight_obj: FlightSchedule = state["flight"]  # type: ignore[assignment]
            events: Deque[BaggageEvent] = state["events"]  # type: ignore[assignment]
            if flight_id not in window_ids and not events and now > flight_obj.arr_time + grace:
                to_remove.append(flight_id)
        for fid in to_remove:
            del self._active[fid]

        self._current_window_ids = window_ids
        self._current_flights = flights
        self._current_meta = meta

    def _build_flight_state(self, flight: FlightSchedule, meta: Dict[str, object]) -> Dict[str, object]:
        assignments, _, bag_checkins = self._generate_assignments_for_flight(flight, meta)
        assignment_map = {a.bag_id: a for a in assignments}
        events_list = list(self._generate_events(assignments, {}, [flight], {flight.flight_id: meta}, bag_checkins))
        events_list.sort(key=lambda e: e.event_time)
        return {
            "flight": flight,
            "meta": meta,
            "assignments": assignment_map,
            "events": deque(events_list),
        }

    def _load_schedule(self) -> List[Dict[str, object]]:
        path = Path(config.SCHEDULE_RAW_PATH)
        if not path.exists():
            raise FileNotFoundError(f"Schedule file not found at {path}")
        raw = json.loads(path.read_text())
        flights: List[Dict[str, object]] = []
        for item in raw:
            flight = item.get("flight", {})
            number = flight.get("identification", {}).get("number", {}).get("default")
            if not number:
                continue
            airline = flight.get("airline", {}).get("code", {}).get("iata") or "XX"
            dest = flight.get("airport", {}).get("destination", {}).get("code", {}).get("iata")
            if not dest:
                continue
            scheduled = flight.get("time", {}).get("scheduled", {}) or {}
            dep_ts = scheduled.get("departure")
            if not dep_ts:
                continue
            arr_ts = scheduled.get("arrival")
            dep_time = datetime.fromtimestamp(dep_ts, tz=timezone.utc)
            if arr_ts:
                arr_time = datetime.fromtimestamp(arr_ts, tz=timezone.utc)
            else:
                arr_time = dep_time + timedelta(minutes=self._block_minutes(dest))
            origin_info = flight.get("airport", {}).get("origin", {}).get("info", {}) or {}
            aircraft_code = (flight.get("aircraft", {}) or {}).get("model", {}).get("code")
            flights.append(
                {
                    "flight_number": number,
                    "airline": airline,
                    "dest": dest,
                    "dep_time": dep_time,
                    "arr_time": arr_time,
                    "terminal": origin_info.get("terminal"),
                    "gate": origin_info.get("gate"),
                    "aircraft_code": aircraft_code,
                }
            )
        if not flights:
            raise ValueError(f"No flights parsed from {path}")
        return flights

    def _route_class(self, dest: str) -> str:
        if dest in config.LONG_HAUL_DESTS:
            return "long"
        if dest in config.MEDIUM_HAUL_DESTS:
            return "medium"
        return "short"

    def _block_minutes(self, dest: str) -> int:
        if dest in config.BLOCK_TIME_BY_DEST:
            return config.BLOCK_TIME_BY_DEST[dest]
        route_class = self._route_class(dest)
        return config.BLOCK_TIME_BY_CLASS.get(route_class, 160)

    def _seat_capacity(self, aircraft_code: str | None) -> int:
        if aircraft_code and aircraft_code in config.AIRCRAFT_CAPACITY:
            return config.AIRCRAFT_CAPACITY[aircraft_code]
        return config.PASSENGER_CAPACITY_DEFAULT

    def _roll_schedule(self, base_schedule: List[Dict[str, object]]) -> List[Dict[str, object]]:
        base_dates = sorted({f["dep_time"].date() for f in base_schedule})  # type: ignore[index]
        flights_by_date: Dict[datetime.date, List[Dict[str, object]]] = {}
        for f in base_schedule:
            dep_date = f["dep_time"].date()  # type: ignore[index]
            flights_by_date.setdefault(dep_date, []).append(f)

        start_date = datetime.now(timezone.utc).date()
        rolled: List[Dict[str, object]] = []
        for day_offset in range(config.SCHEDULE_DAYS):
            target_date = start_date + timedelta(days=day_offset)
            base_date = base_dates[day_offset % len(base_dates)]
            delta_days = (target_date - base_date).days
            for base in flights_by_date.get(base_date, []):
                dep_time = base["dep_time"] + timedelta(days=delta_days)  # type: ignore[index]
                arr_time = base["arr_time"] + timedelta(days=delta_days)  # type: ignore[index]
                dest = base["dest"]  # type: ignore[index]
                flight_number = base["flight_number"]  # type: ignore[index]
                flight_id = f"{flight_number}|{target_date}|{config.AIRPORT}->{dest}"
                rolled.append(
                    {
                        "flight_id": flight_id,
                        "dep_time": dep_time,
                        "arr_time": arr_time,
                        "dest": dest,
                        "terminal": base.get("terminal"),
                        "gate": base.get("gate"),
                        "airline": base.get("airline"),
                        "aircraft_code": base.get("aircraft_code"),
                        "route_class": self._route_class(dest),
                        "capacity": self._seat_capacity(base.get("aircraft_code")),  # type: ignore[arg-type]
                    }
                )
        return rolled

    def _instantiate_flights(self) -> Tuple[List[FlightSchedule], Dict[str, Dict[str, object]]]:
        flights: List[FlightSchedule] = []
        meta: Dict[str, Dict[str, object]] = {}
        for base in self._rolled_schedule:
            dep_time = base["dep_time"]  # type: ignore[index]
            arr_time = base["arr_time"]  # type: ignore[index]
            flight_id = base["flight_id"]  # type: ignore[index]
            schedule = FlightSchedule(
                flight_id=flight_id,
                airport_origin=config.AIRPORT,
                airport_dest=base["dest"],  # type: ignore[index]
                dep_time=dep_time,
                arr_time=arr_time,
                bag_cutoff_time=dep_time - config.DEFAULT_BAG_CUTOFF_OFFSET,
                expected_bags=0,
            )
            flights.append(schedule)
            meta[flight_id] = {
                "terminal": base.get("terminal"),
                "gate": base.get("gate"),
                "airline": base.get("airline"),
                "aircraft_code": base.get("aircraft_code"),
                "route_class": base.get("route_class"),
                "capacity": base.get("capacity"),
            }
        return flights, meta

    def _window_flights(
        self, flights: List[FlightSchedule], meta: Dict[str, Dict[str, object]]
    ) -> Tuple[List[FlightSchedule], Dict[str, Dict[str, object]]]:
        now = datetime.now(timezone.utc)
        start = now - timedelta(minutes=config.ACTIVE_WINDOW_PAST_MINUTES)
        end = now + timedelta(minutes=config.ACTIVE_WINDOW_FUTURE_MINUTES)
        in_window = [(f, meta[f.flight_id]) for f in flights if start <= f.dep_time <= end]
        if not in_window:
            future = [(f, meta[f.flight_id]) for f in flights if f.dep_time >= now]
            future.sort(key=lambda fm: fm[0].dep_time)
            in_window = future[: config.MAX_FLIGHTS_PER_BATCH]
        else:
            in_window.sort(key=lambda fm: fm[0].dep_time)
            in_window = in_window[: config.MAX_FLIGHTS_PER_BATCH]

        filtered_flights = [fm[0] for fm in in_window]
        filtered_meta = {fm[0].flight_id: fm[1] for fm in in_window}
        return filtered_flights, filtered_meta

    def _sample_passenger_count(self, capacity: int, route_class: str) -> int:
        mean, std, minimum, maximum = config.LOAD_FACTOR.get(route_class, config.LOAD_FACTOR["short"])
        load_factor = _truncated_gauss(self.random, mean, std, minimum, maximum)
        passengers = int(round(capacity * load_factor))
        return max(1, min(passengers, config.MAX_PASSENGERS_PER_FLIGHT))

    def _sample_group_size(self) -> int:
        return self.random.choices([1, 2, 3, 4], weights=config.GROUP_SIZE_WEIGHTS, k=1)[0]

    def _sample_bag_count(self, route_class: str) -> int:
        weights = config.BAG_COUNT_WEIGHTS_BY_ROUTE.get(route_class, config.BAG_COUNT_WEIGHTS_BY_ROUTE["short"])
        return self.random.choices([0, 1, 2, 3], weights=weights, k=1)[0]

    def _sample_checkin_offset(self, route_class: str) -> int:
        p50 = config.CHECKIN_P50.get(route_class, config.CHECKIN_P50["short"])
        p90 = config.CHECKIN_P90.get(route_class, config.CHECKIN_P90["short"])
        minutes = _lognormal_minutes(self.random, p50, p90)
        minutes = max(config.CHECKIN_MIN, min(minutes, config.CHECKIN_MAX))
        return int(minutes)

    def _passenger_arrivals(self, flight: FlightSchedule, route_class: str, count: int) -> List[datetime]:
        arrivals: List[datetime] = []
        remaining = count
        while remaining > 0:
            group_size = min(self._sample_group_size(), remaining)
            offset = self._sample_checkin_offset(route_class)
            arrivals.extend([flight.dep_time - timedelta(minutes=offset)] * group_size)
            remaining -= group_size
        return arrivals

    def _generate_assignments_for_flight(
        self, flight: FlightSchedule, meta: Dict[str, object]
    ) -> Tuple[List[BagAssignment], Dict[str, int], Dict[str, datetime]]:
        assignments: List[BagAssignment] = []
        bag_counts: Dict[str, int] = {}
        bag_checkins: Dict[str, datetime] = {}

        route_class = str(meta.get("route_class", "short"))
        capacity = int(meta.get("capacity") or config.PASSENGER_CAPACITY_DEFAULT)
        passengers = self._sample_passenger_count(capacity, route_class)
        arrivals = self._passenger_arrivals(flight, route_class, passengers)
        for arrival_time in arrivals:
            bags = self._sample_bag_count(route_class)
            if bags == 0:
                continue
            itinerary_id = f"ITI{self._itinerary_counter:09d}"
            self._itinerary_counter += 1
            customer_id = f"CUST{self._customer_counter:09d}"
            self._customer_counter += 1
            legs: List[Leg] = [
                Leg(leg_index=0, flight_id=flight.flight_id, origin=flight.airport_origin, dest=flight.airport_dest)
            ]

            for _ in range(bags):
                bag_id = f"BAG{self._bag_counter:09d}"
                self._bag_counter += 1
                assignment = BagAssignment(
                    bag_id=bag_id,
                    itinerary_id=itinerary_id,
                    customer_id=customer_id,
                    bags_in_itinerary=bags,
                    legs=legs,
                )
                assignments.append(assignment)
                bag_checkins[bag_id] = arrival_time
                for leg in legs:
                    bag_counts.setdefault(leg.flight_id, 0)
                    bag_counts[leg.flight_id] += 1

        flight.expected_bags = bag_counts.get(flight.flight_id, 0)
        return assignments, bag_counts, bag_checkins

    def _event_times_for_leg(
        self, schedule: FlightSchedule, check_in_at: datetime, route_class: str
    ) -> Dict[str, datetime]:
        inducted_at = check_in_at + timedelta(
            minutes=_lognormal_minutes(self.random, config.INDUCTED_P50, config.INDUCTED_P90)
        )
        screened_at = inducted_at + timedelta(
            minutes=_lognormal_minutes(self.random, config.SCREENED_P50, config.SCREENED_P90)
        )
        makeup_at = screened_at + timedelta(
            minutes=_lognormal_minutes(self.random, config.MAKEUP_P50, config.MAKEUP_P90)
        )
        loaded_at = max(
            schedule.bag_cutoff_time - timedelta(minutes=config.LOADED_BUFFER_MINUTES),
            makeup_at + timedelta(minutes=2),
        )
        offloaded_at = schedule.arr_time + timedelta(minutes=5)
        carousel_at = offloaded_at + timedelta(minutes=10)
        return {
            "CheckedIn": check_in_at,
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
        flight_meta: Dict[str, Dict[str, object]],
        bag_checkins: Dict[str, datetime],
    ) -> Iterable[BaggageEvent]:
        flight_map = {f.flight_id: f for f in flights}
        for assignment in assignments:
            for leg in assignment.legs:
                schedule = flight_map[leg.flight_id]
                meta = flight_meta.get(leg.flight_id, {})
                route_class = str(meta.get("route_class", "short"))
                check_in = bag_checkins.get(assignment.bag_id, schedule.dep_time - timedelta(minutes=120))
                times = self._event_times_for_leg(schedule, check_in, route_class)
                for event_type, event_time in times.items():
                    ingest_time = self._ingest_time(event_time)
                    yield BaggageEvent(
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
                        attributes=self._attributes(event_type, leg, schedule, meta),
                    )

    def _ingest_time(self, event_time: datetime) -> datetime:
        if self.random.random() < config.OUT_OF_ORDER_RATE:
            delay = self.random.uniform(
                config.OUT_OF_ORDER_MIN_DELAY.total_seconds(),
                config.OUT_OF_ORDER_MAX_DELAY.total_seconds(),
            )
            return event_time + timedelta(seconds=delay)
        return event_time + timedelta(seconds=self.random.uniform(5, 45))

    def _attributes(self, event_type: str, leg: Leg, schedule: FlightSchedule, meta: Dict[str, object]) -> Dict[str, str]:
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
        if meta.get("terminal"):
            attributes["terminal"] = str(meta["terminal"])
        if meta.get("gate"):
            attributes["gate"] = str(meta["gate"])
        if meta.get("aircraft_code"):
            attributes["aircraft"] = str(meta["aircraft_code"])
        return attributes


def events_to_json(events: Iterable[BaggageEvent]) -> str:
    return "\n".join(json.dumps(event.to_json_payload()) for event in events)
