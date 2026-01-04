from collections import defaultdict

import pytest

from producer.generator import MockDataGenerator


@pytest.fixture()
def seeded_data():
    generator = MockDataGenerator(seed=7)
    return generator.generate()


def test_generates_expected_flight_count():
    generator = MockDataGenerator(seed=42)
    flights, assignments, events = generator.generate()
    assert len(flights) == 120
    assert all(f.expected_bags >= 0 for f in flights)
    assert any(f.airport_dest == "AMS" for f in flights)
    # ensure we have assignments and events generated
    assert assignments
    assert events


def test_assignments_have_legs(seeded_data):
    flights, assignments, _events = seeded_data
    assert all(assignment.legs for assignment in assignments)
    # verify expected_bags matches counted assignments
    expected_totals = defaultdict(int)
    for assignment in assignments:
        for leg in assignment.legs:
            expected_totals[leg.flight_id] += 1
    for flight in flights:
        assert flight.expected_bags == expected_totals.get(flight.flight_id, 0)


def test_events_are_sorted_and_complete(seeded_data):
    _flights, assignments, events = seeded_data
    assert events == sorted(events, key=lambda e: e.event_time)
    required_types = {
        "CheckedIn",
        "Inducted",
        "ScreenedCleared",
        "MakeupArrived",
        "LoadedOnAircraft",
        "Offloaded",
        "CarouselArrived",
    }
    events_by_bag = defaultdict(list)
    for event in events:
        events_by_bag[(event.bag_id, event.leg_index)].append(event.event_type)
        assert event.ingest_time >= event.event_time
    for assignment in assignments:
        for leg in assignment.legs:
            key = (assignment.bag_id, leg.leg_index)
            assert required_types.issubset(events_by_bag[key])
