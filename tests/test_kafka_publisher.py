import json

from producer.generator import MockDataGenerator
from producer.kafka_producer import BaggageKafkaPublisher, InMemoryProducer


def test_in_memory_producer_collects_messages():
    flights, assignments, events = MockDataGenerator(seed=10).generate()
    client = InMemoryProducer()
    publisher = BaggageKafkaPublisher(client)
    publisher.publish(flights, assignments, events)

    topics = [t for t, _k, _v in client.messages]
    assert topics.count("flight.schedule.v1") == len(flights)
    assert topics.count("baggage.assignment.v1") == len(assignments)
    assert topics.count("baggage.events.v1") == len(events)

    # Ensure payload can be parsed as JSON
    for _topic, _key, value in client.messages[:5]:
        json.loads(value)
