from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path

from .generator import MockDataGenerator
from .kafka_producer import BaggageKafkaPublisher, KafkaProducerClient


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Mock baggage generator")
    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
        help="Kafka bootstrap servers; if omitted, print to stdout",
    )
    parser.add_argument("--seed", type=int, default=None, help="Seed for deterministic output")
    parser.add_argument("--output", type=Path, help="Optional path to write generated events as NDJSON")
    parser.add_argument(
        "--loop-seconds",
        type=int,
        default=int(os.getenv("PRODUCER_LOOP_SECONDS", "0")),
        help="If >0, continuously republish with this pause between batches",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    generator = MockDataGenerator(seed=args.seed)

    while True:
        flights, assignments, events = generator.generate()

        if args.bootstrap_servers:
            client = KafkaProducerClient(args.bootstrap_servers)
            publisher = BaggageKafkaPublisher(client)
            publisher.publish(flights, assignments, events)
            print(
                f"Published {len(flights)} flights, {len(assignments)} assignments, {len(events)} events to Kafka at {args.bootstrap_servers}"
            )
        else:
            print(json.dumps({"flights": len(flights), "assignments": len(assignments), "events": len(events)}))
            if args.output:
                args.output.write_text("\n".join(json.dumps(e.to_json_payload()) for e in events))
                print(f"Wrote events to {args.output}")

        if args.loop_seconds <= 0:
            break

        print(f"Sleeping {args.loop_seconds}s before publishing next batch")
        time.sleep(args.loop_seconds)


if __name__ == "__main__":
    main()
