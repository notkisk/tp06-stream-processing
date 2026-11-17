import argparse
import json
import random
import time
from datetime import datetime, timedelta, timezone

from kafka import KafkaProducer

TOPIC = "campus-events"
DEPARTMENTS = ["computer science", "mathematics", "physics"]


def main():
    parser = argparse.ArgumentParser(description="send synthetic campus check-in events to kafka")
    parser.add_argument(
        "--morning",
        action="store_true",
        help="use a fixed 08:14:30 utc base so some check-ins arrive after 08:15",
    )
    args = parser.parse_args()

    # the default base is now, which keeps the exercise realistic.
    # --morning makes the late-check-in alert easy to demonstrate.
    if args.morning:
        now = datetime.now(timezone.utc)
        base = now.replace(hour=8, minute=14, second=30, microsecond=0)
    else:
        base = datetime.now(timezone.utc).replace(microsecond=0)

    events = [
        ("S101", "check-in", "computer science", 2),
        ("S204", "check-in", "mathematics", 8),
        ("S101", "open-course", "computer science", 15),
        ("S305", "check-in", "physics", 22),
        ("S411", "check-in", "computer science", 35),
        ("S204", "check-in", "mathematics", 48),
        ("S512", "check-in", "physics", 12),  # deliberately out of order
    ]

    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        acks="all",
    )

    try:
        for event_id, (student, event_type, dept, seconds) in enumerate(events, 1):
            event_time = base + timedelta(seconds=seconds)
            event = {
                "eventId": event_id,
                "studentId": student,
                "eventType": event_type,
                "department": dept,
                "eventTime": event_time.isoformat(),
            }
            producer.send(TOPIC, value=event)
            print("sent:", json.dumps(event), flush=True)
            time.sleep(1)
        producer.flush()
    finally:
        producer.close()


if __name__ == "__main__":
    main()
