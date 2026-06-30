#!/usr/bin/env python3
"""Continuously replay MovieLens ratings as accelerated Kafka JSON events."""

import argparse
import csv
import json
import subprocess
import sys
import time
from pathlib import Path


def rows_forever(csv_path):
    while True:
        with csv_path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                yield row


def start_producer(producer_cmd, connection_option, bootstrap_servers, topic):
    return subprocess.Popen(
        [
            producer_cmd,
            connection_option,
            bootstrap_servers,
            "--topic",
            topic,
        ],
        stdin=subprocess.PIPE,
        universal_newlines=True,
        encoding="utf-8",
    )


def main():
    parser = argparse.ArgumentParser(
        description="Continuously replay ratings.csv to Kafka with accelerated event windows."
    )
    parser.add_argument("--csv", default="dataset/ratings.csv")
    parser.add_argument("--producer", default="kafka-console-producer.sh")
    parser.add_argument("--producer-connection-option", default="--bootstrap-server")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", default="ratings_personal_realtime")
    parser.add_argument("--events-per-window", type=int, default=80)
    parser.add_argument("--sleep-seconds", type=float, default=4.0)
    parser.add_argument("--window-seconds", type=int, default=300)
    parser.add_argument("--start-timestamp", type=int, default=0)
    args = parser.parse_args()

    csv_path = Path(args.csv)
    if not csv_path.is_file():
        print(f"[ERROR] CSV not found: {csv_path}", file=sys.stderr)
        return 1

    if args.events_per_window <= 0:
        print("[ERROR] --events-per-window must be positive", file=sys.stderr)
        return 1

    base_timestamp = args.start_timestamp
    if base_timestamp <= 0:
        base_timestamp = int(time.time() // args.window_seconds * args.window_seconds)

    producer = start_producer(
        args.producer, args.producer_connection_option, args.bootstrap_servers, args.topic
    )
    source = rows_forever(csv_path)
    window_index = 0

    print(
        "[INFO] Continuous Kafka replay started: "
        f"csv={csv_path}, topic={args.topic}, "
        f"events_per_window={args.events_per_window}, "
        f"sleep={args.sleep_seconds}s, event_window={args.window_seconds}s",
        flush=True,
    )

    try:
        while True:
            event_timestamp = base_timestamp + window_index * args.window_seconds
            # Make visible chart changes instead of producing identical windows.
            event_count = args.events_per_window + (window_index % 5) * 20
            rating_shift = (window_index % 4) * 0.1

            for event_index in range(event_count):
                row = next(source)
                rating = max(0.5, min(5.0, float(row["rating"]) + rating_shift))
                message = {
                    "userId": int(row["userId"]),
                    "movieId": int(row["movieId"]),
                    "rating": round(rating, 1),
                    "timestamp": event_timestamp + (event_index % args.window_seconds),
                }
                producer.stdin.write(json.dumps(message, separators=(",", ":")) + "\n")

            producer.stdin.flush()
            print(
                f"[INFO] window={window_index} timestamp={event_timestamp} "
                f"events={event_count}",
                flush=True,
            )
            window_index += 1
            time.sleep(args.sleep_seconds)
    except KeyboardInterrupt:
        print("[INFO] Stopping continuous Kafka replay", flush=True)
    finally:
        if producer.stdin:
            try:
                producer.stdin.close()
            except BrokenPipeError:
                pass
        return_code = producer.wait(timeout=30)
        if return_code != 0:
            print(f"[ERROR] Kafka producer exited with {return_code}", file=sys.stderr)
            return return_code

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
