#!/usr/bin/env python3
import argparse
import time
from pathlib import Path


WINDOW_PATTERNS = [
    (8, 1, 1, 0),
    (12, 1, 2, 1),
    (16, 2, 3, 2),
    (34, 3, 5, 4),
    (7, 1, 1, 0),
    (14, 2, 2, 2),
    (18, 2, 4, 2),
    (38, 4, 6, 5),
    (8, 1, 1, 0),
    (15, 2, 3, 1),
    (20, 2, 4, 3),
    (24, 3, 5, 4),
]


def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate continuous five-minute UserBehavior demo windows.")
    parser.add_argument(
        "--output",
        default="/tmp/UserBehavior_realtime_demo.csv",
        help="Output CSV path.")
    return parser.parse_args()


def build_rows():
    current_window = int(time.time()) // 300 * 300
    first_window = current_window - (len(WINDOW_PATTERNS) - 1) * 300
    rows = []

    for window_no, counts in enumerate(WINDOW_PATTERNS):
        window_start = first_window + window_no * 300
        behaviors = (
            ["pv"] * counts[0]
            + ["fav"] * counts[1]
            + ["cart"] * counts[2]
            + ["buy"] * counts[3]
        )

        for event_no, behavior in enumerate(behaviors):
            user_id = 900000 + window_no * 1000 + event_no
            item_id = 800000 + (window_no % 4) * 100 + event_no % 8
            category_id = 7000 + window_no % 5
            event_time = window_start + min(event_no * 4, 240)
            rows.append(
                f"{user_id},{item_id},{category_id},{behavior},{event_time}")

    return rows


def main():
    args = parse_args()
    output = Path(args.output)
    rows = build_rows()
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text("\n".join(rows) + "\n", encoding="utf-8")
    print(f"[SUCCESS] Generated {len(rows)} events in "
          f"{len(WINDOW_PATTERNS)} continuous windows: {output}")


if __name__ == "__main__":
    main()
