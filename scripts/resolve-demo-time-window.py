#!/usr/bin/env python3
import os
import shlex
import sys
from datetime import datetime, timedelta, timezone


def parse_instant(value: str) -> datetime:
    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def format_instant(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def emit(name: str, value: str) -> None:
    print(f"{name}={shlex.quote(value)}")


def main() -> int:
    duration_seconds = int(os.environ["DEMO_DURATION_SECONDS"])
    base_time = os.environ.get("BASE_TIME", "").strip()
    event_start_time = os.environ.get("EVENT_START_TIME", "").strip()

    if base_time and event_start_time:
        print("Set only one of BASE_TIME or EVENT_START_TIME.", file=sys.stderr)
        return 2

    if base_time:
        event_end = parse_instant(base_time)
        event_start = event_end - timedelta(seconds=duration_seconds)
    elif event_start_time:
        event_start = parse_instant(event_start_time)
        event_end = event_start + timedelta(seconds=duration_seconds)
    else:
        event_start = datetime.now(timezone.utc).replace(microsecond=0)
        event_end = event_start + timedelta(seconds=duration_seconds)

    default_dashboard_from = event_start
    default_dashboard_to = event_end
    first_cut = event_start + timedelta(seconds=duration_seconds / 3)
    second_cut = event_start + timedelta(seconds=(duration_seconds * 2) / 3)
    default_first_from = event_start
    default_first_to = first_cut
    default_middle_from = default_first_to
    default_middle_to = second_cut
    default_last_from = default_middle_to
    default_last_to = event_end

    values = {
        "EVENT_START_TIME": format_instant(event_start),
        "BASE_TIME": format_instant(event_end),
        "DASHBOARD_TIME_FROM": os.environ.get("DASHBOARD_TIME_FROM") or format_instant(default_dashboard_from),
        "DASHBOARD_TIME_TO": os.environ.get("DASHBOARD_TIME_TO") or format_instant(default_dashboard_to),
        "FIRST_FROM": os.environ.get("FIRST_FROM") or format_instant(default_first_from),
        "FIRST_TO": os.environ.get("FIRST_TO") or format_instant(default_first_to),
        "MIDDLE_FROM": os.environ.get("MIDDLE_FROM") or format_instant(default_middle_from),
        "MIDDLE_TO": os.environ.get("MIDDLE_TO") or format_instant(default_middle_to),
        "LAST_FROM": os.environ.get("LAST_FROM") or format_instant(default_last_from),
        "LAST_TO": os.environ.get("LAST_TO") or format_instant(default_last_to),
    }

    for name, value in values.items():
        emit(name, value)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
