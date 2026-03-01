from __future__ import annotations

import argparse
import json
import os
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

import django
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parents[3]
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "aggregator_project.settings")
load_dotenv(BASE_DIR / ".env")
django.setup()

from connectors.models import ConnectorAccount  # noqa: E402
from providers.habitica.client import HabiticaClient  # noqa: E402
from providers.habitica.settings import DEFAULT_SETTINGS  # noqa: E402
from providers.habitica.normalizer import normalize_habitica  # noqa: E402


def force_settings(task: dict[str, Any]) -> dict[str, Any]:
    settings = DEFAULT_SETTINGS.copy()
    settings.update(
        {
            "emit_history_occurrences": True,
            "emit_completion_occurrences": True,
            "task_state_created": True,
            "task_state_updated": True,
            "task_state_completed": True,
            "sync_habits": True,
            "sync_dailies": True,
            "sync_todos": True,
        }
    )
    task["_habitica_settings"] = settings
    return task


def audit(account: ConnectorAccount, limit: int | None, verbose: bool, force_all: bool) -> dict[str, Any]:
    client = HabiticaClient(account)
    raw = client.fetch_since()
    if limit:
        raw = raw[:limit]

    event_counts: dict[str, Counter] = defaultdict(Counter)
    timestamp_sources: Counter = Counter()
    skips: Counter = Counter()

    for task in raw:
        if force_all:
            task = force_settings(task)
        events = normalize_habitica(task)
        if not events:
            skips["no_events"] += 1
            continue
        for ev in events:
            event_counts[ev["source_entity_type"]][ev["event_type"]] += 1
            if ev.get("source_event_version"):
                timestamp_sources[ev["event_type"] + ":" + ev["source_entity_type"]] += 1
        missing_history = task.get("type") == "habit" and not task.get("history")
        if missing_history:
            skips["habit_missing_history"] += 1
        if task.get("type") in {"daily", "todo"} and not task.get("dateCompleted"):
            skips[f"{task.get('type')}_missing_dateCompleted"] += 1

    report = {
        "total_tasks": len(raw),
        "event_counts": {k: dict(v) for k, v in event_counts.items()},
        "timestamp_sources": dict(timestamp_sources),
        "skips": dict(skips),
    }

    if verbose:
        print(f"Total tasks: {report['total_tasks']}")
        print("\nEvent counts by entity type:")
        for entity, counter in report["event_counts"].items():
            print(f"  {entity}: " + ", ".join(f"{etype}={count}" for etype, count in counter.items()))
        print("\nSkip reasons:")
        for reason, count in report["skips"].items():
            print(f"  {reason}: {count}")

    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Audit Habitica normalized events (no DB writes).")
    parser.add_argument("--connector-account-id", type=int, required=True)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--output", type=Path, default=None)
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--force-all", action="store_true", help="Force-enable all options when normalizing")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    account = ConnectorAccount.objects.filter(id=args.connector_account_id).first()
    if not account:
        raise SystemExit("ConnectorAccount not found.")
    report = audit(account, args.limit, args.verbose, args.force_all)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2))
        print(f"\nSaved JSON report to {args.output}")


if __name__ == "__main__":
    main()
