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
from providers.todoist.client import TodoistClient  # noqa: E402
from providers.todoist.normalizer import normalize_todoist  # noqa: E402
from providers.todoist.settings import DEFAULT_SETTINGS  # noqa: E402


def with_all_settings(task: dict[str, Any]) -> dict[str, Any]:
    settings = DEFAULT_SETTINGS.copy()
    task["__todoist_settings"] = settings
    return task


def audit(account: ConnectorAccount, limit: int | None, verbose: bool, force_all: bool) -> dict[str, Any]:
    client = TodoistClient(account)
    tasks = client.fetch_since()
    if limit:
        tasks = tasks[:limit]

    event_counts: dict[str, Counter] = defaultdict(Counter)
    timestamp_sources: Counter = Counter()
    skips: Counter = Counter()

    for task in tasks:
        t = with_all_settings(task) if force_all else task
        events = normalize_todoist(t)
        if not events:
            skips["no_events"] += 1
            continue
        if isinstance(events, dict):
            events = [events]
        for ev in events:
            event_counts[ev["event_type"]][ev["source_entity_type"]] += 1
            if ev.get("source_event_version"):
                timestamp_sources[ev["event_type"]] += 1
        if task.get("completed") and not task.get("completed_at"):
            skips["completed_missing_completed_at"] += 1

    report = {
        "total_tasks": len(tasks),
        "event_counts": {k: dict(v) for k, v in event_counts.items()},
        "timestamp_sources": dict(timestamp_sources),
        "skips": dict(skips),
    }

    if verbose:
        print(f"Total tasks: {report['total_tasks']}")
        print("\nEvent counts:")
        for etype, counter in report["event_counts"].items():
            print(f"  {etype}: " + ", ".join(f"{entity}={count}" for entity, count in counter.items()))
        print("\nSkips:")
        for reason, count in report["skips"].items():
            print(f"  {reason}: {count}")

    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Audit Todoist normalized events (no DB writes).")
    parser.add_argument("--connector-account-id", type=int, required=True)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--output", type=Path, default=None)
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--force-all", action="store_true")
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
