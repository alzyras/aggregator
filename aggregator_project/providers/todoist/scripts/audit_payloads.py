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


KEY_FIELDS = [
    "id",
    "content",
    "description",
    "priority",
    "labels",
    "completed",
    "checked",
    "added_at",
    "completed_at",
    "due",
    "url",
    "section_id",
    "project_id",
    "parent_id",
]


def presence_stats(tasks: list[dict[str, Any]], fields: list[str]) -> dict[str, Any]:
    total = len(tasks)
    stats: dict[str, Any] = {}
    for field in fields:
        count = sum(1 for t in tasks if field in t and t.get(field) not in (None, ""))
        stats[field] = {
            "count": count,
            "pct": round((count / total) * 100, 1) if total else 0,
        }
    return stats


def audit(account: ConnectorAccount, limit: int | None, verbose: bool) -> dict[str, Any]:
    client = TodoistClient(account)
    tasks = client.fetch_since()
    if limit:
        tasks = tasks[:limit]

    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for task in tasks:
        grouped[task.get("resource_type", "task")].append(task)

    report: dict[str, Any] = {"total": len(tasks), "by_type": {}}
    for task_type, items in grouped.items():
        report["by_type"][task_type] = {
            "count": len(items),
            "presence": presence_stats(items, KEY_FIELDS),
            "labels_usage": Counter(
                label
                for t in items
                for label in (t.get("labels") or [])
            ),
        }

    if verbose:
        for task_type, stats in report["by_type"].items():
            print(f"\n=== {task_type} ({stats['count']} items) ===")
            for field, meta in stats["presence"].items():
                print(f"  {field:15} {meta['count']:4}/{stats['count']:4} ({meta['pct']:5.1f}%)")
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Audit Todoist payload field availability.")
    parser.add_argument("--connector-account-id", type=int, required=True)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--output", type=Path, default=None)
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    account = ConnectorAccount.objects.filter(id=args.connector_account_id).first()
    if not account:
        raise SystemExit("ConnectorAccount not found.")
    report = audit(account, args.limit, args.verbose)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2))
        print(f"\nSaved JSON report to {args.output}")


if __name__ == "__main__":
    main()
