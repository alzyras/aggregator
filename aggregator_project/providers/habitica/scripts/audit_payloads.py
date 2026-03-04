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


def presence_stats(tasks: list[dict[str, Any]], fields: list[str]) -> dict[str, Any]:
    stats: dict[str, Any] = {}
    total = len(tasks)
    for field in fields:
        count = sum(1 for t in tasks if field in t and t.get(field) not in (None, ""))
        stats[field] = {"count": count, "pct": round((count / total) * 100, 1) if total else 0}
    return stats


def history_stats(tasks: list[dict[str, Any]]) -> dict[str, Any]:
    total = len(tasks)
    with_history = [t for t in tasks if isinstance(t.get("history"), list)]
    entries = sum(len(t.get("history") or []) for t in with_history)
    keys_counter = Counter()
    for t in with_history:
        for entry in t.get("history") or []:
            keys_counter.update(entry.keys())
    return {
        "has_history": {"count": len(with_history), "pct": round((len(with_history) / total) * 100, 1) if total else 0},
        "total_entries": entries,
        "avg_entries": round(entries / len(with_history), 2) if with_history else 0,
        "entry_keys": dict(keys_counter.most_common()),
    }


def audit(account: ConnectorAccount, limit: int | None, verbose: bool) -> dict[str, Any]:
    client = HabiticaClient(account)
    raw = client.fetch_since()
    if limit:
        raw = raw[:limit]

    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for task in raw:
        grouped[(task.get("type") or "unknown")].append(task)

    report: dict[str, Any] = {"total": len(raw), "by_type": {}}
    key_fields = [
        "id",
        "_id",
        "text",
        "notes",
        "value",
        "priority",
        "completed",
        "dateCreated",
        "dateCompleted",
        "updatedAt",
    ]

    for task_type, items in grouped.items():
        stats = {
            "count": len(items),
            "presence": presence_stats(items, key_fields),
            "history": history_stats(items),
        }
        report["by_type"][task_type] = stats

    if verbose:
        for task_type, stats in report["by_type"].items():
            print(f"\n=== {task_type} ({stats['count']} items) ===")
            print("Field presence:")
            for field, meta in stats["presence"].items():
                print(f"  {field:15} {meta['count']:4}/{stats['count']:4} ({meta['pct']:5.1f}%)")
            h = stats["history"]
            print(f"History: has={h['has_history']['count']} ({h['has_history']['pct']}%), avg_entries={h['avg_entries']}")
            if h["entry_keys"]:
                print("  Entry keys:", ", ".join(f"{k}:{v}" for k, v in h["entry_keys"].items()))
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Audit Habitica payload field availability.")
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
