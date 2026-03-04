from __future__ import annotations

import argparse
import os
from datetime import timedelta
from pathlib import Path

import sys
import django
from django.db.models import Q
from django.utils import timezone
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parents[2]
sys.path.append(str(BASE_DIR))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "aggregator_project.settings")
load_dotenv(BASE_DIR / ".env")
django.setup()

from ingestion.models import Job  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Cancel or delete stuck jobs.")
    parser.add_argument(
        "--status",
        nargs="+",
        default=[Job.STATUS_QUEUED, Job.STATUS_RUNNING],
        help="Statuses to target (default: queued running)",
    )
    parser.add_argument(
        "--job-type",
        default="sync",
        help="Job type to target (default: sync)",
    )
    parser.add_argument(
        "--connector-account-id",
        type=str,
        help="Limit to a specific connector_account_id",
    )
    parser.add_argument(
        "--older-than-minutes",
        type=int,
        default=5,
        help="Only cancel jobs older than this many minutes (default: 5)",
    )
    parser.add_argument(
        "--delete",
        action="store_true",
        help="Hard delete instead of marking cancelled",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would change without modifying jobs",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    cutoff = timezone.now() - timedelta(minutes=args.older_than_minutes)

    qs = Job.objects.filter(job_type=args.job_type, status__in=args.status)
    if args.connector_account_id:
        qs = qs.filter(connector_account_id=args.connector_account_id)
    qs = qs.filter(Q(queued_at__lte=cutoff) | Q(started_at__lte=cutoff))

    ids = list(qs.values_list("id", flat=True))
    print(f"Found {len(ids)} job(s) matching criteria.")
    if not ids:
        return

    if args.dry_run:
        print("Dry run; no changes made.")
        return

    if args.delete:
        deleted, _ = qs.delete()
        print(f"Deleted {deleted} job(s).")
    else:
        updated = qs.update(status=Job.STATUS_CANCELLED, finished_at=timezone.now())
        print(f"Marked {updated} job(s) as cancelled.")


if __name__ == "__main__":
    main()
