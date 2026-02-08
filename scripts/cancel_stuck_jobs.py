from __future__ import annotations

import os
import sys
from pathlib import Path
from datetime import timedelta


def main() -> int:
    repo_root = Path(__file__).resolve().parents[1]
    project_root = repo_root / "aggregator_project"
    sys.path.insert(0, str(repo_root))
    sys.path.insert(0, str(project_root))
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "aggregator_project.settings")
    try:
        import django
    except ImportError:
        print("Django is not installed in the current environment.")
        return 1

    django.setup()
    from django.utils import timezone
    from ingestion.models import Job

    minutes = int(sys.argv[1]) if len(sys.argv) > 1 else 15
    cutoff = timezone.now() - timedelta(minutes=minutes)

    updated = Job.objects.filter(
        status=Job.STATUS_RUNNING,
        started_at__lt=cutoff,
    ).update(
        status=Job.STATUS_CANCELLED,
        finished_at=timezone.now(),
        error_message=f"Cancelled manually after {minutes} minutes stuck.",
        locked_at=None,
        locked_by=None,
    )
    print(f"Cancelled {updated} stuck jobs (>{minutes} minutes).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
