#!/usr/bin/env python
import os
import sys
from pathlib import Path


def main() -> None:
    repo_root = Path(__file__).resolve().parent
    django_root = repo_root / "aggregator_project"
    sys.path.insert(0, str(repo_root))
    sys.path.insert(0, str(django_root))

    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "aggregator_project.settings")
    try:
        from django.core.management import execute_from_command_line
    except ImportError as exc:
        raise ImportError(
            "Couldn't import Django. Are you sure it's installed and "
            "available on your PYTHONPATH environment variable? Did you "
            "forget to activate a virtual environment?"
        ) from exc
    execute_from_command_line(sys.argv)


if __name__ == "__main__":
    main()
