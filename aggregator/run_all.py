"""Deprecated wrapper kept for compatibility; prefer `python manage.py run`."""

from aggregator.core.management import execute_from_command_line


def main() -> None:
    execute_from_command_line("run", [])


if __name__ == "__main__":
    main()
