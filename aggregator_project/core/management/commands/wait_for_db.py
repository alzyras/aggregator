from __future__ import annotations

import time

from django.core.management.base import BaseCommand, CommandError
from django.db import connections
from django.db.utils import OperationalError


class Command(BaseCommand):
    help = "Wait for the default database connection to become available."

    def add_arguments(self, parser) -> None:
        parser.add_argument("--timeout", type=int, default=120)
        parser.add_argument("--interval", type=int, default=2)

    def handle(self, *args: object, **options: object) -> None:
        timeout = int(options["timeout"])
        interval = int(options["interval"])
        deadline = time.monotonic() + timeout

        self.stdout.write("Waiting for database connection...")
        last_error: Exception | None = None

        while time.monotonic() < deadline:
            try:
                connection = connections["default"]
                connection.ensure_connection()
                with connection.cursor() as cursor:
                    cursor.execute("SELECT 1")
                self.stdout.write(self.style.SUCCESS("Database is available."))
                return
            except OperationalError as exc:
                last_error = exc
                time.sleep(interval)

        message = "Database is not available before timeout."
        if last_error:
            message = f"{message} Last error: {last_error}"
        raise CommandError(message)
