from __future__ import annotations

import os

import psycopg
from psycopg import sql
from django.core.management.base import BaseCommand


class Command(BaseCommand):
    help = "Ensure the target PostgreSQL database exists."

    def handle(self, *args: object, **options: object) -> None:
        target_db = os.getenv("PGDATABASE") or os.getenv("POSTGRES_DB", "aggregator")
        maintenance_db = os.getenv("PGMAINTENANCE_DB", "postgres")
        conn_info = {
            "host": os.getenv("PGHOST") or os.getenv("POSTGRES_HOST", "localhost"),
            "port": os.getenv("PGPORT") or os.getenv("POSTGRES_PORT", "5432"),
            "user": os.getenv("PGUSER") or os.getenv("POSTGRES_USER", "postgres"),
            "password": os.getenv("PGPASSWORD") or os.getenv("POSTGRES_PASSWORD", ""),
            "dbname": maintenance_db,
        }

        self.stdout.write(
            f"Ensuring database '{target_db}' exists (maintenance DB: '{maintenance_db}')."
        )

        with psycopg.connect(**conn_info) as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (target_db,))
                exists = cur.fetchone() is not None
                if exists:
                    self.stdout.write(self.style.SUCCESS("Database already exists."))
                    return
                cur.execute(
                    sql.SQL("CREATE DATABASE {};").format(sql.Identifier(target_db))
                )
                self.stdout.write(self.style.SUCCESS("Database created."))
