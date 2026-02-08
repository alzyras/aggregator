from __future__ import annotations

from django.apps import AppConfig


class IngestionConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "ingestion"

    def ready(self) -> None:
        import os
        from ingestion.services.worker import start_worker_thread

        if os.environ.get("RUN_MAIN") == "true":
            start_worker_thread()
