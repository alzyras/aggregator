from __future__ import annotations

from pathlib import Path

from django.apps import AppConfig
from django.conf import settings
from ingestion.providers import ProviderSpec


class TodoistProviderConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "providers.todoist"
    verbose_name = "Todoist"

    provider_spec: ProviderSpec | None = None

    def ready(self) -> None:
        from providers.todoist.client import TodoistClient
        from providers.todoist.credentials import validate_credentials
        from providers.todoist.forms import TodoistConnectForm
        from providers.todoist.normalizer import normalize_todoist
        from providers.todoist.sanitizer import sanitize_raw
        from providers.todoist.status_writer import TodoistStatusWriter

        template_dir = Path(__file__).resolve().parent / "templates"
        template_dirs = settings.TEMPLATES[0].setdefault("DIRS", [])
        if template_dir not in template_dirs:
            template_dirs.append(template_dir)

        self.provider_spec = ProviderSpec(
            source="todoist",
            label="Todoist",
            client_factory=lambda account: TodoistClient(account),
            normalizer=normalize_todoist,
            required_fields=[
                ("api_token", "API Token", "TODOIST_API_TOKEN"),
            ],
            auth_type="api_token",
            validate_credentials=validate_credentials,
            form_class=TodoistConnectForm,
            icon="bi-check2-square",
            status_writer_factory=lambda account: TodoistStatusWriter(account),
            raw_sanitizer=sanitize_raw,
        )
