from __future__ import annotations

from pathlib import Path

from django.apps import AppConfig
from django.conf import settings
from ingestion.providers import ProviderSpec


class HabiticaProviderConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "providers.habitica"
    verbose_name = "Habitica"

    provider_spec: ProviderSpec | None = None

    def ready(self) -> None:
        from providers.habitica.client import HabiticaClient
        from providers.habitica.credentials import validate_credentials
        from providers.habitica.forms import HabiticaConnectForm
        from providers.habitica.normalizer import normalize_habitica
        from providers.habitica.sanitizer import sanitize_raw
        from providers.habitica.status_writer import HabiticaStatusWriter

        template_dir = Path(__file__).resolve().parent / "templates"
        template_dirs = settings.TEMPLATES[0].setdefault("DIRS", [])
        if template_dir not in template_dirs:
            template_dirs.append(template_dir)

        self.provider_spec = ProviderSpec(
            source="habitica",
            label="Habitica",
            client_factory=lambda account: HabiticaClient(account),
            normalizer=normalize_habitica,
            required_fields=[
                ("user_id", "User ID", "HABITICA_USER_ID"),
                ("api_token", "API Token", "HABITICA_API_TOKEN"),
            ],
            auth_type="api_token",
            validate_credentials=validate_credentials,
            form_class=HabiticaConnectForm,
            icon="bi-heart-pulse",
            status_writer_factory=lambda account: HabiticaStatusWriter(account),
            raw_sanitizer=sanitize_raw,
        )
