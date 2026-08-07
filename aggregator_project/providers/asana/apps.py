from __future__ import annotations

from pathlib import Path

from django.apps import AppConfig
from django.conf import settings
from ingestion.providers import ProviderSpec


class AsanaProviderConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "providers.asana"
    verbose_name = "Asana"

    provider_spec: ProviderSpec | None = None

    def ready(self) -> None:
        from providers.asana.client import AsanaClient
        from providers.asana.credentials import validate_credentials
        from providers.asana.forms import AsanaConnectForm
        from providers.asana.normalizer import normalize_asana
        from providers.asana.planner_badges import planner_badges
        from providers.asana.sanitizer import sanitize_raw
        from providers.asana.settings import (
            apply_credentials,
            asana_form_initial,
            resolve_masked_credentials,
            source_url,
        )
        from providers.asana.status_writer import AsanaStatusWriter
        from providers.asana.verify import verify_asana

        template_dir = Path(__file__).resolve().parent / "templates"
        template_dirs = settings.TEMPLATES[0].setdefault("DIRS", [])
        if template_dir not in template_dirs:
            template_dirs.append(template_dir)

        self.provider_spec = ProviderSpec(
            source="asana",
            label="Asana",
            client_factory=lambda account: AsanaClient(account),
            normalizer=normalize_asana,
            required_fields=[
                ("access_token", "Access Token", "ASANA_ACCESS_TOKEN / ASANA_PERSONAL_ACCESS_TOKEN"),
                ("workspace_gid", "Workspace GID", "ASANA_WORKSPACE_GID"),
            ],
            auth_type="api_token",
            validate_credentials=validate_credentials,
            form_class=AsanaConnectForm,
            icon="bi-kanban",
            connection_verifier=verify_asana,
            credentials_applier=apply_credentials,
            form_initial_factory=asana_form_initial,
            masked_credentials_resolver=resolve_masked_credentials,
            form_template="providers/asana/form_fields.html",
            status_writer_factory=lambda account: AsanaStatusWriter(account),
            description_writer_factory=lambda account: AsanaStatusWriter(account),
            raw_sanitizer=sanitize_raw,
            planner_badge_extractor=planner_badges,
            source_url_extractor=source_url,
        )
