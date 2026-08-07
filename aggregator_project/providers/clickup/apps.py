from __future__ import annotations

from django.apps import AppConfig

from ingestion.providers import ProviderSpec


class ClickUpProviderConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "providers.clickup"
    verbose_name = "ClickUp"

    provider_spec: ProviderSpec | None = None

    def ready(self) -> None:
        from providers.clickup.client import ClickUpClient
        from providers.clickup.credentials import validate_credentials
        from providers.clickup.forms import ClickUpConnectForm
        from providers.clickup.normalizer import normalize_clickup_task
        from providers.clickup.planner_badges import planner_badges
        from providers.clickup.sanitizer import sanitize_raw
        from providers.clickup.settings import (
            apply_credentials,
            clickup_form_initial,
            resolve_masked_credentials,
            source_url,
        )
        from providers.clickup.status_writer import ClickUpStatusWriter
        from providers.clickup.verify import verify_clickup

        self.provider_spec = ProviderSpec(
            source="clickup",
            label="ClickUp",
            client_factory=lambda account: ClickUpClient(account),
            normalizer=normalize_clickup_task,
            required_fields=[("api_token", "Personal API token", "CLICKUP_API_TOKEN")],
            auth_type="api_token",
            validate_credentials=validate_credentials,
            form_class=ClickUpConnectForm,
            icon="bi-check2-square",
            connection_verifier=verify_clickup,
            credentials_applier=apply_credentials,
            form_initial_factory=clickup_form_initial,
            masked_credentials_resolver=resolve_masked_credentials,
            status_writer_factory=lambda account: ClickUpStatusWriter(account),
            description_writer_factory=lambda account: ClickUpStatusWriter(account),
            raw_sanitizer=sanitize_raw,
            planner_badge_extractor=planner_badges,
            source_url_extractor=source_url,
        )
