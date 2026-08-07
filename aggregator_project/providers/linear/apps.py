from __future__ import annotations

from django.apps import AppConfig

from ingestion.providers import ProviderSpec


class LinearProviderConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "providers.linear"
    verbose_name = "Linear"

    provider_spec: ProviderSpec | None = None

    def ready(self) -> None:
        from providers.linear.client import LinearClient
        from providers.linear.credentials import validate_credentials
        from providers.linear.forms import LinearConnectForm
        from providers.linear.normalizer import normalize_linear_issue
        from providers.linear.planner_badges import planner_badges
        from providers.linear.sanitizer import sanitize_raw
        from providers.linear.settings import (
            apply_credentials,
            linear_form_initial,
            resolve_masked_credentials,
            source_url,
        )
        from providers.linear.status_writer import LinearStatusWriter
        from providers.linear.verify import verify_linear

        self.provider_spec = ProviderSpec(
            source="linear",
            label="Linear",
            client_factory=lambda account: LinearClient(account),
            normalizer=normalize_linear_issue,
            required_fields=[("api_key", "Personal API key", "LINEAR_API_KEY")],
            auth_type="api_token",
            validate_credentials=validate_credentials,
            form_class=LinearConnectForm,
            icon="bi-list-task",
            connection_verifier=verify_linear,
            credentials_applier=apply_credentials,
            form_initial_factory=linear_form_initial,
            masked_credentials_resolver=resolve_masked_credentials,
            form_template="providers/linear/form_fields.html",
            status_writer_factory=lambda account: LinearStatusWriter(account),
            description_writer_factory=lambda account: LinearStatusWriter(account),
            raw_sanitizer=sanitize_raw,
            planner_badge_extractor=planner_badges,
            source_url_extractor=source_url,
        )
