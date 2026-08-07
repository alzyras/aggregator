from __future__ import annotations

from pathlib import Path

from django.apps import AppConfig
from django.conf import settings
from ingestion.providers import ProviderSpec


class JiraProviderConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "providers.jira"
    verbose_name = "Jira"

    provider_spec: ProviderSpec | None = None

    def ready(self) -> None:
        from providers.jira.client import JiraClient
        from providers.jira.credentials import validate_credentials
        from providers.jira.forms import JiraConnectForm
        from providers.jira.normalizer import normalize_jira
        from providers.jira.planner_badges import planner_badges
        from providers.jira.sanitizer import sanitize_raw
        from providers.jira.settings import (
            apply_credentials,
            jira_form_initial,
            resolve_masked_credentials,
            source_url,
        )
        from providers.jira.status_writer import JiraStatusWriter
        from providers.jira.verify import verify_jira

        template_dir = Path(__file__).resolve().parent / "templates"
        template_dirs = settings.TEMPLATES[0].setdefault("DIRS", [])
        if template_dir not in template_dirs:
            template_dirs.append(template_dir)

        self.provider_spec = ProviderSpec(
            source="jira",
            label="Jira",
            client_factory=lambda account: JiraClient(account),
            normalizer=normalize_jira,
            required_fields=[
                ("base_url", "Base URL", "https://<site>.atlassian.net"),
                ("auth_method", "Auth Method", "cloud_api_token | personal_access_token"),
            ],
            auth_type="api_token",
            validate_credentials=validate_credentials,
            form_class=JiraConnectForm,
            icon="bi-journal-text",
            connection_verifier=verify_jira,
            credentials_applier=apply_credentials,
            form_initial_factory=jira_form_initial,
            masked_credentials_resolver=resolve_masked_credentials,
            form_template="providers/jira/form_fields.html",
            status_writer_factory=lambda account: JiraStatusWriter(account),
            description_writer_factory=lambda account: JiraStatusWriter(account),
            raw_sanitizer=sanitize_raw,
            planner_badge_extractor=planner_badges,
            source_url_extractor=source_url,
        )
