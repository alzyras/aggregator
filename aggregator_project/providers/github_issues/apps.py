from __future__ import annotations

from django.apps import AppConfig

from ingestion.providers import ProviderSpec


class GitHubIssuesProviderConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "providers.github_issues"
    verbose_name = "GitHub Issues"

    provider_spec: ProviderSpec | None = None

    def ready(self) -> None:
        from providers.github_issues.client import GitHubIssuesClient
        from providers.github_issues.credentials import validate_credentials
        from providers.github_issues.forms import GitHubIssuesConnectForm
        from providers.github_issues.normalizer import normalize_github_issue
        from providers.github_issues.planner_badges import planner_badges
        from providers.github_issues.sanitizer import sanitize_raw
        from providers.github_issues.settings import (
            apply_credentials,
            github_form_initial,
            resolve_masked_credentials,
            source_url,
        )
        from providers.github_issues.status_writer import GitHubIssuesStatusWriter
        from providers.github_issues.verify import verify_github

        self.provider_spec = ProviderSpec(
            source="github",
            label="GitHub Issues",
            client_factory=lambda account: GitHubIssuesClient(account),
            normalizer=normalize_github_issue,
            required_fields=[
                ("api_token", "Personal access token", "GITHUB_ISSUES_TOKEN"),
            ],
            auth_type="api_token",
            validate_credentials=validate_credentials,
            form_class=GitHubIssuesConnectForm,
            icon="bi-github",
            connection_verifier=verify_github,
            credentials_applier=apply_credentials,
            form_initial_factory=github_form_initial,
            masked_credentials_resolver=resolve_masked_credentials,
            form_template="providers/github_issues/form_fields.html",
            status_writer_factory=lambda account: GitHubIssuesStatusWriter(account),
            description_writer_factory=lambda account: GitHubIssuesStatusWriter(
                account
            ),
            raw_sanitizer=sanitize_raw,
            planner_badge_extractor=planner_badges,
            source_url_extractor=source_url,
        )
