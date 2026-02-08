from __future__ import annotations

from django.apps import AppConfig

from ingestion.providers import ProviderSpec


class TodoistProviderConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "providers.todoist"
    verbose_name = "Todoist"

    provider_spec: ProviderSpec | None = None

    def ready(self) -> None:
        from providers.todoist.client import TodoistClient
        from providers.todoist.credentials import validate_credentials
        from providers.todoist.normalizer import normalize_todoist
        from connectors.forms import TodoistConnectForm

        self.provider_spec = ProviderSpec(
            source="todoist",
            label="Todoist",
            client_factory=lambda workspace, account=None: TodoistClient(workspace, account=account),
            normalizer=normalize_todoist,
            required_fields=[
                ("api_token", "API Token", "TODOIST_API_TOKEN"),
            ],
            auth_type="api_token",
            validate_credentials=validate_credentials,
            form_class=TodoistConnectForm,
            icon="bi-check2-square",
        )
