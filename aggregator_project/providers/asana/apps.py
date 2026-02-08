from __future__ import annotations

from django.apps import AppConfig

from ingestion.providers import ProviderSpec


class AsanaProviderConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "providers.asana"
    verbose_name = "Asana"

    provider_spec: ProviderSpec | None = None

    def ready(self) -> None:
        from providers.asana.client import AsanaClient
        from providers.asana.credentials import validate_credentials
        from providers.asana.normalizer import normalize_asana
        from connectors.forms import AsanaConnectForm

        self.provider_spec = ProviderSpec(
            source="asana",
            label="Asana",
            client_factory=lambda workspace, account=None: AsanaClient(workspace, account=account),
            normalizer=normalize_asana,
            required_fields=[
                ("access_token", "Access Token", "ASANA_ACCESS_TOKEN / ASANA_PERSONAL_ACCESS_TOKEN"),
            ],
            auth_type="api_token",
            validate_credentials=validate_credentials,
            form_class=AsanaConnectForm,
            icon="bi-kanban",
        )
