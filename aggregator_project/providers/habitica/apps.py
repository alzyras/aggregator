from __future__ import annotations

from django.apps import AppConfig

from ingestion.providers import ProviderSpec


class HabiticaProviderConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "providers.habitica"
    verbose_name = "Habitica"

    provider_spec: ProviderSpec | None = None

    def ready(self) -> None:
        from providers.habitica.client import HabiticaClient
        from providers.habitica.credentials import validate_credentials
        from providers.habitica.normalizer import normalize_habitica
        from connectors.forms import HabiticaConnectForm

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
        )
