from __future__ import annotations

from django.apps import AppConfig

from ingestion.providers import ProviderSpec


class GoogleFitProviderConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "providers.google_fit"
    verbose_name = "Google Fit"

    provider_spec: ProviderSpec | None = None

    def ready(self) -> None:
        from providers.google_fit.client import GoogleFitClient
        from providers.google_fit.credentials import validate_credentials
        from providers.google_fit.normalizer import normalize_google_fit
        from providers.google_fit.sanitizer import sanitize_raw
        from connectors.forms import GoogleFitConnectForm

        self.provider_spec = ProviderSpec(
            source="google_fit",
            label="Google Fit",
            client_factory=lambda account: GoogleFitClient(account),
            normalizer=normalize_google_fit,
            required_fields=[
                ("client_id", "Client ID", "GOOGLE_FIT_CLIENT_ID"),
                ("client_secret", "Client Secret", "GOOGLE_FIT_CLIENT_SECRET"),
                ("refresh_token", "Refresh Token", "GOOGLE_FIT_REFRESH_TOKEN"),
                ("access_token", "Access Token (optional)", "GOOGLE_FIT_ACCESS_TOKEN"),
            ],
            auth_type="oauth",
            validate_credentials=validate_credentials,
            form_class=GoogleFitConnectForm,
            icon="bi-activity",
            raw_sanitizer=sanitize_raw,
        )
