from __future__ import annotations

from django.apps import AppConfig

from ingestion.providers import ProviderSpec


class TrelloProviderConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "providers.trello"
    verbose_name = "Trello"

    provider_spec: ProviderSpec | None = None

    def ready(self) -> None:
        from providers.trello.client import TrelloClient
        from providers.trello.credentials import validate_credentials
        from providers.trello.forms import TrelloConnectForm
        from providers.trello.normalizer import normalize_trello_card
        from providers.trello.planner_badges import planner_badges
        from providers.trello.sanitizer import sanitize_raw
        from providers.trello.settings import (
            apply_credentials,
            resolve_masked_credentials,
            source_url,
            trello_form_initial,
        )
        from providers.trello.status_writer import TrelloStatusWriter
        from providers.trello.verify import verify_trello

        self.provider_spec = ProviderSpec(
            source="trello",
            label="Trello",
            client_factory=lambda account: TrelloClient(account),
            normalizer=normalize_trello_card,
            required_fields=[
                ("api_key", "API key", "TRELLO_API_KEY"),
                ("api_token", "API token", "TRELLO_API_TOKEN"),
            ],
            auth_type="api_token",
            validate_credentials=validate_credentials,
            form_class=TrelloConnectForm,
            icon="bi-trello",
            connection_verifier=verify_trello,
            credentials_applier=apply_credentials,
            form_initial_factory=trello_form_initial,
            masked_credentials_resolver=resolve_masked_credentials,
            status_writer_factory=lambda account: TrelloStatusWriter(account),
            description_writer_factory=lambda account: TrelloStatusWriter(account),
            raw_sanitizer=sanitize_raw,
            planner_badge_extractor=planner_badges,
            source_url_extractor=source_url,
        )
