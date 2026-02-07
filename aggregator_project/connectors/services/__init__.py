from __future__ import annotations

from connectors.models import ConnectorAccount
from ingestion.providers import ProviderSpec


def get_active_account(source: str) -> ConnectorAccount | None:
    return (
        ConnectorAccount.objects.filter(source=source, is_active=True)
        .order_by("-updated_at")
        .first()
    )


def resolve_credentials(spec: ProviderSpec) -> tuple[dict, str | None]:
    account = get_active_account(spec.source)
    if account:
        return account.get_credentials(), "stored"
    env_creds = spec.env_credentials()
    if any(env_creds.values()):
        return env_creds, "env"
    return {}, None


def validate_provider(spec: ProviderSpec) -> tuple[bool, str, str | None]:
    credentials, source = resolve_credentials(spec)
    ok, message = spec.validate_credentials(credentials)
    return ok, message, source


def sanitize_error(message: str) -> str:
    if not message:
        return "Verification failed."
    return message.splitlines()[0][:300]
