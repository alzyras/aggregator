from __future__ import annotations

from connectors.models import ConnectorAccount
from workspaces.models import Workspace


def get_active_account(provider: str, workspace: Workspace) -> ConnectorAccount | None:
    return (
        ConnectorAccount.objects.for_workspace(workspace)
        .filter(provider=provider, is_active=True, revoked_at__isnull=True)
        .order_by("-updated_at")
        .first()
    )

def get_required_account(provider: str, workspace: Workspace) -> ConnectorAccount:
    account = get_active_account(provider, workspace)
    if not account:
        raise ValueError("No active connector account found.")
    return account


def sanitize_error(message: str) -> str:
    if not message:
        return "Verification failed."
    return message.splitlines()[0][:300]
