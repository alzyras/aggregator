from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path


def _load_dotenv() -> None:
    dotenv_path = Path(__file__).resolve().parents[2] / ".env"
    if not dotenv_path.exists():
        return
    for line in dotenv_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("\"'")  # naive unquote
        os.environ.setdefault(key, value)


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        print(f"Missing required env var: {name}")
        sys.exit(1)
    return value


def main() -> None:
    _load_dotenv()
    repo_root = Path(__file__).resolve().parents[2]
    project_root = repo_root / "aggregator_project"
    sys.path.insert(0, str(repo_root))
    sys.path.insert(0, str(project_root))
    access_token = os.environ.get("ASANA_PERSONAL_ACCESS_TOKEN") or os.environ.get("ASANA_ACCESS_TOKEN")
    workspace_gid = os.environ.get("ASANA_WORKSPACE_GID")
    if not access_token:
        print("Missing ASANA_PERSONAL_ACCESS_TOKEN or ASANA_ACCESS_TOKEN")
        sys.exit(1)
    if not workspace_gid:
        print("Missing ASANA_WORKSPACE_GID")
        sys.exit(1)

    # Adapter path via Django
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "aggregator_project.settings")
    if not os.environ.get("ENCRYPTION_KEY"):
        # Development-only fallback for local testing
        os.environ["ENCRYPTION_KEY"] = ""  # must be set before Django import
        print("ENCRYPTION_KEY is required; set it in your env and retry.")
        sys.exit(1)

    import django

    django.setup()

    from connectors.models import ConnectorAccount
    from connectors.encryption import encrypt_value
    from events.models import Event
    from ingestion.services.sync import sync_connector_account
    from workspaces.models import Workspace

    workspace, _ = Workspace.objects.get_or_create(name="Asana Test Workspace")

    account = ConnectorAccount.objects.create(
        workspace=workspace,
        source="asana",
        display_name="Asana Test",
        auth_type=ConnectorAccount.AUTH_API_TOKEN,
        encrypted_access_token=encrypt_value(access_token),
        external_account_id=workspace_gid,
        status=ConnectorAccount.STATUS_CONNECTED,
        is_active=True,
    )

    days_to_fetch = int(os.environ.get("ASANA_DAYS", "7"))
    since = datetime.now(tz=timezone.utc) - timedelta(days=days_to_fetch)
    print(f"[adapter] syncing (last {days_to_fetch} days)...")
    stats = sync_connector_account(workspace, account, since=since)
    print(f"[adapter] stats: {stats}")

    count = Event.objects.for_workspace(workspace).filter(source="asana").count()
    print(f"[adapter] total events for workspace: {count}")


if __name__ == "__main__":
    main()
