from __future__ import annotations

import argparse
import os
import sys
import time
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
        os.environ.setdefault(key.strip(), value.strip().strip("\"'"))


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        print(f"Missing required env var: {name}")
        sys.exit(1)
    return value


def main() -> None:
    _load_dotenv()

    parser = argparse.ArgumentParser(description="Full Habitica sync into Events.")
    parser.add_argument("--verbose", action="store_true", help="Print sync progress")
    args = parser.parse_args()

    user_id = _require_env("HABITICA_USER_ID")
    api_token = _require_env("HABITICA_API_TOKEN")
    encryption_key = os.environ.get("ENCRYPTION_KEY")
    if not encryption_key:
        print("Missing ENCRYPTION_KEY")
        sys.exit(1)

    repo_root = Path(__file__).resolve().parents[2]
    project_root = repo_root / "aggregator_project"
    sys.path.insert(0, str(repo_root))
    sys.path.insert(0, str(project_root))
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "aggregator_project.settings")

    import django

    django.setup()

    from connectors.encryption import encrypt_value
    from connectors.models import ConnectorAccount
    from events.models import Event
    from ingestion.services.sync import sync_connector_account
    from providers.habitica.client import HabiticaClient
    from workspaces.models import Workspace

    workspace, _ = Workspace.objects.get_or_create(name="Habitica Full Sync")

    account, created = ConnectorAccount.objects.get_or_create(
        workspace=workspace,
        source="habitica",
        defaults={
            "display_name": "Habitica Full Sync",
            "auth_type": ConnectorAccount.AUTH_API_TOKEN,
            "encrypted_access_token": encrypt_value(api_token),
            "external_account_id": user_id,
            "status": ConnectorAccount.STATUS_CONNECTED,
            "is_active": True,
        },
    )
    if not created:
        account.set_access_token(api_token)
        account.external_account_id = user_id
        account.status = ConnectorAccount.STATUS_CONNECTED
        account.is_active = True
        account.save(
            update_fields=[
                "encrypted_access_token",
                "external_account_id",
                "status",
                "is_active",
                "updated_at",
            ]
        )

    if args.verbose:
        os.environ["SYNC_PROGRESS"] = "1"
        os.environ.setdefault("SYNC_PROGRESS_EVERY", "200")

        original_fetch = HabiticaClient._fetch_tasks

        def _fetch_tasks_with_progress(self, user_id, api_token, task_type):
            print(f"[habitica] fetching {task_type}...")
            tasks = original_fetch(self, user_id, api_token, task_type)
            print(f"[habitica] {task_type}: {len(tasks)} tasks")
            return tasks

        HabiticaClient._fetch_tasks = _fetch_tasks_with_progress  # type: ignore[method-assign]

    print("Starting Habitica full sync (all time)...")
    start = time.time()
    stats = sync_connector_account(workspace, account, since=None)
    elapsed = time.time() - start
    print(f"Sync stats: {stats}")
    print(f"Elapsed: {elapsed:.2f}s")
    print(
        "Total Habitica events in workspace:",
        Event.objects.for_workspace(workspace).filter(source="habitica").count(),
    )


if __name__ == "__main__":
    main()
