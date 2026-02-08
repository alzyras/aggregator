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

    parser = argparse.ArgumentParser(description="Full Asana sync into Events.")
    parser.add_argument("--verbose", action="store_true", help="Print pagination progress")
    args = parser.parse_args()

    token = os.environ.get("ASANA_PERSONAL_ACCESS_TOKEN") or os.environ.get("ASANA_ACCESS_TOKEN")
    if not token:
        print("Missing ASANA_PERSONAL_ACCESS_TOKEN or ASANA_ACCESS_TOKEN")
        sys.exit(1)

    workspace_gid = _require_env("ASANA_WORKSPACE_GID")
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
    from providers.asana.client import AsanaClient
    from workspaces.models import Workspace

    if args.verbose:
        original_paginate = AsanaClient._paginate

        def _paginate_with_progress(self, url, access_token, params):
            count = 0
            results = []
            offset = None
            page = 1
            while True:
                if offset:
                    params["offset"] = offset
                response = self._request(url, access_token, params)
                data = response.get("data", [])
                results.extend(data)
                count += len(data)
                print(f"[asana] page {page} fetched {len(data)} items (total {count})")
                next_page = response.get("next_page") or {}
                offset = next_page.get("offset")
                if not offset:
                    break
                page += 1
            return results

        def _request(self, url, access_token, params):
            import requests

            headers = {"Authorization": f"Bearer {access_token}"}
            resp = requests.get(url, headers=headers, params=params, timeout=30)
            resp.raise_for_status()
            return resp.json()

        AsanaClient._paginate = _paginate_with_progress  # type: ignore[method-assign]
        AsanaClient._request = _request  # type: ignore[method-assign]

    workspace, _ = Workspace.objects.get_or_create(name="Asana Full Sync")

    account, created = ConnectorAccount.objects.get_or_create(
        workspace=workspace,
        source="asana",
        defaults={
            "display_name": "Asana Full Sync",
            "auth_type": ConnectorAccount.AUTH_API_TOKEN,
            "encrypted_access_token": encrypt_value(token),
            "external_account_id": workspace_gid,
            "status": ConnectorAccount.STATUS_CONNECTED,
            "is_active": True,
        },
    )
    if not created:
        account.set_access_token(token)
        account.external_account_id = workspace_gid
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

    print("Starting Asana full sync (all time)...")
    start = time.time()
    stats = sync_connector_account(workspace, account, since=None)
    elapsed = time.time() - start
    print(f"Sync stats: {stats}")
    print(f"Elapsed: {elapsed:.2f}s")
    print(
        "Total Asana events in workspace:",
        Event.objects.for_workspace(workspace).filter(source="asana").count(),
    )


if __name__ == "__main__":
    main()
