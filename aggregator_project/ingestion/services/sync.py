from __future__ import annotations

from datetime import datetime

import os

from django.db import IntegrityError, transaction
from django.utils import timezone

from connectors.models import ConnectorAccount
from events.models import Event
from ingestion.normalizers.base import build_dedupe_hash
from ingestion.normalizers.utils import serialize_raw
from ingestion.providers import get_provider_spec


@transaction.atomic
def sync_connector_account(
    workspace,
    connector_account: ConnectorAccount,
    since: datetime | None = None,
) -> dict[str, int]:
    if connector_account.workspace_id != workspace.id:
        raise ValueError("Connector account does not belong to workspace.")
    if not connector_account.is_active or connector_account.revoked_at:
        raise ValueError("Connector account is inactive or revoked.")

    spec = get_provider_spec(connector_account.source)
    if not spec:
        raise ValueError(f"Unknown provider source: {connector_account.source}")

    client = spec.client_factory(connector_account)
    raw_items = client.fetch_since(since)

    inserted = 0
    skipped = 0
    progress_enabled = os.getenv("SYNC_PROGRESS") == "1"
    progress_every = int(os.getenv("SYNC_PROGRESS_EVERY", "500"))
    for raw_index, raw in enumerate(raw_items, start=1):
        normalized_items = spec.normalizer(raw)
        if isinstance(normalized_items, dict):
            normalized_items = [normalized_items]
        for normalized in normalized_items:
            if "raw" not in normalized:
                normalized["raw"] = raw
            normalized["raw"] = serialize_raw(normalized["raw"])
            normalized["dedupe_hash"] = build_dedupe_hash(normalized)
            if not normalized.get("source_entity_id") or not normalized.get("event_type"):
                skipped += 1
                continue
            try:
                with transaction.atomic():
                    Event.objects.create(
                        workspace=workspace,
                        connector_account=connector_account,
                        **normalized,
                    )
                inserted += 1
            except IntegrityError:
                skipped += 1
            if progress_enabled and (inserted + skipped) % progress_every == 0:
                print(
                    f"[sync] processed {inserted + skipped} events "
                    f"(inserted={inserted}, skipped={skipped})"
                )
        if progress_enabled and raw_index % progress_every == 0:
            print(f"[sync] processed {raw_index} raw items")

    connector_account.last_sync_at = timezone.now()
    connector_account.last_sync_status = ConnectorAccount.SYNC_STATUS_SUCCESS
    connector_account.save(update_fields=["last_sync_at", "last_sync_status"])

    return {
        "inserted": inserted,
        "skipped": skipped,
        "total": len(raw_items),
        "connector_account_id": str(connector_account.id),
    }
