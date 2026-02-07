from __future__ import annotations

from datetime import datetime

from django.db import transaction
from django.utils import timezone

from connectors.services import get_active_account
from events.models import Event
from ingestion.models import SyncRun
from ingestion.normalizers.base import build_dedupe_hash
from ingestion.providers import get_provider_spec, get_provider_specs


@transaction.atomic
def sync_source(source: str, since: datetime | None = None) -> SyncRun:
    started_at = timezone.now()
    sync_run = SyncRun.objects.create(
        source=source,
        started_at=started_at,
        status=SyncRun.STATUS_SUCCESS,
    )

    inserted = 0
    updated = 0
    skipped = 0

    try:
        spec = get_provider_spec(source)
        if not spec:
            raise ValueError(f"Unknown provider source: {source}")

        client = spec.client_factory()
        raw_items = client.fetch_since(since)
        for raw in raw_items:
            normalized = spec.normalizer(raw)
            normalized["raw"] = raw
            normalized["dedupe_hash"] = build_dedupe_hash(normalized)
            if not normalized.get("source_entity_id"):
                skipped += 1
                continue
            _event, created = Event.objects.update_or_create(
                source=normalized["source"],
                dedupe_hash=normalized["dedupe_hash"],
                defaults=normalized,
            )
            if created:
                inserted += 1
            else:
                updated += 1

        account = get_active_account(source)
        if account:
            account.last_sync_at = timezone.now()
            account.save(update_fields=["last_sync_at"])

        sync_run.stats = {
            "inserted": inserted,
            "updated": updated,
            "skipped": skipped,
            "total": len(raw_items),
        }
        sync_run.finished_at = timezone.now()
        sync_run.status = SyncRun.STATUS_SUCCESS
    except Exception as exc:  # noqa: BLE001 - surface error in SyncRun
        sync_run.status = SyncRun.STATUS_FAILURE
        sync_run.error = str(exc)
        sync_run.finished_at = timezone.now()
    sync_run.save()
    return sync_run


def sync_all_sources(
    since: datetime | None = None, sources: list[str] | None = None
) -> list[SyncRun]:
    runs = []
    available_sources = [spec.source for spec in get_provider_specs()]
    source_list = sources or available_sources
    for source in source_list:
        runs.append(sync_source(source, since=since))
    return runs
