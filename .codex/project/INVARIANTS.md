# Project Invariants

## Data & tenancy
- All persisted domain data is scoped to a `Workspace`.
- Querying multi-tenant models must always filter by workspace (use `WorkspaceQuerySet.for_workspace`).
- `core.middleware.WorkspaceMiddleware` must set `request.workspace` for authenticated users.

## Events model
- `events.Event` is the canonical normalized data store.
- Each event must include a stable `dedupe_hash` and a non-empty `source_entity_id`.
- Uniqueness is enforced by `(workspace, source, dedupe_hash)`.
- Raw source payloads are preserved in `Event.raw`.

## Connectors & credentials
- Each workspace has at most one `ConnectorAccount` per source (`unique_connector_account`).
- Credentials should be stored via `ConnectorAccount.set_credentials` and read via `get_credentials`.
- If `ENCRYPTION_KEY` is set, credentials are encrypted at rest.
- Credentials must never be logged.

## Syncing
- `ingestion.services.sync.sync_source` is the canonical sync entrypoint.
- Syncs run in a single transaction and always record a `SyncRun` with stats and status.
- Providers must register a `provider_spec` on their `AppConfig`.

## Time & data shape
- Server time zone is UTC; normalized timestamps should be timezone-aware or UTC-safe.
- Normalizers must return fields compatible with `events.Event` columns.

## Legacy code
- The `aggregator/` folder is the legacy MySQL plugin runner and is not the primary pipeline for the Django app.
