# Architecture

## High-level layout
- `aggregator_project/` — Django project root.
- `core/` — shared utilities, middleware, encryption, constants.
- `workspaces/` — multi-tenant workspace + membership.
- `connectors/` — connector accounts and credentials.
- `providers/` — provider-specific clients and normalizers.
- `ingestion/` — sync orchestration, provider registry, sync runs.
- `events/` — normalized Event model and views.

## Data flow
1. A sync command (`manage.py sync_all` or `sync_source`) calls `ingestion.services.sync.sync_source`.
2. A provider spec is selected (`ingestion.providers.get_provider_spec`).
3. The provider client fetches raw items using credentials from `ConnectorAccount`.
4. The normalizer maps raw items to `events.Event`-compatible fields.
5. Events are inserted with a `dedupe_hash`; duplicates are skipped by constraint.
6. `SyncRun` records the outcome and stats.

## Persistence
- Primary database: PostgreSQL configured in `aggregator_project/settings.py`.
- Tenancy enforced by foreign keys to `Workspace`.
- Credentials stored in `ConnectorAccount.credentials` (encrypted if `ENCRYPTION_KEY` is set).

## Legacy subsystem
- `aggregator/` is a separate MySQL-based plugin runner (Asana/Habitica/Toggl/Google Fit).
- It is not integrated into the Django sync pipeline.
