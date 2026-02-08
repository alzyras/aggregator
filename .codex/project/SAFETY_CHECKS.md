# Safety Checks

## Before running or deploying
- Confirm `DJANGO_SECRET_KEY` is set (not the dev default).
- Confirm `ENCRYPTION_KEY` is set if storing connector credentials.
- Confirm `.env` is present and not committed.
- Confirm database connectivity (`PGHOST`, `PGUSER`, `PGPASSWORD`, `PGDATABASE`).

## Repeatable checks
- Run Django system checks:
  - `python aggregator_project/manage.py check` (or `python3` if `python` is missing)
- Apply migrations:
  - `python aggregator_project/manage.py migrate` (or `python3` if `python` is missing)
- Run tests:
  - `python aggregator_project/manage.py test` (or `python3` if `python` is missing)

## Tenant isolation
- Workspace scoping tests live in:
  - `aggregator_project/core/tests/test_tenant_isolation.py`
  - `aggregator_project/ingestion/tests/test_sync_isolation.py`
- Ensure new query paths add workspace filters.

## Data integrity
- Events must include `source_entity_id` and `dedupe_hash`.
- Normalizers should be deterministic for the same input.
