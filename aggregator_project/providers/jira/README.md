# Jira Provider

This provider ingests Jira issues into `events.Event` with workspace-scoped connector accounts.

## Supported auth

- `cloud_api_token` (recommended for Jira Cloud): `email + api_token`.
- `personal_access_token` (Server/Data Center friendly): `pat_token`.
- `oauth2` fields are present for future expansion, but live OAuth verification/flow is not enabled in this build.

Secrets are stored only in encrypted connector token fields. Non-secret options are stored in `ConnectorAccount.config["jira"]`.

## Sync behavior

- Incremental sync is default.
- `since` comes from the latest stored event timestamp unless full sync is requested.
- Jira client applies an additional configurable lookback window (`incremental_lookback_minutes`) to avoid missing near-boundary updates.
- First sync/backfill uses `initial_backfill_days` when no `since` is available.

## Options summary

- Connection: deployment type, base URL, auth method.
- Scope: project keys, JQL, issue types, status categories, done-age exclusion, timezone.
- Data detail: changelog, comments, worklogs, sprint/link/attachment metadata toggles.
- Event scope: created/updated/completed/reopened/deleted/state and worklog metric toggles.
- Sync behavior: full sync flag, backfill days, lookback minutes, page size.

## Notes

- Deleted issue detection depends on visible status naming (for example, `Deleted`); Jira does not expose hard-delete events in normal search results.
- Changelog-driven transitions provide the most accurate `task_completed` and `task_reopened` timestamps.

