# Jira Event Mapping

## Canonical fields

- `source`: `jira`
- `source_entity_type`:
  - `issue` for lifecycle/state events
  - `worklog` for time metrics
- `source_entity_id`:
  - Jira issue `id` for issue events
  - Jira worklog `id` for worklog events

## Lifecycle mapping

- Issue created -> `task_created`
  - timestamp: `fields.created`
- Issue updated -> `task_updated`
  - timestamp: changelog history timestamp (preferred) or `fields.updated`
  - emitted only when meaningful changes exist
- Done transition -> `task_completed`
  - timestamp: changelog status transition to done (preferred), else `resolutiondate`/`updated`
- Reopen transition -> `task_reopened`
  - timestamp: changelog status transition from done to non-done
- Deleted/removed status -> `task_deleted`
  - timestamp: `fields.updated` (fallback `fields.created`)
- Snapshot -> `task_state`
  - timestamp: `fields.updated`
  - optional via toggle

## Metrics mapping

- Worklog entry -> `metric_recorded`
  - `metric_type`: `time_spent`
  - `metric_value`: `timeSpentSeconds`
  - `metric_unit`: `seconds`
  - actor: worklog author

## Actor attribution

- Issue created: creator/reporter
- Changelog events: changelog author
- Fallback issue updates/state: assignee/reporter
- Worklog metrics: worklog author

All actor fields are populated when data exists:
`external_actor_id`, `external_actor_type`, `external_actor_display_name`, `external_actor_raw`.

## Dedupe/version strategy

Each event emits deterministic `source_event_version`:

- issue create/update/state: `issue:<issue_id>:<event_type>:<iso_timestamp>`
- changelog transitions: `history:<issue_id>:<history_id>:<event_type>`
- worklogs: `worklog:<worklog_id>:<iso_timestamp>`

Existing dedupe hash pipeline uses:
`source + source_entity_type + source_entity_id + event_type + source_event_version`.

## Arbitration rule

For issue lifecycle collisions at the same timestamp, priority is:
`task_completed > task_updated > task_created > task_state`.
Lower-priority events at that timestamp are dropped.

