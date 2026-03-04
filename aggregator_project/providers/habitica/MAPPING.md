# Habitica → Event Mapping

- Habit history → `metric_recorded` (if enabled).
- Daily/Todo completion → `task_completed` using `history.completed` date or `dateCompleted`.
- Task state snapshots follow toggles; completed snapshot skipped if completion occurrence at same timestamp.

Arbitration rule per timestamp per task: `task_completed` > `task_updated` > `task_created` > `task_state`; lower-priority at that timestamp is dropped.
