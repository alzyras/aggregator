from __future__ import annotations

from providers.jira.normalizer import normalize_jira


def _base_issue() -> dict:
    return {
        "id": "10001",
        "key": "ENG-10",
        "fields": {
            "summary": "Ship Jira connector",
            "description": "Implement connector",
            "created": "2026-02-01T09:00:00.000+0000",
            "updated": "2026-02-04T13:00:00.000+0000",
            "resolutiondate": "2026-02-03T12:00:00.000+0000",
            "status": {
                "name": "In Progress",
                "statusCategory": {"key": "indeterminate", "name": "In Progress"},
            },
            "reporter": {"accountId": "u1", "displayName": "Reporter"},
            "assignee": {"accountId": "u2", "displayName": "Assignee"},
            "labels": ["backend"],
        },
        "__jira_config": {
            "include_changelog": True,
            "include_worklogs": False,
            "emit_worklog_metrics": False,
            "emit_task_created": True,
            "emit_task_updated": True,
            "emit_task_completed": True,
            "emit_task_reopened": True,
            "emit_task_deleted": False,
            "emit_task_state": False,
        },
    }


def test_changelog_maps_lifecycle_events():
    issue = _base_issue()
    issue["changelog"] = {
        "histories": [
            {
                "id": "h1",
                "created": "2026-02-03T12:00:00.000+0000",
                "author": {"accountId": "actor-1", "displayName": "Actor One"},
                "items": [
                    {"field": "status", "fromString": "In Progress", "toString": "Done"}
                ],
            },
            {
                "id": "h2",
                "created": "2026-02-04T08:00:00.000+0000",
                "author": {"accountId": "actor-2", "displayName": "Actor Two"},
                "items": [
                    {"field": "status", "fromString": "Done", "toString": "In Progress"}
                ],
            },
            {
                "id": "h3",
                "created": "2026-02-04T13:00:00.000+0000",
                "author": {"accountId": "actor-3", "displayName": "Actor Three"},
                "items": [{"field": "summary", "fromString": "A", "toString": "B"}],
            },
        ]
    }

    events = normalize_jira(issue)
    event_types = {event["event_type"] for event in events}
    assert "task_created" in event_types
    assert "task_completed" in event_types
    assert "task_reopened" in event_types
    assert "task_updated" in event_types


def test_worklog_metric_mapping():
    issue = _base_issue()
    issue["__jira_config"]["include_worklogs"] = True
    issue["__jira_config"]["emit_worklog_metrics"] = True
    issue["fields"]["worklog"] = {
        "worklogs": [
            {
                "id": "wl-1",
                "started": "2026-02-05T10:00:00.000+0000",
                "timeSpentSeconds": 1800,
                "author": {"accountId": "worker-1", "displayName": "Worker One"},
            }
        ]
    }

    events = normalize_jira(issue)
    metric_events = [event for event in events if event["event_type"] == "metric_recorded"]
    assert len(metric_events) == 1
    metric = metric_events[0]
    assert metric["metric_type"] == "time_spent"
    assert float(metric["metric_value"]) == 1800.0
    assert metric["metric_unit"] == "seconds"
    assert metric["external_actor_id"] == "worker-1"


def test_source_event_version_stable():
    issue = _base_issue()
    issue["changelog"] = {"histories": []}
    first = normalize_jira(issue)
    second = normalize_jira(issue)
    assert sorted(event["source_event_version"] for event in first) == sorted(
        event["source_event_version"] for event in second
    )

