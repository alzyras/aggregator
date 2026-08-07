from __future__ import annotations

SOURCE_GOOGLE_FIT = "google_fit"
SOURCE_ASANA = "asana"
SOURCE_TODOIST = "todoist"
SOURCE_HABITICA = "habitica"
SOURCE_JIRA = "jira"
SOURCE_GITHUB = "github"
SOURCE_LINEAR = "linear"
SOURCE_CLICKUP = "clickup"
SOURCE_TRELLO = "trello"

SOURCE_CHOICES = [
    (SOURCE_GOOGLE_FIT, "Google Fit"),
    (SOURCE_ASANA, "Asana"),
    (SOURCE_TODOIST, "Todoist"),
    (SOURCE_HABITICA, "Habitica"),
    (SOURCE_JIRA, "Jira"),
    (SOURCE_GITHUB, "GitHub Issues"),
    (SOURCE_LINEAR, "Linear"),
    (SOURCE_CLICKUP, "ClickUp"),
    (SOURCE_TRELLO, "Trello"),
]

PROVIDER_CHOICES = SOURCE_CHOICES
