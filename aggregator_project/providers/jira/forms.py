from __future__ import annotations

import re
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from django import forms

DEPLOYMENT_CHOICES = [
    ("cloud", "Cloud"),
    ("server", "Server"),
    ("datacenter", "Data Center"),
]

AUTH_METHOD_CHOICES = [
    ("cloud_api_token", "Cloud API token"),
    ("personal_access_token", "Personal access token"),
    ("oauth2", "OAuth2 (refresh token)"),
]

ISSUE_TYPE_CHOICES = [
    ("Story", "Story"),
    ("Task", "Task"),
    ("Bug", "Bug"),
    ("Epic", "Epic"),
    ("Sub-task", "Sub-task"),
]

STATUS_CATEGORY_CHOICES = [
    ("todo", "To Do"),
    ("in_progress", "In Progress"),
    ("done", "Done"),
]

PROJECT_KEY_RE = re.compile(r"^[A-Z][A-Z0-9_]+$")


class JiraConnectForm(forms.Form):
    deployment_type = forms.ChoiceField(
        label="Deployment type",
        choices=DEPLOYMENT_CHOICES,
        initial="cloud",
    )
    base_url = forms.CharField(label="Base URL", max_length=500)
    auth_method = forms.ChoiceField(
        label="Auth method",
        choices=AUTH_METHOD_CHOICES,
        initial="cloud_api_token",
    )

    email = forms.EmailField(label="Email", required=False)
    api_token = forms.CharField(
        label="API token",
        max_length=255,
        required=False,
        widget=forms.PasswordInput(render_value=True),
    )
    pat_token = forms.CharField(
        label="PAT token",
        max_length=255,
        required=False,
        widget=forms.PasswordInput(render_value=True),
    )
    client_id = forms.CharField(label="Client ID", max_length=255, required=False)
    client_secret = forms.CharField(
        label="Client secret",
        max_length=255,
        required=False,
        widget=forms.PasswordInput(render_value=True),
    )
    refresh_token = forms.CharField(
        label="Refresh token",
        max_length=255,
        required=False,
        widget=forms.PasswordInput(render_value=True),
    )

    project_keys = forms.CharField(
        label="Project keys",
        required=False,
        help_text="Comma-separated project keys, e.g. ENG, OPS.",
    )
    jql_filter = forms.CharField(
        label="JQL filter",
        required=False,
        widget=forms.Textarea(attrs={"rows": 3}),
        initial="ORDER BY updated DESC",
    )
    issue_types = forms.MultipleChoiceField(
        label="Issue types",
        choices=ISSUE_TYPE_CHOICES,
        required=False,
    )
    include_status_categories = forms.MultipleChoiceField(
        label="Status categories",
        choices=STATUS_CATEGORY_CHOICES,
        required=False,
        initial=["todo", "in_progress", "done"],
    )
    exclude_done_before_days = forms.IntegerField(
        label="Exclude done before (days)",
        required=False,
        min_value=0,
        max_value=3650,
    )
    timezone = forms.CharField(label="Timezone", required=False, initial="UTC")

    include_comments = forms.BooleanField(label="Include comments", required=False, initial=False)
    include_worklogs = forms.BooleanField(label="Include worklogs", required=False, initial=False)
    include_changelog = forms.BooleanField(label="Include changelog", required=False, initial=True)
    include_sprints = forms.BooleanField(label="Include sprints", required=False, initial=False)
    include_attachments_metadata = forms.BooleanField(
        label="Include attachment metadata",
        required=False,
        initial=False,
    )
    include_linked_issues = forms.BooleanField(
        label="Include linked issues",
        required=False,
        initial=False,
    )

    emit_task_created = forms.BooleanField(label="Created", required=False, initial=True)
    emit_task_updated = forms.BooleanField(label="Updated", required=False, initial=True)
    emit_task_completed = forms.BooleanField(label="Completed", required=False, initial=True)
    emit_task_reopened = forms.BooleanField(label="Reopened", required=False, initial=True)
    emit_task_deleted = forms.BooleanField(label="Deleted", required=False, initial=False)
    emit_task_state = forms.BooleanField(label="Task state", required=False, initial=False)
    emit_worklog_metrics = forms.BooleanField(
        label="Worklog metrics",
        required=False,
        initial=True,
    )

    full_sync = forms.BooleanField(label="Full sync", required=False, initial=False)
    initial_backfill_days = forms.IntegerField(
        label="Initial backfill days",
        required=False,
        min_value=1,
        max_value=3650,
        initial=365,
    )
    incremental_lookback_minutes = forms.IntegerField(
        label="Incremental lookback (minutes)",
        required=False,
        min_value=0,
        max_value=1440,
        initial=30,
    )
    page_size = forms.IntegerField(
        label="Page size",
        required=False,
        min_value=10,
        max_value=100,
        initial=100,
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        for field_name, field in self.fields.items():
            if isinstance(field.widget, forms.CheckboxInput):
                field.widget.attrs.update({"class": "field-checkbox"})
            elif isinstance(field.widget, forms.Textarea):
                field.widget.attrs.update({"class": "field-input", "maxlength": "2000"})
            else:
                field.widget.attrs.update({"class": "field-input"})
            if field_name in {"issue_types", "include_status_categories"}:
                field.widget.attrs.update({"class": "field-input field-multiselect"})

    def clean_base_url(self) -> str:
        value = (self.cleaned_data.get("base_url") or "").strip().rstrip("/")
        if not value.startswith(("http://", "https://")):
            raise forms.ValidationError("Base URL must start with http:// or https://.")
        if "." not in value:
            raise forms.ValidationError("Base URL is invalid.")
        return value

    def clean_project_keys(self) -> list[str]:
        raw = self.cleaned_data.get("project_keys") or ""
        if not raw.strip():
            return []
        project_keys = [part.strip().upper() for part in raw.split(",") if part.strip()]
        invalid = [key for key in project_keys if not PROJECT_KEY_RE.match(key)]
        if invalid:
            raise forms.ValidationError(
                "Project keys must match pattern [A-Z][A-Z0-9_]+."
            )
        return sorted(set(project_keys))

    def clean_jql_filter(self) -> str:
        jql = (self.cleaned_data.get("jql_filter") or "").strip()
        if len(jql) > 2000:
            raise forms.ValidationError("JQL filter cannot exceed 2000 characters.")
        if ";" in jql:
            raise forms.ValidationError("JQL filter cannot contain semicolons.")
        return jql

    def clean_timezone(self) -> str:
        value = (self.cleaned_data.get("timezone") or "UTC").strip() or "UTC"
        try:
            ZoneInfo(value)
        except ZoneInfoNotFoundError as exc:
            raise forms.ValidationError("Timezone must be a valid IANA timezone.") from exc
        return value

    def clean(self) -> dict[str, object]:
        cleaned = super().clean()
        deployment_type = cleaned.get("deployment_type")
        auth_method = cleaned.get("auth_method")

        if deployment_type == "cloud" and ".atlassian.net" not in (cleaned.get("base_url") or ""):
            self.add_error("base_url", "Jira Cloud base URL must use *.atlassian.net.")

        if auth_method == "cloud_api_token":
            if not cleaned.get("email"):
                self.add_error("email", "Email is required for Jira Cloud API token auth.")
            if not cleaned.get("api_token"):
                self.add_error("api_token", "API token is required.")
        elif auth_method == "personal_access_token":
            if not cleaned.get("pat_token"):
                self.add_error("pat_token", "PAT token is required.")
        elif auth_method == "oauth2":
            if not cleaned.get("client_id"):
                self.add_error("client_id", "Client ID is required for OAuth2.")
            if not cleaned.get("client_secret"):
                self.add_error("client_secret", "Client secret is required for OAuth2.")
            if not cleaned.get("refresh_token"):
                self.add_error("refresh_token", "Refresh token is required for OAuth2.")

        if not cleaned.get("include_worklogs"):
            cleaned["emit_worklog_metrics"] = False
        return cleaned

