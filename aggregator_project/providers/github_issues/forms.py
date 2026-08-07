from __future__ import annotations

import re

from django import forms


REPOSITORY_RE = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")


class GitHubIssuesConnectForm(forms.Form):
    api_token = forms.CharField(
        label="Personal access token",
        max_length=255,
        widget=forms.PasswordInput,
    )
    repositories = forms.CharField(
        label="Repositories",
        required=False,
        help_text="Leave empty to sync issues assigned to you across GitHub.",
        widget=forms.Textarea(
            attrs={
                "rows": 3,
                "placeholder": "owner/repository, another/repository",
            }
        ),
    )
    include_closed = forms.BooleanField(
        label="Include closed issues", required=False, initial=True
    )
    include_pull_requests = forms.BooleanField(
        label="Include pull requests", required=False, initial=False
    )
    emit_task_created = forms.BooleanField(
        label="Created", required=False, initial=True
    )
    emit_task_updated = forms.BooleanField(
        label="Updated", required=False, initial=True
    )
    emit_task_completed = forms.BooleanField(
        label="Closed", required=False, initial=True
    )
    emit_task_state = forms.BooleanField(
        label="State snapshots", required=False, initial=False
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        for field in self.fields.values():
            if isinstance(field.widget, forms.CheckboxInput):
                field.widget.attrs.update({"class": "field-checkbox"})
            else:
                field.widget.attrs.update({"class": "field-input"})

    def clean_repositories(self) -> list[str]:
        raw = self.cleaned_data.get("repositories") or ""
        values = [
            value.strip().strip("/")
            for value in re.split(r"[\s,]+", raw)
            if value.strip()
        ]
        invalid = [value for value in values if not REPOSITORY_RE.fullmatch(value)]
        if invalid:
            raise forms.ValidationError(
                "Use owner/repository format, for example openai/openai-python."
            )
        return list(dict.fromkeys(values))
