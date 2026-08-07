from __future__ import annotations

import re

from django import forms


TEAM_KEY_RE = re.compile(r"^[A-Za-z0-9_-]+$")


class LinearConnectForm(forms.Form):
    api_key = forms.CharField(
        label="Personal API key",
        max_length=255,
        widget=forms.PasswordInput,
    )
    team_keys = forms.CharField(
        label="Team keys",
        required=False,
        widget=forms.Textarea(attrs={"rows": 2, "placeholder": "ENG, PRODUCT"}),
    )
    only_assigned_to_me = forms.BooleanField(
        label="Only issues assigned to me", required=False, initial=True
    )
    include_completed = forms.BooleanField(
        label="Include completed", required=False, initial=True
    )
    include_canceled = forms.BooleanField(
        label="Include canceled", required=False, initial=False
    )
    include_archived = forms.BooleanField(
        label="Include archived", required=False, initial=False
    )
    emit_task_created = forms.BooleanField(
        label="Created", required=False, initial=True
    )
    emit_task_updated = forms.BooleanField(
        label="Updated", required=False, initial=True
    )
    emit_task_completed = forms.BooleanField(
        label="Completed", required=False, initial=True
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

    def clean_team_keys(self) -> list[str]:
        raw = self.cleaned_data.get("team_keys") or ""
        values = [value.strip() for value in re.split(r"[\s,]+", raw) if value.strip()]
        if any(not TEAM_KEY_RE.fullmatch(value) for value in values):
            raise forms.ValidationError(
                "Team keys may contain letters, numbers, - and _."
            )
        return list(dict.fromkeys(values))
