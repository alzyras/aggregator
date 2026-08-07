from __future__ import annotations

import re

from django import forms


LIST_ID_RE = re.compile(r"^\d+$")


class ClickUpConnectForm(forms.Form):
    api_token = forms.CharField(
        label="Personal API token",
        max_length=255,
        widget=forms.PasswordInput,
    )
    list_ids = forms.CharField(
        label="List IDs",
        help_text="One or more ClickUp List IDs, separated by commas or new lines.",
        widget=forms.Textarea(attrs={"rows": 3, "placeholder": "901234567890, 901234567891"}),
    )
    include_closed = forms.BooleanField(
        label="Include closed tasks", required=False, initial=True
    )
    todo_status = forms.CharField(label="To do status", max_length=100, initial="to do")
    in_progress_status = forms.CharField(
        label="In progress status", max_length=100, initial="in progress"
    )
    done_status = forms.CharField(label="Done status", max_length=100, initial="complete")
    emit_task_created = forms.BooleanField(label="Created", required=False, initial=True)
    emit_task_updated = forms.BooleanField(label="Updated", required=False, initial=True)
    emit_task_completed = forms.BooleanField(label="Completed", required=False, initial=True)
    emit_task_state = forms.BooleanField(label="State snapshots", required=False, initial=False)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        for field in self.fields.values():
            if isinstance(field.widget, forms.CheckboxInput):
                field.widget.attrs["class"] = "field-checkbox"
            else:
                field.widget.attrs["class"] = "field-input"

    def clean_list_ids(self) -> list[str]:
        raw = self.cleaned_data.get("list_ids") or ""
        values = [value.strip() for value in re.split(r"[\s,]+", raw) if value.strip()]
        if not values:
            raise forms.ValidationError("Add at least one ClickUp List ID.")
        if any(not LIST_ID_RE.fullmatch(value) for value in values):
            raise forms.ValidationError("ClickUp List IDs must be numeric.")
        return list(dict.fromkeys(values))

    def clean(self):
        cleaned = super().clean()
        for field_name in ("todo_status", "in_progress_status", "done_status"):
            cleaned[field_name] = str(cleaned.get(field_name) or "").strip()
            if not cleaned[field_name]:
                self.add_error(field_name, "Enter the exact ClickUp status name.")
        return cleaned
