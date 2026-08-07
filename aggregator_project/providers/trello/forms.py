from __future__ import annotations

import re

from django import forms


BOARD_ID_RE = re.compile(r"^[A-Za-z0-9]+$")


class TrelloConnectForm(forms.Form):
    api_key = forms.CharField(label="API key", max_length=255, widget=forms.PasswordInput)
    api_token = forms.CharField(label="API token", max_length=255, widget=forms.PasswordInput)
    board_ids = forms.CharField(
        label="Board IDs",
        help_text="One or more Trello board IDs, separated by commas or new lines.",
        widget=forms.Textarea(attrs={"rows": 3, "placeholder": "67c4c0f1aa22bb33cc44dd55"}),
    )
    include_closed = forms.BooleanField(
        label="Include archived cards", required=False, initial=True
    )
    todo_list_name = forms.CharField(label="To do list", max_length=100, initial="To Do")
    in_progress_list_name = forms.CharField(
        label="In progress list", max_length=100, initial="Doing"
    )
    emit_task_created = forms.BooleanField(label="Created", required=False, initial=True)
    emit_task_updated = forms.BooleanField(label="Updated", required=False, initial=True)
    emit_task_completed = forms.BooleanField(label="Archived", required=False, initial=True)
    emit_task_state = forms.BooleanField(label="State snapshots", required=False, initial=False)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        for field in self.fields.values():
            if isinstance(field.widget, forms.CheckboxInput):
                field.widget.attrs["class"] = "field-checkbox"
            else:
                field.widget.attrs["class"] = "field-input"

    def clean_board_ids(self) -> list[str]:
        raw = self.cleaned_data.get("board_ids") or ""
        values = [value.strip() for value in re.split(r"[\s,]+", raw) if value.strip()]
        if not values:
            raise forms.ValidationError("Add at least one Trello board ID.")
        if any(not BOARD_ID_RE.fullmatch(value) for value in values):
            raise forms.ValidationError("Trello board IDs may contain letters and numbers only.")
        return list(dict.fromkeys(values))

    def clean(self):
        cleaned = super().clean()
        for field_name in ("todo_list_name", "in_progress_list_name"):
            cleaned[field_name] = str(cleaned.get(field_name) or "").strip()
            if not cleaned[field_name]:
                self.add_error(field_name, "Enter the exact Trello list name.")
        return cleaned
