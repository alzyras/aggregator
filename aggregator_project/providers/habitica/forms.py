from __future__ import annotations

from django import forms


class HabiticaConnectForm(forms.Form):
    user_id = forms.CharField(label="User ID", max_length=255)
    api_token = forms.CharField(
        label="API Token", max_length=255, widget=forms.PasswordInput
    )

    sync_habits = forms.BooleanField(label="Sync habits", required=False, initial=True)
    sync_todos = forms.BooleanField(label="Sync todos", required=False, initial=True)
    sync_dailies = forms.BooleanField(label="Sync dailies", required=False, initial=True)

    task_state_completed = forms.BooleanField(
        label="Completed", required=False, initial=False
    )
    task_state_created = forms.BooleanField(
        label="Created", required=False, initial=False
    )
    task_state_updated = forms.BooleanField(
        label="Updated", required=False, initial=True
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        for field_name, field in self.fields.items():
            if isinstance(field.widget, forms.CheckboxInput):
                field.widget.attrs.update({"class": "field-checkbox"})
                if field_name.startswith("task_state_"):
                    field.widget.attrs["data-group"] = "task_state"
                if field_name.startswith("sync_"):
                    field.widget.attrs["data-group"] = "sync_scope"
            else:
                field.widget.attrs.update({"class": "field-input"})
