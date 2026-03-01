from __future__ import annotations

from django import forms


class TodoistConnectForm(forms.Form):
    api_token = forms.CharField(label="API Token", max_length=255, widget=forms.PasswordInput)

    sync_tasks = forms.BooleanField(label="Sync tasks", required=False, initial=True)
    include_completed = forms.BooleanField(label="Include completed", required=False, initial=True)
    include_archived = forms.BooleanField(label="Include archived", required=False, initial=False)

    emit_task_created = forms.BooleanField(label="Created", required=False, initial=True)
    emit_task_updated = forms.BooleanField(label="Updated", required=False, initial=True)
    emit_task_completed = forms.BooleanField(label="Completed", required=False, initial=True)
    emit_task_deleted = forms.BooleanField(label="Deleted", required=False, initial=True)

    task_state_created = forms.BooleanField(label="Created", required=False, initial=False)
    task_state_updated = forms.BooleanField(label="Updated", required=False, initial=True)
    task_state_completed = forms.BooleanField(label="Completed", required=False, initial=False)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        for name, field in self.fields.items():
            if isinstance(field.widget, forms.CheckboxInput):
                field.widget.attrs.update({"class": "field-checkbox"})
                if name.startswith("task_state_"):
                    field.widget.attrs["data-group"] = "task_state"
                elif name.startswith("emit_task_"):
                    field.widget.attrs["data-group"] = "event_scope"
                else:
                    field.widget.attrs["data-group"] = "sync_scope"
            else:
                field.widget.attrs.update({"class": "field-input"})
