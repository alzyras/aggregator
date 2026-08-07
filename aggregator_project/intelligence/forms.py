from __future__ import annotations

from urllib.parse import urlparse

from django import forms

from intelligence.models import WorkspaceAISettings


class WorkspaceAISettingsForm(forms.ModelForm):
    api_key = forms.CharField(
        label="API key",
        required=False,
        widget=forms.PasswordInput(render_value=False),
        help_text="Leave blank to keep the existing key or use the server environment key.",
    )

    class Meta:
        model = WorkspaceAISettings
        fields = ("is_enabled", "backend", "model", "base_url")
        widgets = {
            "model": forms.TextInput(attrs={"placeholder": "e.g. qwen3 or your approved OpenAI model"}),
            "base_url": forms.URLInput(attrs={"placeholder": "http://192.168.1.20:8000/v1"}),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        for field in self.fields.values():
            field.widget.attrs["class"] = "field-input"
        self.fields["is_enabled"].widget.attrs["class"] = "field-checkbox"

    def clean_base_url(self):
        value = (self.cleaned_data.get("base_url") or "").strip()
        backend = self.cleaned_data.get("backend")
        if not value:
            return ""
        parsed = urlparse(value)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc or parsed.username or parsed.password:
            raise forms.ValidationError("Use a plain http(s) server URL without embedded credentials.")
        if backend == WorkspaceAISettings.BACKEND_OPENAI_RESPONSES:
            raise forms.ValidationError("The OpenAI Responses backend uses the managed endpoint; leave this blank.")
        return value.rstrip("/")

    def save(self, commit: bool = True):
        instance = super().save(commit=False)
        api_key = self.cleaned_data.get("api_key")
        if api_key:
            instance.set_api_key(api_key)
        if commit:
            instance.save()
        return instance
