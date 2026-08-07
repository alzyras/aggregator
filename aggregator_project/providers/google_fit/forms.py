from __future__ import annotations

from django import forms


class GoogleFitConnectForm(forms.Form):
    client_id = forms.CharField(label="Client ID", max_length=255, required=False)
    client_secret = forms.CharField(
        label="Client Secret",
        max_length=255,
        required=False,
        widget=forms.PasswordInput,
    )
    refresh_token = forms.CharField(
        label="Refresh Token",
        max_length=255,
        required=False,
        widget=forms.PasswordInput,
    )
    access_token = forms.CharField(
        label="Access Token",
        max_length=255,
        required=False,
        widget=forms.PasswordInput,
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        for field in self.fields.values():
            field.widget.attrs.update({"class": "field-input"})

    def clean(self):
        cleaned = super().clean()
        if not cleaned.get("refresh_token"):
            raise forms.ValidationError("Refresh token is required for Google Fit.")
        if not cleaned.get("client_id") or not cleaned.get("client_secret"):
            raise forms.ValidationError(
                "Client ID and Client Secret are required with a refresh token."
            )
        return cleaned
