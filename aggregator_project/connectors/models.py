from __future__ import annotations

from typing import Any

from django.db import models

from core.constants import SOURCE_CHOICES
from core.encryption import decrypt_payload, encrypt_payload
from core.models import TimestampedModel


class ConnectorAccount(TimestampedModel):
    AUTH_API_TOKEN = "api_token"
    AUTH_OAUTH = "oauth"
    STATUS_DISCONNECTED = "disconnected"
    STATUS_CONNECTING = "connecting"
    STATUS_CONNECTED = "connected"
    STATUS_ERROR = "error"

    AUTH_TYPE_CHOICES = [
        (AUTH_API_TOKEN, "API Token"),
        (AUTH_OAUTH, "OAuth"),
    ]
    STATUS_CHOICES = [
        (STATUS_DISCONNECTED, "Disconnected"),
        (STATUS_CONNECTING, "Connecting"),
        (STATUS_CONNECTED, "Connected"),
        (STATUS_ERROR, "Error"),
    ]

    source = models.CharField(max_length=50, choices=SOURCE_CHOICES)
    display_name = models.CharField(max_length=255)
    auth_type = models.CharField(max_length=20, choices=AUTH_TYPE_CHOICES)
    credentials = models.TextField(
        help_text="Encrypted JSON payload when ENCRYPTION_KEY is set."
    )
    credentials_encrypted = models.BooleanField(default=False)
    status = models.CharField(
        max_length=20, choices=STATUS_CHOICES, default=STATUS_DISCONNECTED
    )
    last_verified_at = models.DateTimeField(null=True, blank=True)
    last_error = models.TextField(null=True, blank=True)
    is_active = models.BooleanField(default=True)
    last_sync_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        indexes = [
            models.Index(fields=["source", "is_active"]),
        ]

    def set_credentials(self, data: dict[str, Any]) -> None:
        result = encrypt_payload(data)
        self.credentials = result.payload
        self.credentials_encrypted = result.encrypted

    def get_credentials(self) -> dict[str, Any]:
        if not self.credentials:
            return {}
        return decrypt_payload(self.credentials)

    def clear_credentials(self) -> None:
        self.credentials = ""
        self.credentials_encrypted = False

    def __str__(self) -> str:
        return f"{self.get_source_display()} ({self.display_name})"
