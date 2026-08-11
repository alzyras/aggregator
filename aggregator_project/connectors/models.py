from __future__ import annotations

from django.db import models

from core.constants import PROVIDER_CHOICES
from core.models import TimestampedModel
from connectors.encryption import decrypt_value, encrypt_value
from workspaces.models import Workspace


class WorkspaceQuerySet(models.QuerySet):
    def for_workspace(self, workspace: Workspace) -> "WorkspaceQuerySet":
        return self.filter(workspace=workspace)


class ConnectorAccount(TimestampedModel):
    AUTH_API_TOKEN = "api_token"
    AUTH_OAUTH = "oauth"
    STATUS_VALIDATING = "validating"
    STATUS_CONNECTED = "connected"
    STATUS_ERROR = "error"
    STATUS_REVOKED = "revoked"
    SYNC_STATUS_SUCCESS = "success"
    SYNC_STATUS_FAILED = "failed"

    AUTH_TYPE_CHOICES = [
        (AUTH_API_TOKEN, "API Token"),
        (AUTH_OAUTH, "OAuth"),
    ]
    STATUS_CHOICES = [
        (STATUS_VALIDATING, "Validating"),
        (STATUS_CONNECTED, "Connected"),
        (STATUS_ERROR, "Error"),
        (STATUS_REVOKED, "Revoked"),
    ]
    SYNC_STATUS_CHOICES = [
        (SYNC_STATUS_SUCCESS, "Success"),
        (SYNC_STATUS_FAILED, "Failed"),
    ]

    workspace = models.ForeignKey(Workspace, on_delete=models.CASCADE)
    source = models.CharField(max_length=50, choices=PROVIDER_CHOICES)
    display_name = models.CharField(max_length=255)
    auth_type = models.CharField(max_length=20, choices=AUTH_TYPE_CHOICES)
    encrypted_access_token = models.BinaryField()
    encrypted_refresh_token = models.BinaryField(null=True, blank=True)
    token_expires_at = models.DateTimeField(null=True, blank=True)
    scopes = models.JSONField(default=list, blank=True)
    config = models.JSONField(default=dict, blank=True)
    external_account_id = models.CharField(max_length=255, null=True, blank=True)
    revoked_at = models.DateTimeField(null=True, blank=True)
    status = models.CharField(
        max_length=20, choices=STATUS_CHOICES, default=STATUS_VALIDATING
    )
    last_verified_at = models.DateTimeField(null=True, blank=True)
    last_error = models.TextField(null=True, blank=True)
    is_active = models.BooleanField(default=True)
    last_sync_at = models.DateTimeField(null=True, blank=True)
    last_sync_status = models.CharField(
        max_length=20,
        choices=SYNC_STATUS_CHOICES,
        null=True,
        blank=True,
    )
    sync_cursor_at = models.DateTimeField(null=True, blank=True)
    last_incremental_sync_at = models.DateTimeField(null=True, blank=True)
    last_full_sync_at = models.DateTimeField(null=True, blank=True)

    objects = WorkspaceQuerySet.as_manager()

    class Meta:
        indexes = [
            models.Index(fields=["workspace", "source", "is_active"]),
        ]

    def set_access_token(self, value: str) -> None:
        self.encrypted_access_token = encrypt_value(value)

    def get_access_token(self) -> str:
        return decrypt_value(self.encrypted_access_token)

    def set_refresh_token(self, value: str | None) -> None:
        if value:
            self.encrypted_refresh_token = encrypt_value(value)
        else:
            self.encrypted_refresh_token = None

    def get_refresh_token(self) -> str | None:
        if not self.encrypted_refresh_token:
            return None
        return decrypt_value(self.encrypted_refresh_token)

    def clear_tokens(self) -> None:
        self.encrypted_access_token = encrypt_value("")
        self.encrypted_refresh_token = None

    def __str__(self) -> str:
        return f"{self.get_source_display()} ({self.display_name})"
