from __future__ import annotations

from django.conf import settings
from django.db import models

from core.models import TimestampedModel


class Workspace(TimestampedModel):
    name = models.CharField(max_length=255)

    def __str__(self) -> str:
        return self.name


class WorkspaceMember(models.Model):
    ROLE_OWNER = "owner"
    ROLE_ADMIN = "admin"
    ROLE_MEMBER = "member"

    ROLE_CHOICES = [
        (ROLE_OWNER, "Owner"),
        (ROLE_ADMIN, "Admin"),
        (ROLE_MEMBER, "Member"),
    ]

    workspace = models.ForeignKey(Workspace, on_delete=models.CASCADE)
    user = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE)
    role = models.CharField(max_length=20, choices=ROLE_CHOICES)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["workspace", "user"], name="unique_workspace_member"
            )
        ]

    def __str__(self) -> str:
        return f"{self.user} in {self.workspace}"
