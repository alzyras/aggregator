from __future__ import annotations

from django.conf import settings
from django.db import models

from connectors.encryption import decrypt_value, encrypt_value
from core.models import TimestampedModel
from planner.models import PlannerItem
from workspaces.models import Workspace


class WorkspaceQuerySet(models.QuerySet):
    def for_workspace(self, workspace: Workspace) -> "WorkspaceQuerySet":
        return self.filter(workspace=workspace)


class WorkspaceAISettings(TimestampedModel):
    BACKEND_OPENAI_RESPONSES = "openai_responses"
    BACKEND_OPENAI_COMPATIBLE = "openai_compatible"

    BACKEND_CHOICES = [
        (BACKEND_OPENAI_RESPONSES, "OpenAI Responses / GPT-5.6 Luna"),
        (BACKEND_OPENAI_COMPATIBLE, "OpenAI-compatible local model (Qwen)"),
    ]

    workspace = models.OneToOneField(
        Workspace,
        on_delete=models.CASCADE,
        related_name="ai_settings",
    )
    backend = models.CharField(
        max_length=32,
        choices=BACKEND_CHOICES,
        default=BACKEND_OPENAI_RESPONSES,
    )
    model = models.CharField(max_length=160, blank=True)
    base_url = models.URLField(blank=True)
    encrypted_api_key = models.BinaryField(null=True, blank=True)
    is_enabled = models.BooleanField(default=True)
    last_verified_at = models.DateTimeField(null=True, blank=True)
    last_error = models.TextField(blank=True)

    objects = WorkspaceQuerySet.as_manager()

    def set_api_key(self, value: str | None) -> None:
        self.encrypted_api_key = encrypt_value(value or "") if value else None

    def get_api_key(self) -> str | None:
        if not self.encrypted_api_key:
            return None
        return decrypt_value(self.encrypted_api_key) or None

    def __str__(self) -> str:
        return f"AI settings for {self.workspace}"


class UnifiedTag(TimestampedModel):
    KIND_DOMAIN = "domain"
    KIND_WORK_TYPE = "work_type"
    KIND_SKILL = "skill"
    KIND_CONTEXT = "context"
    KIND_PRIORITY = "priority"
    KIND_OTHER = "other"

    KIND_CHOICES = [
        (KIND_DOMAIN, "Domain"),
        (KIND_WORK_TYPE, "Work type"),
        (KIND_SKILL, "Skill"),
        (KIND_CONTEXT, "Context"),
        (KIND_PRIORITY, "Priority"),
        (KIND_OTHER, "Other"),
    ]

    workspace = models.ForeignKey(
        Workspace,
        on_delete=models.CASCADE,
        related_name="unified_tags",
    )
    name = models.CharField(max_length=80)
    slug = models.SlugField(max_length=100)
    kind = models.CharField(max_length=20, choices=KIND_CHOICES, default=KIND_OTHER)
    color = models.CharField(max_length=16, default="#477a64")
    is_system = models.BooleanField(default=False)

    objects = WorkspaceQuerySet.as_manager()

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["workspace", "slug"],
                name="intelligence_tag_workspace_slug_unique",
            )
        ]
        indexes = [
            models.Index(fields=["workspace", "kind", "name"]),
        ]
        ordering = ["kind", "name"]

    def __str__(self) -> str:
        return self.name


class TaskTag(TimestampedModel):
    SOURCE_RULE = "rule"
    SOURCE_AI = "ai"
    SOURCE_MANUAL = "manual"

    SOURCE_CHOICES = [
        (SOURCE_RULE, "Rule"),
        (SOURCE_AI, "AI"),
        (SOURCE_MANUAL, "Manual"),
    ]

    item = models.ForeignKey(
        PlannerItem,
        on_delete=models.CASCADE,
        related_name="tag_assignments",
    )
    tag = models.ForeignKey(
        UnifiedTag,
        on_delete=models.CASCADE,
        related_name="task_assignments",
    )
    source = models.CharField(max_length=12, choices=SOURCE_CHOICES)
    confidence = models.DecimalField(max_digits=4, decimal_places=3, null=True, blank=True)
    evidence = models.CharField(max_length=280, blank=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["item", "tag"],
                name="intelligence_task_tag_unique",
            )
        ]
        indexes = [
            models.Index(fields=["item", "source"]),
            models.Index(fields=["tag", "source"]),
        ]

    def __str__(self) -> str:
        return f"{self.item_id}:{self.tag.name}"


class TaskAnalysis(TimestampedModel):
    STATUS_PENDING = "pending"
    STATUS_RULES = "rules"
    STATUS_READY = "ready"
    STATUS_FAILED = "failed"

    STATUS_CHOICES = [
        (STATUS_PENDING, "Pending"),
        (STATUS_RULES, "Rules applied"),
        (STATUS_READY, "AI enriched"),
        (STATUS_FAILED, "Failed"),
    ]

    ENERGY_LOW = "low"
    ENERGY_MEDIUM = "medium"
    ENERGY_HIGH = "high"

    ENERGY_CHOICES = [
        (ENERGY_LOW, "Low"),
        (ENERGY_MEDIUM, "Medium"),
        (ENERGY_HIGH, "High"),
    ]

    item = models.OneToOneField(
        PlannerItem,
        on_delete=models.CASCADE,
        related_name="intelligence_analysis",
    )
    content_hash = models.CharField(max_length=64, blank=True)
    status = models.CharField(max_length=16, choices=STATUS_CHOICES, default=STATUS_PENDING)
    summary = models.TextField(blank=True)
    task_type = models.CharField(max_length=64, blank=True)
    difficulty = models.PositiveSmallIntegerField(null=True, blank=True)
    energy = models.CharField(max_length=12, choices=ENERGY_CHOICES, blank=True)
    strengths = models.JSONField(default=list, blank=True)
    risks = models.JSONField(default=list, blank=True)
    model = models.CharField(max_length=160, blank=True)
    backend = models.CharField(max_length=32, blank=True)
    last_error = models.TextField(blank=True)
    analyzed_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        indexes = [
            models.Index(fields=["status", "analyzed_at"]),
            models.Index(fields=["task_type"]),
        ]

    def __str__(self) -> str:
        return f"Analysis for {self.item_id}"


class ChatThread(TimestampedModel):
    workspace = models.ForeignKey(
        Workspace,
        on_delete=models.CASCADE,
        related_name="chat_threads",
    )
    user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="aggregator_chat_threads",
    )
    title = models.CharField(max_length=140, default="New conversation")

    objects = WorkspaceQuerySet.as_manager()

    class Meta:
        indexes = [
            models.Index(fields=["workspace", "user", "-updated_at"]),
        ]
        ordering = ["-updated_at"]

    def __str__(self) -> str:
        return self.title


class ChatMessage(TimestampedModel):
    ROLE_USER = "user"
    ROLE_ASSISTANT = "assistant"

    ROLE_CHOICES = [
        (ROLE_USER, "User"),
        (ROLE_ASSISTANT, "Assistant"),
    ]

    thread = models.ForeignKey(
        ChatThread,
        on_delete=models.CASCADE,
        related_name="messages",
    )
    role = models.CharField(max_length=16, choices=ROLE_CHOICES)
    content = models.TextField()
    model = models.CharField(max_length=160, blank=True)

    class Meta:
        indexes = [
            models.Index(fields=["thread", "created_at"]),
        ]
        ordering = ["created_at", "id"]

    def __str__(self) -> str:
        return f"{self.thread_id}:{self.role}"
