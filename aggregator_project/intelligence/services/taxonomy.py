from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from decimal import Decimal

from django.db import transaction
from django.utils.text import slugify

from intelligence.models import TaskAnalysis, TaskTag, UnifiedTag

TAG_COLORS = (
    "#246b55",
    "#3177a8",
    "#946a28",
    "#8a4c6d",
    "#5d698e",
    "#557052",
)


@dataclass(frozen=True)
class TagCandidate:
    name: str
    kind: str
    confidence: Decimal | None = None
    evidence: str = ""


RULES: tuple[tuple[str, str, tuple[str, ...]], ...] = (
    ("Engineering", UnifiedTag.KIND_DOMAIN, ("code", "api", "bug", "fix", "deploy", "repository", "python", "django", "database", "frontend", "backend")),
    ("Design", UnifiedTag.KIND_DOMAIN, ("design", "ux", "ui", "figma", "prototype", "visual", "layout")),
    ("Operations", UnifiedTag.KIND_DOMAIN, ("incident", "monitor", "infrastructure", "server", "ops", "support", "production")),
    ("Research", UnifiedTag.KIND_WORK_TYPE, ("research", "investigate", "evaluate", "compare", "analyse", "analyze", "discovery")),
    ("Planning", UnifiedTag.KIND_WORK_TYPE, ("plan", "roadmap", "strategy", "backlog", "estimate", "priorit")),
    ("Documentation", UnifiedTag.KIND_WORK_TYPE, ("document", "docs", "write up", "report", "specification", "readme")),
    ("Review", UnifiedTag.KIND_WORK_TYPE, ("review", "feedback", "approve", "qa", "test", "validate")),
    ("Coordination", UnifiedTag.KIND_WORK_TYPE, ("meeting", "call", "email", "message", "stakeholder", "sync with", "follow up")),
    ("Feature delivery", UnifiedTag.KIND_WORK_TYPE, ("feature", "implement", "build", "create", "ship", "launch")),
    ("Bug fixing", UnifiedTag.KIND_WORK_TYPE, ("bug", "fix", "broken", "regression", "error", "issue")),
    ("Writing", UnifiedTag.KIND_SKILL, ("write", "copy", "article", "content", "proposal", "brief")),
    ("Analysis", UnifiedTag.KIND_SKILL, ("analysis", "metric", "data", "dashboard", "sql", "insight")),
)


def task_content_hash(item) -> str:
    value = "\n".join((item.title or "", item.description or "")).strip()
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def rule_candidates_for(item) -> list[TagCandidate]:
    text = f"{item.title or ''}\n{item.description or ''}".casefold()
    candidates = []
    for name, kind, terms in RULES:
        if any(term in text for term in terms):
            candidates.append(TagCandidate(name=name, kind=kind, confidence=Decimal("0.620")))
    if not candidates:
        candidates.append(
            TagCandidate(
                name="General work",
                kind=UnifiedTag.KIND_WORK_TYPE,
                confidence=Decimal("0.300"),
            )
        )
    return _dedupe_candidates(candidates)[:6]


def apply_rule_enrichment(item, *, force: bool = False) -> TaskAnalysis:
    """Apply immediate deterministic tags while a model enrichment is pending."""
    content_hash = task_content_hash(item)
    analysis = TaskAnalysis.objects.filter(item=item).first()
    if analysis and analysis.content_hash == content_hash and not force:
        return analysis

    candidates = rule_candidates_for(item)
    with transaction.atomic():
        TaskTag.objects.filter(item=item, source__in=[TaskTag.SOURCE_RULE, TaskTag.SOURCE_AI]).delete()
        for candidate in candidates:
            tag = get_or_create_tag(
                workspace=item.workspace,
                name=candidate.name,
                kind=candidate.kind,
                is_system=True,
            )
            if TaskTag.objects.filter(
                item=item,
                tag=tag,
                source=TaskTag.SOURCE_MANUAL,
            ).exists():
                continue
            TaskTag.objects.update_or_create(
                item=item,
                tag=tag,
                defaults={
                    "source": TaskTag.SOURCE_RULE,
                    "confidence": candidate.confidence,
                    "evidence": candidate.evidence,
                },
            )
        analysis, _created = TaskAnalysis.objects.update_or_create(
            item=item,
            defaults={
                "content_hash": content_hash,
                "status": TaskAnalysis.STATUS_RULES,
                "summary": "",
                "task_type": candidates[0].name if candidates else "",
                "difficulty": None,
                "energy": "",
                "strengths": [],
                "risks": [],
                "model": "",
                "backend": "",
                "last_error": "",
                "analyzed_at": None,
            },
        )
    return analysis


def apply_ai_enrichment(item, *, payload: dict, model: str, backend: str, content_hash: str) -> TaskAnalysis:
    """Replace non-manual tags with a validated model result for the current content."""
    if task_content_hash(item) != content_hash:
        return TaskAnalysis.objects.get(item=item)
    candidates = candidates_from_ai_payload(payload)
    if not candidates:
        candidates = rule_candidates_for(item)

    with transaction.atomic():
        locked_item = type(item).objects.select_for_update().get(id=item.id)
        if task_content_hash(locked_item) != content_hash:
            return TaskAnalysis.objects.get(item=locked_item)
        TaskTag.objects.filter(
            item=locked_item,
            source__in=[TaskTag.SOURCE_RULE, TaskTag.SOURCE_AI],
        ).delete()
        for candidate in candidates:
            tag = get_or_create_tag(
                workspace=locked_item.workspace,
                name=candidate.name,
                kind=candidate.kind,
                is_system=True,
            )
            if TaskTag.objects.filter(
                item=locked_item,
                tag=tag,
                source=TaskTag.SOURCE_MANUAL,
            ).exists():
                continue
            TaskTag.objects.update_or_create(
                item=locked_item,
                tag=tag,
                defaults={
                    "source": TaskTag.SOURCE_AI,
                    "confidence": candidate.confidence,
                    "evidence": candidate.evidence,
                },
            )
        analysis, _created = TaskAnalysis.objects.update_or_create(
            item=locked_item,
            defaults={
                "content_hash": content_hash,
                "status": TaskAnalysis.STATUS_READY,
                "summary": _clean_text(payload.get("summary"), 480),
                "task_type": _clean_text(payload.get("task_type"), 64),
                "difficulty": _difficulty(payload.get("difficulty")),
                "energy": _energy(payload.get("energy")),
                "strengths": _clean_text_list(payload.get("strengths")),
                "risks": _clean_text_list(payload.get("risks")),
                "model": model[:160],
                "backend": backend[:32],
                "last_error": "",
            },
        )
    return analysis


def mark_analysis_failed(item, message: str) -> None:
    TaskAnalysis.objects.filter(item=item).update(
        status=TaskAnalysis.STATUS_FAILED,
        last_error=message[:1000],
    )


def get_or_create_tag(*, workspace, name: str, kind: str, is_system: bool) -> UnifiedTag:
    cleaned_name = _clean_tag_name(name)
    slug = _tag_slug(cleaned_name)
    tag, created = UnifiedTag.objects.get_or_create(
        workspace=workspace,
        slug=slug,
        defaults={
            "name": cleaned_name,
            "kind": kind if kind in dict(UnifiedTag.KIND_CHOICES) else UnifiedTag.KIND_OTHER,
            "color": _tag_color(slug),
            "is_system": is_system,
        },
    )
    return tag


def candidates_from_ai_payload(payload: dict) -> list[TagCandidate]:
    raw_tags = payload.get("tags")
    if not isinstance(raw_tags, list):
        return []
    candidates: list[TagCandidate] = []
    valid_kinds = dict(UnifiedTag.KIND_CHOICES)
    for raw in raw_tags[:8]:
        if not isinstance(raw, dict):
            continue
        name = _clean_tag_name(raw.get("name"))
        if not name:
            continue
        kind = str(raw.get("kind") or UnifiedTag.KIND_OTHER).strip().lower()
        if kind not in valid_kinds:
            kind = UnifiedTag.KIND_OTHER
        candidates.append(
            TagCandidate(
                name=name,
                kind=kind,
                confidence=_confidence(raw.get("confidence")),
                evidence=_clean_text(raw.get("evidence"), 280),
            )
        )
    return _dedupe_candidates(candidates)[:8]


def _dedupe_candidates(candidates: list[TagCandidate]) -> list[TagCandidate]:
    seen = set()
    result = []
    for candidate in candidates:
        key = _tag_slug(candidate.name)
        if not key or key in seen:
            continue
        seen.add(key)
        result.append(candidate)
    return result


def _clean_tag_name(value) -> str:
    if not isinstance(value, str):
        return ""
    cleaned = re.sub(r"\s+", " ", value).strip(" -_#")
    return cleaned[:80]


def _tag_slug(name: str) -> str:
    slug = slugify(name, allow_unicode=True)[:90]
    if slug:
        return slug
    digest = hashlib.sha1(name.encode("utf-8")).hexdigest()[:10]
    return f"tag-{digest}"


def _tag_color(slug: str) -> str:
    index = int(hashlib.sha1(slug.encode("utf-8")).hexdigest(), 16) % len(TAG_COLORS)
    return TAG_COLORS[index]


def _confidence(value) -> Decimal | None:
    try:
        numeric = Decimal(str(value))
    except Exception:  # noqa: BLE001
        return None
    return max(Decimal("0"), min(Decimal("1"), numeric)).quantize(Decimal("0.001"))


def _difficulty(value) -> int | None:
    try:
        numeric = int(value)
    except (TypeError, ValueError):
        return None
    return numeric if 1 <= numeric <= 5 else None


def _energy(value) -> str:
    result = str(value or "").strip().lower()
    return result if result in dict(TaskAnalysis.ENERGY_CHOICES) else ""


def _clean_text(value, limit: int) -> str:
    if not isinstance(value, str):
        return ""
    return re.sub(r"\s+", " ", value).strip()[:limit]


def _clean_text_list(value) -> list[str]:
    if not isinstance(value, list):
        return []
    result = []
    for item in value[:4]:
        cleaned = _clean_text(item, 160)
        if cleaned:
            result.append(cleaned)
    return result
