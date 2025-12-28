import json
import logging
import math
import time
from collections import defaultdict
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any, Dict, List, Tuple

import requests

from aggregator.core.apps import PluginService
from aggregator.plugins.llm_summary.models import (
    CategoryTrend,
    ChatMessage,
    ChatRequest,
    ChatResponse,
    ContextPayload,
    PluginSummary,
    TrendPoint,
)
from aggregator.plugins.llm_summary.repositories import LlmSummaryRepository
from aggregator.settings import settings

logger = logging.getLogger(__name__)

STOPWORDS = {
    "the",
    "and",
    "of",
    "to",
    "a",
    "in",
    "for",
    "with",
    "on",
    "at",
    "by",
    "an",
    "is",
    "it",
    "from",
    "task",
    "project",
    "habit",
    "todo",
}


class LlmSummaryService(PluginService):
    name = "llm_summary"

    def __init__(self, project_settings=None) -> None:
        self.settings = project_settings or settings
        self.repo = LlmSummaryRepository()
        llm = self.settings.llm_summary
        self.base_url = llm["base_url"]
        self.model = llm["model"]
        self.temperature = llm["temperature"]
        self.max_tokens = llm["max_tokens"]
        self.timeout = llm["timeout"]
        self.months = llm["months"]
        self.top_n = llm["top_n"]
        self.max_context_chars = llm["max_context_chars"]
        self.emerge_threshold_pct = llm["emerge_threshold_pct"]
        self.decline_threshold_pct = llm["decline_threshold_pct"]

    def setup(self) -> bool:
        return True

    def fetch_data(self):
        return None

    def write_data(self, payload) -> Tuple[int, int]:
        return (0, 0)

    def build_context(
        self, start_date: date, end_date: date, months: int | None = None, top_n: int | None = None
    ) -> Tuple[str, ContextPayload]:
        window_months = months or self.months
        top_n = top_n or self.top_n
        presence = self.repo.get_plugin_presence()
        summaries: List[PluginSummary] = []
        emerging: List[CategoryTrend] = []
        declining: List[CategoryTrend] = []
        data_gaps: List[str] = []

        def params():
            return {
                "start_date": start_date,
                "end_date": end_date,
                "limit_rows": window_months + 2,
            }

        if presence.get("asana"):
            summaries.append(
                self._build_plugin_summary(
                    plugin="asana",
                    monthly_file="asana_monthly_summary.sql",
                    category_file="asana_categories.sql",
                    params=params(),
                    top_n=top_n,
                )
            )
        else:
            data_gaps.append("asana table missing; skipping.")

        if presence.get("toggl"):
            summaries.append(
                self._build_plugin_summary(
                    plugin="toggl",
                    monthly_file="toggl_monthly_summary.sql",
                    category_file="toggl_categories.sql",
                    params=params(),
                    top_n=top_n,
                )
            )
        else:
            data_gaps.append("toggl table missing; skipping.")

        if presence.get("habitica"):
            summaries.append(
                self._build_plugin_summary(
                    plugin="habitica",
                    monthly_file="habitica_monthly_summary.sql",
                    category_file="habitica_categories.sql",
                    params=params(),
                    top_n=top_n,
                )
            )
        else:
            data_gaps.append("habitica table missing; skipping.")

        if all(presence.get(k) for k in ["google_fit_steps", "google_fit_heart", "google_fit_general"]):
            summaries.append(
                self._build_plugin_summary(
                    plugin="google_fit",
                    monthly_file="google_fit_monthly_summary.sql",
                    category_file="google_fit_categories.sql",
                    params=params(),
                    top_n=top_n,
                )
            )
        else:
            data_gaps.append("google_fit tables missing; skipping.")

        for summary in summaries:
            trends = self._detect_trends(summary)
            emerging.extend([t for t in trends if t.direction == "emerging"])
            declining.extend([t for t in trends if t.direction == "declining"])

        themes = self._derive_themes(summaries, top_n)
        correlations = self._correlate_health_productivity(summaries)

        payload = ContextPayload(
            start_date=start_date,
            end_date=end_date,
            window_months=window_months,
            summaries=summaries,
            emerging=emerging[:top_n],
            declining=declining[:top_n],
            anomalies=[],
            data_gaps=data_gaps,
        )
        context_text = self._compact_context(
            payload,
            top_n=top_n,
            themes=themes,
            correlations=correlations,
        )
        return context_text, payload

    def generate_progress_summary(self, period: str = "last_month") -> str:
        start_date, end_date = self._date_range(period)
        context_text, payload = self.build_context(start_date, end_date)
        prompt = (
            "Produce a concise, data-grounded progress summary.\n"
            "Use this exact section order:\n"
            "1) Executive Summary (5–7 bullets, no fluff)\n"
            "2) Current Focus & Trajectory (top relevant themes only; label lifecycle: Active/Consolidated/Paused/Abandoned; trajectory: Rising/Stable/Fading)\n"
            "3) What’s Going Well (stable, low-friction patterns worth protecting)\n"
            "4) Recent Reality vs Baseline (recency-weighted; highlight divergence or say no strong divergence)\n"
            "5) Signals Worth Watching (emerging/declining; only if real growth; else say no strong signal)\n"
            "6) Strategic Options (2–3 choices tied to themes/trends; frame as options, not advice)\n"
            "7) Data Confidence & Caveats (what was strong, what was thin, what was omitted as low relevance)\n"
            "Rules: do NOT hallucinate. Cite sources (Asana, Toggl, Habitica, Google Fit). "
            "Inactivity is not failure; paused/abandoned are neutral. Do not highlight noise. "
            "Analyst tone; short, precise sentences; no prescriptions."
        )
        try:
            return self._ask_llm(context_text, prompt)
        except Exception as exc:
            logger.error("LLM call failed: %s", exc, exc_info=True)
            return self._fallback_summary(payload)

    def chat(self, question: str, period: str = "last_12_months") -> str:
        start_date, end_date = self._date_range(period)
        context_text, payload = self.build_context(start_date, end_date)
        prompt = question
        try:
            return self._ask_llm(context_text, prompt)
        except Exception as exc:
            logger.error("LLM call failed: %s", exc, exc_info=True)
            return self._fallback_summary(payload)

    def _build_plugin_summary(
        self,
        plugin: str,
        monthly_file: str,
        category_file: str,
        params: Dict[str, Any],
        top_n: int,
    ) -> PluginSummary:
        monthly = self.repo.get_monthly_summary(monthly_file, params)
        categories = self.repo.get_categories(category_file, {**params, "limit_rows": top_n * 2})
        monthly_points = [
            TrendPoint(
                period=row["period"],
                value=float(
                    row.get("entry_count")
                    or row.get("completed_tasks")
                    or row.get("steps_total")
                    or row.get("completed_items")
                    or 0
                ),
            )
            for row in monthly
        ]

        recent_30d, recent_90d = {}, {}
        if monthly:
            recent_30d = monthly[0]
            if len(monthly) > 1:
                recent_90d = monthly[1]

        return PluginSummary(
            plugin=plugin,
            monthly=monthly_points,
            recent_30d=recent_30d,
            recent_90d=recent_90d,
            categories=categories[:top_n],
            notes=[],
        )

    def _detect_trends(self, summary: PluginSummary) -> List[CategoryTrend]:
        trends: List[CategoryTrend] = []
        monthly = summary.monthly
        if len(monthly) < 2:
            return trends

        last = monthly[0].value
        prior = monthly[1].value
        if prior == 0 and last == 0:
            return trends
        change_pct = ((last - prior) / prior * 100) if prior else 100.0
        direction = "steady"
        if change_pct >= self.emerge_threshold_pct:
            direction = "emerging"
        elif change_pct <= -self.decline_threshold_pct:
            direction = "declining"

        trends.append(
            CategoryTrend(
                name=f"{summary.plugin}_overall",
                source=summary.plugin,
                last_value=last,
                prior_value=prior,
                change_pct=round(change_pct, 1),
                direction=direction,
            )
        )
        return trends

    def _normalize_tokens(self, text_val: str) -> List[str]:
        tokens = []
        for raw in text_val.replace("/", " ").replace("-", " ").replace("_", " ").split():
            t = raw.strip().lower()
            if not t or t in STOPWORDS:
                continue
            tokens.append(t)
        return tokens

    def _derive_themes(self, summaries: List[PluginSummary], top_n: int) -> List[Dict[str, Any]]:
        # Collect category names across plugins
        candidates = []
        plugin_recency = {s.plugin: self._recent_activity_factor(s) for s in summaries}
        for summary in summaries:
            for cat in summary.categories:
                name = str(cat.get("category") or cat.get("project") or cat.get("data_type") or "uncategorized")
                value = float(cat.get("items") or cat.get("total_minutes") or cat.get("records") or 0)
                candidates.append(
                    {
                        "name": name,
                        "value": value,
                        "source": summary.plugin,
                        "recency": plugin_recency.get(summary.plugin, 0.5),
                    }
                )

        # Simple token-based clustering
        clusters: List[Dict[str, Any]] = []
        for cand in sorted(candidates, key=lambda c: c["value"], reverse=True):
            tokens = set(self._normalize_tokens(cand["name"]))
            if not tokens:
                tokens = {cand["name"].lower()}
            placed = False
            for cluster in clusters:
                overlap = len(tokens & cluster["tokens"])
                union = len(tokens | cluster["tokens"])
                jaccard = overlap / union if union else 0
                if jaccard >= 0.3:
                    cluster["tokens"] |= tokens
                    cluster["total"] += cand["value"]
                    cluster["sources"].add(cand["source"])
                    cluster["names"].append(cand["name"])
                    cluster["recency"] = max(cluster["recency"], cand["recency"])
                    placed = True
                    break
            if not placed:
                clusters.append(
                    {
                        "tokens": tokens,
                        "total": cand["value"],
                        "sources": {cand["source"]},
                        "names": [cand["name"]],
                        "recency": cand["recency"],
                    }
                )

        themes = []
        total_value = sum(c["total"] for c in clusters) or 1
        for cluster in sorted(clusters, key=lambda c: c["total"], reverse=True):
            share = cluster["total"] / total_value
            relevance = self._relevance_score(share, cluster["recency"], consistency_weight=1.0)
            lifecycle, trajectory = self._classify_theme(share, cluster["recency"])
            if lifecycle == "noise":
                continue
            label = " / ".join(sorted(list(cluster["tokens"]))[:3])
            themes.append(
                {
                    "theme": label,
                    "share_pct": round(share * 100, 1),
                    "sources": sorted(list(cluster["sources"])),
                    "examples": cluster["names"][:3],
                    "relevance": round(relevance, 3),
                    "lifecycle": lifecycle,
                    "trajectory": trajectory,
                }
            )
        return sorted(themes, key=lambda t: t["relevance"], reverse=True)[:top_n]

    def _relevance_score(self, share: float, recency_weight: float, consistency_weight: float) -> float:
        # Favor recent and meaningful share; consistency weight currently neutral (1.0)
        return recency_weight * max(share, 0.01) * consistency_weight

    def _classify_theme(self, share: float, recency_weight: float) -> Tuple[str, str]:
        # lifecycle: active / consolidated / paused / abandoned / noise
        # trajectory: rising / stable / fading (based on recency)
        if share < 0.02:
            return "noise", "stable"
        if recency_weight >= 0.3:
            lifecycle = "active"
        elif recency_weight >= 0.1:
            lifecycle = "paused"
        elif recency_weight >= 0.05 and share >= 0.1:
            lifecycle = "consolidated"
        elif recency_weight < 0.05 and share >= 0.05:
            lifecycle = "abandoned"
        else:
            lifecycle = "noise"

        if recency_weight >= 0.25:
            trajectory = "rising"
        elif recency_weight <= 0.05:
            trajectory = "fading"
        else:
            trajectory = "stable"
        return lifecycle, trajectory

    def _recent_activity_factor(self, summary: PluginSummary) -> float:
        # Ratio of latest month to average of others
        if not summary.monthly:
            return 0.0
        last = summary.monthly[0].value
        prior_values = [p.value for p in summary.monthly[1:]] or [0]
        prior_avg = sum(prior_values) / len(prior_values)
        if prior_avg == 0:
            return 1.0 if last > 0 else 0.0
        return min(1.0, last / (prior_avg * 2))

    def _correlate_health_productivity(self, summaries: List[PluginSummary]) -> List[Dict[str, Any]]:
        def extract_series(plugin: str) -> List[Tuple[str, float]]:
            for s in summaries:
                if s.plugin == plugin:
                    return [(p.period, p.value) for p in s.monthly if p.value is not None]
            return []

        steps = extract_series("google_fit")
        tasks = extract_series("asana")
        time_entries = extract_series("toggl")

        correlations = []
        if steps and tasks:
            corr = self._pearson(steps, tasks)
            if corr is not None:
                correlations.append({"pair": "steps_vs_tasks", "r": corr, "source": ["google_fit", "asana"]})
        if steps and time_entries:
            corr = self._pearson(steps, time_entries)
            if corr is not None:
                correlations.append({"pair": "steps_vs_time", "r": corr, "source": ["google_fit", "toggl"]})
        return correlations

    def _pearson(self, a: List[Tuple[str, float]], b: List[Tuple[str, float]]) -> float | None:
        # align by period key
        b_map = {k: v for k, v in b}
        xs, ys = [], []
        for k, v in a:
            if k in b_map:
                xs.append(v)
                ys.append(b_map[k])
        n = len(xs)
        if n < 3:
            return None
        mean_x = sum(xs) / n
        mean_y = sum(ys) / n
        num = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys))
        den = math.sqrt(sum((x - mean_x) ** 2 for x in xs) * sum((y - mean_y) ** 2 for y in ys))
        if den == 0:
            return None
        return round(num / den, 3)

    def _compact_context(
        self,
        payload: ContextPayload,
        top_n: int,
        themes: List[Dict[str, Any]],
        correlations: List[Dict[str, Any]],
    ) -> str:
        def clamp(text: str) -> str:
            if len(text) <= self.max_context_chars:
                return text
            return text[: self.max_context_chars] + "... (truncated)"

        summaries_block = []
        for summary in payload.summaries:
            cat_display = summary.categories[:top_n]
            summaries_block.append(
                {
                    "plugin": summary.plugin,
                    "monthly": [{tp.period: tp.value} for tp in summary.monthly[: payload.window_months]],
                    "categories": cat_display,
                    "recent_30d": summary.recent_30d,
                    "recent_90d": summary.recent_90d,
                }
            )

        context = {
            "date_range": {"start": str(payload.start_date), "end": str(payload.end_date)},
            "summaries": summaries_block,
            "themes": themes,
            "emerging": [trend.__dict__ for trend in payload.emerging],
            "declining": [trend.__dict__ for trend in payload.declining],
            "correlations": correlations,
            "anomalies": payload.anomalies,
            "data_gaps": payload.data_gaps,
            "source_notes": "All numbers are read-only rollups from plugin tables; source keys match plugin names. Only top relevant themes are surfaced; noise omitted.",
        }

        def _encode(obj):
            if isinstance(obj, Decimal):
                return float(obj)
            return str(obj)

        context_text = json.dumps(context, ensure_ascii=False, default=_encode)
        return clamp(context_text)

    def _ask_llm(self, context_text: str, prompt: str) -> str:
        messages = [
            ChatMessage(
                role="system",
                content=(
                    "You are an analytical assistant summarizing quantified behavior data. "
                    "Rules: do NOT hallucinate. Base every claim on provided numbers. "
                    "Cite sources explicitly (Asana, Toggl, Habitica, Google Fit). "
                    "Be concise, direct, and practical. If data is missing or weak, say so. "
                    "No generic motivational advice."
                ),
            ),
            ChatMessage(role="user", content=f"Context:\n{context_text}\n\nQuestion: {prompt}"),
        ]
        req = ChatRequest(
            messages=messages,
            model=self.model,
            temperature=self.temperature,
            max_tokens=self.max_tokens,
        )
        payload = {
            "model": req.model,
            "messages": [m.__dict__ for m in req.messages],
            "temperature": req.temperature,
            "max_tokens": req.max_tokens,
            "stream": False,
        }
        start = time.time()
        resp = requests.post(self.base_url, json=payload, timeout=self.timeout)
        elapsed = time.time() - start
        resp.raise_for_status()
        data = resp.json()
        content = data.get("choices", [{}])[0].get("message", {}).get("content", "")
        logger.info("LLM call complete (%.2fs)", elapsed)
        return ChatResponse(content=content, raw=data).content

    def _fallback_summary(self, payload: ContextPayload) -> str:
        lines = ["LLM unavailable; local fallback summary."]
        for summary in payload.summaries:
            lines.append(
                f"- {summary.plugin}: {len(summary.monthly)} months, "
                f"latest={summary.monthly[0].value if summary.monthly else 0}, "
                f"top categories: {summary.categories[:3]}"
            )
        if payload.emerging:
            lines.append("Emerging: " + ", ".join([t.name for t in payload.emerging]))
        if payload.declining:
            lines.append("Declining: " + ", ".join([t.name for t in payload.declining]))
        if payload.data_gaps:
            lines.append("Data gaps: " + "; ".join(payload.data_gaps))
        return "\n".join(lines)

    def _date_range(self, period: str) -> Tuple[date, date]:
        end = datetime.utcnow().date()
        if period == "last_month":
            start = end - timedelta(days=30)
        elif period == "last_90_days":
            start = end - timedelta(days=90)
        else:
            start = end - timedelta(days=365)
        return start, end
