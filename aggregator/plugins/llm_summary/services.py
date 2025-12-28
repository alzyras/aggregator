import json
import logging
import math
import time
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
        # Read-only plugin; nothing to set up.
        return True

    def fetch_data(self):
        # Not used in the aggregation loop; functionality is exposed via chat/generate methods.
        return None

    def write_data(self, payload) -> Tuple[int, int]:
        # Read-only plugin; nothing to write.
        return (0, 0)

    # Public API
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
        context_text = self._compact_context(payload, top_n=top_n)
        return context_text, payload

    def generate_progress_summary(self, period: str = "last_month") -> str:
        start_date, end_date = self._date_range(period)
        context_text, payload = self.build_context(start_date, end_date)
        prompt = (
            "Provide a concise progress summary based on the context.\n"
            "Sections: Executive summary (5-8 bullets), Categories, Emerging, Suggestions, Data caveats.\n"
            "Cite sources."
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

    # Internals
    def _build_plugin_summary(
        self,
        plugin: str,
        monthly_file: str,
        category_file: str,
        params: Dict[str, Any],
        top_n: int,
    ) -> PluginSummary:
        monthly = self.repo.get_monthly_summary(monthly_file, params)
        categories = self.repo.get_categories(category_file, {**params, "limit_rows": top_n})
        monthly_points = [
            TrendPoint(period=row["period"], value=float(row.get("entry_count") or row.get("completed_tasks") or row.get("steps_total") or row.get("completed_items") or 0))
            for row in monthly
        ]

        # Recent snapshots
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
            categories=categories,
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

    def _compact_context(self, payload: ContextPayload, top_n: int) -> str:
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
            "emerging": [trend.__dict__ for trend in payload.emerging],
            "declining": [trend.__dict__ for trend in payload.declining],
            "anomalies": payload.anomalies,
            "data_gaps": payload.data_gaps,
            "source_notes": "All numbers are read-only rollups from plugin tables; source keys match plugin names.",
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
                    "You summarize user productivity and health data. "
                    "Be concise, factual, and cite sources (asana, toggl, habitica, google_fit). "
                    "If data is missing, say so. No hallucinations."
                ),
            ),
            ChatMessage(
                role="system",
                content="Use sections: Executive summary (5-8 bullets), Categories, Emerging, Suggestions, Data caveats.",
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
            lines.append(f"- {summary.plugin}: {len(summary.monthly)} months, top categories: {summary.categories[:3]}")
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
