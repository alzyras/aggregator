from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from typing import Any, Protocol
from urllib.parse import urlparse

import requests

from intelligence.models import WorkspaceAISettings

OPENAI_RESPONSES_URL = "https://api.openai.com/v1/responses"
DEFAULT_OPENAI_MODEL = "gpt-5.6-luna"
DEFAULT_QWEN_MODEL = "qwen3"
DEFAULT_TIMEOUT_SECONDS = 60


class AIBackendError(RuntimeError):
    """An AI provider received a request but could not complete it."""


class AIBackendNotConfigured(AIBackendError):
    """No enabled model backend is available for the workspace."""


class AIBackend(Protocol):
    backend_id: str
    model: str

    def complete(
        self,
        *,
        instructions: str,
        messages: list[dict[str, str]],
        max_output_tokens: int,
    ) -> "AIResult":
        ...


@dataclass(frozen=True)
class AIResult:
    text: str
    model: str
    provider_response_id: str = ""
    usage: dict[str, Any] | None = None


class OpenAIResponsesBackend:
    backend_id = WorkspaceAISettings.BACKEND_OPENAI_RESPONSES

    def __init__(
        self,
        *,
        api_key: str,
        model: str,
        session: requests.Session | None = None,
    ) -> None:
        self.api_key = api_key
        self.model = model
        self.session = session or requests.Session()

    def complete(
        self,
        *,
        instructions: str,
        messages: list[dict[str, str]],
        max_output_tokens: int,
    ) -> AIResult:
        payload = {
            "model": self.model,
            "store": False,
            "instructions": instructions,
            "input": messages,
            "max_output_tokens": max_output_tokens,
            "text": {"verbosity": "low"},
        }
        try:
            response = self.session.post(
                OPENAI_RESPONSES_URL,
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json",
                },
                json=payload,
                timeout=DEFAULT_TIMEOUT_SECONDS,
            )
        except requests.RequestException as exc:
            raise AIBackendError("The OpenAI service could not be reached.") from exc
        body = _response_json(response)
        if response.status_code >= 400:
            raise AIBackendError(_provider_error(body, "The OpenAI service rejected the request."))
        text = _responses_text(body)
        if not text:
            raise AIBackendError("The OpenAI service returned no text.")
        return AIResult(
            text=text,
            model=str(body.get("model") or self.model),
            provider_response_id=str(body.get("id") or ""),
            usage=body.get("usage") if isinstance(body.get("usage"), dict) else {},
        )


class OpenAICompatibleBackend:
    backend_id = WorkspaceAISettings.BACKEND_OPENAI_COMPATIBLE

    def __init__(
        self,
        *,
        base_url: str,
        model: str,
        api_key: str | None = None,
        session: requests.Session | None = None,
    ) -> None:
        self.endpoint = _compatible_completion_url(base_url)
        self.model = model
        self.api_key = api_key or ""
        self.session = session or requests.Session()

    def complete(
        self,
        *,
        instructions: str,
        messages: list[dict[str, str]],
        max_output_tokens: int,
    ) -> AIResult:
        payload = {
            "model": self.model,
            "messages": [{"role": "system", "content": instructions}, *messages],
            "temperature": 0.2,
            "max_tokens": max_output_tokens,
        }
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        try:
            response = self.session.post(
                self.endpoint,
                headers=headers,
                json=payload,
                timeout=DEFAULT_TIMEOUT_SECONDS,
            )
        except requests.RequestException as exc:
            raise AIBackendError("The local AI service could not be reached.") from exc
        body = _response_json(response)
        if response.status_code >= 400:
            raise AIBackendError(_provider_error(body, "The local AI service rejected the request."))
        text = _compatible_text(body)
        if not text:
            raise AIBackendError("The local AI service returned no text.")
        return AIResult(
            text=text,
            model=str(body.get("model") or self.model),
            provider_response_id=str(body.get("id") or ""),
            usage=body.get("usage") if isinstance(body.get("usage"), dict) else {},
        )


def get_workspace_backend(workspace) -> AIBackend:
    settings = WorkspaceAISettings.objects.for_workspace(workspace).first()
    if settings and not settings.is_enabled:
        raise AIBackendNotConfigured("AI enrichment is disabled for this workspace.")

    backend = settings.backend if settings else _default_backend()
    if backend == WorkspaceAISettings.BACKEND_OPENAI_COMPATIBLE:
        base_url = (settings.base_url if settings else "") or os.getenv("AI_QWEN_BASE_URL", "")
        if not base_url:
            raise AIBackendNotConfigured(
                "Add the Qwen server URL in AI settings before running enrichment."
            )
        return OpenAICompatibleBackend(
            base_url=base_url,
            model=(settings.model if settings else "") or os.getenv("AI_QWEN_MODEL", DEFAULT_QWEN_MODEL),
            api_key=_workspace_key(settings) or os.getenv("AI_QWEN_API_KEY", ""),
        )

    if backend != WorkspaceAISettings.BACKEND_OPENAI_RESPONSES:
        raise AIBackendNotConfigured("Choose a supported AI backend in AI settings.")
    api_key = _workspace_key(settings) or os.getenv("OPENAI_API_KEY", "")
    if not api_key:
        raise AIBackendNotConfigured(
            "Add an OpenAI API key in AI settings or set OPENAI_API_KEY on the server."
        )
    return OpenAIResponsesBackend(
        api_key=api_key,
        model=(settings.model if settings else "") or os.getenv("OPENAI_CHAT_MODEL", DEFAULT_OPENAI_MODEL),
    )


def backend_configuration(workspace) -> dict[str, str | bool]:
    try:
        backend = get_workspace_backend(workspace)
    except AIBackendNotConfigured as exc:
        return {"configured": False, "backend": "", "model": "", "message": str(exc)}
    return {
        "configured": True,
        "backend": backend.backend_id,
        "model": backend.model,
        "message": "",
    }


def stable_safety_identifier(*, workspace_id: int, user_id: int) -> str:
    raw = f"aggregator:{workspace_id}:{user_id}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()[:48]


def _default_backend() -> str:
    value = os.getenv("AI_DEFAULT_BACKEND", WorkspaceAISettings.BACKEND_OPENAI_RESPONSES)
    return value.strip().lower() or WorkspaceAISettings.BACKEND_OPENAI_RESPONSES


def _workspace_key(settings: WorkspaceAISettings | None) -> str:
    if settings is None or not settings.encrypted_api_key:
        return ""
    return settings.get_api_key() or ""


def _compatible_completion_url(base_url: str) -> str:
    normalized = base_url.strip().rstrip("/")
    parsed = urlparse(normalized)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc or parsed.username or parsed.password:
        raise AIBackendNotConfigured("The local AI server URL must be a valid http(s) URL.")
    if normalized.endswith("/chat/completions"):
        return normalized
    if normalized.endswith("/v1"):
        return f"{normalized}/chat/completions"
    return f"{normalized}/v1/chat/completions"


def _response_json(response: requests.Response) -> dict[str, Any]:
    try:
        body = response.json()
    except ValueError as exc:
        raise AIBackendError("The AI service returned an unreadable response.") from exc
    if not isinstance(body, dict):
        raise AIBackendError("The AI service returned an invalid response.")
    return body


def _provider_error(body: dict[str, Any], fallback: str) -> str:
    error = body.get("error")
    if isinstance(error, dict) and isinstance(error.get("message"), str):
        return error["message"][:400]
    if isinstance(error, str):
        return error[:400]
    return fallback


def _responses_text(body: dict[str, Any]) -> str:
    chunks: list[str] = []
    for item in body.get("output", []):
        if not isinstance(item, dict) or item.get("type") != "message":
            continue
        for content in item.get("content", []):
            if isinstance(content, dict) and content.get("type") == "output_text":
                text = content.get("text")
                if isinstance(text, str) and text.strip():
                    chunks.append(text.strip())
    return "\n\n".join(chunks)


def _compatible_text(body: dict[str, Any]) -> str:
    choices = body.get("choices")
    if not isinstance(choices, list) or not choices:
        return ""
    first = choices[0]
    if not isinstance(first, dict):
        return ""
    message = first.get("message")
    if not isinstance(message, dict):
        return ""
    content = message.get("content")
    if isinstance(content, str):
        return content.strip()
    if isinstance(content, list):
        return "\n".join(
            str(part.get("text") or "").strip()
            for part in content
            if isinstance(part, dict) and str(part.get("text") or "").strip()
        )
    return ""


def parse_json_object(text: str) -> dict[str, Any]:
    normalized = text.strip()
    if normalized.startswith("```"):
        lines = normalized.splitlines()
        normalized = "\n".join(
            line for line in lines if not line.strip().startswith("```")
        ).strip()
    try:
        value = json.loads(normalized)
    except json.JSONDecodeError as exc:
        raise AIBackendError("The AI response was not valid JSON.") from exc
    if not isinstance(value, dict):
        raise AIBackendError("The AI response must be a JSON object.")
    return value
