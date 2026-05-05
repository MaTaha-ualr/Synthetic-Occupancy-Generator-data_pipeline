"""LLM provider adapters for quality-first hosted model APIs.

The SOG assistant is tool-heavy, so provider defaults intentionally use only
approved top-tier models. Fast/basic role-specific model downgrades are not
allowed in the app configuration.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from typing import Any

from .guardrails import redact_secrets


@dataclass(frozen=True)
class ProviderProfile:
    provider: str
    label: str
    api_key_env: str
    base_url: str | None
    quality_model: str
    api_key_placeholder: str
    description: str
    extra_headers: dict[str, str] = field(default_factory=dict)
    extra_body: dict[str, Any] = field(default_factory=dict)
    timeout_seconds: int = 90
    temperature: float = 0.2


_PROFILES: dict[str, ProviderProfile] = {
    "anthropic": ProviderProfile(
        provider="anthropic",
        label="Anthropic Claude",
        api_key_env="ANTHROPIC_API_KEY",
        base_url=None,
        quality_model="claude-opus-4-7",
        api_key_placeholder="sk-ant-...",
        description="Premium Anthropic backend using Claude Opus only.",
    ),
    "together": ProviderProfile(
        provider="together",
        label="Together AI",
        api_key_env="TOGETHER_API_KEY",
        base_url="https://api.together.ai/v1",
        quality_model="zai-org/GLM-5.1",
        api_key_placeholder="tgp_...",
        description="Hosted open-model backend using GLM-5.1 for every assistant role.",
    ),
    "nvidia": ProviderProfile(
        provider="nvidia",
        label="NVIDIA NIM",
        api_key_env="NVIDIA_API_KEY",
        base_url="https://integrate.api.nvidia.com/v1",
        quality_model="moonshotai/kimi-k2.6",
        api_key_placeholder="nvapi-...",
        description="Hosted NVIDIA NIM endpoint using Moonshot Kimi K2.6 for every assistant role.",
        extra_headers={"Accept": "text/event-stream"},
        extra_body={"stream": True, "chat_template_kwargs": {"thinking": False}},
        timeout_seconds=180,
        temperature=0.0,
    ),
}

_ALIASES = {
    "claude": "anthropic",
    "anthropic": "anthropic",
    "togetherai": "together",
    "together": "together",
    "nvidia": "nvidia",
    "nim": "nvidia",
    "nvidia_nim": "nvidia",
    "kimi": "nvidia",
    "moonshot": "nvidia",
}


@dataclass(frozen=True)
class LLMProviderConfig:
    provider: str
    label: str
    api_key: str
    api_key_env: str
    model: str
    base_url: str | None = None
    timeout_seconds: int = 90
    temperature: float = 0.2
    extra_headers: dict[str, str] = field(default_factory=dict)
    extra_body: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class NormalizedToolCall:
    id: str
    name: str
    input: dict[str, Any]
    raw: Any = None


@dataclass(frozen=True)
class LLMResponse:
    text: str
    stop_reason: str
    tool_calls: list[NormalizedToolCall] = field(default_factory=list)
    raw_content: Any = None
    raw_message: dict[str, Any] | None = None


def available_provider_ids() -> list[str]:
    return list(_PROFILES)


def normalize_provider(provider: str | None = None) -> str:
    raw = (provider or os.environ.get("SOG_LLM_PROVIDER") or "together").strip().lower()
    normalized = _ALIASES.get(raw, raw)
    if normalized not in _PROFILES:
        raise ValueError(
            f"Unsupported quality-first SOG_LLM_PROVIDER '{provider or raw}'. "
            f"Choose one of: {', '.join(available_provider_ids())}. "
            "Fast/basic hosted routes are intentionally disabled."
        )
    return normalized


def provider_profile(provider: str | None = None) -> ProviderProfile:
    return _PROFILES[normalize_provider(provider)]


def provider_label(provider: str | None = None) -> str:
    return provider_profile(provider).label


def provider_api_key_env(provider: str | None = None) -> str:
    return provider_profile(provider).api_key_env


def provider_api_key_placeholder(provider: str | None = None) -> str:
    return provider_profile(provider).api_key_placeholder


def has_provider_credentials(provider: str | None = None) -> bool:
    profile = provider_profile(provider)
    return bool(os.environ.get(profile.api_key_env) or os.environ.get("SOG_LLM_API_KEY"))


def _validate_quality_model(profile: ProviderProfile, model: str) -> str:
    if model != profile.quality_model:
        raise ValueError(
            f"Model '{model}' is not allowed for provider '{profile.provider}'. "
            f"Quality-first policy requires '{profile.quality_model}'."
        )
    return model


def resolve_llm_config(
    *,
    provider: str | None = None,
    api_key: str | None = None,
    model: str | None = None,
) -> LLMProviderConfig:
    profile = provider_profile(provider)
    resolved_key = api_key or os.environ.get(profile.api_key_env) or os.environ.get("SOG_LLM_API_KEY", "")
    if not resolved_key:
        raise ValueError(f"{profile.api_key_env} not set")

    for disabled_env in ("SOG_LLM_SMART_MODEL", "SOG_LLM_FAST_MODEL", "SOG_LLM_CLASSIFY_MODEL"):
        if os.environ.get(disabled_env):
            raise ValueError(
                f"{disabled_env} is disabled by the quality-first model policy. "
                "Use SOG_LLM_MODEL with the approved provider model instead."
            )

    resolved_model = (model or os.environ.get("SOG_LLM_MODEL", "") or profile.quality_model).strip()
    resolved_model = _validate_quality_model(profile, resolved_model)

    base_url = profile.base_url
    if os.environ.get("SOG_LLM_BASE_URL"):
        base_url = os.environ["SOG_LLM_BASE_URL"].strip()

    return LLMProviderConfig(
        provider=profile.provider,
        label=profile.label,
        api_key=resolved_key,
        api_key_env=profile.api_key_env,
        model=resolved_model,
        base_url=base_url,
        timeout_seconds=int(os.environ.get("SOG_LLM_TIMEOUT_SECONDS", str(profile.timeout_seconds))),
        temperature=float(os.environ.get("SOG_LLM_TEMPERATURE", str(profile.temperature))),
        extra_headers=dict(profile.extra_headers),
        extra_body=dict(profile.extra_body),
    )


def _anthropic_tool_results(tool_results: list[dict[str, str]]) -> list[dict[str, Any]]:
    return [
        {
            "role": "user",
            "content": [
                {
                    "type": "tool_result",
                    "tool_use_id": item["tool_call_id"],
                    "content": item["content"],
                }
                for item in tool_results
            ],
        }
    ]


class AnthropicLLMClient:
    def __init__(self, config: LLMProviderConfig):
        import anthropic

        self.config = config
        self._client = anthropic.Anthropic(api_key=config.api_key)

    def complete(
        self,
        *,
        messages: list[dict[str, Any]],
        system: str,
        tools: list[dict[str, Any]],
        max_tokens: int,
    ) -> LLMResponse:
        kwargs: dict[str, Any] = {
            "model": self.config.model,
            "max_tokens": max_tokens,
            "system": system,
            "messages": messages,
        }
        if tools:
            kwargs["tools"] = tools
        response = self._client.messages.create(**kwargs)

        text_parts: list[str] = []
        tool_calls: list[NormalizedToolCall] = []
        for block in response.content:
            if getattr(block, "type", None) == "tool_use":
                tool_calls.append(
                    NormalizedToolCall(
                        id=str(block.id),
                        name=str(block.name),
                        input=dict(block.input or {}),
                        raw=block,
                    )
                )
            elif hasattr(block, "text"):
                text_parts.append(str(block.text))

        stop_reason = "tool_use" if response.stop_reason == "tool_use" and tool_calls else str(response.stop_reason or "end_turn")
        return LLMResponse(
            text="\n".join(text_parts).strip(),
            stop_reason=stop_reason,
            tool_calls=tool_calls,
            raw_content=response.content,
        )

    def assistant_message(self, response: LLMResponse) -> dict[str, Any]:
        return {"role": "assistant", "content": response.raw_content}

    def tool_result_messages(self, tool_results: list[dict[str, str]]) -> list[dict[str, Any]]:
        return _anthropic_tool_results(tool_results)


def _openai_tools(tools: list[dict[str, Any]]) -> list[dict[str, Any]]:
    converted = []
    for tool in tools:
        converted.append(
            {
                "type": "function",
                "function": {
                    "name": tool.get("name", ""),
                    "description": tool.get("description", ""),
                    "parameters": tool.get("input_schema", {"type": "object", "properties": {}}),
                },
            }
        )
    return converted


class OpenAICompatibleLLMClient:
    def __init__(self, config: LLMProviderConfig):
        if not config.base_url:
            raise ValueError("OpenAI-compatible provider requires a base URL")
        self.config = config

    def complete(
        self,
        *,
        messages: list[dict[str, Any]],
        system: str,
        tools: list[dict[str, Any]],
        max_tokens: int,
    ) -> LLMResponse:
        import requests

        api_messages = [{"role": "system", "content": system}, *messages]
        payload: dict[str, Any] = {
            "model": self.config.model,
            "messages": api_messages,
            "max_tokens": max_tokens,
            "temperature": self.config.temperature,
        }
        payload.update(self.config.extra_body)
        if tools:
            payload["tools"] = _openai_tools(tools)
            payload["tool_choice"] = "auto"

        headers = {
            "Authorization": f"Bearer {self.config.api_key}",
            "Content-Type": "application/json",
            **self.config.extra_headers,
        }
        endpoint = f"{self.config.base_url.rstrip('/')}/chat/completions"
        response = requests.post(
            endpoint,
            headers=headers,
            json=payload,
            stream=bool(payload.get("stream")),
            timeout=self.config.timeout_seconds,
        )
        if response.status_code >= 400:
            raise RuntimeError(
                f"{self.config.label} request failed with HTTP {response.status_code}: "
                f"{redact_secrets(response.text[:500])}"
            )
        if payload.get("stream"):
            return self._parse_streaming_response(response)

        data = response.json()
        try:
            choice = data["choices"][0]
            message = choice.get("message", {})
        except Exception as exc:
            raise RuntimeError(f"{self.config.label} returned an unexpected response shape: {exc}") from exc

        tool_calls: list[NormalizedToolCall] = []
        for item in message.get("tool_calls") or []:
            function = item.get("function", {}) if isinstance(item, dict) else {}
            raw_args = function.get("arguments") or "{}"
            parsed_args: dict[str, Any]
            try:
                loaded = json.loads(raw_args) if isinstance(raw_args, str) else raw_args
                parsed_args = loaded if isinstance(loaded, dict) else {}
            except Exception:
                parsed_args = {}
            tool_calls.append(
                NormalizedToolCall(
                    id=str(item.get("id", f"tool-{len(tool_calls) + 1}")),
                    name=str(function.get("name", "")),
                    input=parsed_args,
                    raw=item,
                )
            )

        finish_reason = str(choice.get("finish_reason") or "")
        stop_reason = "tool_use" if tool_calls else (finish_reason or "end_turn")
        return LLMResponse(
            text=str(message.get("content") or "").strip(),
            stop_reason=stop_reason,
            tool_calls=tool_calls,
            raw_message=message,
        )

    def _parse_streaming_response(self, response: Any) -> LLMResponse:
        content_parts: list[str] = []
        reasoning_parts: list[str] = []
        tool_builders: dict[int, dict[str, Any]] = {}
        finish_reason = ""

        for line in response.iter_lines():
            if not line:
                continue
            decoded = line.decode("utf-8") if isinstance(line, bytes) else str(line)
            decoded = decoded.strip()
            if not decoded:
                continue
            if decoded == "data: [DONE]":
                break
            if not decoded.startswith("data: "):
                continue

            try:
                chunk = json.loads(decoded[6:])
            except json.JSONDecodeError:
                continue
            choices = chunk.get("choices") or []
            if not choices:
                continue
            choice = choices[0]
            finish_reason = str(choice.get("finish_reason") or finish_reason)
            delta = choice.get("delta") or {}

            content = delta.get("content")
            if content:
                content_parts.append(str(content))
            reasoning_content = delta.get("reasoning_content")
            if reasoning_content:
                reasoning_parts.append(str(reasoning_content))

            for item in delta.get("tool_calls") or []:
                try:
                    index = int(item.get("index", len(tool_builders)))
                except Exception:
                    index = len(tool_builders)
                builder = tool_builders.setdefault(
                    index,
                    {"id": "", "type": "function", "function": {"name": "", "arguments": ""}},
                )
                if item.get("id"):
                    builder["id"] = str(item["id"])
                if item.get("type"):
                    builder["type"] = str(item["type"])
                function = item.get("function") or {}
                if function.get("name"):
                    builder["function"]["name"] += str(function["name"])
                if function.get("arguments") is not None:
                    builder["function"]["arguments"] += str(function["arguments"])

        message: dict[str, Any] = {
            "role": "assistant",
            "content": "".join(content_parts) or None,
        }
        if reasoning_parts:
            message["reasoning_content"] = "".join(reasoning_parts)
        if tool_builders:
            message["tool_calls"] = [
                {
                    "id": value.get("id") or f"tool-{index + 1}",
                    "type": value.get("type") or "function",
                    "function": value.get("function") or {"name": "", "arguments": "{}"},
                }
                for index, value in sorted(tool_builders.items())
            ]

        tool_calls: list[NormalizedToolCall] = []
        for item in message.get("tool_calls") or []:
            function = item.get("function", {}) if isinstance(item, dict) else {}
            raw_args = function.get("arguments") or "{}"
            try:
                loaded = json.loads(raw_args) if isinstance(raw_args, str) else raw_args
                parsed_args = loaded if isinstance(loaded, dict) else {}
            except Exception:
                parsed_args = {}
            tool_calls.append(
                NormalizedToolCall(
                    id=str(item.get("id", f"tool-{len(tool_calls) + 1}")),
                    name=str(function.get("name", "")),
                    input=parsed_args,
                    raw=item,
                )
            )

        return LLMResponse(
            text=str(message.get("content") or "").strip(),
            stop_reason="tool_use" if tool_calls else (finish_reason or "end_turn"),
            tool_calls=tool_calls,
            raw_message=message,
        )

    def assistant_message(self, response: LLMResponse) -> dict[str, Any]:
        message = dict(response.raw_message or {})
        message.setdefault("role", "assistant")
        message.setdefault("content", response.text or "")
        return message

    def tool_result_messages(self, tool_results: list[dict[str, str]]) -> list[dict[str, Any]]:
        return [
            {
                "role": "tool",
                "tool_call_id": item["tool_call_id"],
                "name": item["name"],
                "content": item["content"],
            }
            for item in tool_results
        ]


def build_llm_client(config: LLMProviderConfig):
    if config.provider == "anthropic":
        return AnthropicLLMClient(config)
    return OpenAICompatibleLLMClient(config)
