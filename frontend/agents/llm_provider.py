"""LLM provider adapters for Anthropic and hosted open-model APIs.

Most hosted open-weight providers expose OpenAI-compatible chat completions.
This module keeps the rest of the agent code provider-neutral while preserving
Anthropic as the default backend.
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
    smart_model: str
    fast_model: str
    classify_model: str
    api_key_placeholder: str
    description: str


_PROFILES: dict[str, ProviderProfile] = {
    "anthropic": ProviderProfile(
        provider="anthropic",
        label="Anthropic Claude",
        api_key_env="ANTHROPIC_API_KEY",
        base_url=None,
        smart_model="claude-sonnet-4-6",
        fast_model="claude-haiku-4-5-20251001",
        classify_model="claude-haiku-4-5-20251001",
        api_key_placeholder="sk-ant-...",
        description="Default proprietary backend already supported by the app.",
    ),
    "groq": ProviderProfile(
        provider="groq",
        label="Groq hosted open models",
        api_key_env="GROQ_API_KEY",
        base_url="https://api.groq.com/openai/v1",
        smart_model="openai/gpt-oss-120b",
        fast_model="llama-3.1-8b-instant",
        classify_model="llama-3.1-8b-instant",
        api_key_placeholder="gsk_...",
        description="Fast hosted open-model inference with OpenAI-compatible tool calling.",
    ),
    "together": ProviderProfile(
        provider="together",
        label="Together AI",
        api_key_env="TOGETHER_API_KEY",
        base_url="https://api.together.ai/v1",
        smart_model="zai-org/GLM-5.1",
        fast_model="meta-llama/Llama-3.3-70B-Instruct-Turbo",
        classify_model="meta-llama/Llama-3.3-70B-Instruct-Turbo",
        api_key_placeholder="tgp_...",
        description="Hosted serverless open-source models; GLM-5.1 is the default tool-calling model.",
    ),
    "fireworks": ProviderProfile(
        provider="fireworks",
        label="Fireworks AI",
        api_key_env="FIREWORKS_API_KEY",
        base_url="https://api.fireworks.ai/inference/v1",
        smart_model="accounts/fireworks/models/kimi-k2-instruct-0905",
        fast_model="accounts/fireworks/models/llama-v3p3-70b-instruct",
        classify_model="accounts/fireworks/models/llama-v3p1-8b-instruct",
        api_key_placeholder="fw_...",
        description="Open-source model platform with OpenAI-compatible tools and structured outputs.",
    ),
    "huggingface": ProviderProfile(
        provider="huggingface",
        label="Hugging Face Inference Providers",
        api_key_env="HF_TOKEN",
        base_url="https://router.huggingface.co/v1",
        smart_model="meta-llama/Llama-3.3-70B-Instruct:fastest",
        fast_model="meta-llama/Llama-3.1-8B-Instruct:fastest",
        classify_model="meta-llama/Llama-3.1-8B-Instruct:fastest",
        api_key_placeholder="hf_...",
        description="Hosted model router across Hugging Face inference providers.",
    ),
    "openrouter": ProviderProfile(
        provider="openrouter",
        label="OpenRouter",
        api_key_env="OPENROUTER_API_KEY",
        base_url="https://openrouter.ai/api/v1",
        smart_model="openai/gpt-oss-120b",
        fast_model="meta-llama/llama-3.1-8b-instruct",
        classify_model="meta-llama/llama-3.1-8b-instruct",
        api_key_placeholder="sk-or-...",
        description="Aggregated hosted model router with OpenAI-compatible metadata and chat APIs.",
    ),
    "openai_compatible": ProviderProfile(
        provider="openai_compatible",
        label="Custom OpenAI-compatible",
        api_key_env="SOG_OPENAI_COMPAT_API_KEY",
        base_url=None,
        smart_model="",
        fast_model="",
        classify_model="",
        api_key_placeholder="provider key",
        description="Any hosted /v1/chat/completions endpoint that follows the OpenAI tool-call schema.",
    ),
}

_ALIASES = {
    "claude": "anthropic",
    "anthropic": "anthropic",
    "groq": "groq",
    "togetherai": "together",
    "together": "together",
    "fireworksai": "fireworks",
    "fireworks": "fireworks",
    "hf": "huggingface",
    "huggingface": "huggingface",
    "hugging_face": "huggingface",
    "openrouter": "openrouter",
    "openai-compatible": "openai_compatible",
    "openai_compatible": "openai_compatible",
    "custom": "openai_compatible",
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
    raw = (provider or os.environ.get("SOG_LLM_PROVIDER") or "anthropic").strip().lower()
    normalized = _ALIASES.get(raw, raw)
    if normalized not in _PROFILES:
        raise ValueError(
            f"Unsupported SOG_LLM_PROVIDER '{provider or raw}'. "
            f"Choose one of: {', '.join(available_provider_ids())}"
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
    has_key = bool(os.environ.get(profile.api_key_env) or os.environ.get("SOG_LLM_API_KEY"))
    if profile.provider == "openai_compatible":
        has_base_url = bool(os.environ.get("SOG_OPENAI_COMPAT_BASE_URL") or os.environ.get("SOG_LLM_BASE_URL"))
        return has_key and has_base_url
    return has_key


def _role_model_env(model_role: str) -> str:
    role = (model_role or "smart").strip().lower()
    if role == "fast":
        return "SOG_LLM_FAST_MODEL"
    if role == "classify":
        return "SOG_LLM_CLASSIFY_MODEL"
    return "SOG_LLM_SMART_MODEL"


def _profile_default_model(profile: ProviderProfile, model_role: str) -> str:
    role = (model_role or "smart").strip().lower()
    if role == "fast":
        return profile.fast_model
    if role == "classify":
        return profile.classify_model
    return profile.smart_model


def resolve_llm_config(
    *,
    provider: str | None = None,
    api_key: str | None = None,
    model: str | None = None,
    model_role: str = "smart",
) -> LLMProviderConfig:
    profile = provider_profile(provider)
    resolved_key = api_key or os.environ.get(profile.api_key_env) or os.environ.get("SOG_LLM_API_KEY", "")
    if not resolved_key:
        raise ValueError(f"{profile.api_key_env} not set")

    role_model = os.environ.get(_role_model_env(model_role), "").strip()
    default_model = _profile_default_model(profile, model_role)
    resolved_model = (model or role_model or os.environ.get("SOG_LLM_MODEL", "") or default_model).strip()
    if not resolved_model:
        raise ValueError(
            f"No model configured for provider '{profile.provider}'. "
            "Set SOG_LLM_MODEL or a role-specific model env var."
        )

    base_url = profile.base_url
    if profile.provider == "openai_compatible":
        base_url = (
            os.environ.get("SOG_OPENAI_COMPAT_BASE_URL")
            or os.environ.get("SOG_LLM_BASE_URL")
            or ""
        ).strip()
        if not base_url:
            raise ValueError("SOG_OPENAI_COMPAT_BASE_URL or SOG_LLM_BASE_URL must be set")
    elif os.environ.get("SOG_LLM_BASE_URL"):
        base_url = os.environ["SOG_LLM_BASE_URL"].strip()

    headers: dict[str, str] = {}
    if profile.provider == "openrouter":
        if os.environ.get("OPENROUTER_HTTP_REFERER"):
            headers["HTTP-Referer"] = os.environ["OPENROUTER_HTTP_REFERER"]
        headers["X-Title"] = os.environ.get("OPENROUTER_APP_TITLE", "SOG Benchmark Studio")

    return LLMProviderConfig(
        provider=profile.provider,
        label=profile.label,
        api_key=resolved_key,
        api_key_env=profile.api_key_env,
        model=resolved_model,
        base_url=base_url,
        timeout_seconds=int(os.environ.get("SOG_LLM_TIMEOUT_SECONDS", "90")),
        temperature=float(os.environ.get("SOG_LLM_TEMPERATURE", "0.2")),
        extra_headers=headers,
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
            timeout=self.config.timeout_seconds,
        )
        if response.status_code >= 400:
            raise RuntimeError(
                f"{self.config.label} request failed with HTTP {response.status_code}: "
                f"{redact_secrets(response.text[:500])}"
            )

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
