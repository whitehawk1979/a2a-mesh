"""A2A Mesh Provider Health Check — checks LLM provider availability.

Called from the heartbeat loop to include provider_status in the heartbeat payload.
Checks:
1. Local Ollama (if configured) — GET /api/tags
2. Mesh-LLM (if configured) — GET /v1/models
3. Remote Ollama (Morzsa) — GET /api/tags

Returns a dict suitable for heartbeat payload inclusion:
    {
        "primary": {"status": "ok|fail", "model": "glm-5.2:cloud", "latency_ms": 45},
        "fallback": {"status": "ok|fail", "model": "mesh", "latency_ms": 12},
    }
"""

import asyncio
import json
import logging
import os
import time
import urllib.request
import urllib.error
from typing import Dict, Any, Optional

log = logging.getLogger("a2a_mesh.provider_health")


def _check_http(url: str, timeout: float = 3.0, headers: dict = None) -> tuple[bool, float]:
    """Check an HTTP endpoint. Returns (ok, latency_ms)."""
    start = time.monotonic()
    try:
        req = urllib.request.Request(url, method="GET", headers=headers or {})
        resp = urllib.request.urlopen(req, timeout=timeout)
        latency = (time.monotonic() - start) * 1000
        return resp.status == 200, round(latency, 1)
    except Exception:
        latency = (time.monotonic() - start) * 1000
        return False, round(latency, 1)


def _read_hermes_config() -> Dict[str, Any]:
    """Read Hermes config.yaml to find provider URLs."""
    config_path = os.path.expanduser("~/.hermes/config.yaml")
    try:
        import yaml
        with open(config_path, "r") as f:
            return yaml.safe_load(f)
    except Exception:
        return {}


# Canonical URLs for Hermes built-in cloud providers (no providers-dict entry).
# These endpoints answer /models without an API key (401 still means "reachable").
_BUILTIN_PROVIDER_URLS = {
    "openai-api": "https://api.openai.com/v1",
    "anthropic-api": "https://api.anthropic.com/v1",
    "nous-api": "https://api.nousresearch.com/v1",
    "openrouter-api": "https://openrouter.ai/api/v1",
}


def _resolve_api_key(config: Dict[str, Any], provider_name: str, base_url: str) -> Optional[str]:
    """Resolve the API key for a provider — needed for /models checks that
    return 401 for unauthenticated requests (e.g. DashScope).
    Order: providers[name].api_key (literal) -> key_env env var -> .env file."""
    providers = config.get("providers", {})
    p = providers.get(provider_name, {})
    key = p.get("api_key")
    if key and not key.startswith("env:"):
        return key
    key_env = p.get("key_env")
    if not key_env:
        # match by base_url
        for pv in providers.values():
            api = (pv.get("api") or pv.get("base_url") or "")
            if base_url and api and api.rstrip("/") == base_url.rstrip("/"):
                key_env = pv.get("key_env")
                key = pv.get("api_key")
                if key and not key.startswith("env:"):
                    return key
                break
    if not key_env:
        return None
    val = os.environ.get(key_env)
    if val:
        return val
    # try .env file (~/.hermes/.env)
    env_path = os.path.expanduser("~/.hermes/.env")
    if os.path.exists(env_path):
        try:
            with open(env_path) as f:
                for line in f:
                    line = line.strip()
                    if line.startswith(key_env + "="):
                        return line.split("=", 1)[1].strip().strip('"').strip("'")
        except Exception:
            pass
    return None


def check_provider_health(node_name: str = "auto") -> Dict[str, Any]:
    """Check all LLM providers for this node.

    Returns a dict with primary and fallback status.
    """
    config = _read_hermes_config()
    model_config = config.get("model", {})

    # Determine primary provider URL
    primary_provider = model_config.get("provider", "")
    primary_model = model_config.get("default", "")
    primary_url = model_config.get("base_url", "")

    # If provider is ollama-launch or similar, try to find the Ollama URL
    if not primary_url or "launch" in primary_provider:
        # Look in custom_providers list for an Ollama entry
        for cp in config.get("custom_providers", []):
            if cp.get("name", "").lower() in ("ollama", "cloudollama"):
                if not primary_url:
                    primary_url = cp.get("base_url", "")
                break

    # If still no URL, look in providers dict using the provider name
    if not primary_url:
        providers_dict = config.get("providers", {})
        # Try exact match on provider name
        if primary_provider in providers_dict:
            primary_url = providers_dict[primary_provider].get("api", "") or providers_dict[primary_provider].get("base_url", "")
        # Try common Ollama provider names
        if not primary_url:
            for pname in ("custom", "ollama", "local-ollama", "ollama-cloud", "ollama-launch"):
                if pname in providers_dict:
                    p = providers_dict[pname]
                    url = p.get("api", "") or p.get("base_url", "")
                    if url and (":11434" in url or "ollama" in pname.lower()):
                        primary_url = url
                        break
        # Built-in cloud providers with no providers-dict entry (e.g. openai-api):
        # use the canonical public API URL so the /models health check works.
        if not primary_url and primary_provider in _BUILTIN_PROVIDER_URLS:
            primary_url = _BUILTIN_PROVIDER_URLS[primary_provider]

    # Determine fallback provider (mesh-llm)
    fallback_url = ""
    fallback_model = ""
    for fp in model_config.get("fallback_providers", []):
        if isinstance(fp, dict) and fp.get("provider") == "mesh-llm":
            fallback_model = fp.get("model", "mesh")
            # Look in custom_providers list for mesh-llm
            for cp in config.get("custom_providers", []):
                if cp.get("name", "") == "mesh-llm":
                    fallback_url = cp.get("base_url", "") or cp.get("api", "")
                    break
            # Also check providers dict
            if not fallback_url and "mesh-llm" in config.get("providers", {}):
                fallback_url = config["providers"]["mesh-llm"].get("api", "") or config["providers"]["mesh-llm"].get("base_url", "")
            break
    # If no fallback_providers list, try providers dict directly
    if not fallback_url and "mesh-llm" in config.get("providers", {}):
        fallback_url = config["providers"]["mesh-llm"].get("api", "") or config["providers"]["mesh-llm"].get("base_url", "")
        fallback_model = "mesh"

    # Check primary
    primary_status = {"status": "unknown", "model": primary_model, "latency_ms": 0}
    if primary_url:
        # Ollama uses /api/tags, OpenAI-compatible uses /v1/models
        if ":11434" in primary_url:
            check_url = primary_url.rstrip("/").replace("/v1", "") + "/api/tags"
        else:
            check_url = primary_url.rstrip("/") + "/models"
        headers = None
        api_key = _resolve_api_key(config, primary_provider, primary_url)
        if api_key:
            headers = {"Authorization": "Bearer " + api_key}
        ok, latency = _check_http(check_url, headers=headers)
        primary_status = {"status": "ok" if ok else "fail", "model": primary_model, "latency_ms": latency}

    # Check fallback (mesh-llm) — longer timeout: the mesh-llm MoA router can
    # block /models for several seconds while an inference is in flight;
    # the old 3.0s default caused frequent false "fail" during MoA load.
    fallback_status = {"status": "unknown", "model": fallback_model, "latency_ms": 0}
    if fallback_url:
        check_url = fallback_url.rstrip("/") + "/models"
        headers = None
        fb_key = _resolve_api_key(config, "mesh-llm", fallback_url)
        if fb_key:
            headers = {"Authorization": "Bearer " + fb_key}
        ok, latency = _check_http(check_url, timeout=8.0, headers=headers)
        fallback_status = {"status": "ok" if ok else "fail", "model": fallback_model, "latency_ms": latency}

    result = {
        "primary": primary_status,
        "fallback": fallback_status,
        "checked_at": int(time.time()),
    }

    log.debug(f"Provider health [{node_name}]: {result}")
    return result