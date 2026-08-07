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


def _check_http(url: str, timeout: float = 3.0) -> tuple[bool, float]:
    """Check an HTTP endpoint. Returns (ok, latency_ms)."""
    start = time.monotonic()
    try:
        req = urllib.request.Request(url, method="GET")
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
        # Look in custom_providers for an Ollama entry
        for cp in config.get("custom_providers", []):
            if cp.get("name", "").lower() in ("ollama", "cloudollama"):
                if not primary_url:
                    primary_url = cp.get("base_url", "")
                break

    # Determine fallback provider (mesh-llm)
    fallback_url = ""
    fallback_model = ""
    for fp in model_config.get("fallback_providers", []):
        if isinstance(fp, dict) and fp.get("provider") == "mesh-llm":
            fallback_model = fp.get("model", "mesh")
            # Look in custom_providers for mesh-llm
            for cp in config.get("custom_providers", []):
                if cp.get("name", "") == "mesh-llm":
                    fallback_url = cp.get("base_url", "")
                    break
            break

    # Check primary
    primary_status = {"status": "unknown", "model": primary_model, "latency_ms": 0}
    if primary_url:
        # Ollama uses /api/tags, OpenAI-compatible uses /v1/models
        if ":11434" in primary_url:
            check_url = primary_url.rstrip("/").replace("/v1", "") + "/api/tags"
        else:
            check_url = primary_url.rstrip("/") + "/models"
        ok, latency = _check_http(check_url)
        primary_status = {"status": "ok" if ok else "fail", "model": primary_model, "latency_ms": latency}

    # Check fallback (mesh-llm)
    fallback_status = {"status": "unknown", "model": fallback_model, "latency_ms": 0}
    if fallback_url:
        check_url = fallback_url.rstrip("/") + "/models"
        ok, latency = _check_http(check_url)
        fallback_status = {"status": "ok" if ok else "fail", "model": fallback_model, "latency_ms": latency}

    result = {
        "primary": primary_status,
        "fallback": fallback_status,
        "checked_at": int(time.time()),
    }

    log.debug(f"Provider health [{node_name}]: {result}")
    return result