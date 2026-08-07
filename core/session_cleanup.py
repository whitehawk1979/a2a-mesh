#!/usr/bin/env python3
"""A2A Mesh Session State Cleanup — clears stale model overrides from Hermes state.

The problem: when a user runs /model <model> --provider <provider>, the override
persists in TWO places:
1. state.db → gateway_routing table (model_override JSON column)
2. sessions.json → session entry with provider/model fields

If the override points to a dead provider (e.g. custom:ollama when local Ollama
is stopped), the gateway keeps failing with "Transient agent failure" even after
the config is fixed — because the session override takes precedence.

This script:
1. Reads all model_override entries from state.db gateway_routing
2. Validates each provider against current config.yaml custom_providers
3. Removes stale overrides (provider not in config or provider URL unreachable)
4. Cleans up sessions.json entries with stale provider references

Usage:
    python3 session_cleanup.py [--node nova|morzsa|runa] [--dry-run] [--force]

    --dry-run: Show what would be cleaned without making changes
    --force: Remove ALL overrides, not just stale ones
"""

import argparse
import json
import logging
import os
import sqlite3
import sys
import time
from typing import Dict, List, Any, Optional
from urllib.request import Request, urlopen

# Setup logging
LOG_FILE = os.path.expanduser("~/.hermes/logs/session_cleanup.log")
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stderr),
    ],
)
log = logging.getLogger("session_cleanup")

STATE_DB = os.path.expanduser("~/.hermes/state.db")
SESSIONS_JSON = os.path.expanduser("~/.hermes/sessions/sessions.json")
CONFIG_PATH = os.path.expanduser("~/.hermes/config.yaml")


def read_config_providers() -> Dict[str, Any]:
    """Read Hermes config to get valid provider names and URLs."""
    try:
        import yaml
        with open(CONFIG_PATH, "r") as f:
            config = yaml.safe_load(f)

        providers = {}
        # Standard providers
        for name in ["ollama", "ollama-cloud", "nous", "openai", "anthropic",
                     "gemini", "groq", "openrouter"]:
            if name in config.get("model", {}).get("provider", ""):
                providers[name] = {"url": config["model"].get("base_url", "")}

        # Custom providers
        for cp in config.get("custom_providers", []):
            name = cp.get("name", "")
            if name:
                providers[f"custom:{name}"] = {
                    "url": cp.get("base_url", ""),
                    "models": cp.get("models", []),
                }

        # Mesh providers
        for fp in config.get("model", {}).get("fallback_providers", []):
            if isinstance(fp, dict):
                pname = fp.get("provider", "")
                if pname and pname not in providers:
                    providers[pname] = {"url": "", "models": [fp.get("model", "")]}

        return providers
    except Exception as e:
        log.error(f"Failed to read config: {e}")
        return {}


def check_provider_url(url: str, timeout: float = 3.0) -> bool:
    """Quick check if a provider URL is reachable."""
    if not url:
        return False
    # Ollama uses /api/tags, OpenAI-compatible uses /v1/models
    if ":11434" in url:
        check_url = url.rstrip("/").replace("/v1", "") + "/api/tags"
    else:
        check_url = url.rstrip("/") + "/models"
    try:
        req = Request(check_url, method="GET")
        resp = urlopen(req, timeout=timeout)
        return resp.status == 200
    except Exception:
        return False


def get_stale_overrides(valid_providers: Dict, force: bool = False) -> List[Dict]:
    """Find stale model overrides in state.db."""
    if not os.path.exists(STATE_DB):
        log.info("No state.db found")
        return []

    conn = sqlite3.connect(STATE_DB)
    c = conn.cursor()

    # Check if gateway_routing table exists
    c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='gateway_routing'")
    if not c.fetchone():
        log.info("No gateway_routing table")
        conn.close()
        return []

    c.execute("SELECT scope, session_key, entry_json FROM gateway_routing")
    stale = []
    for scope, session_key, entry_json in c.fetchall():
        try:
            entry = json.loads(entry_json) if isinstance(entry_json, str) else entry_json
        except Exception:
            continue

        override = entry.get("model_override") if isinstance(entry, dict) else None
        if not override:
            # Also check if entry itself has model_override fields
            if isinstance(entry, dict) and ("model" in entry or "provider" in entry):
                override = entry

        if not override or not isinstance(override, dict):
            continue

        provider = override.get("provider", "")
        model = override.get("model", "")
        base_url = override.get("base_url", "")

        is_stale = force
        if not force:
            # Check if provider is valid
            if provider and provider not in valid_providers:
                is_stale = True
            # Check if URL is reachable
            elif base_url and not check_provider_url(base_url):
                is_stale = True
            # Check if model exists in provider's model list
            elif provider in valid_providers:
                models = valid_providers[provider].get("models", [])
                if models and model and model not in models:
                    is_stale = True

        if is_stale:
            stale.append({
                "scope": scope,
                "session_key": session_key,
                "provider": provider,
                "model": model,
                "base_url": base_url,
            })

    conn.close()
    return stale


def clean_state_db(stale_entries: List[Dict], dry_run: bool = False) -> int:
    """Remove stale overrides from state.db."""
    if not stale_entries or dry_run:
        return 0

    conn = sqlite3.connect(STATE_DB)
    c = conn.cursor()
    cleaned = 0

    for entry in stale_entries:
        try:
            c.execute(
                "DELETE FROM gateway_routing WHERE session_key = ?",
                (entry["session_key"],)
            )
            cleaned += c.rowcount
        except Exception as e:
            log.error(f"Failed to clean {entry['session_key']}: {e}")

    conn.commit()
    conn.close()
    return cleaned


def clean_sessions_json(stale_providers: List[str], dry_run: bool = False) -> int:
    """Remove sessions.json entries with stale provider references."""
    if not os.path.exists(SESSIONS_JSON):
        return 0

    try:
        with open(SESSIONS_JSON, "r") as f:
            data = json.load(f)
    except Exception:
        return 0

    if not isinstance(data, dict):
        return 0

    cleaned = 0
    for key in list(data.keys()):
        if key.startswith("_"):
            continue
        session = data[key]
        if not isinstance(session, dict):
            continue

        provider = session.get("provider", "")
        if provider and provider in stale_providers:
            if not dry_run:
                del data[key]
                cleaned += 1
            else:
                log.info(f"[dry-run] Would remove sessions.json entry: {key} (provider={provider})")

    if not dry_run and cleaned > 0:
        with open(SESSIONS_JSON, "w") as f:
            json.dump(data, f, indent=2)

    return cleaned


def main():
    parser = argparse.ArgumentParser(description="Session State Cleanup")
    parser.add_argument("--node", default="auto", help="Node name")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be cleaned")
    parser.add_argument("--force", action="store_true", help="Remove ALL overrides")
    args = parser.parse_args()

    log.info(f"[{args.node}] Session cleanup starting (dry_run={args.dry_run}, force={args.force})")

    # Read valid providers from config
    valid_providers = read_config_providers()
    log.info(f"[{args.node}] Valid providers: {list(valid_providers.keys())}")

    # Find stale overrides
    stale = get_stale_overrides(valid_providers, force=args.force)

    if not stale:
        log.info(f"[{args.node}] ✅ No stale overrides found")
        return

    log.warning(f"[{args.node}] Found {len(stale)} stale override(s):")
    for s in stale:
        log.warning(f"  - {s['session_key']}: provider={s['provider']} model={s['model']} url={s['base_url']}")

    if args.dry_run:
        log.info("Dry run — no changes made")
        return

    # Clean state.db
    db_cleaned = clean_state_db(stale)
    log.info(f"[{args.node}] Cleaned {db_cleaned} entries from state.db")

    # Clean sessions.json
    stale_providers = list(set(s["provider"] for s in stale if s["provider"]))
    json_cleaned = clean_sessions_json(stale_providers)
    log.info(f"[{args.node}] Cleaned {json_cleaned} entries from sessions.json")

    # Also clean sessions table
    if os.path.exists(STATE_DB):
        conn = sqlite3.connect(STATE_DB)
        c = conn.cursor()
        c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='sessions'")
        if c.fetchone():
            for s in stale:
                try:
                    c.execute("DELETE FROM sessions WHERE session_key = ?", (s["session_key"],))
                except Exception:
                    pass
            conn.commit()
        conn.close()

    log.info(f"[{args.node}] ✅ Cleanup complete")


if __name__ == "__main__":
    main()