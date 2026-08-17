"""
Token Usage Tracker — track token consumption from delegation transcripts.

Inspired by Marveen's token-usage:
  - Parse Claude Code transcripts for token counts
  - Record per-agent, per-model token usage
  - Feed into CostOps ledger

For A2A Mesh:
  - Reads delegation results for token counts
  - Records to CostOps ledger
  - Provides aggregate stats
"""

import time
import json
import os
import logging

log = logging.getLogger("token_usage")

STATE_FILE = os.path.expanduser("~/.hermes/scripts/a2a_mesh/data/token_usage.json")


def load_state():
    try:
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {"total": {"input": 0, "output": 0, "requests": 0}, "by_agent": {}, "by_model": {}}


def save_state(state):
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


def record_usage(agent, model, input_tokens, output_tokens, task_id=""):
    """Record token usage from a completed delegation."""
    state = load_state()
    
    # Update totals
    t = state.setdefault("total", {"input": 0, "output": 0, "requests": 0})
    t["input"] += input_tokens
    t["output"] += output_tokens
    t["requests"] += 1
    
    # Update per-agent
    a = state.setdefault("by_agent", {}).setdefault(agent, {"input": 0, "output": 0, "requests": 0})
    a["input"] += input_tokens
    a["output"] += output_tokens
    a["requests"] += 1
    
    # Update per-model
    m = state.setdefault("by_model", {}).setdefault(model, {"input": 0, "output": 0, "requests": 0})
    m["input"] += input_tokens
    m["output"] += output_tokens
    m["requests"] += 1
    
    # Also record in CostOps
    try:
        from .costops import record_cost
        record_cost(agent, model, input_tokens, output_tokens, task_id)
    except Exception:
        pass
    
    save_state(state)
    return {"input": input_tokens, "output": output_tokens, "total_input": t["input"], "total_output": t["output"]}


def get_summary():
    """Get token usage summary."""
    state = load_state()
    t = state.get("total", {"input": 0, "output": 0, "requests": 0})
    return {
        "total_input": t.get("input", 0),
        "total_output": t.get("output", 0),
        "total_tokens": t.get("input", 0) + t.get("output", 0),
        "total_requests": t.get("requests", 0),
        "by_agent": state.get("by_agent", {}),
        "by_model": state.get("by_model", {}),
    }


def get_agent_usage(agent):
    """Get token usage for a specific agent."""
    state = load_state()
    return state.get("by_agent", {}).get(agent, {"input": 0, "output": 0, "requests": 0})