"""
CostOps — Cost tracking ledger for A2A Mesh.

Inspired by Marveen's CostOps:
  - Track token usage per agent, per model, per task
  - Monthly budget tracking
  - Cost line items with confidence levels
  - Deterministic (SQL + arithmetic, no LLM)

For A2A Mesh:
  - Tracks delegations + their token costs
  - Per-node, per-model cost breakdown
  - Budget alerts when thresholds exceeded
"""

import time
import json
import os
import logging
from datetime import datetime, timezone

log = logging.getLogger("costops")

COST_FILE = os.path.expanduser("~/.hermes/scripts/a2a_mesh/data/cost_ledger.json")

# Approximate cost per 1M tokens (USD)
MODEL_COSTS = {
    "glm-5.2:cloud": {"input": 0.07, "output": 0.07},
    "glm-5.3:cloud": {"input": 0.07, "output": 0.07},
    "step-3.7-flash:free": {"input": 0.0, "output": 0.0},
    "gemma4:31b-cloud": {"input": 0.0, "output": 0.0},
    "smollm2:135m": {"input": 0.0, "output": 0.0},
    "mesh": {"input": 0.0, "output": 0.0},
    "default": {"input": 0.07, "output": 0.07},
}


def load_ledger():
    """Load cost ledger from JSON file."""
    try:
        with open(COST_FILE, "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {"entries": [], "budgets": {}, "monthly_totals": {}}


def save_ledger(ledger):
    """Save cost ledger to JSON file."""
    os.makedirs(os.path.dirname(COST_FILE), exist_ok=True)
    with open(COST_FILE, "w") as f:
        json.dump(ledger, f, indent=2)


def record_cost(agent, model, input_tokens, output_tokens, task_id="", node=""):
    """Record a cost entry."""
    costs = MODEL_COSTS.get(model, MODEL_COSTS["default"])
    cost_usd = (input_tokens / 1_000_000 * costs["input"]) + (output_tokens / 1_000_000 * costs["output"])

    ledger = load_ledger()
    entry = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "agent": agent,
        "model": model,
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "cost_usd": round(cost_usd, 6),
        "task_id": task_id,
        "node": node,
    }
    ledger["entries"].append(entry)
    # Keep last 10000 entries
    if len(ledger["entries"]) > 10000:
        ledger["entries"] = ledger["entries"][-10000:]
    save_ledger(ledger)
    return entry


def get_monthly_summary(year_month=None):
    """Get monthly cost summary."""
    if not year_month:
        year_month = datetime.now(timezone.utc).strftime("%Y-%m")
    ledger = load_ledger()
    entries = [e for e in ledger["entries"] if e["timestamp"].startswith(year_month)]
    by_agent = {}
    by_model = {}
    total = 0.0
    total_in = 0
    total_out = 0
    for e in entries:
        agent = e["agent"]
        model = e["model"]
        by_agent[agent] = by_agent.get(agent, 0) + e["cost_usd"]
        by_model[model] = by_model.get(model, 0) + e["cost_usd"]
        total += e["cost_usd"]
        total_in += e["input_tokens"]
        total_out += e["output_tokens"]
    return {
        "month": year_month,
        "total_cost_usd": round(total, 4),
        "total_input_tokens": total_in,
        "total_output_tokens": total_out,
        "total_requests": len(entries),
        "by_agent": {k: round(v, 4) for k, v in sorted(by_agent.items(), key=lambda x: -x[1])},
        "by_model": {k: round(v, 4) for k, v in sorted(by_model.items(), key=lambda x: -x[1])},
    }


def set_budget(category, monthly_limit_usd):
    """Set a monthly budget for a category (agent or model)."""
    ledger = load_ledger()
    ledger["budgets"][category] = monthly_limit_usd
    save_ledger(ledger)
    return {"category": category, "monthly_limit_usd": monthly_limit_usd}


def check_budget_alerts():
    """Check if any category is over budget. Returns list of alerts."""
    summary = get_monthly_summary()
    ledger = load_ledger()
    budgets = ledger.get("budgets", {})
    alerts = []
    for category, limit in budgets.items():
        spent = summary["by_agent"].get(category, 0) + summary["by_model"].get(category, 0)
        if spent > limit:
            alerts.append({
                "category": category,
                "spent": round(spent, 4),
                "limit": limit,
                "pct": round(spent / limit * 100, 1) if limit > 0 else 0,
                "severity": "critical" if spent > limit * 1.5 else "warning"
            })
    return alerts