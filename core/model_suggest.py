"""
Model Suggest — persona→model classifier for optimal model selection.

Inspired by Marveen's model-suggest:
  - Classify agent personas into model tiers
  - Suggest best model based on task type
  - Cost vs performance tradeoff

For A2A Mesh:
  - Map node roles (Nova/Morzsa/Runa) to optimal models
  - Suggest downgrade when cost is high
  - Suggest upgrade when quality is critical
"""

import logging

log = logging.getLogger("model_suggest")

# Model tiers (cheapest → most expensive)
MODEL_TIERS = {
    "routine_lowcost": {
        "models": ["smollm2:135m", "gemma2:9b"],
        "use_for": ["monitoring", "heartbeat", "simple_qa", "logging"],
        "cost_per_1k": 0.0,
    },
    "analysis_efficient": {
        "models": ["step-3.7-flash:free", "gemma4:31b-cloud"],
        "use_for": ["code_review", "summarization", "classification", "triage"],
        "cost_per_1k": 0.0,
    },
    "build_strong": {
        "models": ["glm-5.2:cloud", "qwen2.5:32b"],
        "use_for": ["coding", "architecture", "debugging", "planning"],
        "cost_per_1k": 0.0,
    },
    "premium_reasoning": {
        "models": ["glm-5.2:cloud", "qwen2.5:72b"],
        "use_for": ["complex_reasoning", "multi_step", "critical_decisions"],
        "cost_per_1k": 0.0,
    },
}

# Node persona → recommended tier
NODE_PERSONAS = {
    "Nova": {
        "role": "orchestrator",
        "recommended_tier": "premium_reasoning",
        "current_model": "glm-5.2:cloud",
        "reason": "Main orchestrator needs strong reasoning for delegation decisions",
    },
    "Morzsa": {
        "role": "worker",
        "recommended_tier": "build_strong",
        "current_model": "glm-5.2:cloud",
        "reason": "Worker node for coding tasks needs good model",
    },
    "Runa": {
        "role": "worker",
        "recommended_tier": "analysis_efficient",
        "current_model": "glm-5.2:cloud",
        "reason": "Worker node can use efficient model for most tasks",
    },
}


def suggest_model_for_task(task_type, node_name=None):
    """Suggest the best model tier for a task type."""
    for tier_id, tier in MODEL_TIERS.items():
        if task_type in tier["use_for"]:
            return {
                "tier": tier_id,
                "recommended_model": tier["models"][0],
                "alternatives": tier["models"][1:],
                "reason": f"Task '{task_type}' fits {tier_id} tier",
            }
    return {
        "tier": "build_strong",
        "recommended_model": "glm-5.2:cloud",
        "alternatives": [],
        "reason": "Default to strong tier for unknown task types",
    }


def get_model_suggestions():
    """Get all model suggestions for dashboard."""
    return {
        "tiers": MODEL_TIERS,
        "node_personas": NODE_PERSONAS,
        "suggestions": [
            suggest_model_for_task(tt)
            for tier in MODEL_TIERS.values()
            for tt in tier["use_for"]
        ][:10],  # Top 10
    }


def get_node_suggestion(node_name):
    """Get model suggestion for a specific node."""
    persona = NODE_PERSONAS.get(node_name)
    if not persona:
        return {"error": f"Unknown node: {node_name}"}
    
    tier = MODEL_TIERS.get(persona["recommended_tier"], {})
    current = persona["current_model"]
    recommended = tier.get("models", [current])[0]
    
    change_advised = current != recommended
    
    return {
        "node": node_name,
        "role": persona["role"],
        "current_model": current,
        "recommended_model": recommended,
        "recommended_tier": persona["recommended_tier"],
        "change_advised": change_advised,
        "reason": persona["reason"],
        "tier_models": tier.get("models", []),
    }