"""
PreCompact Hook — save critical decisions before context compression.

Concept (from Marveen):
  Before the LLM context is compressed, extract and save:
  1. Key decisions (user chose X over Y)
  2. Important values (IPs, passwords, config changes)
  3. Task progress (what was done, what remains)
  4. Open questions

Implementation:
  - Scans recent session messages for decision patterns
  - Saves to Brain PG (agent_memory) with high importance
  - Also saves to Obsidian vault as backup

This runs as part of the self-healing loop, checking if compression
is likely (high token count) and proactively saving context.
"""
import re
import json
import time
import asyncio
import os
from typing import Dict, List, Optional

# Decision patterns to detect
DECISION_PATTERNS = [
    r"(?:döntés|elhatározt|választ|prefer|agreed|decided|chose)",
    r"(?:üzembe|deploy|indíts|stop|restart|telepít)",
    r"(?:jelszó|password|token|key|secret|credential)",
    r"(?:IP|192\.168|10\.0|172\.\d+)",
    r"(?:port|endpoint|URL|http)",
    r"(?:fix|javíts|resolve|bug|error|hiba)",
    r"(?:create|create|hozz létre|generál|build)",
]

# Patterns for values to preserve
VALUE_PATTERNS = [
    r"(?:user|felhasználó|login)[:\s]+(\S+)",
    r"(?:password|jelszó)[:\s]+(\S+)",
    r"(?:IP|ip)[:\s]+(\d+\.\d+\.\d+\.\d+)",
    r"(?:port)[:\s]+(\d+)",
    r"(?:URL|url|link)[:\s]+(https?://\S+)",
]


def extract_critical_info(messages: List[Dict]) -> Dict:
    """Extract critical decisions and values from messages.
    
    Args:
        messages: List of {role, content, timestamp} dicts
    
    Returns:
        {decisions: [...], values: {...}, tasks_done: [...], open_questions: [...]}
    """
    decisions = []
    values = {}
    tasks_done = []
    open_questions = []
    
    for msg in messages:
        content = msg.get("content", "")
        if not content or not isinstance(content, str):
            continue
        
        # Extract decisions
        for pattern in DECISION_PATTERNS:
            matches = re.findall(pattern, content, re.IGNORECASE)
            for match in matches:
                # Get surrounding context (100 chars)
                idx = content.lower().find(match.lower() if isinstance(match, str) else match)
                if idx >= 0:
                    start = max(0, idx - 50)
                    end = min(len(content), idx + 100)
                    decisions.append(content[start:end].strip())
        
        # Extract values
        for pattern in VALUE_PATTERNS:
            matches = re.findall(pattern, content, re.IGNORECASE)
            for match in matches:
                if isinstance(match, str) and len(match) < 200:
                    values[match] = True
        
        # Detect completed tasks
        if re.search(r"(?:kész|done|completed|✅|✓|elkészült|befejez)", content, re.IGNORECASE):
            # Get the first 200 chars as task description
            tasks_done.append(content[:200])
        
        # Detect open questions
        if "?" in content and msg.get("role") == "user":
            open_questions.append(content[:200])
    
    # Dedupe and limit
    decisions = list(dict.fromkeys(decisions))[:10]
    tasks_done = list(dict.fromkeys(tasks_done))[:10]
    open_questions = list(dict.fromkeys(open_questions))[:5]
    
    return {
        "decisions": decisions,
        "values": list(values.keys())[:20],
        "tasks_done": tasks_done,
        "open_questions": open_questions,
        "timestamp": time.time(),
    }


async def save_pre_compact(conn, info: Dict, node_name: str = "unknown") -> str:
    """Save critical info to Brain PG before compression.
    
    Args:
        conn: asyncpg connection
        info: Output of extract_critical_info()
        node_name: Name of the node
    
    Returns:
        Memory ID or empty string
    """
    if not info["decisions"] and not info["values"] and not info["tasks_done"]:
        return ""
    
    title = f"PreCompact {time.strftime('%Y-%m-%d %H:%M', time.localtime())}"
    content_parts = []
    
    if info["decisions"]:
        content_parts.append("## 🎯 Döntések\n" + "\n".join(f"- {d}" for d in info["decisions"]))
    if info["values"]:
        content_parts.append("## 🔑 Értékek\n" + "\n".join(f"- {v}" for v in info["values"]))
    if info["tasks_done"]:
        content_parts.append("## ✅ Befejezett feladatok\n" + "\n".join(f"- {t}" for t in info["tasks_done"]))
    if info["open_questions"]:
        content_parts.append("## ❓ Nyitott kérdések\n" + "\n".join(f"- {q}" for q in info["open_questions"]))
    
    content = "\n\n".join(content_parts)
    
    try:
        row = await conn.fetchrow(
            """INSERT INTO agent_memory 
               (category, subcategory, keywords, title, content, importance, collection, source_session)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
               RETURNING id""",
            "precompact",
            "critical_context",
            ["precompact", "decision", "context", node_name],
            title,
            content,
            90,  # High importance — these are critical
            "shared",
            f"{node_name}-precompact-{int(time.time())}",
        )
        return str(row["id"]) if row else ""
    except Exception:
        return ""


async def precompact_audit(pg_pool, node_name: str = "unknown", max_age_hours: int = 24) -> Dict:
    """Audit recent precompact memories.
    
    Returns summary of how many precompact saves exist and their age.
    """
    try:
        if not pg_pool or not pg_pool.is_connected():
            return {"error": "PG not connected"}
        
        async with pg_pool.acquire() as conn:
            rows = await conn.fetch(
                """SELECT id, title, importance, created_at,
                          EXTRACT(EPOCH FROM (NOW() - created_at))/3600 AS age_hours
                   FROM agent_memory 
                   WHERE category = 'precompact'
                   ORDER BY created_at DESC
                   LIMIT 10""",
            )
            return {
                "total": len(rows),
                "recent": [{"id": str(r["id"]), "title": r["title"], "age_hours": float(r["age_hours"])} for r in rows],
            }
    except Exception as e:
        return {"error": str(e)}