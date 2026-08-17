"""
LLM Breakdown — auto-split large tasks into subtasks.

Inspired by Marveen's llm-breakdown:
  - Large kanban card → LLM generates subtask suggestions
  - Each subtask: title, description, assignee, priority
  - Configurable max subtasks (default 10)

For A2A Mesh:
  - Takes a task description + returns subtask list
  - Uses local LLM (ollama) if available
  - Falls back to deterministic decomposition
  - Creates kanban cards from subtasks
"""

import json
import logging

log = logging.getLogger("llm_breakdown")

MAX_SUBTASKS = 10
MIN_SUBTASKS = 2


def deterministic_breakdown(task_title, task_description=""):
    """Break down a task without LLM — rule-based decomposition.
    Returns list of subtask dicts."""
    subtasks = []
    title_lower = task_title.lower()
    
    # Common patterns
    if any(w in title_lower for w in ["implement", "develop", "build", "create"]):
        subtasks = [
            {"title": f"Tervezés: {task_title}", "description": "Architektúra + komponensek meghatározása", "assignee": None, "priority": "high"},
            {"title": f"Implementáció: {task_title}", "description": "Funkció kódolása", "assignee": None, "priority": "high"},
            {"title": f"Tesztelés: {task_title}", "description": "Egység + integrációs tesztek", "assignee": None, "priority": "normal"},
            {"title": f"Dokumentáció: {task_title}", "description": "README + API docs frissítése", "assignee": None, "priority": "low"},
        ]
    elif any(w in title_lower for w in ["fix", "bug", "error", "debug"]):
        subtasks = [
            {"title": f"Reproduckálás: {task_title}", "description": "Hiba reprodukálása", "assignee": None, "priority": "high"},
            {"title": f"Root cause: {task_title}", "description": "Hiba okának azonosítása", "assignee": None, "priority": "high"},
            {"title": f"Javítás: {task_title}", "description": "Kód javítása", "assignee": None, "priority": "high"},
            {"title": f"Verifikáció: {task_title}", "description": "Javítás tesztelése", "assignee": None, "priority": "normal"},
        ]
    elif any(w in title_lower for w in ["deploy", "release", "publish"]):
        subtasks = [
            {"title": f"Pre-flight: {task_title}", "description": "Ellenőrzés deploy előtt", "assignee": None, "priority": "high"},
            {"title": f"Deploy: {task_title}", "description": "Éles deploy végrehajtása", "assignee": None, "priority": "high"},
            {"title": f"Post-deploy: {task_title}", "description": "Health check + monitorozás", "assignee": None, "priority": "normal"},
        ]
    else:
        # Generic decomposition
        subtasks = [
            {"title": f"Előkészület: {task_title}", "description": "Kontextus + követelmények", "assignee": None, "priority": "normal"},
            {"title": f"Végrehajtás: {task_title}", "description": task_description or "Feladat elvégzése", "assignee": None, "priority": "high"},
            {"title": f"Ellenőrzés: {task_title}", "description": "Eredmény verifikálása", "assignee": None, "priority": "normal"},
        ]
    
    return subtasks[:MAX_SUBTASKS]


async def llm_breakdown(task_title, task_description="", ollama_url="http://localhost:11434"):
    """Use LLM to break down a task. Falls back to deterministic."""
    try:
        import aiohttp
        prompt = f"""Break down this task into {MAX_SUBTASKS} subtasks. Return JSON array.
Task: {task_title}
Description: {task_description}

Format: [{{"title": "...", "description": "...", "priority": "high|normal|low"}}]
Return ONLY the JSON array, no explanation."""
        
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{ollama_url}/api/generate",
                json={"model": "glm-5.2:cloud", "prompt": prompt, "stream": False, "options": {"num_predict": 500}},
                timeout=aiohttp.ClientTimeout(total=30)
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    text = data.get("response", "")
                    # Extract JSON array
                    start = text.find("[")
                    end = text.rfind("]") + 1
                    if start >= 0 and end > start:
                        subtasks = json.loads(text[start:end])
                        return subtasks[:MAX_SUBTASKS]
    except Exception as e:
        log.debug(f"LLM breakdown failed, using deterministic: {e}")
    
    return deterministic_breakdown(task_title, task_description)