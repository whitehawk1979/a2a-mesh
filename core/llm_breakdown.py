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


async def breakdown_and_delegate(task_title, task_description="", node=None, board_id=None, parent_card_id=None):
    """Break down a task via LLM, create Kanban cards + delegations for each subtask.
    
    This is the auto-pipeline: LLM breakdown → Kanban cards → Delegations → Agents.
    Every agent in the mesh gets access automatically.
    
    Returns: {"subtasks": [...], "cards": [...], "delegations": [...]}
    """
    import time as _time
    import os
    import json as _json
    
    result = {"subtasks": [], "cards": [], "delegations": []}
    
    # 1. Get subtasks from LLM (or deterministic fallback)
    subtasks = await llm_breakdown(task_title, task_description)
    result["subtasks"] = subtasks
    log.info(f"Breakdown: {len(subtasks)} subtasks for '{task_title[:50]}'")
    
    # 2. Create Kanban cards for each subtask
    kanban_path = os.path.expanduser("~/.hermes/scripts/a2a_mesh/data/kanban.json")
    created_cards = []
    try:
        with open(kanban_path, "r") as f:
            kanban_boards = _json.load(f)
        
        if not kanban_boards:
            log.warning("No Kanban boards found")
            return result
        
        board = kanban_boards[0]
        if board_id:
            board = next((b for b in kanban_boards if b.get("id") == board_id), kanban_boards[0])
        
        for i, st in enumerate(subtasks):
            card_id = f"card-{int(_time.time()*1000)}-{len(board.get('cards',[])) + i}"
            priority = 8 if st.get("priority") == "high" else 5 if st.get("priority") == "normal" else 3
            card = {
                "id": card_id,
                "title": st.get("title", f"Subtask {i+1}"),
                "column": "todo",
                "priority": priority,
                "assigned_to": st.get("assignee") or "",
                "created_at": _time.time(),
                "updated_at": _time.time(),
                "description": st.get("description", ""),
                "parent_card_id": parent_card_id or "",
                "task_type": "breakdown_subtask",
                "source": "llm_breakdown",
            }
            board.setdefault("cards", []).append(card)
            created_cards.append(card)
            result["cards"].append({"id": card_id, "title": card["title"], "column": "todo"})
        
        with open(kanban_path, "w") as f:
            _json.dump(kanban_boards, f, indent=2)
        log.info(f"Created {len(created_cards)} Kanban cards from breakdown")
    except Exception as e:
        log.warning(f"Kanban card creation failed: {e}")
    
    # 3. Create delegations for each subtask (if node is available)
    if node and hasattr(node, 'delegation') and node.delegation:
        for card in created_cards:
            try:
                task_id = await node.delegation.delegate_task(
                    to_agent=card.get("assigned_to") or "any",
                    subject=card["title"],
                    description=card.get("description", ""),
                    task_type="breakdown_subtask",
                    priority=card.get("priority", 5),
                    available=True if not card.get("assigned_to") else False,
                    context={"kanban_card_id": card["id"], "source": "llm_breakdown"},
                )
                # Write kanban_card_id back to card
                card["delegation_task_id"] = str(task_id)
                result["delegations"].append({"task_id": str(task_id), "card_id": card["id"], "title": card["title"]})
                log.info(f"Delegation created: {str(task_id)[:12]} → card {card['id']}")
            except Exception as e:
                log.warning(f"Delegation creation failed for card {card['id']}: {e}")
        
        # Re-save kanban with delegation_task_id links
        try:
            with open(kanban_path, "r") as f:
                kanban_boards = _json.load(f)
            for b in kanban_boards:
                for c in b.get("cards", []):
                    for card in created_cards:
                        if c["id"] == card["id"] and "delegation_task_id" in card:
                            c["delegation_task_id"] = card["delegation_task_id"]
            with open(kanban_path, "w") as f:
                _json.dump(kanban_boards, f, indent=2)
        except:
            pass
    
    return result