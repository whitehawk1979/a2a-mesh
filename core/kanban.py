"""
Kanban task management for A2A Mesh dashboard.

Features:
  - Auto-breakdown: LLM splits large tasks into subtasks
  - Card-aging: visual indicators for stale tasks
  - Audit loop: periodic check for stuck tasks

Storage: data/kanban.json (simple, agent-updatable)
"""
import asyncio
import json
import time
import os
from typing import Dict, List, Optional

KANBAN_FILE = os.path.join(os.path.dirname(__file__), "..", "data", "kanban.json")

# Card age thresholds (hours)
AGE_WARN = 2    # yellow
AGE_STALE = 6   # orange  
AGE_DEAD = 24    # red


def _load_boards() -> List[Dict]:
    """Load all kanban boards from JSON file."""
    try:
        if os.path.exists(KANBAN_FILE):
            with open(KANBAN_FILE, "r") as f:
                return json.load(f)
    except Exception:
        pass
    return []


def _save_boards(boards: List[Dict]):
    """Save kanban boards to JSON file."""
    os.makedirs(os.path.dirname(KANBAN_FILE), exist_ok=True)
    with open(KANBAN_FILE, "w") as f:
        json.dump(boards, f, indent=2, ensure_ascii=False)


def _card_age_hours(card: Dict) -> float:
    """Calculate card age in hours since last update."""
    updated = card.get("updated_at", card.get("created_at", time.time()))
    return (time.time() - updated) / 3600.0


def _card_age_status(card: Dict) -> str:
    """Return age status: fresh, warn, stale, dead."""
    if card.get("status") in ("done", "cancelled"):
        return "fresh"
    age = _card_age_hours(card)
    if age >= AGE_DEAD:
        return "dead"
    elif age >= AGE_STALE:
        return "stale"
    elif age >= AGE_WARN:
        return "warn"
    return "fresh"


class KanbanManager:
    """Manages kanban boards, cards, and audit loop."""
    
    def __init__(self, pg_pool=None, node_name="unknown"):
        self.pg_pool = pg_pool
        self.node_name = node_name
    
    # ─── Board CRUD ───
    
    def get_boards(self) -> List[Dict]:
        boards = _load_boards()
        # Add age status to each card
        for board in boards:
            for card in board.get("cards", []):
                card["age_status"] = _card_age_status(card)
        return boards
    
    def get_board(self, board_id: str) -> Optional[Dict]:
        boards = _load_boards()
        for board in boards:
            if board["id"] == board_id:
                for card in board.get("cards", []):
                    card["age_status"] = _card_age_status(card)
                return board
        return None
    
    def create_board(self, title: str, columns: List[str] = None) -> Dict:
        boards = _load_boards()
        board = {
            "id": f"board-{int(time.time())}-{len(boards)}",
            "title": title,
            "columns": columns or ["todo", "in_progress", "review", "done"],
            "cards": [],
            "created_at": time.time(),
            "updated_at": time.time(),
        }
        boards.append(board)
        _save_boards(boards)
        return board
    
    def delete_board(self, board_id: str) -> bool:
        boards = _load_boards()
        initial = len(boards)
        boards = [b for b in boards if b["id"] != board_id]
        if len(boards) < initial:
            _save_boards(boards)
            return True
        return False
    
    # ─── Card CRUD ───
    
    def add_card(self, board_id: str, title: str, column: str = "todo",
                 description: str = "", priority: str = "medium",
                 assigned_to: str = "", parent_id: str = None) -> Dict:
        boards = _load_boards()
        for board in boards:
            if board["id"] == board_id:
                card = {
                    "id": f"card-{int(time.time()*1000)}-{len(board['cards'])}",
                    "title": title,
                    "description": description,
                    "column": column,
                    "priority": priority,
                    "assigned_to": assigned_to,
                    "parent_id": parent_id,
                    "created_at": time.time(),
                    "updated_at": time.time(),
                    "created_by": self.node_name,
                }
                board["cards"].append(card)
                board["updated_at"] = time.time()
                _save_boards(boards)
                return card
        return {}
    
    def update_card(self, board_id: str, card_id: str, updates: Dict) -> Dict:
        boards = _load_boards()
        for board in boards:
            if board["id"] == board_id:
                for card in board["cards"]:
                    if card["id"] == card_id:
                        for k, v in updates.items():
                            if k not in ("id", "created_at", "created_by"):
                                card[k] = v
                        card["updated_at"] = time.time()
                        board["updated_at"] = time.time()
                        _save_boards(boards)
                        return card
        return {}
    
    def move_card(self, board_id: str, card_id: str, column: str) -> Dict:
        return self.update_card(board_id, card_id, {"column": column})
    
    def delete_card(self, board_id: str, card_id: str) -> bool:
        boards = _load_boards()
        for board in boards:
            if board["id"] == board_id:
                initial = len(board["cards"])
                board["cards"] = [c for c in board["cards"] if c["id"] != card_id]
                if len(board["cards"]) < initial:
                    board["updated_at"] = time.time()
                    _save_boards(boards)
                    return True
        return False
    
    # ─── Auto-breakdown ───
    
    async def auto_breakdown(self, board_id: str, card_id: str, subtasks: List[Dict]) -> List[Dict]:
        """Break a card into subtasks. subtasks = [{title, description, priority}]"""
        created = []
        for st in subtasks:
            card = self.add_card(
                board_id, st["title"], column="todo",
                description=st.get("description", ""),
                priority=st.get("priority", "medium"),
                parent_id=card_id
            )
            created.append(card)
        
        # Mark parent as "broken down"
        self.update_card(board_id, card_id, {"broken_down": True})
        return created
    
    # ─── Audit ───
    
    async def audit_stale_cards(self) -> Dict:
        """Audit all boards for stale cards. Returns summary."""
        boards = _load_boards()
        stale = []
        dead = []
        
        for board in boards:
            for card in board.get("cards", []):
                if card.get("status") in ("done", "cancelled"):
                    continue
                age_status = _card_age_status(card)
                if age_status == "stale":
                    stale.append({"board": board.get("title") or board.get("name", "?"), "card": card.get("title", "?"), "id": card.get("id", "?"), "board_id": board.get("id", "?")})
                elif age_status == "dead":
                    dead.append({"board": board.get("title") or board.get("name", "?"), "card": card.get("title", "?"), "id": card.get("id", "?"), "board_id": board.get("id", "?")})
        
        return {
            "stale_count": len(stale),
            "dead_count": len(dead),
            "stale": stale,
            "dead": dead,
            "total_boards": len(boards),
            "total_cards": sum(len(b.get("cards", [])) for b in boards),
        }