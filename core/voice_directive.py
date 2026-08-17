"""
Voice Directive — voice command routing for A2A Mesh.

Inspired by Marveen's voice-directive:
  - Parse voice commands from Telegram voice messages
  - Route to appropriate agent/node
  - Support Hungarian + English

For A2A Mesh:
  - Voice → text via Hermes STT
  - Parse directives: "küldj", "indítsd", "állítsd le", "mutasd"
  - Route to node or dashboard action
"""

import re
import logging

log = logging.getLogger("voice_directive")

# Directive patterns (Hungarian + English)
DIRECTIVES = [
    {
        "pattern": r"(?:küldj|send)\s+(.+?)\s+(?:to\s+|nak|nek)\s+(\w+)",
        "action": "send_message",
        "params": ["content", "target"],
    },
    {
        "pattern": r"(?:indítsd|start|restart|újraindít)\s+(?:el\s+)?a\s+(\w+)",
        "action": "restart_node",
        "params": ["node"],
    },
    {
        "pattern": r"(?:állítsd|stop|leállít)\s+(?:le\s+)?(?:a\s+)?(\w+)",
        "action": "stop_node",
        "params": ["node"],
    },
    {
        "pattern": r"(?:mutasd|show|mutat)\s+(?:a\s+|the\s+)?(.+)",
        "action": "show_info",
        "params": ["what"],
    },
    {
        "pattern": r"(?:keress|search|find)\s+(.+)",
        "action": "search",
        "params": ["query"],
    },
    {
        "pattern": r"(?:deploy|telepítsd|publikálj)",
        "action": "deploy",
        "params": [],
    },
    {
        "pattern": r"(?:kanban|task)\s+(.+)",
        "action": "kanban_command",
        "params": ["command"],
    },
]


def parse_voice_directive(text):
    """Parse a voice transcript into a structured directive."""
    text = text.strip()
    
    for directive in DIRECTIVES:
        match = re.match(directive["pattern"], text, re.IGNORECASE)
        if match:
            params = {}
            for i, param_name in enumerate(directive["params"]):
                if i < len(match.groups()):
                    params[param_name] = match.group(i + 1).strip()
            
            return {
                "action": directive["action"],
                "params": params,
                "raw_text": text,
                "matched": True,
            }
    
    return {
        "action": "unknown",
        "params": {},
        "raw_text": text,
        "matched": False,
    }


def get_voice_status():
    """Get voice directive support status."""
    return {
        "enabled": True,
        "languages": ["hu", "en"],
        "directives": [
            {"action": d["action"], "pattern": d["pattern"]}
            for d in DIRECTIVES
        ],
        "directive_count": len(DIRECTIVES),
    }