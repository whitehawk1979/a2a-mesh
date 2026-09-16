"""
Tool Timeouts — per-tool HTTP deadline configuration.

Inspired by Marveen's tool-timeouts:
  - Each external service has a specific timeout
  - If timeout exceeded, request aborted → caller falls back gracefully
  - Prevents agent session hangs

For A2A Mesh:
  - P2P transport timeouts
  - Dashboard API timeouts
  - LLM provider timeouts
  - PG connection timeouts
"""

import asyncio
import logging

log = logging.getLogger("tool_timeouts")

# Per-tool timeout configuration (ms → seconds)
TOOL_TIMEOUTS = {
    "p2p_transport": 30,
    "ssh_transport": 15,
    "dashboard_api": 10,
    "pg_query": 15,
    "pg_connect": 10,
    "llm_request": 300,
    "ollama_embedding": 90,
    "web_search": 30,
    "web_extract": 30,
    "git_push": 60,
    "scp_transfer": 60,
    "health_check": 5,
    "heartbeat": 10,
    "delegation": 300,
    "kanban_api": 10,
    "dream_engine": 120,
}


def get_timeout(tool_name, default=30):
    """Get timeout for a specific tool."""
    return TOOL_TIMEOUTS.get(tool_name, default)


async def with_timeout(coro, tool_name, default=30):
    """Run a coroutine with the tool's configured timeout.
    Returns result or raises asyncio.TimeoutError."""
    timeout = get_timeout(tool_name, default)
    try:
        return await asyncio.wait_for(coro, timeout=timeout)
    except asyncio.TimeoutError:
        log.warning(f"Tool timeout: {tool_name} exceeded {timeout}s")
        raise
    except Exception as e:
        log.debug(f"Tool error: {tool_name}: {e}")
        raise


def get_all_timeouts():
    """Get all configured timeouts for dashboard display."""
    return {
        "timeouts": TOOL_TIMEOUTS,
        "categories": {
            "transport": {k: v for k, v in TOOL_TIMEOUTS.items() if "transport" in k or "heartbeat" in k},
            "database": {k: v for k, v in TOOL_TIMEOUTS.items() if "pg" in k},
            "llm": {k: v for k, v in TOOL_TIMEOUTS.items() if "llm" in k or "ollama" in k or "delegation" in k},
            "web": {k: v for k, v in TOOL_TIMEOUTS.items() if "web" in k or "git" in k or "scp" in k},
            "dashboard": {k: v for k, v in TOOL_TIMEOUTS.items() if "dashboard" in k or "kanban" in k or "health" in k or "dream" in k},
        }
    }