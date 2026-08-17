"""
Message Router — intelligent message routing for A2A Mesh.

Inspired by Marveen's message-router:
  - Route messages to correct agent/node
  - Federated delivery (cross-mesh)
  - Retry queue for failed deliveries
  - Trace tracking (otel spans)

For A2A Mesh:
  - Route A2A messages between nodes
  - Retry on transient failures
  - Track delivery status
"""

import time
import logging
import uuid
import asyncio

log = logging.getLogger("message_router")

# In-memory message store
_pending = {}  # msg_id → {from, to, content, status, attempts, trace}
_delivered = []
_failed = []
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds


def create_message(from_node, to_node, content, msg_type="a2a"):
    """Create a new routed message."""
    msg_id = str(uuid.uuid4())[:12]
    msg = {
        "id": msg_id,
        "from": from_node,
        "to": to_node,
        "content": content[:500],
        "type": msg_type,
        "status": "pending",
        "attempts": 0,
        "created_at": time.time(),
        "trace": [f"{from_node}→{to_node}"],
    }
    _pending[msg_id] = msg
    return msg


async def route_message(msg_id, transport_fn):
    """Attempt to route a message via the transport function."""
    msg = _pending.get(msg_id)
    if not msg:
        return False
    
    msg["attempts"] += 1
    try:
        result = await transport_fn(msg["to"], msg["content"])
        if result:
            msg["status"] = "delivered"
            msg["delivered_at"] = time.time()
            _delivered.append(msg)
            del _pending[msg_id]
            log.info(f"Message {msg_id} delivered to {msg['to']}")
            return True
    except Exception as e:
        log.warning(f"Message {msg_id} attempt {msg['attempts']} failed: {e}")
    
    # Retry logic
    if msg["attempts"] >= MAX_RETRIES:
        msg["status"] = "failed"
        _failed.append(msg)
        del _pending[msg_id]
        log.error(f"Message {msg_id} permanently failed after {MAX_RETRIES} attempts")
        return False
    
    return False


def get_router_status():
    """Get message router status for dashboard."""
    now = time.time()
    return {
        "pending": len(_pending),
        "delivered": len(_delivered),
        "failed": len(_failed),
        "total": len(_pending) + len(_delivered) + len(_failed),
        "success_rate": round(len(_delivered) / max(len(_delivered) + len(_failed), 1) * 100, 1),
        "pending_messages": [
            {
                "id": m["id"],
                "from": m["from"],
                "to": m["to"],
                "age_sec": int(now - m["created_at"]),
                "attempts": m["attempts"],
            }
            for m in list(_pending.values())[:10]
        ],
        "recent_failures": [
            {
                "id": m["id"],
                "from": m["from"],
                "to": m["to"],
                "attempts": m["attempts"],
            }
            for m in _failed[-5:]
        ],
    }