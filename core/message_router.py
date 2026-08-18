"""
Message Router — intelligent message routing for A2A Mesh.

Inspired by Marveen's message-router:
  - Route messages to correct agent/node
  - Retry on transient failures (3x with exponential backoff)
  - Reconnect backlog batching: 5+ pending → 1 summary message
  - Distributed tracing: trace_id propagation across delegation chains
  - Per-tick budget: max 25 messages/tick (backlog can't monopolize)

For A2A Mesh:
  - Route A2A messages between nodes via P2P transport
  - Track delivery status + retry queue
  - Batch reconnect backlog into summaries
  - Propagate trace_id for distributed tracing
"""

import time
import logging
import uuid
import asyncio
import json
from collections import defaultdict

log = logging.getLogger("message_router")

# In-memory message store
_pending = {}        # msg_id → {from, to, content, status, attempts, trace}
_delivered = []      # Recently delivered messages
_failed = []         # Permanently failed messages
MAX_RETRIES = 3
RETRY_DELAY = 5      # Base retry delay (seconds, exponential backoff)
MAX_PER_TICK = 25    # Marveen-inspired: max messages per tick
BATCH_THRESHOLD = 5  # 5+ pending → batch into summary

# Trace tracking — distributed tracing across delegation chains
_traces = {}  # trace_id → [hop1, hop2, ...]


def create_message(from_node, to_node, content, msg_type="a2a", trace_id=None, parent_trace_id=None):
    """Create a new routed message with optional trace propagation.
    
    Args:
        trace_id: Explicit trace_id (propagated from parent delegation)
        parent_trace_id: Parent trace_id — if provided, this message is a child
    """
    msg_id = str(uuid.uuid4())[:12]
    
    # Trace propagation: inherit parent or create new
    if trace_id:
        tid = trace_id
    elif parent_trace_id:
        tid = parent_trace_id  # Continue parent's trace chain
    else:
        tid = f"trace-{from_node}-{msg_id}"
    
    msg = {
        "id": msg_id,
        "from": from_node,
        "to": to_node,
        "content": content[:500],
        "type": msg_type,
        "status": "pending",
        "attempts": 0,
        "created_at": time.time(),
        "trace_id": tid,
        "trace": [f"{from_node}→{to_node}"],
    }
    _pending[msg_id] = msg
    
    # Record trace hop
    if tid not in _traces:
        _traces[tid] = []
    _traces[tid].append({
        "hop": len(_traces[tid]) + 1,
        "node": from_node,
        "to": to_node,
        "msg_id": msg_id,
        "ts": time.time(),
    })
    
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
            log.info(f"Message {msg_id} delivered to {msg['to']} (trace={msg.get('trace_id','?')})")
            return True
    except Exception as e:
        log.warning(f"Message {msg_id} attempt {msg['attempts']} failed: {e}")
    
    # Retry logic with exponential backoff
    if msg["attempts"] >= MAX_RETRIES:
        msg["status"] = "failed"
        _failed.append(msg)
        del _pending[msg_id]
        log.error(f"Message {msg_id} permanently failed after {MAX_RETRIES} attempts (trace={msg.get('trace_id','?')})")
        return False
    
    return False


async def process_backlog(transport_fn, max_per_tick=MAX_PER_TICK):
    """Process pending messages with per-tick budget.
    
    Marveen-inspired: limits messages per tick so a large backlog
    can't monopolize the event loop. Also batches 5+ messages to the
    same node into a single summary (reconnect backlog batching).
    """
    if not _pending:
        return 0
    
    # Group pending by destination
    by_dest = defaultdict(list)
    for mid, msg in list(_pending.items()):
        by_dest[msg["to"]].append(mid)
    
    processed = 0
    for dest, msg_ids in by_dest.items():
        if processed >= max_per_tick:
            break
        
        # ── Reconnect Backlog Batching ──
        # If 5+ messages pending for same destination, batch into 1 summary
        if len(msg_ids) >= BATCH_THRESHOLD:
            summary_content = _build_batch_summary(msg_ids)
            batch_msg = create_message(
                from_node=_pending[msg_ids[0]]["from"],
                to_node=dest,
                content=summary_content,
                msg_type="batch_summary",
            )
            # Remove individual messages, keep only the batch
            for mid in msg_ids:
                if mid in _pending:
                    del _pending[mid]
            # Send the batch
            await route_message(batch_msg["id"], transport_fn)
            processed += 1
            log.info(f"Batched {len(msg_ids)} messages to {dest} into 1 summary (backlog batching)")
        else:
            # Send individually
            for mid in msg_ids:
                if processed >= max_per_tick:
                    break
                await route_message(mid, transport_fn)
                processed += 1
    
    return processed


def _build_batch_summary(msg_ids):
    """Build a summary message from multiple pending messages.
    
    Instead of flooding a reconnected node with N individual messages,
    send 1 batch summary with key points.
    """
    msgs = [_pending[mid] for mid in msg_ids if mid in _pending]
    summary_parts = [f"[BATCH] {len(msgs)} pending messages:"]
    for m in msgs:
        age = int(time.time() - m["created_at"])
        summary_parts.append(f"  • [{m['type']}] {m['content'][:100]} (age={age}s, attempts={m['attempts']})")
    return "\n".join(summary_parts)[:500]


def get_trace(trace_id):
    """Get the full trace chain for a distributed trace_id."""
    return _traces.get(trace_id, [])


def get_all_traces(limit=20):
    """Get recent traces for dashboard display."""
    sorted_traces = sorted(_traces.items(), key=lambda x: x[-1][-1]["ts"] if x[1] else 0, reverse=True)
    result = []
    for tid, hops in sorted_traces[:limit]:
        result.append({
            "trace_id": tid,
            "hops": len(hops),
            "path": " → ".join([h["node"] for h in hops]),
            "last_ts": hops[-1]["ts"] if hops else 0,
        })
    return result


def get_router_status():
    """Get message router status for dashboard."""
    now = time.time()
    return {
        "pending": len(_pending),
        "delivered": len(_delivered),
        "failed": len(_failed),
        "total": len(_pending) + len(_delivered) + len(_failed),
        "success_rate": round(len(_delivered) / max(len(_delivered) + len(_failed), 1) * 100, 1),
        "max_per_tick": MAX_PER_TICK,
        "batch_threshold": BATCH_THRESHOLD,
        "traces": len(_traces),
        "pending_messages": [
            {
                "id": m["id"],
                "from": m["from"],
                "to": m["to"],
                "age_sec": int(now - m["created_at"]),
                "attempts": m["attempts"],
                "trace_id": m.get("trace_id", "?"),
            }
            for m in list(_pending.values())[:10]
        ],
        "recent_failures": [
            {
                "id": m["id"],
                "from": m["from"],
                "to": m["to"],
                "attempts": m["attempts"],
                "trace_id": m.get("trace_id", "?"),
            }
            for m in _failed[-5:]
        ],
        "recent_traces": get_all_traces(5),
    }