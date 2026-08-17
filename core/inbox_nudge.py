"""
Inbox Nudge Watcher — alert on unread messages in the mesh chat.

Inspired by Marveen's inbox-nudge-watcher:
  - Monitor inbox for unread messages
  - Nudge assigned agent after threshold
  - Escalate if still unread after longer period

For A2A Mesh:
  - Track unread A2A messages per node
  - Nudge after 30min, alert after 2h
  - Dashboard visualization
"""

import time
import logging

log = logging.getLogger("inbox_nudge")

# In-memory unread tracking
_unread = {}  # message_id → {to_node, from_node, timestamp, content_preview, nudged}
NUDGE_AFTER = 1800  # 30 min
ALERT_AFTER = 7200  # 2 hours


def record_unread(msg_id, to_node, from_node, content_preview=""):
    """Record an unread message."""
    _unread[msg_id] = {
        "to_node": to_node,
        "from_node": from_node,
        "timestamp": time.time(),
        "content_preview": content_preview[:80],
        "nudged": False,
        "alerted": False,
    }


def mark_read(msg_id):
    """Mark a message as read."""
    _unread.pop(msg_id, None)


def check_nudges():
    """Check for messages needing nudge/alert. Returns list of actions."""
    now = time.time()
    actions = []
    
    for msg_id, info in list(_unread.items()):
        age = now - info["timestamp"]
        
        if age > ALERT_AFTER and not info["alerted"]:
            actions.append({
                "action": "alert",
                "msg_id": msg_id,
                "to_node": info["to_node"],
                "from_node": info["from_node"],
                "age_min": int(age / 60),
                "preview": info["content_preview"],
            })
            info["alerted"] = True
            
        elif age > NUDGE_AFTER and not info["nudged"]:
            actions.append({
                "action": "nudge",
                "msg_id": msg_id,
                "to_node": info["to_node"],
                "from_node": info["from_node"],
                "age_min": int(age / 60),
                "preview": info["content_preview"],
            })
            info["nudged"] = True
    
    return actions


def get_inbox_status():
    """Get inbox status for dashboard."""
    now = time.time()
    unread_list = [
        {
            "msg_id": mid,
            "to_node": info["to_node"],
            "from_node": info["from_node"],
            "age_min": int((now - info["timestamp"]) / 60),
            "preview": info["content_preview"],
            "nudged": info["nudged"],
            "alerted": info["alerted"],
        }
        for mid, info in _unread.items()
    ]
    unread_list.sort(key=lambda x: x["age_min"], reverse=True)
    
    return {
        "total_unread": len(_unread),
        "nudged": sum(1 for i in _unread.values() if i["nudged"]),
        "alerted": sum(1 for i in _unread.values() if i["alerted"]),
        "messages": unread_list[:20],
        "nudge_after_min": NUDGE_AFTER // 60,
        "alert_after_min": ALERT_AFTER // 60,
    }