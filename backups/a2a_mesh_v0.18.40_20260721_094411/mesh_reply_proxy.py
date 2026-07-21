#!/usr/bin/env python3
"""A2A Mesh Reply Proxy — Forwards webhook responses to the mesh chat.

This script listens for webhook responses from the Hermes agent and forwards
them to the mesh chat dashboard via the /api/agent-reply endpoint.

It acts as a bridge between the Hermes webhook system and the mesh chat,
ensuring that agent responses appear in the dashboard chat in real-time.
"""

import json
import hashlib
import hmac
import http.server
import threading
import urllib.request
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [mesh-reply-proxy] %(message)s")
log = logging.getLogger(__name__)

MESH_DASHBOARD = "http://localhost:8650"
MESH_REPLY_SECRET = b"mesh-reply-secret-2026"
WEBHOOK_SECRET = b"a2a-instant-secret-2026"


def send_to_mesh_chat(sender: str, content: str, recipient: str = "broadcast",
                      priority: int = 5, reply_to: str = ""):
    """Send an agent reply to the mesh chat dashboard."""
    try:
        payload = json.dumps({
            "sender": sender,
            "content": content,
            "recipient": recipient,
            "priority": priority,
            "reply_to": reply_to,
        }).encode()
        sig = hmac.new(MESH_REPLY_SECRET, payload, hashlib.sha256).hexdigest()
        req = urllib.request.Request(
            f"{MESH_DASHBOARD}/api/agent-reply",
            data=payload,
            headers={
                "Content-Type": "application/json",
                "X-Mesh-Signature": f"sha256={sig}",
            },
        )
        with urllib.request.urlopen(req, timeout=5) as resp:
            result = json.loads(resp.read().decode())
            log.info(f"Reply sent to mesh chat: {result}")
            return result
    except Exception as e:
        log.error(f"Failed to send reply to mesh chat: {e}")
        return None


if __name__ == "__main__":
    # Quick test
    send_to_mesh_chat(
        sender="nova",
        content="Mesh reply proxy is running!",
        recipient="broadcast",
        priority=5,
    )