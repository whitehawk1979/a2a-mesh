"""A2A Mesh Chat — Per-user DM system for dashboard ↔ agent communication.

Each dashboard user gets a personal chat identity. Messages are stored in PG
(mesh.mesh_chat_messages) and routed to agents via the mesh. Agent replies are
stored as DMs back to the user.
"""
import uuid
import logging

log = logging.getLogger("mesh.chat")


async def _ensure_chat_user(pool, username, display_name, node_name):
    """Create or update a chat user in PG."""
    try:
        await pool.execute(
            """INSERT INTO mesh.mesh_chat_users (username, display_name, node_name, last_seen)
               VALUES ($1, $2, $3, NOW())
               ON CONFLICT (username)
               DO UPDATE SET last_seen = NOW(), display_name = $2, node_name = $3""",
            username, display_name or username, node_name
        )
    except Exception as e:
        log.warning(f"Failed to ensure chat user: {e}")


async def handle_chat_send(node, request, pool, user):
    """POST /api/chat/send — Send a DM from dashboard user to an agent.

    Body: { recipient: "morzsa", content: "hello", msg_type: "chat" }
    """
    from aiohttp import web
    data = await request.json()
    recipient = data.get("recipient", "broadcast")
    content = data.get("content", "") or data.get("text", "")
    msg_type = data.get("msg_type", "chat")
    username = getattr(user, "username", None) or (user.get("username", "dashboard") if isinstance(user, dict) else "dashboard")
    display_name = getattr(user, "display_name", None) or username
    node_name = getattr(node, "node_name", "nova")

    if not content.strip():
        return web.json_response({"error": "content is required"}, status=400)

    await _ensure_chat_user(pool, username, display_name, node_name)

    msg_uuid = str(uuid.uuid4())

    # Store in PG
    try:
        row = await pool.fetchrow(
            """INSERT INTO mesh.mesh_chat_messages
               (message_uuid, username, sender, recipient, content, msg_type, status)
               VALUES ($1, $2, $3, $4, $5, $6, 'sent')
               RETURNING id, created_at""",
            msg_uuid, username, username, recipient, content, msg_type
        )
        msg_id = row["id"] if row else None
        created_at = str(row["created_at"]) if row else None
    except Exception as e:
        return web.json_response({"error": f"DB error: {e}"}, status=500)

    # Route to agent via mesh (if not broadcast and not self)
    # If recipient starts with "user:", it's a user→user DM — store only in PG
    # (the other user will see it via /api/chat/messages polling)
    mesh_sent = False
    is_user_dm = recipient.startswith("user:")

    if is_user_dm:
        # User→user DM: already stored in PG above, the recipient user will see it
        # when they poll /api/chat/messages. No mesh routing needed.
        log.info(f"💬 Chat user DM {username}→{recipient}: stored in PG")
        mesh_sent = True
    elif recipient == node_name:
        # User → self (this node): process locally
        # The node can respond immediately or queue for async processing
        try:
            from .dashboard_chat import store_agent_reply
            # Auto-acknowledge: store a reply that the message was received
            await store_agent_reply(pool, username, node_name,
                "✅ Üzenet megkapva! Feldolgozás alatt...", "agent_reply")
            mesh_sent = True
            log.info(f"💬 Chat local {username}→{recipient}: auto-ack stored")
            # TODO: trigger actual agent processing here
        except Exception as e:
            log.warning(f"💬 Chat local reply failed: {e}")
    elif recipient not in ("broadcast", ""):
        try:
            payload = {
                "text": content,
                "subject": content[:80],
                "sender_display": display_name,
                "chat_username": username,
                "chat_msg_uuid": msg_uuid,
                "chat_type": "user_dm"
            }
            result = await node.send_direct(recipient, "a2a_message", payload, priority=5)
            mesh_sent = True
            log.info(f"💬 Chat DM {username}→{recipient}: sent via mesh")
        except Exception as e:
            log.warning(f"💬 Chat DM {username}→{recipient}: mesh send failed: {e}")

    return web.json_response({
        "ok": True,
        "message_id": msg_uuid,
        "db_id": msg_id,
        "recipient": recipient,
        "mesh_sent": mesh_sent,
        "created_at": created_at,
        "status": "sent"
    })


async def handle_chat_messages(node, request, pool, user):
    """GET /api/chat/messages?with=morzsa&limit=50 — Get chat history with a specific agent."""
    from aiohttp import web
    username = getattr(user, "username", None) or (user.get("username", "dashboard") if isinstance(user, dict) else "dashboard")
    peer = request.query.get("with", "")
    limit = int(request.query.get("limit", 50))

    try:
        if peer:
            # DM conversation between user and specific agent/user
            rows = await pool.fetch(
                """SELECT id, message_uuid, username, sender, recipient, content,
                          msg_type, status, created_at, read_at
                   FROM mesh.mesh_chat_messages
                   WHERE username = $1 AND (sender = $2 OR recipient = $2)
                   ORDER BY created_at DESC LIMIT $3""",
                username, peer, limit
            )
        else:
            # All messages for this user
            rows = await pool.fetch(
                """SELECT id, message_uuid, username, sender, recipient, content,
                          msg_type, status, created_at, read_at
                   FROM mesh.mesh_chat_messages
                   WHERE username = $1
                   ORDER BY created_at DESC LIMIT $2""",
                username, limit
            )

        messages = []
        for row in rows:
            r = dict(row)
            r["created_at"] = str(r["created_at"]) if r.get("created_at") else None
            r["read_at"] = str(r["read_at"]) if r.get("read_at") else None
            messages.append(r)

        return web.json_response({"messages": messages, "count": len(messages)})
    except Exception as e:
        return web.json_response({"error": str(e)}, status=500)


async def handle_chat_inbox(node, request, pool, user):
    """GET /api/chat/inbox — Unread messages for this user from agents."""
    from aiohttp import web
    username = getattr(user, "username", None) or (user.get("username", "dashboard") if isinstance(user, dict) else "dashboard")

    try:
        rows = await pool.fetch(
            """SELECT id, message_uuid, sender, recipient, content,
                      msg_type, created_at
               FROM mesh.mesh_chat_messages
               WHERE username = $1 AND sender != $2 AND read_at IS NULL
               ORDER BY created_at DESC LIMIT 50""",
            username, username
        )

        unread = []
        for row in rows:
            r = dict(row)
            r["created_at"] = str(r["created_at"]) if r.get("created_at") else None
            unread.append(r)

        return web.json_response({"unread": unread, "count": len(unread)})
    except Exception as e:
        return web.json_response({"error": str(e)}, status=500)


async def handle_chat_mark_read(node, request, pool, user):
    """POST /api/chat/read — Mark messages from a specific agent as read.
    Body: { from_agent: "morzsa" }
    """
    from aiohttp import web
    username = getattr(user, "username", None) or (user.get("username", "dashboard") if isinstance(user, dict) else "dashboard")
    data = await request.json()
    from_agent = data.get("from_agent", "")

    try:
        result = await pool.execute(
            """UPDATE mesh.mesh_chat_messages
               SET read_at = NOW()
               WHERE username = $1 AND sender = $2 AND read_at IS NULL""",
            username, from_agent
        )
        return web.json_response({"ok": True, "updated": result})
    except Exception as e:
        return web.json_response({"error": str(e)}, status=500)


async def handle_chat_contacts(node, request, pool, user):
    """GET /api/chat/contacts — List agents this user has chatted with + unread counts."""
    from aiohttp import web
    username = getattr(user, "username", None) or (user.get("username", "dashboard") if isinstance(user, dict) else "dashboard")

    try:
        rows = await pool.fetch(
            """SELECT DISTINCT recipient as agent,
                      COUNT(*) as total,
                      COUNT(CASE WHEN read_at IS NULL AND sender != $1 THEN 1 END) as unread,
                      MAX(created_at) as last_msg
               FROM mesh.mesh_chat_messages
               WHERE username = $1
               GROUP BY recipient
               ORDER BY last_msg DESC""",
            username
        )

        contacts = []
        for row in rows:
            r = dict(row)
            r["last_msg"] = str(r["last_msg"]) if r.get("last_msg") else None
            contacts.append(r)

        # Also list all available mesh agents (including self)
        try:
            agent_rows = await pool.fetch(
                "SELECT node_name FROM mesh.mesh_nodes ORDER BY node_name"
            )
            existing = {c["agent"] for c in contacts}
            local_node = getattr(node, "node_name", "nova")
            for ar in agent_rows:
                name = ar["node_name"]
                if name not in existing:
                    contacts.append({"agent": name, "total": 0, "unread": 0, "last_msg": None})
                    existing.add(name)
        except Exception:
            pass

        # Also list dashboard users (for user↔user DM)
        try:
            user_rows = await pool.fetch(
                "SELECT username, display_name FROM mesh.mesh_chat_users WHERE username != $1 ORDER BY username",
                username
            )
            for ur in user_rows:
                uname = "user:" + ur["username"]
                contacts.append({
                    "agent": uname,
                    "display_name": ur.get("display_name") or ur["username"],
                    "total": 0, "unread": 0, "last_msg": None,
                    "is_user": True
                })
        except Exception:
            pass

        return web.json_response({"contacts": contacts, "count": len(contacts)})
    except Exception as e:
        return web.json_response({"error": str(e)}, status=500)


async def store_agent_reply(pool, username, from_agent, content, msg_type="agent_reply"):
    """Store an agent reply as a DM to the user. Called when an agent sends
    a reply message that contains chat_username in the payload."""
    try:
        await pool.execute(
            """INSERT INTO mesh.mesh_chat_messages
               (message_uuid, username, sender, recipient, content, msg_type, status)
               VALUES ($1, $2, $3, $2, $4, $5, 'delivered')""",
            str(uuid.uuid4()), username, from_agent, content, msg_type
        )
        log.info(f"💬 Chat reply stored: {from_agent}→{username} ({len(content)} chars)")
        return True
    except Exception as e:
        log.warning(f"Failed to store agent reply: {e}")
        return False