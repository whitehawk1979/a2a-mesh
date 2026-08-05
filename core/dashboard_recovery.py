"""
Recovery Notes API — Agent recovery note system.

When an agent goes down and someone fixes it, they leave a recovery note.
When the agent starts up, it reads unread notes and logs them.
"""

import json
import logging
from datetime import datetime

log = logging.getLogger(__name__)


class RecoveryNotesMixin:
    """Mixin for DashboardHandler — recovery notes API endpoints."""

    async def _api_recovery_notes(self, request):
        """GET /api/recovery-notes — list notes (for self or all).
        POST /api/recovery-notes — create a new note.
        """
        from aiohttp import web

        if request.method == "GET":
            user, err = self._require_auth(request)
            if err:
                return err

            target = request.query.get("target", "")
            unread_only = request.query.get("unread", "false").lower() == "true"

            pg_pool = getattr(self.node, "_pg_pool", None)
            if not pg_pool:
                return web.json_response({"error": "PG pool not available"}, status=503)

            try:
                if target:
                    if unread_only:
                        rows = await pg_pool.fetch(
                            """SELECT id, target_node, author, note, actions, created_at, read_at
                               FROM mesh.mesh_recovery_notes
                               WHERE target_node = $1 AND read_at IS NULL
                               ORDER BY created_at DESC""",
                            target,
                        )
                    else:
                        rows = await pg_pool.fetch(
                            """SELECT id, target_node, author, note, actions, created_at, read_at
                               FROM mesh.mesh_recovery_notes
                               WHERE target_node = $1
                               ORDER BY created_at DESC LIMIT 50""",
                            target,
                        )
                else:
                    rows = await pg_pool.fetch(
                        """SELECT id, target_node, author, note, actions, created_at, read_at
                           FROM mesh.mesh_recovery_notes
                           ORDER BY created_at DESC LIMIT 100""",
                    )

                notes = []
                for r in rows:
                    notes.append({
                        "id": r["id"],
                        "target_node": r["target_node"],
                        "author": r["author"],
                        "note": r["note"],
                        "actions": list(r["actions"]) if r["actions"] else [],
                        "created_at": r["created_at"].isoformat() if r["created_at"] else None,
                        "read_at": r["read_at"].isoformat() if r["read_at"] else None,
                    })

                return web.json_response({"notes": notes})
            except Exception as e:
                log.error(f"Recovery notes GET error: {e}", exc_info=True)
                return web.json_response({"error": str(e)}, status=500)

        elif request.method == "POST":
            user, err = self._require_auth(request)
            if err:
                return err

            try:
                data = await request.json()
            except Exception:
                return web.json_response({"error": "Invalid JSON"}, status=400)

            target_node = data.get("target_node", "")
            note_text = data.get("note", "")
            actions = data.get("actions", [])
            author = data.get("author", user.display_name if user else self.node.node_name)

            if not target_node or not note_text:
                return web.json_response({"error": "target_node and note are required"}, status=400)

            pg_pool = getattr(self.node, "_pg_pool", None)
            if not pg_pool:
                return web.json_response({"error": "PG pool not available"}, status=503)

            try:
                row = await pg_pool.fetchrow(
                    """INSERT INTO mesh.mesh_recovery_notes (target_node, author, note, actions)
                       VALUES ($1, $2, $3, $4)
                       RETURNING id, created_at""",
                    target_node, author, note_text, actions,
                )

                log.info(f"Recovery note created: id={row['id']}, target={target_node}, author={author}")

                # Also send a mesh message to the target if it's online
                try:
                    from .message import A2AMessage, MSG_TYPE_DIRECTIVE
                    msg = A2AMessage(
                        sender=author,
                        recipient=target_node,
                        type=MSG_TYPE_DIRECTIVE,
                        priority=3,
                        payload={
                            "text": f"📋 Recovery note from {author}: {note_text}",
                            "source": "recovery_notes",
                            "note_id": row["id"],
                        },
                    )
                    await self.node.router.send(msg)
                except Exception:
                    pass  # Non-critical — the note is in PG

                return web.json_response({
                    "status": "created",
                    "id": row["id"],
                    "created_at": row["created_at"].isoformat(),
                })
            except Exception as e:
                log.error(f"Recovery notes POST error: {e}", exc_info=True)
                return web.json_response({"error": str(e)}, status=500)

    async def _api_recovery_note_read(self, request):
        """POST /api/recovery-notes/{id}/read — mark note as read."""
        from aiohttp import web

        user, err = self._require_auth(request)
        if err:
            return err

        try:
            note_id = int(request.match_info["id"])
        except (ValueError, KeyError):
            return web.json_response({"error": "Invalid note ID"}, status=400)

        pg_pool = getattr(self.node, "_pg_pool", None)
        if not pg_pool:
            return web.json_response({"error": "PG pool not available"}, status=503)

        try:
            await pg_pool.execute(
                "UPDATE mesh.mesh_recovery_notes SET read_at = NOW() WHERE id = $1",
                note_id,
            )
            return web.json_response({"status": "read", "id": note_id})
        except Exception as e:
            log.error(f"Recovery note read error: {e}", exc_info=True)
            return web.json_response({"error": str(e)}, status=500)


async def read_unread_recovery_notes(pg_pool, node_name: str) -> list:
    """Read unread recovery notes for this node at startup.

    Called from node.py during startup. Returns list of notes.
    Also marks them as read.
    """
    if not pg_pool:
        return []

    try:
        rows = await pg_pool.fetch(
            """SELECT id, target_node, author, note, actions, created_at
               FROM mesh.mesh_recovery_notes
               WHERE target_node = $1 AND read_at IS NULL
               ORDER BY created_at ASC""",
            node_name,
        )

        if not rows:
            return []

        notes = []
        for r in rows:
            notes.append({
                "id": r["id"],
                "author": r["author"],
                "note": r["note"],
                "actions": list(r["actions"]) if r["actions"] else [],
                "created_at": r["created_at"].isoformat() if r["created_at"] else None,
            })

            # Mark as read
            await pg_pool.execute(
                "UPDATE mesh.mesh_recovery_notes SET read_at = NOW() WHERE id = $1",
                r["id"],
            )

        return notes
    except Exception as e:
        log.error(f"Failed to read recovery notes: {e}")
        return []