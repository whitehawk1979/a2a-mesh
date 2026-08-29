"""A2A Mesh Dashboard — Files mixin.

File upload, list, download endpoints.
"""

import logging
import os

log = logging.getLogger("a2a_mesh.dashboard.files")


class DashboardFilesMixin:
    """File transfer endpoints for the dashboard."""

    async def _api_send_file(self, request):
        """Upload a file to the mesh via P2P file transfer.

        Accepts multipart form with:
        - file: the file to upload
        - recipient: target agent name or 'broadcast' (default: broadcast)

        For broadcast files, sends to all known peers.
        For targeted files, sends to a specific agent.
        """
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err

        reader = await request.multipart()
        file_data = None
        file_name = "upload"
        recipient = ""

        async for part in reader:
            if part.name == "file":
                # Read file data immediately during multipart iteration
                file_data = await part.read()
                file_name = part.filename or "upload"
            elif part.name == "recipient":
                recipient = (await part.text()).strip()

        if not file_data:
            return web.json_response({"error": "No file uploaded"}, status=400)

        file_size = len(file_data)

        # Save uploaded file to incoming_files directory (persistent)
        upload_dir = os.path.join(
            os.path.expanduser("~/.hermes/scripts/a2a_mesh/incoming_files"),
            "uploads"
        )
        os.makedirs(upload_dir, exist_ok=True)

        # Add timestamp to avoid filename collisions
        import time as _time
        ts = int(_time.time())
        safe_name = f"{ts}_{file_name}"
        file_path = os.path.join(upload_dir, safe_name)

        with open(file_path, "wb") as f:
            f.write(file_data)

        if file_size == 0:
            os.unlink(file_path)
            return web.json_response({"error": "Empty file"}, status=400)

        # Max file size: 50MB
        if file_size > 50 * 1024 * 1024:
            os.unlink(file_path)
            return web.json_response({"error": "File too large (max 50MB)"}, status=400)

        # ── Chat attachment record: make the file visible in the chat timeline ──
        # Store a chat message with an attachment payload so it renders as a file
        # card in both the general room and DM conversations.
        import mimetypes as _mimes
        _mime, _ = _mimes.guess_type(file_name)
        _mime = _mime or "application/octet-stream"
        channel = "broadcast" if (recipient or "broadcast") in ("broadcast", "") else recipient
        try:
            pg_pool = getattr(self.node, "pg_pool", None) or getattr(self.node, "_pg_pool", None)
            if pg_pool:
                import uuid as _uuid
                _chat_uuid = str(_uuid.uuid4())
                await pg_pool.execute(
                    """INSERT INTO mesh.mesh_chat_messages
                       (message_uuid, username, sender, recipient, content, msg_type, status)
                       VALUES ($1, $2, $3, $4, $5, 'file', 'sent')""",
                    _chat_uuid,
                    user.username if user else "dashboard",
                    user.username if user else "dashboard",
                    channel,
                    file_name,
                )
                await pg_pool.execute(
                    """INSERT INTO mesh.mesh_chat_files
                       (message_uuid, chat_username, channel, sender, file_name, safe_name, file_type, mime_type, file_size)
                       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)""",
                    _chat_uuid,
                    user.username if user else "dashboard",
                    channel,
                    user.username if user else "dashboard",
                    file_name,
                    safe_name,
                    "uploaded",
                    _mime,
                    file_size,
                )
                log.info(f"📎 Chat file attachment recorded: {safe_name} → channel={channel}")
        except Exception as _fe:
            log.warning(f"Chat file record failed (non-blocking): {_fe}")

        # Determine recipients
        target = recipient or "broadcast"
        results = []

        try:
            if target == "broadcast":
                # Send to all known peers
                peers = self.node.peer_discovery.get_all_peers()
                for peer_name, peer in peers.items():
                    if peer.p2p_available or peer.host:
                        try:
                            offer_msg, file_id = self.node.file_transfer.create_offer_message(
                                file_path, peer_name, priority=5
                            )
                            send_result = await self.node.send(offer_msg)
                            results.append({
                                "peer": peer_name,
                                "file_id": file_id,
                                "success": send_result.success,
                                "error": send_result.error or "",
                            })
                            log.info(f"File upload broadcast: {safe_name} → {peer_name} (file_id={file_id})")
                        except Exception as e:
                            log.error(f"File upload to {peer_name} failed: {e}")
                            results.append({
                                "peer": peer_name,
                                "file_id": "",
                                "success": False,
                                "error": str(e),
                            })
            else:
                # Send to specific agent
                try:
                    offer_msg, file_id = self.node.file_transfer.create_offer_message(
                        file_path, target, priority=5
                    )
                    send_result = await self.node.send(offer_msg)
                    results.append({
                        "peer": target,
                        "file_id": file_id,
                        "success": send_result.success,
                        "error": send_result.error or "",
                    })
                    log.info(f"File upload direct: {safe_name} → {target} (file_id={file_id})")
                except Exception as e:
                    log.error(f"File upload to {target} failed: {e}")
                    results.append({
                        "peer": target,
                        "file_id": "",
                        "success": False,
                        "error": str(e),
                    })

            # Notify dashboard via WebSocket
            await self._broadcast_ws({
                "type": "file_transfer",
                "filename": file_name,
                "safe_name": safe_name,
                "size": file_size,
                "sender": (user.display_name if user else self.node.node_name) or self.node.node_name,
                "recipient": target,
                "results": results,
            })

            # Also broadcast a chat message about the file
            from .message import A2AMessage
            chat_msg = A2AMessage.create(
                sender=self.node.node_name,
                recipient=target,
                msg_type="chat",
                priority=5,
                payload={"text": f"📎 Fájl megosztva: {file_name} ({self._format_size(file_size)})", "username": (user.display_name if user else self.node.node_name) or self.node.node_name, "source": "web_dashboard"}
            )
            await self.node.send(chat_msg)

            return web.json_response({
                "status": "ok",
                "filename": file_name,
                "safe_name": safe_name,
                "size": file_size,
                "results": results,
            })

        except Exception as e:
            log.error(f"File upload failed: {e}")
            return web.json_response({"error": str(e)}, status=500)

    async def _api_list_files(self, request):
        """List received/uploaded files in the mesh."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err

        files = []
        incoming_dir = os.path.expanduser("~/.hermes/scripts/a2a_mesh/incoming_files")
        uploads_dir = os.path.join(incoming_dir, "uploads")

        # Scan incoming files
        for dir_path, label in [(incoming_dir, "received"), (uploads_dir, "uploaded")]:
            if not os.path.isdir(dir_path):
                continue
            for fname in sorted(os.listdir(dir_path), reverse=True):
                fpath = os.path.join(dir_path, fname)
                if os.path.isfile(fpath):
                    stat = os.stat(fpath)
                    files.append({
                        "name": fname,
                        "size": stat.st_size,
                        "size_human": self._format_size(stat.st_size),
                        "modified": stat.st_mtime,
                        "type": label,
                        "url": f"/api/files/{label}/{fname}",
                    })

        return web.json_response({"files": files, "total": len(files)})

    async def _api_download_file(self, request):
        """Download a file from the mesh incoming/uploaded files."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err

        file_type = request.match_info.get("type", "")
        filename = request.match_info.get("filename", "")

        if not filename or file_type not in ("received", "uploaded"):
            return web.json_response({"error": "Invalid request"}, status=400)

        incoming_dir = os.path.expanduser("~/.hermes/scripts/a2a_mesh/incoming_files")
        uploads_dir = os.path.join(incoming_dir, "uploads")

        if file_type == "uploaded":
            base_dir = uploads_dir
        else:
            base_dir = incoming_dir

        file_path = os.path.join(base_dir, filename)

        # Security: prevent directory traversal
        if not os.path.abspath(file_path).startswith(os.path.abspath(base_dir)):
            return web.json_response({"error": "Access denied"}, status=403)

        if not os.path.isfile(file_path):
            return web.json_response({"error": "File not found"}, status=404)

        # download=1 → force download with original-style name; otherwise inline preview
        import mimetypes as _mimetypes
        import urllib.parse as _up
        _mime, _ = _mimetypes.guess_type(filename)
        _mime = _mime or "application/octet-stream"
        headers = {"Content-Type": _mime}
        if request.query.get("download") == "1":
            # Strip the leading timestamp prefix from safe_name for the saved filename
            _orig = filename.split("_", 1)[1] if "_" in filename and filename.split("_", 1)[0].isdigit() else filename
            _disp = f"attachment; filename*=UTF-8''{_up.quote(_orig)}"
            headers["Content-Disposition"] = _disp

        return web.FileResponse(file_path, headers=headers)

    def _format_size(self, size_bytes):
        """Format file size in human-readable form."""
        if size_bytes < 1024:
            return f"{size_bytes} B"
        elif size_bytes < 1024 * 1024:
            return f"{size_bytes / 1024:.1f} KB"
        elif size_bytes < 1024 * 1024 * 1024:
            return f"{size_bytes / (1024 * 1024):.1f} MB"
        else:
            return f"{size_bytes / (1024 * 1024 * 1024):.1f} GB"