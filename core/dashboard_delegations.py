"""A2A Mesh Dashboard — Delegations mixin. Delegation CRUD, status, files endpoints."""

import logging
import os

log = logging.getLogger("a2a_mesh.dashboard.delegations")


class DashboardDelegationsMixin:
    # ── Delegation API endpoints ──

    async def _api_delegations_list(self, request):
        """List delegations. GET /api/delegations?status=pending&agent=nova&task_type=monitoring"""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        try:
            status = request.query.get("status")
            direction = request.query.get("direction", "all")
            agent = request.query.get("agent")
            task_type = request.query.get("task_type")
            limit = int(request.query.get("limit", "50"))
            
            if not self.node or not self.node.delegation:
                return web.json_response({"error": "Delegation not available"}, status=503)
            
            if direction == "sent":
                tasks = await self.node.delegation.get_my_delegations(status=status)
            elif direction == "received":
                tasks = await self.node.delegation.get_assigned_tasks(status=status)
            else:
                tasks = await self.node.delegation.get_all_delegations(limit=limit)
            
            # Client-side filtering by agent and task_type
            if agent:
                tasks = [t for t in tasks if t.get("from_agent") == agent or t.get("to_agent") == agent or t.get("assigned_agent") == agent]
            if task_type:
                tasks = [t for t in tasks if t.get("task_type") == task_type]
            if status:
                tasks = [t for t in tasks if t.get("status") == status]
            
            # Convert UUID and datetime to strings for JSON
            for t in tasks:
                for k, v in t.items():
                    if hasattr(v, 'hex'):
                        t[k] = str(v)
                    elif hasattr(v, 'isoformat'):
                        t[k] = v.isoformat()
            
            return web.json_response({"delegations": tasks, "count": len(tasks)})
        except Exception as e:
            log.error(f"Delegations list error: {e}", exc_info=True)
            return web.json_response({"error": str(e)}, status=500)

    async def _api_delegations_create(self, request):
        """Create a new delegation. POST /api/delegations
        Body: {to_agent, subject, description?, task_type?, priority?, context?, timeout_minutes?, available?}
        to_agent='any' + available=true → any agent can claim
        to_agent='morzsa' → targeted delegation
        """
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        try:
            data = await request.json()
            to_agent = data.get("to_agent")
            subject = data.get("subject")
            available = data.get("available", False)
            
            if not subject:
                return web.json_response({"error": "subject required"}, status=400)
            if not to_agent:
                return web.json_response({"error": "to_agent required (or set available=true)"}, status=400)
            
            # 'any' means available for any agent to claim
            if to_agent == "any" or available:
                to_agent = "any"
                available = True
            
            # Verify target agent exists (best-effort, don't block creation)
            if not available and self.node and self.node.peer_discovery:
                known = list(self.node.peer_discovery._peers.keys())
                if to_agent not in known and to_agent != self.node.node_name:
                    return web.json_response({"error": f"Unknown agent: {to_agent}. Known: {known}"}, status=400)
            
            task_id = await self.node.delegation.delegate_task(
                to_agent=to_agent,
                subject=subject,
                description=data.get("description", ""),
                task_type=data.get("task_type", "generic"),
                priority=int(data.get("priority", "5")),
                context=data.get("context"),
                timeout_minutes=int(data.get("timeout_minutes", "30")),
                available=available,
                fan_out=int(data.get("fan_out", "0")),
                max_retries=int(data.get("max_retries", "2")),
                eligible_agents=data.get("eligible_agents"),
            )
            
            # fan_out returns list of task_ids
            is_fan_out = isinstance(task_id, list)
            task_ids = task_id if is_fan_out else [task_id]
            status = "available" if available else "pending"
            
            if is_fan_out:
                return web.json_response({
                    "task_ids": task_ids,
                    "count": len(task_ids),
                    "status": status,
                    "to_agent": to_agent,
                    "subject": subject,
                    "fan_out": True,
                })
            else:
                return web.json_response({"task_id": task_id, "status": status, "to_agent": to_agent, "subject": subject})
        except Exception as e:
            log.error(f"Delegation create error: {e}", exc_info=True)
            return web.json_response({"error": str(e)}, status=500)

    async def _api_delegations_stats(self, request):
        """Get delegation statistics. GET /api/delegations/stats"""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        try:
            if not self.node or not self.node.delegation:
                return web.json_response({"error": "Delegation not available"}, status=503)
            stats = await self.node.delegation.get_delegation_stats()
            return web.json_response(stats)
        except Exception as e:
            log.error(f"Delegation stats error: {e}", exc_info=True)
            return web.json_response({"error": str(e)}, status=500)

    async def _api_delegations_status(self, request):
        """Get status of a specific delegation. GET /api/delegations/{task_id}"""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        try:
            task_id = request.match_info.get("task_id")
            if not self.node or not self.node.delegation:
                return web.json_response({"error": "Delegation not available"}, status=503)
            task = await self.node.delegation.get_task_status(task_id)
            if not task:
                return web.json_response({"error": "Task not found"}, status=404)
            for k, v in task.items():
                if hasattr(v, 'hex'):
                    task[k] = str(v)
                elif hasattr(v, 'isoformat'):
                    task[k] = v.isoformat()
            return web.json_response(task)
        except Exception as e:
            log.error(f"Delegation status error: {e}", exc_info=True)
            return web.json_response({"error": str(e)}, status=500)

    async def _api_delegations_cancel(self, request):
        """Cancel a pending delegation. POST /api/delegations/{task_id}/cancel"""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        try:
            task_id = request.match_info.get("task_id")
            if not self.node or not self.node.delegation:
                return web.json_response({"error": "Delegation not available"}, status=503)
            success = await self.node.delegation.cancel_task(task_id)
            if success:
                return web.json_response({"task_id": task_id, "status": "cancelled"})
            else:
                return web.json_response({"error": "Task not found or already running/completed"}, status=404)
        except Exception as e:
            log.error(f"Delegation cancel error: {e}", exc_info=True)
            return web.json_response({"error": str(e)}, status=500)

    async def _api_delegations_available(self, request):
        """Get available tasks for claiming. GET /api/delegations/available"""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        try:
            if not self.node or not self.node.delegation:
                return web.json_response({"error": "Delegation not available"}, status=503)
            tasks = await self.node.delegation.get_available_tasks()
            for t in tasks:
                for k, v in t.items():
                    if hasattr(v, 'hex'):
                        t[k] = str(v)
                    elif hasattr(v, 'isoformat'):
                        t[k] = v.isoformat()
            return web.json_response({"available": tasks, "count": len(tasks)})
        except Exception as e:
            log.error(f"Available tasks error: {e}", exc_info=True)
            return web.json_response({"error": str(e)}, status=500)

    async def _api_delegations_claim(self, request):
        """Claim an available task. POST /api/delegations/{task_id}/claim"""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        try:
            task_id = request.match_info.get("task_id")
            if not self.node or not self.node.delegation:
                return web.json_response({"error": "Delegation not available"}, status=503)
            success = await self.node.delegation.claim_task(task_id)
            if success:
                return web.json_response({"task_id": task_id, "status": "accepted", "claimed_by": self.node.node_name})
            else:
                return web.json_response({"error": "Task not available for claiming"}, status=404)
        except Exception as e:
            log.error(f"Claim error: {e}", exc_info=True)
            return web.json_response({"error": str(e)}, status=500)

    async def _api_delegations_reassign(self, request):
        """Reassign a task to a different agent. POST /api/delegations/{task_id}/reassign
        Body: {"agent": "morzsa"}
        """
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        try:
            task_id = request.match_info.get("task_id")
            data = await request.json()
            new_agent = data.get("agent")
            if not new_agent:
                return web.json_response({"error": "agent required"}, status=400)
            if not self.node or not self.node.delegation:
                return web.json_response({"error": "Delegation not available"}, status=503)
            success = await self.node.delegation.reassign_task(task_id, new_agent)
            if success:
                return web.json_response({"task_id": task_id, "assigned_to": new_agent})
            else:
                return web.json_response({"error": "Task not found or cannot be reassigned"}, status=404)
        except Exception as e:
            log.error(f"Reassign error: {e}", exc_info=True)
            return web.json_response({"error": str(e)}, status=500)

    async def _api_delegations_note(self, request):
        """Add a note to a task. POST /api/delegations/{task_id}/note
        Body: {"note": "Progress update text"}
        """
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        try:
            task_id = request.match_info.get("task_id")
            data = await request.json()
            note = data.get("note")
            if not note:
                return web.json_response({"error": "note required"}, status=400)
            if not self.node or not self.node.delegation:
                return web.json_response({"error": "Delegation not available"}, status=503)
            success = await self.node.delegation.add_note(task_id, note)
            if success:
                return web.json_response({"task_id": task_id, "note_added": note, "by": self.node.node_name})
            else:
                return web.json_response({"error": "Task not found"}, status=404)
        except Exception as e:
            log.error(f"Note error: {e}", exc_info=True)
            return web.json_response({"error": str(e)}, status=500)

    async def _api_delegations_progress(self, request):
        """Update task progress. POST /api/delegations/{task_id}/progress
        Body: {"progress": 50, "note": "Halfway done"} (note optional)
        """
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        try:
            task_id = request.match_info.get("task_id")
            data = await request.json()
            progress = int(data.get("progress", 0))
            note = data.get("note")
            if not self.node or not self.node.delegation:
                return web.json_response({"error": "Delegation not available"}, status=503)
            success = await self.node.delegation.update_progress(task_id, progress, note)
            if success:
                return web.json_response({"task_id": task_id, "progress": progress})
            else:
                return web.json_response({"error": "Task not found"}, status=404)
        except Exception as e:
            log.error(f"Progress error: {e}", exc_info=True)
            return web.json_response({"error": str(e)}, status=500)

    async def _api_delegations_files(self, request):
        """Get result files for a delegation. GET /api/delegations/{task_id}/files
        Query param: download=1 to get raw file content instead of JSON metadata
        Query param: token for auth via URL (for file downloads)
        Query param: file_id=UUID to download a specific file
        Query param: zip=1 to download all files as a ZIP archive"""
        import base64
        import io
        import zipfile
        from aiohttp import web
        # Support token auth via query param for file downloads
        token = request.query.get("token", "")
        auth_header = request.headers.get("Authorization", "")
        if token and not auth_header:
            auth_header = f"Bearer {token}"
        user = None
        err = None
        if auth_header:
            try:
                payload = self.auth.verify_token(auth_header.replace("Bearer ", ""))
                if payload:
                    user = payload
            except Exception:
                pass
        if not user:
            user, err = self._require_auth(request)
            if err:
                return err
        try:
            task_id = request.match_info.get("task_id")
            download = request.query.get("download", "0") == "1"
            file_id_param = request.query.get("file_id", "")
            as_zip = request.query.get("zip", "0") == "1"
            if not self.node or not self.node.delegation:
                return web.json_response({"error": "Delegation not available"}, status=503)
            # Get the delegation
            row = await self.node.delegation.pg_pool.fetchrow(
                "SELECT result_file, from_agent, to_agent, assigned_agent, subject FROM shared_delegations WHERE task_id = $1", task_id,
            )
            if not row:
                return web.json_response({"error": "Task not found"}, status=404)
            result_file_id = row.get("result_file")
            assigned = row.get("assigned_agent") or row.get("to_agent") or ""
            subject = row.get("subject", "result")
            # Collect ALL files for this task: result_file + any files from assigned agent about this task
            file_rows = []
            if result_file_id:
                fr = await self.node.delegation.pg_pool.fetchrow(
                    "SELECT id, filename, content_type, file_size, encoding, content, description, created_at FROM shared_files WHERE id = $1", result_file_id,
                )
                if fr:
                    file_rows.append(fr)
            # Also find files linked by sender_agent matching the task subject/timeframe
            extra_files = await self.node.delegation.pg_pool.fetch(
                "SELECT id, filename, content_type, file_size, encoding, content, description, created_at FROM shared_files WHERE sender_agent = $1 AND description LIKE $2 AND id != $3 ORDER BY created_at DESC LIMIT 10",
                assigned, f"%{subject[:30]}%", result_file_id or "00000000-0000-0000-0000-000000000000",
            )
            for ef in extra_files:
                if ef not in file_rows:
                    file_rows.append(ef)
            if not file_rows:
                return web.json_response({"files": [], "message": "No files attached"})
            # Helper: decode file content — returns (decoded_content, is_binary, raw_bytes)
            # For text files: (str, False, None)
            # For binary files: (None, True, bytes)
            def decode_file(f):
                fd = dict(f)
                for k, v in fd.items():
                    if hasattr(v, 'hex'):
                        fd[k] = str(v)
                    elif hasattr(v, 'isoformat'):
                        fd[k] = v.isoformat()
                raw_b64 = fd.get("content", "")
                fn = fd.get("filename", "result.txt")
                # Determine if this is a binary file type
                _BINARY_EXTS = {".pptx", ".xlsx", ".docx", ".pdf", ".png", ".jpg", ".jpeg",
                                ".gif", ".webp", ".ico", ".zip", ".tar", ".gz", ".rar",
                                ".mp3", ".wav", ".mp4", ".avi", ".mkv", ".sqlite", ".db",
                                ".odt", ".ods", ".odp", ".rtf", ".psd", ".tiff", ".bmp"}
                _, fext = os.path.splitext(fn) if 'os' in dir() else (None, fn[fn.rfind('.'):].lower() if '.' in fn else '')
                fext_lower = fext.lower() if fext else ''
                is_binary = fext_lower in _BINARY_EXTS

                if fd.get("encoding") == "base64" and raw_b64:
                    try:
                        raw_bytes = base64.b64decode(raw_b64)
                    except Exception:
                        raw_bytes = raw_b64.encode("utf-8", errors="replace")

                    if is_binary:
                        # Keep as binary — don't try to decode
                        fd["content"] = None  # text content not applicable
                        fd["_raw_bytes"] = raw_bytes
                        fd["_is_binary"] = True
                    else:
                        # Text file — try to decode
                        try:
                            text = raw_bytes.decode("utf-8")
                        except UnicodeDecodeError:
                            # Check for null bytes — treat as binary
                            if b"\x00" in raw_bytes:
                                fd["content"] = None
                                fd["_raw_bytes"] = raw_bytes
                                fd["_is_binary"] = True
                            else:
                                try:
                                    text = raw_bytes.decode("latin-1")
                                except Exception:
                                    text = raw_bytes.decode("utf-8", errors="replace")
                                fd["content"] = text
                                fd["_is_binary"] = False
                        else:
                            fd["content"] = text
                            fd["_is_binary"] = False
                else:
                    # Not base64 — plain text content
                    raw = raw_b64
                    # Check for null bytes
                    if "\x00" in raw:
                        fd["content"] = None
                        fd["_raw_bytes"] = raw.encode("utf-8", errors="replace")
                        fd["_is_binary"] = True
                    else:
                        fd["content"] = raw
                        fd["_is_binary"] = False

                # Fix filename: if .txt but content suggests otherwise
                if not fd.get("_is_binary", False) and fd.get("content"):
                    text_content = fd["content"]
                    if fn.endswith(".txt"):
                        if text_content.strip().startswith("```python"):
                            fn = fn[:-4] + ".py"
                            fd["filename"] = fn
                            fd["content_type"] = "text/x-python"
                        elif text_content.strip().startswith("<!DOCTYPE") or text_content.strip().startswith("<html"):
                            fn = fn[:-4] + ".html"
                            fd["filename"] = fn
                            fd["content_type"] = "text/html"
                    # Strip code fences
                    if text_content.strip().startswith("```python"):
                        fd["content"] = "\n".join(text_content.strip().split("\n")[1:-1]) if text_content.strip().endswith("```") else text_content.strip()[len("```python"):]
                        if fd["content"].endswith("```"):
                            fd["content"] = fd["content"][:-3]
                    elif text_content.strip().startswith("```") and text_content.strip().endswith("```"):
                        fd["content"] = text_content.strip()[3:-3]
                return fd

            # Specific file download by file_id
            if file_id_param and download:
                for f in file_rows:
                    if str(f["id"]) == file_id_param:
                        fd = decode_file(f)
                        filename = fd.get("filename", "result.txt")
                        content_type = fd.get("content_type", "application/octet-stream")
                        if fd.get("_is_binary", False):
                            # Binary file — send raw bytes
                            return web.Response(
                                body=fd["_raw_bytes"],
                                content_type=content_type,
                                headers={"Content-Disposition": f'attachment; filename="{filename}"'},
                            )
                        else:
                            # Text file — send as text
                            return web.Response(
                                text=fd.get("content", ""),
                                content_type=content_type,
                                headers={"Content-Disposition": f'attachment; filename="{filename}"'},
                            )
                return web.json_response({"error": "File not found"}, status=404)

            # ZIP download of all files
            if as_zip and len(file_rows) > 1:
                zip_buf = io.BytesIO()
                with zipfile.ZipFile(zip_buf, 'w', zipfile.ZIP_DEFLATED) as zf:
                    for f in file_rows:
                        fd = decode_file(f)
                        if fd.get("_is_binary", False):
                            zf.writestr(fd.get("filename", "file"), fd["_raw_bytes"])
                        else:
                            content_bytes = (fd.get("content") or "").encode("utf-8")
                            zf.writestr(fd.get("filename", "file"), content_bytes)
                zip_buf.seek(0)
                safe_subject = "".join(c if c.isalnum() or c in "- _" else "_" for c in subject)[:40]
                return web.Response(
                    body=zip_buf.read(),
                    content_type="application/zip",
                    headers={"Content-Disposition": f'attachment; filename="{safe_subject}.zip"'},
                )

            # Single file download (backward compat)
            if download and len(file_rows) == 1:
                fd = decode_file(file_rows[0])
                filename = fd.get("filename", "result.txt")
                content_type = fd.get("content_type", "application/octet-stream")
                if fd.get("_is_binary", False):
                    return web.Response(
                        body=fd["_raw_bytes"],
                        content_type=content_type,
                        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
                    )
                else:
                    return web.Response(
                        text=fd.get("content", ""),
                        content_type=content_type,
                        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
                    )

            # JSON listing of all files
            files_list = []
            for f in file_rows:
                fd = decode_file(f)
                # For binary files, don't include content in listing
                is_binary = fd.get("_is_binary", False)
                content_preview = ""
                if not is_binary and fd.get("content"):
                    content_preview = fd["content"][:500]
                elif is_binary:
                    content_preview = f"[Binary file — {fd.get('content_type', 'application/octet-stream')}]"
                files_list.append({
                    "id": fd["id"],
                    "filename": fd.get("filename", "result.txt"),
                    "content_type": fd.get("content_type", "text/plain"),
                    "file_size": fd.get("file_size", len(fd.get("_raw_bytes", b"")) if is_binary else len(fd.get("content", "") or "")),
                    "is_binary": is_binary,
                    "description": fd.get("description", ""),
                    "created_at": fd.get("created_at", ""),
                    "preview": content_preview,
                })
            return web.json_response({"files": files_list, "count": len(files_list)})
        except Exception as e:
            log.error(f"Files error: {e}", exc_info=True)
            return web.json_response({"error": str(e)}, status=500)

    async def _api_deploy(self, request):
        """Deploy to peer nodes. POST /api/deploy
        
        Body (optional):
        {
            "nodes": ["morzsa", "runa"],  # default: all peers
            "remote": "gitea",            # default: "gitea" for Morzsa, "origin" for Runa
            "branch": "main",
            "timeout": 60
        }
        
        Delegates a "deploy" task to each specified peer node.
        Each peer runs: git pull + restart + health check.
        """
        from aiohttp import web
        import json as _json
        user, err = self._require_auth(request)
        if err:
            return err
        try:
            if not self.node or not self.node.delegation:
                return web.json_response({"error": "Delegation not available"}, status=503)

            body = {}
            try:
                body = await request.json()
            except Exception:
                pass

            target_nodes = body.get("nodes", [])
            branch = body.get("branch", "main")
            timeout = body.get("timeout", 60)

            # If no nodes specified, deploy to all known peers
            if not target_nodes:
                peers = self.node.peer_discovery.get_known_peers() if hasattr(self.node, 'peer_discovery') else []
                target_nodes = [p for p in peers if p != self.node.node_name]

            if not target_nodes:
                return web.json_response({"error": "No peer nodes to deploy to"}, status=400)

            results = []
            for peer_name in target_nodes:
                # Determine remote name per node
                remote = "gitea" if peer_name == "morzsa" else "origin"
                restart_cmd = "systemctl --user restart a2a-mesh"
                health_url = f"http://localhost:8650/api/health"

                deploy_desc = _json.dumps({
                    "remote": remote,
                    "branch": branch,
                    "restart_cmd": restart_cmd,
                    "health_url": health_url,
                    "timeout": timeout,
                })

                try:
                    task_id = await self.node.delegation.delegate_task(
                        to_agent=peer_name,
                        subject=f"[DEPLOY] Deploy {branch} to {peer_name}",
                        description=deploy_desc,
                        task_type="deploy",
                        priority=3,  # high priority
                        timeout_minutes=max(5, timeout // 60 + 2),
                    )
                    results.append({
                        "node": peer_name,
                        "task_id": task_id,
                        "status": "delegated",
                    })
                    log.info(f"Deploy delegated to {peer_name}: task_id={task_id}")
                except Exception as e:
                    results.append({
                        "node": peer_name,
                        "status": "failed",
                        "error": str(e),
                    })
                    log.error(f"Deploy to {peer_name} failed: {e}")

            return web.json_response({
                "deploy_id": f"deploy-{int(__import__('time').time())}",
                "results": results,
                "total": len(results),
            })
        except Exception as e:
            log.error(f"Deploy API error: {e}", exc_info=True)
            return web.json_response({"error": str(e)}, status=500)