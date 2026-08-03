"""Diagnostics-related API handlers for the A2A Mesh dashboard.

Extracted from dashboard.py as part of the mixin refactoring.
Provides diagnostic report, suggestion, and debug log endpoints.
"""

import logging

log = logging.getLogger("a2a_mesh.dashboard.diagnostics")


class DashboardDiagnosticsMixin:
    """Mixin providing diagnostics and debug log API handlers for DashboardHandler."""

    async def _api_debug_logs(self, request):
        """GET /api/debug/logs?level=INFO&category=general&source=morzsa&limit=50 — Query debug logs."""
        from aiohttp import web
        try:
            pool = self.node._pg_pool
            if not pool or not pool.is_connected():
                return web.json_response({"error": "DB not connected", "logs": [], "count": 0}, status=503)
            level = request.query.get("level", "")
            category = request.query.get("category", "")
            source = request.query.get("source", "")
            limit = min(int(request.query.get("limit", "50")), 500)
            query = "SELECT id, source_node, log_level, category, message, metadata, created_at " \
                    "FROM mesh.mesh_debug_logs WHERE 1=1"
            params = []
            idx = 1
            if level:
                query += f" AND log_level = ${idx}"
                params.append(level.upper())
                idx += 1
            if category:
                query += f" AND category = ${idx}"
                params.append(category)
                idx += 1
            if source:
                query += f" AND source_node = ${idx}"
                params.append(source)
                idx += 1
            query += f" ORDER BY created_at DESC LIMIT ${idx}"
            params.append(limit)
            rows = await pool.fetch(query, *params)
            logs = []
            for r in rows:
                logs.append({
                    "id": str(r["id"]),
                    "source_node": r["source_node"],
                    "level": r["log_level"],
                    "category": r["category"],
                    "message": r["message"],
                    "metadata": r["metadata"] if isinstance(r["metadata"], dict) else {},
                    "created_at": str(r["created_at"]),
                })
            return web.json_response({"logs": logs, "count": len(logs)})
        except Exception as e:
            from aiohttp import web
            return web.json_response({"error": str(e), "logs": [], "count": 0}, status=500)

    async def _api_debug_log_create(self, request):
        """POST /api/debug/log — Create a debug log entry.

        Body: {"level": "INFO", "category": "startup", "message": "...", "metadata": {}}
        """
        from aiohttp import web
        try:
            data = await request.json()
            level = data.get("level", "INFO").upper()
            category = data.get("category", "general")
            message = data.get("message", "")
            metadata = data.get("metadata", {})
            if not message:
                return web.json_response({"error": "message is required"}, status=400)
            await self.node.debug_log(level, category, message, metadata)
            return web.json_response({"status": "ok", "level": level, "category": category})
        except Exception as e:
            from aiohttp import web
            return web.json_response({"error": str(e)}, status=500)

    async def _api_diagnostics(self, request):
        """GET /api/diagnostics — Diagnostic engine status."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        diagnostics = getattr(self.node, 'diagnostics', None)
        if not diagnostics:
            return web.json_response({"error": "Diagnostics not available"}, status=503)
        return web.json_response({
            "status": diagnostics.get_status(),
            "recent_reports": [
                {"id": r.report_id, "node": r.node, "severity": r.severity, "summary": r.summary, "timestamp": r.timestamp}
                for r in diagnostics.get_reports(limit=5)
            ],
            "recent_suggestions": [
                {"id": s.suggestion_id, "node": s.node, "category": s.category, "priority": s.priority, "title": s.title, "timestamp": s.timestamp}
                for s in diagnostics.get_suggestions(limit=5)
            ],
        })

    async def _api_diagnostic_reports(self, request):
        """GET /api/diagnostics/reports — List diagnostic reports."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        diagnostics = getattr(self.node, 'diagnostics', None)
        if not diagnostics:
            return web.json_response({"error": "Diagnostics not available"}, status=503)
        limit = int(request.query.get("limit", "20"))
        severity = request.query.get("severity")
        reports = diagnostics.get_reports(limit=limit, severity=severity)
        return web.json_response({
            "count": len(reports),
            "reports": [r.to_dict() for r in reports],
        })

    async def _api_diagnostic_suggestions(self, request):
        """GET /api/diagnostics/suggestions — List config suggestions.
        Merges in-memory suggestions with PG-persisted ones for cross-node visibility."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        diagnostics = getattr(self.node, 'diagnostics', None)
        if not diagnostics:
            return web.json_response({"error": "Diagnostics not available"}, status=503)
        limit = int(request.query.get("limit", "50"))
        category = request.query.get("category")
        status_filter = request.query.get("status")
        
        # Start with in-memory suggestions
        suggestions = diagnostics.get_suggestions(limit=limit, category=category)
        suggestion_ids = {s.suggestion_id for s in suggestions}
        
        # Also load from PG for cross-node visibility and persistence
        pg_pool = getattr(self.node, '_pg_pool', None)
        if pg_pool:
            try:
                query = "SELECT * FROM mesh_suggestions ORDER BY created_at DESC LIMIT $1"
                params = [limit]
                if category:
                    query = "SELECT * FROM mesh_suggestions WHERE category = $1 ORDER BY created_at DESC LIMIT $2"
                    params = [category, limit]
                rows = await pg_pool.fetch(query, *params)
                for row in rows:
                    d = dict(row)
                    sid = d.get("suggestion_id", "")
                    if sid not in suggestion_ids:
                        from core.diagnostics import ConfigSuggestion
                        s = ConfigSuggestion(
                            suggestion_id=sid,
                            node=d.get("node", ""),
                            timestamp=d.get("created_at", "").isoformat() if hasattr(d.get("created_at"), "isoformat") else str(d.get("created_at", "")),
                            category=d.get("category", "general"),
                            priority=d.get("priority", "low"),
                            title=d.get("title", ""),
                            description=d.get("description", ""),
                            current_value=d.get("current_value", ""),
                            suggested_value=d.get("suggested_value", ""),
                            rationale=d.get("rationale", ""),
                            affected_nodes=d.get("affected_nodes", []),
                            status=d.get("status", "pending"),
                        )
                        suggestions.append(s)
                        suggestion_ids.add(sid)
            except Exception as e:
                log.debug(f"Failed to load PG suggestions: {e}")
        
        # Apply status filter after merging
        if status_filter:
            suggestions = [s for s in suggestions if s.status == status_filter]
        
        # Sort by priority then timestamp
        priority_order = {"critical": 0, "high": 1, "medium": 2, "low": 3}
        suggestions.sort(key=lambda s: (priority_order.get(s.priority, 4), s.timestamp or ""), reverse=False)
        
        return web.json_response({
            "count": len(suggestions),
            "suggestions": [s.to_dict() for s in suggestions[-limit:]],
        })

    async def _api_diagnostic_report_generate(self, request):
        """POST /api/diagnostics/report — Generate a diagnostic report on demand."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        diagnostics = getattr(self.node, 'diagnostics', None)
        if not diagnostics:
            return web.json_response({"error": "Diagnostics not available"}, status=503)
        try:
            data = await request.json()
        except Exception:
            data = {}
        report_type = data.get("report_type", "on_demand")
        report = await diagnostics.generate_report(report_type=report_type)
        if report:
            diagnostics._store_report(report)
            await diagnostics._broadcast_report(report)
            # Auto-generate suggestions from on-demand report
            try:
                await diagnostics._generate_suggestions_from_report(report)
            except Exception as e:
                log.warning(f"Failed to auto-generate suggestions from on-demand report: {e}")
            return web.json_response(report.to_dict(), status=201)
        return web.json_response({"error": "Failed to generate report"}, status=500)

    async def _api_diagnostic_suggest(self, request):
        """POST /api/diagnostics/suggest — Submit a config suggestion."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        diagnostics = getattr(self.node, 'diagnostics', None)
        if not diagnostics:
            return web.json_response({"error": "Diagnostics not available"}, status=503)
        try:
            data = await request.json()
        except Exception:
            return web.json_response({"error": "Invalid JSON"}, status=400)
        required = ["title", "description"]
        if not all(data.get(k) for k in required):
            return web.json_response({"error": f"Required fields: {required}"}, status=400)
        suggestion = await diagnostics.generate_suggestion(
            category=data.get("category", "general"),
            title=data["title"],
            description=data["description"],
            current_value=data.get("current_value", ""),
            suggested_value=data.get("suggested_value", ""),
            rationale=data.get("rationale", ""),
            priority=data.get("priority", "medium"),
        )
        # Auto-delegate high/critical development suggestions
        if suggestion.category == "development" and suggestion.priority in ("high", "critical"):
            try:
                delegation_mgr = getattr(self.node, 'delegation', None)
                if delegation_mgr:
                    task_title = f"[DEV] {suggestion.title}"
                    task_desc = f"**Fejlesztési javaslat ({suggestion.priority} prioritás)**\n\n{suggestion.description}\n\n**Jelenlegi érték:** {suggestion.current_value}\n**Célérték:** {suggestion.suggested_value}\n**Indoklás:** {suggestion.rationale}\n\n**Érintett node:** {', '.join(suggestion.affected_nodes)}\n**Javaslat ID:** {suggestion.suggestion_id}"
                    await delegation_mgr.delegate_task(
                        to_agent="any",
                        subject=task_title,
                        description=task_desc,
                        task_type="code",
                        priority=7 if suggestion.priority == "critical" else 5,
                        available=True,
                        eligible_agents=["morzsa", "runa"],
                    )
                    log.info(f"📋 Auto-delegated manual suggestion: {suggestion.title}")
            except Exception as e:
                log.warning(f"Failed to auto-delegate manual suggestion {suggestion.suggestion_id}: {e}")
        return web.json_response(suggestion.to_dict(), status=201)

    async def _api_diagnostic_suggestion_update(self, request):
        """PATCH /api/diagnostics/suggestions/:id — Update suggestion status."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        diagnostics = getattr(self.node, 'diagnostics', None)
        if not diagnostics:
            return web.json_response({"error": "Diagnostics not available"}, status=503)
        suggestion_id = request.match_info.get("id", "")
        try:
            data = await request.json()
        except Exception:
            return web.json_response({"error": "Invalid JSON"}, status=400)
        new_status = data.get("status", "")
        result = diagnostics.update_suggestion_status(suggestion_id, new_status)
        if result is None:
            return web.json_response({"error": "Suggestion not found or invalid status"}, status=404)
        return web.json_response(result.to_dict())

    async def _api_diagnostic_auto_implement(self, request):
        """Auto-accept and implement pending diagnostic suggestions."""
        from aiohttp import web
        try:
            diagnostics = self.node.diagnostics if hasattr(self.node, 'diagnostics') and self.node.diagnostics else None
            if not diagnostics:
                return web.json_response({"error": "Diagnostics engine not available"}, status=503)
            
            implemented = await diagnostics.auto_implement_suggestions()
            
            # Return updated suggestions
            suggestions = diagnostics.get_suggestions()
            return web.json_response({
                "implemented": len(implemented),
                "implemented_ids": implemented,
                "suggestions": [s.to_dict() for s in suggestions],
            })
        except Exception as e:
            log.error(f"Error auto-implementing suggestions: {e}")
            return web.json_response({"error": str(e)}, status=500)