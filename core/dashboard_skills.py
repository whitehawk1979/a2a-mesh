"""A2A Mesh Dashboard — Skills marketplace mixin.

Skill advertisement, search, delegation, and discovery endpoints.
Agents advertise their capabilities; other agents or users can search
and delegate tasks to the best-matching agent.
"""

import json
import logging
import time as _time

log = logging.getLogger("a2a_mesh.dashboard.skills")


class DashboardSkillsMixin:
    """Skills marketplace endpoints — advertise, search, delegate."""

    async def _api_skills_list(self, request):
        """GET /api/skills — List all advertised skills."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        try:
            pg_pool = getattr(self.node, '_pg_pool', None)
            if not pg_pool:
                return web.json_response({"error": "DB not available"}, status=503)

            rows = await pg_pool.fetch(
                """SELECT skill_id, agent_name, skill_name, display_name,
                          description, tags, cost, max_concurrent,
                          avg_latency_ms, success_rate, status, updated_at
                   FROM mesh.mesh_skills WHERE status = 'active'
                   ORDER BY agent_name, skill_name"""
            )
            skills = []
            for r in rows:
                skills.append({
                    "skill_id": r["skill_id"],
                    "agent": r["agent_name"],
                    "skill_name": r["skill_name"],
                    "display_name": r["display_name"],
                    "description": r["description"],
                    "tags": list(r["tags"]) if r["tags"] else [],
                    "cost": r["cost"],
                    "max_concurrent": r["max_concurrent"],
                    "avg_latency_ms": r["avg_latency_ms"],
                    "success_rate": r["success_rate"],
                    "status": r["status"],
                    "updated_at": r["updated_at"].isoformat() if r["updated_at"] else None,
                })
            return web.json_response({"skills": skills, "total": len(skills)})
        except Exception as e:
            log.error(f"Error listing skills: {e}")
            return web.json_response({"error": str(e)}, status=500)

    async def _api_skills_search(self, request):
        """GET /api/skills/search?q=... — Search skills by name, tag, or description."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        try:
            query = request.query.get("q", "").strip().lower()
            if not query:
                return web.json_response({"error": "Query parameter 'q' required"}, status=400)

            pg_pool = getattr(self.node, '_pg_pool', None)
            if not pg_pool:
                return web.json_response({"error": "DB not available"}, status=503)

            # Search in skill_name, display_name, description, tags
            rows = await pg_pool.fetch(
                """SELECT skill_id, agent_name, skill_name, display_name,
                          description, tags, cost, avg_latency_ms, success_rate
                   FROM mesh.mesh_skills
                   WHERE status = 'active'
                     AND (LOWER(skill_name) LIKE '%' || $1 || '%'
                          OR LOWER(display_name) LIKE '%' || $1 || '%'
                          OR LOWER(description) LIKE '%' || $1 || '%'
                          OR $1 = ANY(tags))
                   ORDER BY success_rate DESC, avg_latency_ms ASC""",
                query
            )
            results = []
            for r in rows:
                results.append({
                    "skill_id": r["skill_id"],
                    "agent": r["agent_name"],
                    "skill_name": r["skill_name"],
                    "display_name": r["display_name"],
                    "description": r["description"],
                    "tags": list(r["tags"]) if r["tags"] else [],
                    "cost": r["cost"],
                    "avg_latency_ms": r["avg_latency_ms"],
                    "success_rate": r["success_rate"],
                })
            return web.json_response({"results": results, "total": len(results)})
        except Exception as e:
            log.error(f"Error searching skills: {e}")
            return web.json_response({"error": str(e)}, status=500)

    async def _api_skills_advertise(self, request):
        """POST /api/skills/advertise — Advertise a skill.

        Body: {
            "skill_name": "code_generation",
            "display_name": "Code Generation",
            "description": "Generate code from natural language",
            "tags": ["coding", "python"],
            "cost": 0.0,
            "max_concurrent": 3
        }
        """
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        try:
            body = await request.json()
            skill_name = body.get("skill_name", "").strip()
            if not skill_name:
                return web.json_response({"error": "skill_name required"}, status=400)

            agent_name = self.node.node_name
            skill_id = f"skill-{agent_name}-{skill_name}"

            pg_pool = getattr(self.node, '_pg_pool', None)
            if not pg_pool:
                return web.json_response({"error": "DB not available"}, status=503)

            # Upsert skill (INSERT ... ON CONFLICT UPDATE)
            await pg_pool.execute(
                """INSERT INTO mesh.mesh_skills
                   (skill_id, agent_name, skill_name, display_name, description,
                    tags, cost, max_concurrent, status, updated_at)
                   VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'active', now())
                   ON CONFLICT (skill_id)
                   DO UPDATE SET
                     display_name = EXCLUDED.display_name,
                     description = EXCLUDED.description,
                     tags = EXCLUDED.tags,
                     cost = EXCLUDED.cost,
                     max_concurrent = EXCLUDED.max_concurrent,
                     status = 'active',
                     updated_at = now()""",
                skill_id, agent_name, skill_name,
                body.get("display_name", skill_name),
                body.get("description", ""),
                body.get("tags", []),
                body.get("cost", 0.0),
                body.get("max_concurrent", 1),
            )
            log.info(f"📋 Skill advertised: {agent_name}/{skill_name}")
            return web.json_response({
                "skill_id": skill_id,
                "agent": agent_name,
                "skill_name": skill_name,
                "status": "advertised"
            })
        except Exception as e:
            log.error(f"Error advertising skill: {e}")
            return web.json_response({"error": str(e)}, status=500)

    async def _api_skills_best(self, request):
        """GET /api/skills/best?task=... — Find the best agent for a task.

        Returns the agent with the highest success_rate and lowest latency
        that has the requested skill.
        """
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        try:
            task = request.query.get("task", "").strip().lower()
            if not task:
                return web.json_response({"error": "Query parameter 'task' required"}, status=400)

            pg_pool = getattr(self.node, '_pg_pool', None)
            if not pg_pool:
                return web.json_response({"error": "DB not available"}, status=503)

            rows = await pg_pool.fetch(
                """SELECT skill_id, agent_name, skill_name, display_name,
                          description, tags, cost, max_concurrent,
                          avg_latency_ms, success_rate
                   FROM mesh.mesh_skills
                   WHERE status = 'active'
                     AND (LOWER(skill_name) LIKE '%' || $1 || '%'
                          OR $1 = ANY(tags))
                   ORDER BY success_rate DESC, avg_latency_ms ASC
                   LIMIT 5""",
                task
            )
            if not rows:
                return web.json_response({"error": "No matching skills found"}, status=404)

            best = rows[0]
            return web.json_response({
                "best_agent": best["agent_name"],
                "skill_id": best["skill_id"],
                "skill_name": best["skill_name"],
                "display_name": best["display_name"],
                "success_rate": best["success_rate"],
                "avg_latency_ms": best["avg_latency_ms"],
                "cost": best["cost"],
                "alternatives": [
                    {"agent": r["agent_name"], "skill_name": r["skill_name"],
                     "success_rate": r["success_rate"], "cost": r["cost"]}
                    for r in rows[1:]
                ]
            })
        except Exception as e:
            log.error(f"Error finding best agent: {e}")
            return web.json_response({"error": str(e)}, status=500)

    async def _api_skills_delete(self, request):
        """DELETE /api/skills/{skill_id} — Remove a skill advertisement."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        try:
            skill_id = request.match_info.get("skill_id", "")
            if not skill_id:
                return web.json_response({"error": "skill_id required"}, status=400)

            agent_name = self.node.node_name
            pg_pool = getattr(self.node, '_pg_pool', None)
            if not pg_pool:
                return web.json_response({"error": "DB not available"}, status=503)

            # Only allow deleting own skills (or owner)
            result = await pg_pool.execute(
                """UPDATE mesh.mesh_skills
                   SET status = 'inactive', updated_at = now()
                   WHERE skill_id = $1 AND agent_name = $2""",
                skill_id, agent_name
            )
            if " 0" in str(result):
                return web.json_response({"error": "Skill not found or not owned"}, status=404)
            log.info(f"📋 Skill removed: {skill_id}")
            return web.json_response({"skill_id": skill_id, "status": "removed"})
        except Exception as e:
            log.error(f"Error removing skill: {e}")
            return web.json_response({"error": str(e)}, status=500)

    async def _api_skills_delegate(self, request):
        """POST /api/skills/{skill_id}/delegate — Delegate a task to the skill's agent.

        Body: {"task": "generate a Python fibonacci function", "context": "..."}
        """
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        try:
            skill_id = request.match_info.get("skill_id", "")
            body = await request.json()
            task_text = body.get("task", "").strip()
            if not task_text:
                return web.json_response({"error": "task required"}, status=400)

            pg_pool = getattr(self.node, '_pg_pool', None)
            if not pg_pool:
                return web.json_response({"error": "DB not available"}, status=503)

            # Get skill info
            row = await pg_pool.fetchrow(
                "SELECT agent_name, skill_name, display_name, cost FROM mesh.mesh_skills WHERE skill_id = $1 AND status = 'active'",
                skill_id
            )
            if not row:
                return web.json_response({"error": "Skill not found"}, status=404)

            target_agent = row["agent_name"]
            skill_name = row["skill_name"]

            # Create a delegation via the existing delegation system
            delegation_mgr = getattr(self.node, 'delegation', None) or getattr(self.node, 'delegation_mgr', None)
            if not delegation_mgr:
                return web.json_response({"error": "Delegation system not available"}, status=503)

            # Create delegation
            import asyncio
            delegation = await delegation_mgr.create_delegation(
                from_agent=self.node.node_name,
                to_agent=target_agent,
                task=f"[{skill_name}] {task_text}",
                context=body.get("context", ""),
                priority=body.get("priority", 5),
            )
            log.info(f"📋 Skill delegation: {self.node.node_name} → {target_agent}/{skill_name}")

            return web.json_response({
                "delegation_id": delegation.get("task_id", ""),
                "target_agent": target_agent,
                "skill_name": skill_name,
                "task": task_text,
                "status": "delegated"
            })
        except Exception as e:
            log.error(f"Error delegating to skill: {e}")
            return web.json_response({"error": str(e)}, status=500)

    async def _api_skills_rate(self, request):
        """POST /api/skills/{skill_id}/rate — Rate a skill and optionally leave feedback.

        Body: {"rating": 1-5, "feedback": "optional text", "delegation_id": "optional"}
        """
        from aiohttp import web
        import json as _json
        user, err = self._require_auth(request)
        if err:
            return err
        try:
            skill_id = request.match_info.get("skill_id", "")
            body = await request.json()
            rating = int(body.get("rating", 0))
            if rating < 1 or rating > 5:
                return web.json_response({"error": "Rating must be 1-5"}, status=400)

            feedback = body.get("feedback", "").strip()[:500]
            delegation_id = body.get("delegation_id", "")

            pg_pool = getattr(self.node, '_pg_pool', None)
            if not pg_pool:
                return web.json_response({"error": "DB not available"}, status=503)

            # Store rating in skill metadata
            row = await pg_pool.fetchrow(
                "SELECT metadata FROM mesh.mesh_skills WHERE skill_id = $1 AND status = 'active'",
                skill_id,
            )
            if not row:
                return web.json_response({"error": "Skill not found"}, status=404)

            meta = row['metadata'] if row['metadata'] else {}
            if isinstance(meta, str):
                meta = _json.loads(meta)

            ratings = meta.get('ratings', [])
            if isinstance(ratings, str):
                ratings = _json.loads(ratings)

            ratings.append({
                'rating': rating,
                'feedback': feedback,
                'delegation_id': delegation_id,
                'user': user.username if user else 'anonymous',
                'timestamp': _time.time(),
            })
            ratings = ratings[-50:]  # Keep last 50
            meta['ratings'] = ratings
            meta['avg_rating'] = round(sum(r['rating'] for r in ratings) / len(ratings), 2)

            await pg_pool.execute(
                "UPDATE mesh.mesh_skills SET metadata = $1, updated_at = NOW() WHERE skill_id = $2",
                _json.dumps(meta), skill_id,
            )

            return web.json_response({
                "status": "rated",
                "skill_id": skill_id,
                "rating": rating,
                "avg_rating": meta['avg_rating'],
                "total_ratings": len(ratings),
            })
        except Exception as e:
            log.error(f"Error rating skill: {e}")
            return web.json_response({"error": str(e)}, status=500)