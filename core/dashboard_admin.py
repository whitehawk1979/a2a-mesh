"""A2A Mesh Dashboard — Admin mixin. Nodes, registry, settings, queue, workflow, context, image, logs, topology, plugins, routing, health scores."""
import asyncio
import json
import logging
import os
import time
import uuid

from .workflow import WorkflowCoordinator, WorkflowTask, ConsensusMode

log = logging.getLogger("a2a_mesh.dashboard.admin")


class DashboardAdminMixin:
    """Admin API endpoints for the A2A Mesh Dashboard."""

    async def _api_agents(self, request):
        """Return list of known agents with consistent transport format."""
        from aiohttp import web
        agents = []
        
        # ── DB version + skills lookup (fallback for peers with default '1.0.0' or empty skills) ──
        db_versions = {}
        db_skills = {}
        try:
            if hasattr(self.node, '_pg_pool') and self.node._pg_pool:
                rows = await self.node._pg_pool.fetch("SELECT node_name, version, skills FROM mesh.mesh_nodes")
                db_versions = {r['node_name']: r['version'] for r in rows if r['version'] and r['version'] != '1.0.0'}
                for r in rows:
                    s = r['skills'] if 'skills' in r.keys() else None
                    if s:
                        import json as _json
                        skill_list = _json.loads(s) if isinstance(s, str) else s
                        if isinstance(skill_list, list) and len(skill_list) > 0:
                            db_skills[r['node_name']] = skill_list
                log.debug(f"db_versions from PG: {db_versions}")
                log.debug(f"db_skills from PG: {list(db_skills.keys())}")
            else:
                log.warning(f"PG pool not available for db_versions: hasattr={hasattr(self.node, '_pg_pool')}, pool={getattr(self.node, '_pg_pool', None)}")
        except Exception as e:
            log.warning(f"db_versions query failed: {e}")
        
        # Self — extract transport availability from TransportStatus objects
        status = self.node.get_status()
        raw_transports = status.get("transports", {})
        transport_inner = raw_transports
        if isinstance(raw_transports, dict) and "transports" in raw_transports:
            transport_inner = raw_transports["transports"]
        self_transports = {}
        for key in ("p2p", "pg", "pg_notify", "http", "ble"):
            val = transport_inner.get(key, False)
            if isinstance(val, str) and "available=True" in val:
                self_transports[key] = True
            elif isinstance(val, str) and "available=False" in val:
                self_transports[key] = False
            elif isinstance(val, bool):
                self_transports[key] = val
            elif hasattr(val, "available"):
                self_transports[key] = val.available
            else:
                self_transports[key] = bool(val)
        # Self skills: prefer registry, fall back to config, then DB
        self_skill_list = [s if isinstance(s, str) else s.get('id', str(s)) for s in (self.node.config.skills or [])]
        if self.node.node_name in db_skills and len(db_skills[self.node.node_name]) > len(self_skill_list):
            self_skill_list = [s if isinstance(s, str) else s.get('id', str(s)) for s in db_skills[self.node.node_name]]
        agents.append({
            "name": self.node.node_name,
            "role": self.node.config.topology.node_role,
            "status": "online",
            "host": getattr(self.node.config.p2p, "listen_host", "0.0.0.0"),
            "health_port": getattr(self.node, '_health_port', 8650),
            "version": self.node._resolved_version,
            "skills": self_skill_list,
            "transports": {
                "p2p": self_transports.get("p2p", False),
                "pg": self_transports.get("pg_notify", self_transports.get("pg", False)),
                "http": self_transports.get("http", False),
                "ble": self_transports.get("ble", False),
            },
            "local_store": self.node.local_store.get_stats(),
        })
        # Known peers
        for name, peer in self.node.peer_discovery.get_all_peers().items():
            # Determine peer status: online = P2P + PG, available = P2P only, offline = neither
            if peer.p2p_available and peer.pg_available:
                peer_status = "online"
            elif peer.p2p_available:
                peer_status = "available"
            else:
                peer_status = "offline"
            # Use DB version as fallback for empty/default version
            peer_ver = getattr(peer, 'version', None)
            if not peer_ver or peer_ver in ('1.0.0', 'unknown'):
                peer_ver = db_versions.get(peer.name, peer_ver or '')
            # Get skills from registry, fall back to DB
            peer_skills = []
            card = self.node.registry.get_card(peer.name) if hasattr(self.node, 'registry') else None
            if card and hasattr(card, 'skills') and card.skills:
                peer_skills = [s if isinstance(s, str) else s.get('id', str(s)) for s in card.skills]
            elif peer.name in db_skills:
                peer_skills = [s if isinstance(s, str) else s.get('id', str(s)) for s in db_skills[peer.name]]
            agents.append({
                "name": peer.name,
                "role": peer.role,
                "status": peer_status,
                "host": peer.host,
                "version": peer_ver,
                "p2p_port": peer.p2p_port,
                "health_port": peer.health_port,
                "last_seen": peer.last_seen,
                "skills": peer_skills,
                "transports": {
                    "p2p": peer.p2p_available,
                    "pg": peer.pg_available,
                    "http": peer.http_available,
                },
            })
        return web.json_response({"agents": agents, "total": len(agents)})

    # ─── Admin: Node Approval ──────────────────────────────────

    async def _api_nodes_pending(self, request):
        """List nodes pending approval."""
        from aiohttp import web
        user, err = self._require_owner(request)
        if err:
            return err
        try:
            import psycopg2
            conn = psycopg2.connect(
                host=self.node.config.pg.host, port=self.node.config.pg.port,
                dbname=self.node.config.pg.dbname, user=self.node.config.pg.user,
                password=self.node.config.pg.password,
            )
            cur = conn.cursor()
            cur.execute("SET client_encoding TO UTF8")
            cur.execute("""
                SELECT node_name, role, host, p2p_port, health_port,
                       pg_available, p2p_available, http_available,
                       joined_at, last_heartbeat
                FROM mesh.mesh_nodes WHERE status = 'pending'
                ORDER BY joined_at
            """)
            nodes = []
            for row in cur.fetchall():
                nodes.append({
                    "node_name": row[0], "role": row[1], "host": row[2],
                    "p2p_port": row[3], "health_port": row[4],
                    "pg_available": row[5], "p2p_available": row[6], "http_available": row[7],
                    "joined_at": row[8].isoformat() if row[8] else None,
                    "last_heartbeat": row[9].isoformat() if row[9] else None,
                })
            cur.close()
            conn.close()
            return web.json_response({"nodes": nodes})
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)

    async def _api_node_approve(self, request):
        """Approve a pending node."""
        from aiohttp import web
        user, err = self._require_owner(request)
        if err:
            return err
        node_name = request.match_info["node_name"]
        try:
            import psycopg2
            conn = psycopg2.connect(
                host=self.node.config.pg.host, port=self.node.config.pg.port,
                dbname=self.node.config.pg.dbname, user=self.node.config.pg.user,
                password=self.node.config.pg.password,
            )
            cur = conn.cursor()
            cur.execute("SET client_encoding TO UTF8")
            cur.execute("""
                UPDATE mesh.mesh_nodes SET status = 'active'
                WHERE node_name = %s AND status = 'pending'
            """, (node_name,))
            conn.commit()
            approved = cur.rowcount
            cur.close()
            conn.close()
            if approved:
                log.info(f"Node '{node_name}' approved by {user.username}")
                return web.json_response({"status": "approved", "node_name": node_name})
            else:
                return web.json_response({"error": "Node not found or not pending"}, status=404)
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)

    async def _api_node_reject(self, request):
        """Reject (remove) a pending node."""
        from aiohttp import web
        user, err = self._require_owner(request)
        if err:
            return err
        node_name = request.match_info["node_name"]
        try:
            import psycopg2
            conn = psycopg2.connect(
                host=self.node.config.pg.host, port=self.node.config.pg.port,
                dbname=self.node.config.pg.dbname, user=self.node.config.pg.user,
                password=self.node.config.pg.password,
            )
            cur = conn.cursor()
            cur.execute("SET client_encoding TO UTF8")
            cur.execute("""
                DELETE FROM mesh.mesh_nodes
                WHERE node_name = %s AND status = 'pending'
            """, (node_name,))
            conn.commit()
            removed = cur.rowcount
            cur.close()
            conn.close()
            if removed:
                log.info(f"Node '{node_name}' rejected by {user.username}")
                return web.json_response({"status": "rejected", "node_name": node_name})
            else:
                return web.json_response({"error": "Node not found or not pending"}, status=404)
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)

    async def _api_nodes_list(self, request):
        """List all nodes — merges registry (live) data with PG (persistent) data."""
        from aiohttp import web
        import time as _time
        user, err = self._require_auth(request)
        if err:
            return err
        nodes = {}  # name -> node_dict

        # ── DB skills + version lookup (fallback for nodes with empty skills) ──
        db_skills = {}
        db_versions = {}
        try:
            if hasattr(self.node, '_pg_pool') and self.node._pg_pool:
                rows = await self.node._pg_pool.fetch("SELECT node_name, version, skills FROM mesh.mesh_nodes")
                db_versions = {r['node_name']: r['version'] for r in rows if r['version'] and r['version'] != '1.0.0'}
                for r in rows:
                    s = r['skills'] if 'skills' in r.keys() else None
                    if s:
                        import json as _json
                        skill_list = _json.loads(s) if isinstance(s, str) else s
                        if isinstance(skill_list, list) and len(skill_list) > 0:
                            db_skills[r['node_name']] = skill_list
        except Exception:
            pass

        # 1. Registry data (live, in-memory — always up-to-date)
        reg = self.registry
        if reg:
            try:
                for card, health in reg.list_agents():
                    name = card.name
                    nodes[name] = {
                        "node_name": name,
                        "role": getattr(card, 'metadata', {}).get('role', 'agent') if hasattr(card, 'metadata') and card.metadata else 'agent',
                        "host": card.endpoint.replace("http://", "").split(":")[0] if card.endpoint else "",
                        "p2p_port": getattr(card, 'metadata', {}).get('p2p_port', 8645) if hasattr(card, 'metadata') and card.metadata else 8645,
                        "health_port": int(card.endpoint.split(":")[-1]) if card.endpoint and ":" in card.endpoint else 8650,
                        "pg_available": True,  # in registry = PG works
                        "p2p_available": False,  # will be enriched from P2P below
                        "http_available": True,
                        "status": "active",
                        "skills": list(card.skills) if card.skills else db_skills.get(name, []),
                        "capabilities": list(card.capabilities) if card.capabilities else [],
                        "health_score": round(health.health_score, 3),
                        "uptime_seconds": round(health.last_success - health.last_failure, 1) if health.last_success and health.last_failure else 0,
                        "last_seen": health.last_health_check or 0,
                        "message_count": health.total_requests,
                        "version": card.version if card.version and card.version not in ('1.0.0', 'unknown') else db_versions.get(name, card.version or ''),
                    }
            except Exception as e:
                log.warning(f"Nodes list: registry lookup failed: {e}")

        # 2. P2P peer data (live connection status)
        pd = getattr(self.node, 'peer_discovery', None)
        if pd and hasattr(pd, '_peers'):
            for name, peer in pd._peers.items():
                p2p_available = getattr(peer, 'p2p_available', False)
                pg_available = getattr(peer, 'pg_available', False)
                # Status: online (P2P+PG) > connected (P2P only) > registered/disconnected
                if p2p_available and pg_available:
                    peer_status = "online"
                elif p2p_available:
                    peer_status = "connected"
                else:
                    peer_status = "disconnected"
                if name in nodes:
                    nodes[name]["p2p_available"] = p2p_available
                    nodes[name]["pg_available"] = pg_available
                    nodes[name]["status"] = peer_status if p2p_available else nodes[name].get("status", "registered")
                    # P2P-connected peers are healthy by definition
                    if p2p_available:
                        nodes[name]["health_score"] = 1.0
                else:
                    nodes[name] = {
                        "node_name": name,
                        "role": getattr(peer, 'role', 'router'),
                        "host": getattr(peer, 'host', ''),
                        "p2p_port": getattr(peer, 'p2p_port', 8645),
                        "health_port": getattr(peer, 'health_port', 8650),
                        "pg_available": pg_available,
                        "p2p_available": p2p_available,
                        "http_available": getattr(peer, 'http_available', False),
                        "status": peer_status,
                        "skills": db_skills.get(name, []),
                        "capabilities": list(getattr(peer, 'capabilities', []) or []),
                        "health_score": 1.0,
                        "uptime_seconds": 0,
                        "last_seen": getattr(peer, 'last_seen', 0),
                        "message_count": 0,
                        "version": db_versions.get(name, getattr(peer, 'version', '') or ''),
                    }

        # 3. PG data (persistent — fills gaps for offline/pending nodes)
        try:
            import psycopg2
            conn = psycopg2.connect(
                host=self.node.config.pg.host, port=self.node.config.pg.port,
                dbname=self.node.config.pg.dbname, user=self.node.config.pg.user,
                password=self.node.config.pg.password,
            )
            cur = conn.cursor()
            cur.execute("SET client_encoding TO UTF8")
            cur.execute("""
                SELECT node_name, role, host, p2p_port, health_port,
                       pg_available, p2p_available, http_available,
                       status, joined_at, last_heartbeat, skills, capabilities, version
                FROM mesh.mesh_nodes
                ORDER BY CASE status WHEN 'pending' THEN 0 WHEN 'active' THEN 1 ELSE 2 END, joined_at
            """)
            for row in cur.fetchall():
                name = row[0]
                pg_status = row[8]
                pg_version = row[13] if len(row) > 13 else None
                if name not in nodes:
                    # Not in registry/P2P — offline or pending
                    nodes[name] = {
                        "node_name": name,
                        "role": row[1],
                        "host": row[2],
                        "p2p_port": row[3],
                        "health_port": row[4],
                        "pg_available": row[5],
                        "p2p_available": row[6],
                        "http_available": row[7],
                        "status": pg_status or "unknown",
                        "skills": row[11] if isinstance(row[11], list) else (json.loads(row[11]) if isinstance(row[11], str) else []),
                        "capabilities": row[12] if isinstance(row[12], list) else (json.loads(row[12]) if isinstance(row[12], str) else []),
                        "health_score": 1.0,
                        "uptime_seconds": 0,
                        "last_seen": row[10].isoformat() if row[10] else None,
                        "message_count": 0,
                        "version": pg_version or "",
                        "joined_at": row[9].isoformat() if row[9] else None,
                    }
                else:
                    # Enrich with PG data for fields not in registry
                    if not nodes[name].get("joined_at") and row[9]:
                        nodes[name]["joined_at"] = row[9].isoformat()
                    if pg_status == "pending" and nodes[name].get("status") not in ("connected", "active", "registered"):
                        nodes[name]["status"] = "pending"
                    # Override version from PG if current is default '1.0.0'
                    if pg_version and not nodes[name].get("version"):
                        nodes[name]["version"] = pg_version
                    # Override uptime from PG joined_at if current is 0 or invalid
                    if row[9] and nodes[name].get("uptime_seconds", 0) <= 0:
                        try:
                            joined = row[9]
                            if hasattr(joined, 'timestamp'):
                                joined_ts = joined.timestamp()
                            elif isinstance(joined, str):
                                from datetime import datetime
                                joined_ts = datetime.fromisoformat(joined.replace('Z', '+00:00')).timestamp()
                            else:
                                joined_ts = float(joined)
                            import time as _time_mod
                            nodes[name]["uptime_seconds"] = round(_time_mod.time() - joined_ts, 1)
                        except Exception:
                            pass
            cur.close()
            conn.close()
        except Exception as e:
            log.warning(f"Nodes list: PG lookup failed: {e}")

        # Sort: online > connected > active > registered > pending > others
        status_order = {"online": 0, "connected": 1, "active": 2, "registered": 3, "pending": 4}
        # Fix health_score and P2P for self-node and offline nodes
        self_name = self.node.node_name if self.node else ""
        for name, node in nodes.items():
            if node.get("status") in ("disconnected", "offline", "unknown"):
                node["health_score"] = 0.0
            # Self-node: mark P2P available if P2P transport is running
            if name == self_name and self.node:
                p2p_transport = getattr(self.node, '_p2p_transport', None)
                if p2p_transport and getattr(p2p_transport, '_running', False):
                    node["p2p_available"] = True
                    # Self-node with P2P is online
                    if node.get("status") == "active":
                        node["status"] = "online"
        sorted_nodes = sorted(nodes.values(), key=lambda n: status_order.get(n.get("status", ""), 99))
        return web.json_response({"nodes": sorted_nodes})

    async def _api_registry_stats(self, request):
        """GET /api/registry — Registry statistics and agent health overview."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        stats = self.registry.get_stats()
        stats["self_name"] = self.node.node_name if self.node else ""
        return web.json_response(stats)

    async def _api_registry_list(self, request):
        """GET /api/registry/agents — List all registered agents with health."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        agents = self.registry.list_agents()
        result = []
        for card, health in agents:
            result.append({
                "name": card.name,
                "capabilities": card.capabilities,
                "skills": card.skills if hasattr(card, 'skills') else [],
                "version": card.version,
                "description": card.description,
                "endpoint": card.endpoint,
                "health_score": round(health.health_score, 3),
                "status": health.status,
                "success_rate": round(health.success_rate, 3),
                "avg_latency_ms": round(health.avg_latency_ms, 1),
                "current_load": health.current_load,
                "uptime_pct": round(health.uptime_pct, 1),
                "total_requests": health.total_requests,
                "total_failures": health.total_failures,
                "max_concurrent": card.max_concurrent,
            })
        return web.json_response({"agents": result, "total": len(result)})

    async def _api_registry_get(self, request):
        """GET /api/registry/agents/{name} — Get a specific agent's details."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        name = request.match_info.get("name", "")
        card = self.registry.get(name)
        if not card:
            return web.json_response({"error": f"Agent '{name}' not found"}, status=404)
        health = self.registry.get_health(name) or HealthRecord()
        return web.json_response({
            "name": card.name,
            "capabilities": card.capabilities,
            "skills": card.skills if hasattr(card, 'skills') else [],
            "version": card.version,
            "description": card.description,
            "endpoint": card.endpoint,
            "health_endpoint": card.health_endpoint,
            "max_concurrent": card.max_concurrent,
            "cost_per_task": card.cost_per_task,
            "metadata": card.metadata,
            "health": {
                "health_score": round(health.health_score, 3),
                "status": health.status,
                "success_rate": round(health.success_rate, 3),
                "avg_latency_ms": round(health.avg_latency_ms, 1),
                "current_load": health.current_load,
                "uptime_pct": round(health.uptime_pct, 1),
                "total_requests": health.total_requests,
                "total_failures": health.total_failures,
                "consecutive_successes": health.consecutive_successes,
                "consecutive_failures": health.consecutive_failures,
            },
        })

    async def _api_registry_register(self, request):
        """POST /api/registry/agents — Register or update an agent."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        try:
            data = await request.json()
        except Exception:
            return web.json_response({"error": "Invalid JSON"}, status=400)

        name = data.get("name", "").strip()
        if not name:
            return web.json_response({"error": "Agent name is required"}, status=400)

        card = AgentCard(
            name=name,
            capabilities=data.get("capabilities", []),
            version=data.get("version", ""),
            description=data.get("description", ""),
            endpoint=data.get("endpoint", ""),
            health_endpoint=data.get("health_endpoint", "/health"),
            max_concurrent=data.get("max_concurrent", 10),
            cost_per_task=data.get("cost_per_task", 0.0),
            metadata=data.get("metadata", {}),
            skills=data.get("skills"),
        )

        force = data.get("force", False)
        health = self.registry.register(card, force=force)

        # Auto-register in peer discovery if endpoint provided
        if card.endpoint and self.node and hasattr(self.node, 'peer_discovery'):
            from ..core.peer_discovery import PeerInfo
            import re
            # Parse host:port from endpoint
            match = re.match(r'https?://([^:]+):(\d+)', card.endpoint)
            if match:
                host, port = match.group(1), int(match.group(2))
                self.node.peer_discovery.add_peer(name, host, port + 1)

        return web.json_response({
            "status": "ok",
            "agent": name,
            "health_score": round(health.health_score, 3),
            "capabilities": card.capabilities,
        })

    async def _api_registry_deregister(self, request):
        """DELETE /api/registry/agents/{name} — Remove an agent from the registry."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        name = request.match_info.get("name", "")
        if not self.registry.get(name):
            return web.json_response({"error": f"Agent '{name}' not found"}, status=404)
        self.registry.deregister(name)
        return web.json_response({"status": "ok", "deregistered": name})

    async def _api_registry_find(self, request):
        """GET /api/registry/find?capabilities=cap1,cap2 — Find agents by capability."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err

        caps_str = request.query.get("capabilities", "")
        healthy_only = request.query.get("healthy_only", "true").lower() == "true"
        min_score = float(request.query.get("min_health_score", "0.3"))

        capabilities = [c.strip() for c in caps_str.split(",") if c.strip()] if caps_str else []

        matches = self.registry.find_by_capability(
            capabilities, healthy_only=healthy_only, min_health_score=min_score
        )

        result = []
        for card, health in matches:
            result.append({
                "name": card.name,
                "capabilities": card.capabilities,
                "version": card.version,
                "endpoint": card.endpoint,
                "health_score": round(health.health_score, 3),
                "status": health.status,
                "current_load": health.current_load,
            })
        return web.json_response({"matches": result, "total": len(result)})

    async def _api_registry_success(self, request):
        """POST /api/registry/record-success/{name} — Record a successful interaction."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        name = request.match_info.get("name", "")
        if not self.registry.get(name):
            return web.json_response({"error": f"Agent '{name}' not found"}, status=404)
        try:
            data = await request.json() if request.content_type == "application/json" else {}
        except Exception:
            data = {}
        latency_ms = float(data.get("latency_ms", 0))
        score = self.registry.record_success(name, latency_ms)
        health = self.registry.get_health(name)
        return web.json_response({
            "status": "ok",
            "agent": name,
            "health_score": round(score, 3),
            "success_rate": round(health.success_rate, 3) if health else 0,
        })

    async def _api_registry_failure(self, request):
        """POST /api/registry/record-failure/{name} — Record a failed interaction."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        name = request.match_info.get("name", "")
        if not self.registry.get(name):
            return web.json_response({"error": f"Agent '{name}' not found"}, status=404)
        score = self.registry.record_failure(name)
        health = self.registry.get_health(name)
        return web.json_response({
            "status": "ok",
            "agent": name,
            "health_score": round(score, 3),
            "consecutive_failures": health.consecutive_failures if health else 0,
        })

    async def _api_p2p_reset_backoff(self, request):
        """POST /api/p2p/reset-backoff — Reset P2P backoff for all or specific peers."""
        from aiohttp import web
        p2p = self.node._p2p_transport
        if not p2p:
            return web.json_response({"error": "P2P transport not available"}, status=503)
        body = await request.json() if request.content_type == 'application/json' else {}
        peer_name = body.get("peer")
        if peer_name:
            p2p._peer_backoff.pop(peer_name, None)
            p2p._peer_retry_count.pop(peer_name, None)
            return web.json_response({"status": "ok", "peer": peer_name, "backoff_reset": True})
        # Reset all backoffs
        count = len(p2p._peer_backoff)
        p2p._peer_backoff.clear()
        p2p._peer_retry_count.clear()
        return web.json_response({"status": "ok", "backoffs_reset": count})

    async def _api_p2p_reconnect(self, request):
        """POST /api/p2p/reconnect — Trigger immediate P2P reconnection to all discovered peers."""
        from aiohttp import web
        import logging
        log = logging.getLogger("a2a_mesh.dashboard")
        discovery = self.node.peer_discovery
        if not discovery:
            return web.json_response({"error": "Peer discovery not available"}, status=503)
        # Reset all backoffs first
        p2p = self.node._p2p_transport
        if p2p:
            p2p._peer_backoff.clear()
            p2p._peer_retry_count.clear()
        # Trigger discovery and connect
        try:
            result = await discovery.discover_and_connect()
            return web.json_response({"status": "ok", "discovery_result": str(result)})
        except Exception as e:
            log.error(f"P2P reconnect failed: {e}")
            return web.json_response({"status": "error", "error": str(e)}, status=500)

    async def _api_router_stats(self, request):
        """GET /api/router/stats — Detailed router + stream mux + queue statistics.
        
        Returns comprehensive routing stats including:
        - Message routing counters (sent, received, forwarded, duplicates, etc.)
        - Dedup cache stats (hits, misses, hit rate)
        - Bounded queue stats (enqueued, dequeued, dropped, overflow)
        - Stream multiplexer stats (routed, unmatched, by_stream)
        - Protocol version
        """
        from aiohttp import web
        
        if not self.node.router:
            return web.json_response({"error": "Router not available"}, status=503)
        
        stats = self.node.router.get_stats()
        
        def sanitize(obj):
            if isinstance(obj, dict):
                return {k: sanitize(v) for k, v in obj.items()}
            elif isinstance(obj, (list, tuple)):
                return [sanitize(v) for v in obj]
            elif isinstance(obj, (str, int, float, bool, type(None))):
                return obj
            elif hasattr(obj, '__dataclass_fields__'):
                return sanitize(obj.__dict__)
            elif hasattr(obj, '__dict__'):
                return sanitize(obj.__dict__)
            else:
                return str(obj)
        
        return web.json_response(sanitize(stats))

    # ─── Health Scorer API Handlers ─────────────────────────────────

    async def _api_health_scores(self, request):
        """GET /api/health/scores — All agent health scores."""
        from aiohttp import web
        scorer = getattr(self.node.router, '_health_scorer', None)
        if scorer:
            return web.json_response(scorer.stats)
        return web.json_response({"agent_count": 0, "agents": {}})

    async def _api_health_nodes(self, request):
        """GET /api/health/nodes — Real-time node health metrics (CPU, memory, disk)."""
        from aiohttp import web
        try:
            pool = self.node._pg_pool
            if not pool or not pool.is_connected():
                return web.json_response({"error": "DB not connected", "nodes": [], "count": 0}, status=503)
            rows = await pool.fetch(
                "SELECT node_name, status, cpu_pct, memory_pct, disk_pct, last_seen, updated_at "
                "FROM mesh_node_health ORDER BY node_name"
            )
            nodes = []
            for r in rows:
                nodes.append({
                    "node_name": r["node_name"],
                    "status": r["status"],
                    "cpu_pct": float(r["cpu_pct"]),
                    "memory_pct": float(r["memory_pct"]),
                    "disk_pct": float(r["disk_pct"]),
                    "last_seen": str(r["last_seen"]),
                    "updated_at": str(r["updated_at"]),
                })
            return web.json_response({"nodes": nodes, "count": len(nodes)})
        except Exception as e:
            return web.json_response({"error": str(e), "nodes": [], "count": 0}, status=500)

    async def _api_health_success(self, request):
        """POST /api/health/record-success/{name}?latency_ms=0 — Record agent success."""
        from aiohttp import web
        name = request.match_info['name']
        latency_ms = float(request.query.get('latency_ms', '0'))
        scorer = getattr(self.node.router, '_health_scorer', None)
        if scorer:
            score = scorer.record_success(name, latency_ms)
            return web.json_response({"agent": name, "health_score": round(score, 3)})
        return web.json_response({"error": "health_scorer not available"}, status=503)

    async def _api_health_failure(self, request):
        """POST /api/health/record-failure/{name} — Record agent failure."""
        from aiohttp import web
        name = request.match_info['name']
        scorer = getattr(self.node.router, '_health_scorer', None)
        if scorer:
            score = scorer.record_failure(name)
            return web.json_response({"agent": name, "health_score": round(score, 3)})
        return web.json_response({"error": "health_scorer not available"}, status=503)

    async def _api_tasks_cleanup(self, request):
        """POST /api/tasks/cleanup?max_age_hours=24 — Remove completed/cancelled tasks older than max_age_hours."""
        from aiohttp import web
        try:
            max_age_hours = int(request.query.get("max_age_hours", "24"))
            pool = self.node._pg_pool
            if not pool or not pool.is_connected():
                return web.json_response({"error": "DB not connected"}, status=503)
            result = await pool.execute(
                "DELETE FROM shared_delegations "
                "WHERE status IN ('completed', 'cancelled') "
                "AND created_at < NOW() - ($1 || ' hours')::INTERVAL",
                str(max_age_hours)
            )
            deleted = int(result.split()[-1]) if result else 0
            return web.json_response({"deleted": deleted, "max_age_hours": max_age_hours})
        except Exception as e:
            from aiohttp import web
            return web.json_response({"error": str(e)}, status=500)

    # ─── Smart Router API Handlers ─────────────────────────────────

    async def _api_route(self, request):
        """GET /api/route?capabilities=cap1,cap2&strategy=health_weighted — Route to best agent."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err

        caps_str = request.query.get("capabilities", "")
        strategy = request.query.get("strategy", "health_weighted")
        exclude_str = request.query.get("exclude", "")
        min_score = float(request.query.get("min_health_score", "0.3"))

        capabilities = [c.strip() for c in caps_str.split(",") if c.strip()] if caps_str else None
        exclude = [e.strip() for e in exclude_str.split(",") if e.strip()] if exclude_str else None

        agent = self.smart_router.route(
            required_capabilities=capabilities,
            strategy=strategy,
            exclude_agents=exclude,
            min_health_score=min_score,
        )

        if not agent:
            return web.json_response({
                "error": "No suitable agent found",
                "capabilities": capabilities,
                "strategy": strategy,
            }, status=404)

        health = self.registry.get_health(agent.name) or HealthRecord()
        return web.json_response({
            "agent": agent.name,
            "capabilities": agent.capabilities,
            "version": agent.version,
            "endpoint": agent.endpoint,
            "health_score": round(health.health_score, 3),
            "status": health.status,
            "current_load": health.current_load,
            "strategy": strategy,
        })

    async def _api_route_explain(self, request):
        """GET /api/route/explain?capabilities=cap1,cap2&strategy=health_weighted — Route with explanation."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err

        caps_str = request.query.get("capabilities", "")
        strategy = request.query.get("strategy", "health_weighted")
        min_score = float(request.query.get("min_health_score", "0.3"))

        capabilities = [c.strip() for c in caps_str.split(",") if c.strip()] if caps_str else None

        agent, explanation = self.smart_router.route_with_explanation(
            required_capabilities=capabilities,
            strategy=strategy,
            min_health_score=min_score,
        )

        if not agent:
            return web.json_response({
                "agent": None,
                "explanation": explanation,
                "capabilities": capabilities,
            })

        health = self.registry.get_health(agent.name) or HealthRecord()
        return web.json_response({
            "agent": agent.name,
            "capabilities": agent.capabilities,
            "health_score": round(health.health_score, 3),
            "status": health.status,
            "explanation": explanation,
        })

    async def _api_route_options(self, request):
        """GET /api/route/options?capabilities=cap1,cap2 — List all routing options."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err

        caps_str = request.query.get("capabilities", "")
        min_score = float(request.query.get("min_health_score", "0.3"))

        capabilities = [c.strip() for c in caps_str.split(",") if c.strip()] if caps_str else None

        options = self.smart_router.get_all_routes(
            required_capabilities=capabilities,
            min_health_score=min_score,
        )

        return web.json_response({
            "options": options,
            "total": len(options),
            "capabilities": capabilities,
        })

    # ─── Workflow DAG API Handlers ──────────────────────────────────

    async def _api_workflow_create(self, request):
        """POST /api/workflow — Create and execute a workflow DAG.

        Body:
            {
                "name": "research-task",
                "consensus": "all",  // all, any, majority
                "tasks": [
                    {
                        "id": "search",
                        "name": "Web Search",
                        "capabilities": ["web_search"],
                        "payload": {"query": "AI trends"},
                        "dependencies": [],
                        "timeout": 60
                    },
                    {
                        "id": "summarize",
                        "name": "Summarize",
                        "capabilities": ["summarization@v2"],
                        "dependencies": ["search"],
                        "timeout": 30
                    }
                ]
            }
        """
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err

        try:
            data = await request.json()
        except Exception:
            return web.json_response({"error": "Invalid JSON"}, status=400)

        name = data.get("name", "unnamed-workflow")
        consensus_str = data.get("consensus", "all")
        try:
            consensus = ConsensusMode(consensus_str)
        except ValueError:
            consensus = ConsensusMode.ALL

        # Build workflow
        coordinator = self.workflow_coordinator
        if self.node:
            coordinator = WorkflowCoordinator(self.registry, self.smart_router, node=self.node)

        wf = coordinator.create_workflow(name, consensus_mode=consensus)

        for task_data in data.get("tasks", []):
            task = WorkflowTask(
                id=task_data.get("id", str(uuid.uuid4())[:8]),
                name=task_data.get("name", "task"),
                agent=task_data.get("agent"),
                capabilities=task_data.get("capabilities", []),
                payload=task_data.get("payload", {}),
                dependencies=task_data.get("dependencies", []),
                timeout=task_data.get("timeout", 60),
                # v3 fields
                condition=task_data.get("condition"),
                max_retries=task_data.get("max_retries", 0),
                retry_delay=task_data.get("retry_delay", 5.0),
                input_from=task_data.get("input_from"),
                fan_out_count=task_data.get("fan_out_count", 1),
            )
            wf.add_task(task)

        # Execute workflow
        try:
            result = await coordinator.execute(wf)
            return web.json_response(result)
        except ValueError as e:
            return web.json_response({"error": str(e)}, status=400)
        except Exception as e:
            log.error(f"Workflow execution error: {e}")
            return web.json_response({"error": str(e)}, status=500)

    async def _api_workflow_status(self, request):
        """GET /api/workflow/{wf_id} — Get workflow status."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        wf_id = request.match_info.get("wf_id", "")
        status = self.workflow_coordinator.get_workflow_status(wf_id)
        if not status:
            return web.json_response({"error": f"Workflow '{wf_id}' not found"}, status=404)
        return web.json_response(status)

    async def _api_workflows_list(self, request):
        """GET /api/workflows — List active workflows."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        workflows = self.workflow_coordinator.list_active_workflows()
        return web.json_response({"workflows": workflows, "total": len(workflows)})

    # ─── Pending Agent Approval API Handlers ──────────────────────────

    async def _api_registry_pending(self, request):
        """GET /api/registry/pending — List pending agent registrations."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        pending = self.registry.list_pending()
        result = []
        for card, status in pending:
            result.append({
                "name": card.name,
                "capabilities": card.capabilities,
                "version": card.version,
                "endpoint": card.endpoint,
                "description": card.description,
                "status": status,
            })
        return web.json_response({"pending": result, "total": len(result)})

    async def _api_registry_approve(self, request):
        """POST /api/registry/approve/{name} — Approve a pending agent."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        name = request.match_info.get("name", "")
        card = self.registry.approve_agent(name)
        if not card:
            return web.json_response({"error": f"Agent '{name}' not in pending list"}, status=404)
        return web.json_response({
            "status": "approved",
            "agent": {
                "name": card.name,
                "capabilities": card.capabilities,
                "version": card.version,
                "endpoint": card.endpoint,
            },
        })

    async def _api_registry_reject(self, request):
        """POST /api/registry/reject/{name} — Reject a pending agent."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        name = request.match_info.get("name", "")
        success = self.registry.reject_agent(name)
        if not success:
            return web.json_response({"error": f"Agent '{name}' not in pending list"}, status=404)
        return web.json_response({"status": "rejected", "agent": name})

    # ─── Settings API Handlers ────────────────────────────────────────

    async def _api_settings_get(self, request):
        """GET /api/settings — Get current mesh settings."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err

        settings = {
            "mesh": {
                "node_name": self.node.node_name if self.node else "unknown",
                "p2p_enabled": True,
                "pg_enabled": bool(self.node and hasattr(self.node, 'pg_transport')),
                "dashboard_port": 8650,
            },
            "registry": {
                "auto_approve": self.registry.auto_approve,
                "total_agents": len(self.registry.agents),
                "pending_agents": len(self.registry.pending_agents),
                "health_check_interval": self.registry._health_interval,
            },
            "rate_limits": {
                "api_per_min": 100,
                "p2p_per_min": 200,
                "workflow_per_min": 20,
            },
            "health_scorer": {
                "decay_factor": self.registry.health_scorer.decay_factor,
                "recovery_factor": self.registry.health_scorer.recovery_factor,
                "latency_threshold_ms": self.registry.health_scorer.latency_threshold_ms,
                "weights": self.registry.health_scorer.weights,
            },
        }
        return web.json_response(settings)

    async def _api_settings_update(self, request):
        """POST /api/settings — Update mesh settings."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err

        try:
            data = await request.json()
        except Exception:
            return web.json_response({"error": "Invalid JSON"}, status=400)

        updated = {}

        # Update auto_approve
        if "auto_approve" in data.get("registry", {}):
            self.registry.auto_approve = bool(data["registry"]["auto_approve"])
            updated["auto_approve"] = self.registry.auto_approve

        # Update health check interval
        if "health_check_interval" in data.get("registry", {}):
            self.registry._health_interval = float(data["registry"]["health_check_interval"])
            updated["health_check_interval"] = self.registry._health_interval

        # Update health scorer weights
        if "weights" in data.get("health_scorer", {}):
            for key, val in data["health_scorer"]["weights"].items():
                if key in self.registry.health_scorer.weights:
                    self.registry.health_scorer.weights[key] = float(val)
            updated["weights"] = self.registry.health_scorer.weights

        # Update decay/recovery factors
        if "decay_factor" in data.get("health_scorer", {}):
            self.registry.health_scorer.decay_factor = float(data["health_scorer"]["decay_factor"])
            updated["decay_factor"] = self.registry.health_scorer.decay_factor
        if "recovery_factor" in data.get("health_scorer", {}):
            self.registry.health_scorer.recovery_factor = float(data["health_scorer"]["recovery_factor"])
            updated["recovery_factor"] = self.registry.health_scorer.recovery_factor

        return web.json_response({"status": "ok", "updated": updated})

    async def _api_mesh_topology(self, request):
        """GET /api/mesh/topology — Star topology visualization data."""
        from aiohttp import web
        import time as _time
        try:
            nodes = {}
            connections = []
            now = _time.time()

            # ── DB version + skills lookup (fallback for agent cards with default '1.0.0' or empty skills) ──
            db_versions = {}
            db_skills = {}
            try:
                if hasattr(self.node, '_pg_pool') and self.node._pg_pool:
                    rows = await self.node._pg_pool.fetch("SELECT node_name, version, skills FROM mesh.mesh_nodes")
                    db_versions = {r['node_name']: r['version'] for r in rows if r['version'] and r['version'] != '1.0.0'}
                    for r in rows:
                        s = r['skills'] if 'skills' in r.keys() else None
                        if s:
                            import json as _json
                            skill_list = _json.loads(s) if isinstance(s, str) else s
                            if isinstance(skill_list, list) and len(skill_list) > 0:
                                db_skills[r['node_name']] = skill_list
            except Exception:
                pass

            # ── Self node info ──────────────────────────────────────────
            cfg = self.node.config
            self_uptime = now - self.node._start_time if hasattr(self.node, '_start_time') and self.node._start_time else 0
            # Gather self skills from registry if available
            self_skills = []
            self_caps = list(getattr(cfg, 'capabilities', []) or [])
            reg = self.registry  # Dashboard has its own registry (self.registry), not node.registry
            if reg:
                try:
                    for card, health in reg.list_agents():
                        if card.name == self.node.node_name:
                            self_skills = list(card.skills) if hasattr(card, 'skills') and card.skills else []
                            if card.capabilities:
                                self_caps = list(card.capabilities)
                            break
                except Exception:
                    pass

            self_info = {
                "name": self.node.node_name,
                "host": getattr(cfg, 'listen_host', '0.0.0.0') or '0.0.0.0',
                "port": getattr(cfg, 'health_port', 8650),
                "p2p_port": getattr(cfg.p2p, 'listen_port', 8645),
                "role": getattr(getattr(cfg, 'topology', None), 'node_role', 'router') or 'router',
                "status": "online",
                "health_score": 1.0,
                "capabilities": self_caps,
                "version": self.node._resolved_version,
                "skills": self_skills,
                "uptime_seconds": round(self_uptime, 1),
                "last_seen": now,
                "message_count": 0,
            }
            nodes[self.node.node_name] = self_info

            # ── Registry info (ALL registered agents first) ─────────────
            # Use self.registry (DashboardHandler's own registry), not node.registry
            reg = self.registry
            reg_agents = {}  # name -> (AgentCard, HealthRecord)
            if reg:
                try:
                    for card, health in reg.list_agents():
                        name = card.name
                        reg_agents[name] = (card, health)
                        # Prefer DB version over card default (card may have '1.0.0' fallback)
                        card_version = card.version if card.version and card.version not in ('1.0.0', 'unknown') else db_versions.get(name, '')
                        nodes[name] = {
                            "name": name,
                            "host": card.endpoint.replace("http://", "").split(":")[0] if card.endpoint else "",
                            "port": int(card.endpoint.split(":")[-1]) + 1 if card.endpoint and ":" in card.endpoint else 8650,
                            "p2p_port": 8645,
                            "role": getattr(card, 'metadata', {}).get('role', 'agent'),
                            "status": "registered",
                            "health_score": round(health.health_score, 3),
                            "capabilities": list(card.capabilities) if card.capabilities else [],
                            "version": card_version,
                            "skills": list(card.skills) if card.skills else db_skills.get(name, []),
                            "uptime_seconds": round(health.last_success - health.last_failure, 1) if health.last_success and health.last_failure else 0,
                            "last_seen": health.last_health_check or 0,
                            "message_count": health.total_requests,
                        }
                except Exception as e:
                    log.warning(f"Topology: registry list_agents failed: {e}")

            # ── P2P peer info (enriches registry data with live status) ─────
            pd = getattr(self.node, 'peer_discovery', None)
            p2p_peers = []
            backoff_peers = {}
            if pd:
                if hasattr(pd, '_peers'):
                    for name, peer in pd._peers.items():
                        p2p_peers.append(name)
                        p2p_available = getattr(peer, 'p2p_available', False)
                        # Merge: keep registry skills/caps, enrich with live peer data
                        existing = nodes.get(name, {})
                        peer_caps = getattr(peer, 'capabilities', None) or []
                        existing_caps = existing.get("capabilities", []) or []
                        # Prefer registry data for skills/caps, fall back to peer data
                        final_caps = existing_caps if existing_caps else peer_caps
                        existing_skills = existing.get("skills", []) or []
                        # Fall back to DB skills if registry is empty
                        if not existing_skills and name in db_skills:
                            existing_skills = db_skills[name]
                        # Use DB version as fallback if card version is default/unknown
                        peer_version = existing.get("version")
                        if not peer_version or peer_version in ('1.0.0', 'unknown'):
                            peer_version = db_versions.get(name, '')
                        nodes[name] = {
                            "name": name,
                            "host": getattr(peer, 'host', '') or existing.get("host", ""),
                            "port": getattr(peer, 'health_port', 8650),
                            "p2p_port": getattr(peer, 'p2p_port', 8645),
                            "role": getattr(peer, 'role', '') or existing.get("role", "router"),
                            "status": "connected" if p2p_available else "disconnected",
                            "health_score": existing.get("health_score", 1.0),
                            "capabilities": final_caps,
                            "version": peer_version,
                            "skills": existing_skills if existing_skills else [],
                            "uptime_seconds": existing.get("uptime_seconds", 0),
                            "last_seen": getattr(peer, 'last_seen', 0) or existing.get("last_seen", 0),
                            "message_count": existing.get("message_count", 0),
                            "p2p_available": p2p_available,
                            "http_available": existing.get("http_available", False),
                            "pg_available": existing.get("pg_available", False),
                        }
                if hasattr(pd, '_backoff_until') and pd._backoff_until:
                    backoff_peers = {k: str(v) for k, v in pd._backoff_until.items()}

            # ── Build P2P connections ─────────────────────────────────────
            for peer_name in p2p_peers:
                peer_node = nodes.get(peer_name, {})
                is_connected = peer_node.get("status") == "connected"
                in_backoff = peer_name in backoff_peers
                status = "connected" if is_connected else ("backoff" if in_backoff else "disconnected")
                connections.append({
                    "source": self.node.node_name,
                    "target": peer_name,
                    "transport": "p2p",
                    "status": status,
                    "backoff": backoff_peers.get(peer_name),
                })

            # ── PG connections (all registered agents not on P2P) ────────
            for name in list(nodes.keys()):
                if name != self.node.node_name and name not in p2p_peers:
                    connections.append({
                        "source": self.node.node_name,
                        "target": name,
                        "transport": "pg",
                        "status": "active",
                    })

            return web.json_response({
                "nodes": nodes,
                "connections": connections,
                "topology": "star",
                "local_node": self.node.node_name,
                "timestamp": now,
            })
        except Exception as e:
            log.error(f"Topology API error: {e}", exc_info=True)
            return web.json_response({"error": str(e)}, status=500)

    async def _api_topology_page(self, request):
        """GET /topology — Star topology visualization page."""
        from aiohttp import web
        html_path = os.path.join(os.path.dirname(__file__), "topology.html")
        try:
            with open(html_path, "r", encoding="utf-8") as f:
                return web.Response(text=f.read(), content_type="text/html")
        except FileNotFoundError:
            return web.Response(text="<h1>Topology page not found</h1>", status=404)

    # ─── Plugin API ────────────────────────────────────────────────

    async def _api_plugins(self, request):
        """GET /api/plugins — List all loaded plugins and their status."""
        from aiohttp import web
        try:
            user, err = self._require_auth(request)
            if err:
                return err

            if not hasattr(self.node, 'plugin_loader'):
                return web.json_response({"plugins": {}, "total_plugins": 0})

            status = self.node.plugin_loader.get_status()
            return web.json_response(status)
        except Exception as e:
            log.error(f"Plugins API error: {e}", exc_info=True)
            return web.json_response({"error": str(e)}, status=500)

    async def _api_plugin_detail(self, request):
        """GET /api/plugins/{plugin_name} — Get detailed status of a specific plugin."""
        from aiohttp import web
        try:
            user, err = self._require_auth(request)
            if err:
                return err

            plugin_name = request.match_info.get("plugin_name", "")
            if not hasattr(self.node, 'plugin_loader'):
                return web.json_response({"error": "No plugin loader"}, status=404)

            plugin = self.node.plugin_loader.get_plugin(plugin_name)
            if not plugin:
                return web.json_response({"error": f"Plugin '{plugin_name}' not found"}, status=404)

            # Get plugin-specific status if available
            detail = {
                "name": plugin.name,
                "version": plugin.version,
                "description": plugin.description,
                "author": plugin.author,
                "capabilities": plugin.capabilities,
                "running": plugin._running,
                "config": {k: v for k, v in plugin._config.items()
                           if not k.endswith(('_token', '_secret', '_password', '_key'))},
            }

            # Add plugin-specific status methods
            if hasattr(plugin, 'get_gateway_status'):
                detail["gateway_status"] = plugin.get_gateway_status()
            elif hasattr(plugin, 'get_notification_status'):
                detail["notification_status"] = plugin.get_notification_status()
            elif hasattr(plugin, 'get_health_status'):
                detail["health_monitor_status"] = plugin.get_health_status()

            return web.json_response(detail)
        except Exception as e:
            log.error(f"Plugin detail API error: {e}", exc_info=True)
            return web.json_response({"error": str(e)}, status=500)

    # ─── Queue Management API ──────────────────────────────────────

    async def _api_queue_flush(self, request):
        """Flush (mark as synced) all pending outbound messages in local_store.
        
        POST /api/queue/flush
        Body (optional): {"older_than_hours": 1}  — only flush messages older than N hours
        """
        from aiohttp import web
        try:
            older_than_hours = 0  # default: flush all
            try:
                body = await request.json()
                older_than_hours = body.get("older_than_hours", 0)
            except Exception:
                pass
            
            if self.node and self.node.local_store:
                import time
                cutoff = time.time() - (older_than_hours * 3600) if older_than_hours > 0 else time.time() + 999999999
                
                # Mark all pending unsynced as synced
                conn = self.node.local_store._conn
                if older_than_hours > 0:
                    result = conn.execute(
                        "UPDATE outbound_queue SET pg_synced = 1 WHERE status = 'pending' AND pg_synced = 0 AND created_at < ?",
                        (cutoff,)
                    )
                else:
                    result = conn.execute(
                        "UPDATE outbound_queue SET pg_synced = 1 WHERE status = 'pending' AND pg_synced = 0"
                    )
                conn.commit()
                flushed = result.rowcount
                
                return web.json_response({
                    "status": "ok",
                    "flushed": flushed,
                    "message": f"Marked {flushed} pending outbound messages as synced"
                })
            else:
                return web.json_response({"error": "local_store not available"}, status=503)
        except Exception as e:
            log.error(f"Queue flush API error: {e}", exc_info=True)
            return web.json_response({"error": str(e)}, status=500)

    async def _api_queue_cleanup(self, request):
        """Cleanup old messages from local_store and optionally PG.
        
        POST /api/queue/cleanup
        Body (optional): {
            "local_max_age_hours": 1,        — remove synced local messages older than N hours
            "pg_ack_max_age_days": 3,        — remove acknowledged PG messages older than N days
            "pg_sent_max_age_days": 7,       — remove sent PG messages older than N days
            "pg_expired": true               — remove all expired PG messages
        }
        """
        from aiohttp import web
        try:
            local_max_age_hours = 1
            pg_ack_max_age_days = 3
            pg_sent_max_age_days = 7
            pg_expired = True
            
            try:
                body = await request.json()
                local_max_age_hours = body.get("local_max_age_hours", 1)
                pg_ack_max_age_days = body.get("pg_ack_max_age_days", 3)
                pg_sent_max_age_days = body.get("pg_sent_max_age_days", 7)
                pg_expired = body.get("pg_expired", True)
            except Exception:
                pass
            
            results = {}
            
            # Local store cleanup
            if self.node and self.node.local_store:
                cleaned = self.node.local_store.cleanup_outbound(max_age_hours=local_max_age_hours)
                results["local_cleaned"] = cleaned
            
            # PG cleanup
            try:
                import psycopg2
                pg_config = getattr(self.node.config, 'pg', None) or getattr(self.node.config, 'transport_config', None)
                if hasattr(self.node, 'config') and pg_config:
                    conn = psycopg2.connect(
                        host=pg_config.host,
                        port=pg_config.port,
                        dbname=pg_config.dbname,
                        user=pg_config.user,
                        password=pg_config.password
                    )
                    conn.autocommit = True
                    cur = conn.cursor()
                    
                    # Delete acknowledged messages older than N days
                    cur.execute(
                        "DELETE FROM mesh.mesh_messages WHERE status='acknowledged' AND created_at < NOW() - INTERVAL '%s days'" % pg_ack_max_age_days
                    )
                    results["pg_ack_deleted"] = cur.rowcount
                    
                    # Delete sent messages older than N days
                    cur.execute(
                        "DELETE FROM mesh.mesh_messages WHERE status='sent' AND created_at < NOW() - INTERVAL '%s days'" % pg_sent_max_age_days
                    )
                    results["pg_sent_deleted"] = cur.rowcount
                    
                    # Delete expired messages
                    if pg_expired:
                        cur.execute("DELETE FROM mesh.mesh_messages WHERE status='expired'")
                        results["pg_expired_deleted"] = cur.rowcount
                    
                    cur.execute("SELECT COUNT(*) FROM mesh.mesh_messages")
                    results["pg_remaining"] = cur.fetchone()[0]
                    
                    cur.close()
                    conn.close()
            except Exception as e:
                results["pg_error"] = str(e)
            
            return web.json_response({"status": "ok", "results": results})
        except Exception as e:
            log.error(f"Queue cleanup API error: {e}", exc_info=True)
            return web.json_response({"error": str(e)}, status=500)

    async def _api_queue_stats(self, request):
        """Get queue statistics from local_store and PG.
        
        GET /api/queue/stats
        """
        from aiohttp import web
        try:
            stats = {}
            
            # Local store stats
            if self.node and self.node.local_store:
                stats["local_store"] = self.node.local_store.get_stats()
            
            # Auto-steer stats
            if self.node and hasattr(self.node, 'auto_steer'):
                stats["auto_steer"] = self.node.auto_steer.get_stats()
            
            return web.json_response(stats)
        except Exception as e:
            log.error(f"Queue stats API error: {e}", exc_info=True)
            return web.json_response({"error": str(e)}, status=500)

    # ── Log Viewer API ──

    async def _api_logs(self, request):
        """Central log viewer. GET /api/logs?type=delegation&limit=50&status=failed
        
        Shows recent task events, node health, and system logs from the PG database.
        Query params:
            type: 'delegation' (default), 'health', 'all'
            limit: max results (default 50, max 200)
            status: filter by status (completed, failed, available, etc.)
            agent: filter by agent name
        """
        from aiohttp import web
        log_type = request.query.get("type", "delegation")
        limit = min(int(request.query.get("limit", "50")), 200)
        status_filter = request.query.get("status", "")
        agent_filter = request.query.get("agent", "")
        
        try:
            results = []
            
            if log_type in ("delegation", "all"):
                # Task history
                query = """SELECT task_id, from_agent, to_agent, subject, status, 
                           priority, retry_count, assigned_agent, created_at, completed_at
                           FROM shared_delegations 
                           WHERE 1=1"""
                params = []
                idx = 1
                if status_filter:
                    query += f" AND status = ${idx}"
                    params.append(status_filter)
                    idx += 1
                if agent_filter:
                    query += f" AND (from_agent = ${idx} OR assigned_agent = ${idx})"
                    params.append(agent_filter)
                    idx += 1
                query += f" ORDER BY created_at DESC LIMIT ${idx}"
                params.append(limit)
                
                rows = await self.node.delegation.pg_pool.fetch(query, *params)
                for row in rows:
                    r = dict(row)
                    # Convert UUID and datetime to JSON-safe strings
                    if "task_id" in r and r["task_id"]:
                        r["task_id"] = str(r["task_id"])
                    for k in ("created_at", "completed_at", "accepted_at", "expires_at"):
                        if k in r and r[k]:
                            r[k] = r[k].isoformat() if hasattr(r[k], "isoformat") else str(r[k])
                    results.append({"type": "delegation", **r})
            
            if log_type in ("health", "all"):
                # Node health history
                query = """SELECT node_name, status, cpu_pct, memory_pct, disk_pct, 
                           last_seen, updated_at 
                           FROM mesh_node_health 
                           ORDER BY updated_at DESC LIMIT $1"""
                rows = await self.node.delegation.pg_pool.fetch(query, limit)
                for row in rows:
                    r = dict(row)
                    for k in ("last_seen", "updated_at"):
                        if k in r and r[k]:
                            r[k] = r[k].isoformat() if hasattr(r[k], "isoformat") else str(r[k])
                    results.append({"type": "health", **r})
            
            return web.json_response({
                "logs": results,
                "count": len(results),
                "type": log_type,
                "limit": limit,
            })
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)

    # ── Shared Context API ──

    async def _api_context_list(self, request):
        """List all shared context entries. GET /api/context?prefix=task_"""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        try:
            if not self.node or not self.node.delegation:
                return web.json_response({"error": "Delegation not available"}, status=503)
            prefix = request.query.get("prefix", "")
            entries = await self.node.delegation.get_all_context(prefix)
            for e in entries:
                for k, v in e.items():
                    if hasattr(v, 'hex'):
                        e[k] = str(v)
                    elif hasattr(v, 'isoformat'):
                        e[k] = v.isoformat()
            return web.json_response({"context": entries, "count": len(entries)})
        except Exception as e:
            log.error(f"Context list error: {e}", exc_info=True)
            return web.json_response({"error": str(e)}, status=500)

    async def _api_context_get(self, request):
        """Get a specific context value. GET /api/context/{key}"""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        try:
            if not self.node or not self.node.delegation:
                return web.json_response({"error": "Delegation not available"}, status=503)
            key = request.match_info.get("key")
            value = await self.node.delegation.get_context(key)
            if value is None:
                return web.json_response({"error": "Key not found"}, status=404)
            return web.json_response({"key": key, "value": value})
        except Exception as e:
            log.error(f"Context get error: {e}", exc_info=True)
            return web.json_response({"error": str(e)}, status=500)

    async def _api_context_set(self, request):
        """Set a shared context value. POST /api/context
        Body: {key, value, type?, expires_minutes?}
        """
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        try:
            data = await request.json()
            key = data.get("key")
            value = data.get("value")
            if not key or value is None:
                return web.json_response({"error": "key and value required"}, status=400)
            if not self.node or not self.node.delegation:
                return web.json_response({"error": "Delegation not available"}, status=503)
            value_type = data.get("type", "text")
            expires = int(data.get("expires_minutes", "0"))
            await self.node.delegation.set_context(key, str(value), value_type, expires)
            return web.json_response({"key": key, "value": value, "set_by": self.node.node_name})
        except Exception as e:
            log.error(f"Context set error: {e}", exc_info=True)
            return web.json_response({"error": str(e)}, status=500)

    async def _api_context_delete(self, request):
        """Delete a shared context entry. DELETE /api/context/{key}"""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        try:
            key = request.match_info.get("key")
            if not self.node or not self.node.delegation:
                return web.json_response({"error": "Delegation not available"}, status=503)
            success = await self.node.delegation.delete_context(key)
            if success:
                return web.json_response({"deleted": key})
            else:
                return web.json_response({"error": "Key not found"}, status=404)
        except Exception as e:
            log.error(f"Context delete error: {e}", exc_info=True)
            return web.json_response({"error": str(e)}, status=500)

    # ── Image Generation (Pollinations.ai) ──────────────────────────

    async def _api_image_generate(self, request):
        """Generate image via Pollinations.ai. POST /api/image/generate
        Body: {prompt: str, width?: int, height?: int, model?: str, seed?: int, nologo?: bool}
        Returns: {url, prompt, width, height, seed}
        """
        from aiohttp import web
        import aiohttp
        import random
        import json

        user, err = self._require_auth(request)
        if err:
            return err
        try:
            data = await request.json()
            prompt = data.get("prompt", "").strip()
            if not prompt:
                return web.json_response({"error": "prompt is required"}, status=400)

            width = int(data.get("width", 512))
            height = int(data.get("height", 512))
            model = data.get("model", "flux")
            seed = data.get("seed", random.randint(1, 999999999))
            nologo = data.get("nologo", True)
            enhance = data.get("enhance", True)

            # Build Pollinations URL
            from urllib.parse import quote
            encoded_prompt = quote(prompt)
            params = f"width={width}&height={height}&model={model}&seed={seed}"
            if nologo:
                params += "&nologo=true"
            if enhance:
                params += "&enhance=true"
            pollinations_url = f"https://image.pollinations.ai/prompt/{encoded_prompt}?{params}"

            # Skip HEAD validation to avoid rate limits — proxy will handle download

            # Store generation in context for later retrieval
            gen_id = f"img_{seed}_{random.randint(1000,9999)}"
            if self.node and self.node.delegation:
                await self.node.delegation.set_context(
                    gen_id, json.dumps({
                        "prompt": prompt, "url": pollinations_url,
                        "width": width, "height": height,
                        "seed": seed, "model": model
                    }), "image_generation", expires_minutes=60
                )

            log.info(f"Image generated: {gen_id} prompt='{prompt[:50]}' model={model}")

            # Auto-send to Telegram if configured and target_chat provided
            target_chat = data.get("target_chat", "").strip()
            if self.node and self.node.config.telegram_auto_image and target_chat:
                asyncio.create_task(self._send_image_to_telegram(
                    target_chat, pollinations_url, prompt, model, seed
                ))

            return web.json_response({
                "url": pollinations_url,
                "gen_id": gen_id,
                "prompt": prompt,
                "width": width,
                "height": height,
                "seed": seed,
                "model": model
            })
        except Exception as e:
            log.error(f"Image generate error: {e}", exc_info=True)
            return web.json_response({"error": str(e)}, status=500)

    async def _api_image_proxy(self, request):
        """Proxy image download from Pollinations.ai. GET /api/image/proxy?url=...
        Returns the image binary directly with proper content-type.
        """
        from aiohttp import web
        import aiohttp

        user, err = self._require_auth(request)
        if err:
            return err
        try:
            image_url = request.query.get("url", "")
            if not image_url or not image_url.startswith("https://image.pollinations.ai/"):
                return web.json_response({"error": "Invalid or missing url parameter"}, status=400)

            async with aiohttp.ClientSession() as session:
                async with session.get(image_url, timeout=aiohttp.ClientTimeout(total=120)) as resp:
                    if resp.status != 200:
                        return web.json_response({"error": f"Upstream returned {resp.status}"}, status=502)
                    content_type = resp.headers.get("Content-Type", "image/jpeg")
                    body = await resp.read()

            return web.Response(body=body, content_type=content_type)
        except asyncio.TimeoutError:
            return web.json_response({"error": "Image generation timed out (try simpler prompt)"}, status=504)
        except Exception as e:
            log.error(f"Image proxy error: {e}", exc_info=True)
            return web.json_response({"error": str(e)}, status=500)

    async def _send_image_to_telegram(self, target_chat: str,
                                       image_url: str, prompt: str,
                                       model: str, seed: int):
        """Send generated image to Telegram chat via Hermes CLI.

        Uses 'hermes send' which reuses the gateway's platform credentials.
        Downloads image from Pollinations, saves to temp file, sends via CLI.
        """
        import aiohttp
        import tempfile
        import os
        try:
            # Download image from Pollinations
            async with aiohttp.ClientSession() as session:
                async with session.get(image_url, timeout=aiohttp.ClientTimeout(total=120)) as resp:
                    if resp.status != 200:
                        log.warning(f"Telegram: failed to download image ({resp.status})")
                        return
                    image_data = await resp.read()

            # Save to temp file
            tmp_path = os.path.join(tempfile.gettempdir(), f"a2a_img_{seed}.jpg")
            with open(tmp_path, "wb") as f:
                f.write(image_data)

            # Send via Hermes CLI — target_chat e.g. "telegram:-1003971026331:17585"
            # Use local hermes_cli path per node
            import shutil
            hermes_cli = shutil.which("hermes") or os.path.expanduser("~/.hermes/hermes-agent/venv/bin/python")
            cmd_prefix = [hermes_cli, "-m", "hermes_cli.main", "send"] if hermes_cli.endswith("python") else [hermes_cli, "send"]
            caption = f"🎨 {prompt[:200]}\nModel: {model} | Seed: {seed}"
            import asyncio as _asyncio
            cmd = cmd_prefix + [
                "--to", target_chat,
                f"🎨 {caption[:500]}\nMEDIA:{tmp_path}"
            ]
            proc = await _asyncio.create_subprocess_exec(
                *cmd,
                stdout=_asyncio.subprocess.PIPE,
                stderr=_asyncio.subprocess.PIPE
            )
            stdout, stderr = await _asyncio.wait_for(proc.communicate(), timeout=30)
            if proc.returncode == 0:
                log.info(f"Telegram: image sent via Hermes CLI")
            else:
                log.warning(f"Telegram: Hermes CLI failed ({proc.returncode}): {stderr.decode()[:200]}")
            # Clean up temp file
            try:
                os.unlink(tmp_path)
            except Exception:
                pass
        except Exception as e:
            log.warning(f"Telegram: image send error: {e}")

    # ─── Alert Rules API ───────────────────────────────────────────────

    async def _api_alerts_status(self, request):
        """GET /api/alerts — Get alert manager status and all rules."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        return web.json_response(self.alert_manager.get_status())

    async def _api_alerts_add_rule(self, request):
        """POST /api/alerts/rules — Add a custom alert rule.

        Body: {"id": "my_rule", "name": "My Alert", "metric": "peers_connected",
               "operator": "<", "threshold": 1, "severity": "warning", "cooldown": 300}
        """
        from aiohttp import web
        from .alert_manager import AlertRule, AlertSeverity
        user, err = self._require_auth(request)
        if err:
            return err
        try:
            data = await request.json()
        except Exception:
            return web.json_response({"error": "Invalid JSON"}, status=400)

        rule_id = data.get("id", "")
        if not rule_id:
            return web.json_response({"error": "Rule id required"}, status=400)

        try:
            severity = AlertSeverity(data.get("severity", "warning"))
        except ValueError:
            severity = AlertSeverity.WARNING

        rule = AlertRule(
            id=rule_id,
            name=data.get("name", rule_id),
            metric=data.get("metric", ""),
            operator=data.get("operator", "<"),
            threshold=float(data.get("threshold", 0)),
            severity=severity,
            cooldown=float(data.get("cooldown", 300)),
            enabled=data.get("enabled", True),
        )
        self.alert_manager.add_rule(rule)
        return web.json_response({"status": "ok", "rule": rule.to_dict()})

    async def _api_alerts_delete_rule(self, request):
        """DELETE /api/alerts/rules/{rule_id} — Delete an alert rule."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        rule_id = request.match_info.get("rule_id", "")
        if self.alert_manager.remove_rule(rule_id):
            return web.json_response({"status": "deleted", "rule_id": rule_id})
        return web.json_response({"error": "Rule not found"}, status=404)

    async def _api_alerts_toggle_rule(self, request):
        """POST /api/alerts/rules/{rule_id}/toggle — Enable/disable a rule."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        rule_id = request.match_info.get("rule_id", "")
        rules = self.alert_manager._rules
        if rule_id not in rules:
            return web.json_response({"error": "Rule not found"}, status=404)
        rules[rule_id].enabled = not rules[rule_id].enabled
        return web.json_response({"status": "ok", "rule": rules[rule_id].to_dict()})