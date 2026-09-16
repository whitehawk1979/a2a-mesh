"""A2A Mesh Dashboard — Admin mixin. Nodes, registry, settings, queue, workflow, context, image, logs, topology, plugins, routing, health scores."""
import asyncio
import json
import logging
import os
import time
import uuid

from .workflow import WorkflowCoordinator, WorkflowTask, ConsensusMode

log = logging.getLogger("a2a_mesh.dashboard.admin")


def _fmt_age(seconds: float) -> str:
    """Format age in seconds to human-readable string."""
    if seconds < 60:
        return f"{int(seconds)}s"
    elif seconds < 3600:
        return f"{int(seconds / 60)}m"
    elif seconds < 86400:
        return f"{int(seconds / 3600)}h"
    else:
        return f"{int(seconds / 86400)}d"


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
        # Self capabilities: from registry card or config
        self_card = self.registry.get(self.node.node_name) if hasattr(self, 'registry') else None
        self_caps = list(getattr(self_card, 'capabilities', []) or []) if self_card else []
        if not self_caps:
            self_caps = list(getattr(self.node.config, 'capabilities', []) or [])
        agents.append({
            "name": self.node.node_name,
            "role": self.node.config.topology.node_role,
            "status": "online",
            "host": getattr(self.node.config.p2p, "listen_host", "0.0.0.0"),
            "health_port": getattr(self.node, '_health_port', 8650),
            "version": self.node._resolved_version,
            "skills": self_skill_list,
            "capabilities": self_caps,
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
            card = self.registry.get(peer.name) if hasattr(self, 'registry') else None
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
                "capabilities": list(getattr(card, 'capabilities', []) or []) if card else [],
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
            from .peer_discovery import PeerInfo
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
                "WHERE status IN ('completed', 'cancelled', 'expired') "
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

        # Build workflow — always use self.workflow_coordinator (set node if available)
        coordinator = self.workflow_coordinator
        if self.node and not coordinator.node:
            coordinator.node = self.node
            # Initialize history file if not already done
            if not coordinator._history_file and coordinator.node:
                import os
                hist_dir = os.path.expanduser("~/.hermes/scripts/a2a_mesh/data")
                os.makedirs(hist_dir, exist_ok=True)
                coordinator._history_file = os.path.join(hist_dir, "workflow_history.json")
                coordinator._load_history()

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
        """GET /api/workflows — List all workflows (active + completed history)."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        # Ensure coordinator has node reference for history loading
        if self.node and not self.workflow_coordinator.node:
            self.workflow_coordinator.node = self.node
        workflows = self.workflow_coordinator.list_active_workflows()
        return web.json_response({"workflows": workflows, "total": len(workflows)})

    async def _api_workflow_delete(self, request):
        """DELETE /api/workflow/{wf_id} — Delete a workflow from history."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        wf_id = request.match_info.get("wf_id", "")
        deleted = self.workflow_coordinator.delete_workflow(wf_id)
        if deleted:
            return web.json_response({"status": "deleted", "workflow_id": wf_id})
        return web.json_response({"error": f"Workflow '{wf_id}' not found"}, status=404)

    # ─── P2P Status API ──────────────────────────────────────────────

    async def _api_skills_broadcast(self, request):
        """POST /api/skills/broadcast — Force broadcast skills + capabilities to all peers."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        if not self.node:
            return web.json_response({"error": "Node not available"}, status=503)
        # Reset rate limit + force broadcast
        self.node._last_skills_announcement = 0
        try:
            await self.node._on_peer_discovered("__broadcast__")
        except Exception:
            pass
        # Also try direct P2P announcement to each peer
        skills = list(getattr(self.node.config, 'skills', []) or [])
        reg_card = self.registry.get(self.node.node_name) if hasattr(self, 'registry') else None
        full_caps = list(getattr(reg_card, 'capabilities', []) or []) if reg_card else []
        result = {
            "node": self.node.node_name,
            "skills_count": len(skills),
            "capabilities_count": len(full_caps),
            "capabilities": full_caps,
        }
        return web.json_response(result)

    async def _api_webhook_deploy(self, request):
        """POST /api/webhook/deploy — Gitea push webhook triggers auto-deploy.
        Expects Gitea webhook payload (JSON). Validates secret if configured.
        Runs auto_deploy.py in background."""
        from aiohttp import web
        import json, asyncio, os, subprocess

        # Optional secret validation
        secret = request.headers.get("X-Gitea-Signature", "")
        # For now, accept any POST (webhook is on internal network)
        try:
            payload = await request.json()
        except Exception:
            payload = {}

        # Only trigger on push to main
        ref = payload.get("ref", "")
        if ref and "main" not in ref:
            return web.json_response({"status": "ignored", "reason": f"ref={ref} not main"})

        repo = payload.get("repository", {}).get("full_name", "unknown")
        commit = payload.get("after", "")[:8]

        # Run auto_deploy.py in background
        deploy_script = os.path.expanduser("~/.hermes/scripts/a2a_mesh/scripts/auto_deploy.py")
        if not os.path.exists(deploy_script):
            return web.json_response({"error": "auto_deploy.py not found"}, status=500)

        log.info(f"Webhook deploy triggered: repo={repo} commit={commit}")
        proc = await asyncio.create_subprocess_exec(
            "python3", deploy_script,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        # Don't wait — fire and forget, but capture output for logging
        asyncio.ensure_future(self._wait_deploy(proc, repo, commit))

        return web.json_response({
            "status": "deploying",
            "repo": repo,
            "commit": commit,
            "message": "Auto-deploy started in background"
        })

    async def _wait_deploy(self, proc, repo, commit):
        """Wait for deploy subprocess and log result."""
        try:
            stdout, stderr = await proc.communicate()
            if proc.returncode == 0:
                log.info(f"Deploy success: repo={repo} commit={commit}")
            else:
                log.error(f"Deploy failed: repo={repo} commit={commit} rc={proc.returncode} stderr={stderr.decode()[:200]}")
        except Exception as e:
            log.error(f"Deploy wait error: {e}")

    async def _api_p2p_status(self, request):
        """GET /api/p2p/status — Live P2P transport status."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        if not self.node:
            return web.json_response({"error": "Node not available"}, status=503)
        p2p = getattr(self.node, '_p2p_transport', None)
        if not p2p:
            return web.json_response({"error": "P2P transport not available"}, status=503)
        peers = list(getattr(p2p, '_peers', {}).keys())
        status = {
            "running": getattr(p2p, '_running', False),
            "listen_port": getattr(p2p, '_listen_port', 8645),
            "tls_enabled": True,  # mTLS is always on
            "peers": peers,
            "peer_count": len(peers),
            "backoff_peers": [],  # Backoff is handled per-peer in connect logic
            "incoming_queue": getattr(p2p, '_incoming_queue', None).qsize() if hasattr(p2p, '_incoming_queue') and p2p._incoming_queue else 0,
            "peer_stats": {
                name: {
                    "batch_size": getattr(p2p, '_peer_batch_size', {}).get(name),
                    "drain_time_ms": round(getattr(p2p, '_peer_drain_time', {}).get(name, 0) * 1000, 1),
                    "rtt_ms": round(getattr(p2p, '_peer_latency', {}).get(name, 0), 1),
                    "frame_version": getattr(p2p, '_frame_version', {}).get(name, 1),
                }
                for name in peers
            },
        }
        return web.json_response(status)

    # ─── Memory Sync Status API ──────────────────────────────────────

    async def _api_memory_sync_status(self, request):
        """GET /api/memory/sync/status — Memory sync status."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        if not self.node:
            return web.json_response({"error": "Node not available"}, status=503)
        stats = {
            "node": self.node.node_name,
            "pg_connected": bool(getattr(self.node, '_pg_pool', None) and self.node._pg_pool.is_connected()),
            "local_store_active": hasattr(self.node, 'local_store') and self.node.local_store is not None,
            "memory_entries": 0,
        }
        # Try to get memory count from PG
        try:
            pool = getattr(self.node, '_pg_pool', None)
            if pool and pool.is_connected():
                result = await pool.fetch("SELECT count(*) as cnt FROM shared_a2a_memory WHERE node_name = $1", self.node.node_name)
                if result:
                    stats["memory_entries"] = int(result[0].get('cnt', 0))
        except Exception:
            pass
        # Local store message count
        try:
            if stats["local_store_active"]:
                ls = self.node.local_store
                if ls._conn:
                    row = ls._conn.execute("SELECT count(*) as c FROM outbound_messages").fetchone()
                    stats["local_store_messages"] = int(row['c']) if row else 0
        except Exception:
            stats["local_store_messages"] = 0
        return web.json_response(stats)

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
                "p2p_enabled": bool(getattr(self.node, '_p2p_transport', None)),
                "pg_enabled": bool(getattr(self.node, '_pg_pool', None) and self.node._pg_pool),
                "ssh_tunnel_enabled": bool(getattr(self.node, '_ssh_tunnel_transport', None)),
                "dashboard_port": 8650,
            },
            "transports": {},
            "ssh_tunnels": {},
            "p2p_info": {},
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

        # ── VPN (Tailscale) állapot a Beállítások menübe ──
        try:
            from core import vpn as vpn_mod
            disc = getattr(self.node.config, "discovery", None) if self.node else None
            prefer = getattr(disc, "prefer", "auto") or "auto"
            ts_ip = None
            try:
                ts_ip = await asyncio.get_event_loop().run_in_executor(
                    None, vpn_mod._tailscale_ip)
            except Exception:
                ts_ip = None
            local_vpn = await asyncio.get_event_loop().run_in_executor(
                None, vpn_mod._local_vpn_ip)
            settings["vpn"] = {
                "available": bool(local_vpn or ts_ip),
                "tailscale_ip": ts_ip,
                "local_vpn_ip": local_vpn,
                "prefer": prefer,
                "prefer_options": ["lan", "vpn", "auto"],
            }
        except Exception as e:
            settings["vpn"] = {"available": False, "error": str(e)[:80]}

        # ── Real transport status from live objects ──
        try:
            node = self.node
            # P2P transport
            p2p = getattr(node, '_p2p_transport', None)
            if p2p:
                peers = {}
                pd = getattr(node, 'peer_discovery', None)
                if pd and hasattr(pd, '_peers'):
                    for pname, peer in pd._peers.items():
                        peers[pname] = {
                            "connected": getattr(peer, 'p2p_available', False),
                            "host": getattr(peer, 'host', ''),
                            "port": getattr(peer, 'p2p_port', 8645),
                            "pg_available": getattr(peer, 'pg_available', False),
                        }
                settings["p2p_info"] = {
                    "listen_port": getattr(p2p, '_listen_port', 8645),
                    "tls_enabled": getattr(p2p, '_tls_enabled', False),
                    "peers": list(peers.keys()),
                    "peer_details": peers,
                }
                settings["transports"]["p2p"] = True
            else:
                settings["transports"]["p2p"] = False

            # PG transport
            pg_pool = getattr(node, '_pg_pool', None)
            settings["transports"]["pg"] = bool(pg_pool and pg_pool.is_connected() if pg_pool else False)

            # SSH tunnel transport
            ssh = getattr(node, '_ssh_tunnel_transport', None)
            if ssh:
                settings["transports"]["ssh_tunnel"] = True
                tunnels = {}
                # Get tunnel stats from the transport object
                if hasattr(ssh, '_tunnels'):
                    for tname, tstate in ssh._tunnels.items():
                        tunnels[tname] = {
                            "connected": getattr(tstate, 'connected', False),
                            "ssh_host": getattr(tstate, 'ssh_host', ''),
                            "local_port": getattr(tstate, 'local_port', 0),
                            "remote_port": getattr(tstate, 'remote_port', 0),
                            "retry_count": getattr(tstate, 'retry_count', 0),
                            "uptime_seconds": round(getattr(tstate, 'uptime_seconds', 0) or 0, 1),
                        }
                settings["ssh_tunnels"] = tunnels
            else:
                settings["transports"]["ssh_tunnel"] = False
                settings["ssh_tunnels"] = {}

            # HTTP transport (dashboard itself is running = http OK)
            settings["transports"]["http"] = True

        except Exception as e:
            log.warning(f"Settings transport status error: {e}")

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
            db_hosts = {}
            db_p2p_ports = {}
            try:
                if hasattr(self.node, '_pg_pool') and self.node._pg_pool:
                    rows = await self.node._pg_pool.fetch("SELECT node_name, version, skills, host, p2p_port FROM mesh.mesh_nodes")
                    db_versions = {r['node_name']: r['version'] for r in rows if r['version'] and r['version'] != '1.0.0'}
                    db_hosts = {r['node_name']: r['host'] for r in rows if r['host'] and not str(r['host']).startswith('0.0.0.0')}
                    db_p2p_ports = {r['node_name']: r['p2p_port'] for r in rows if r['p2p_port']}
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
                        # Self node is already in nodes{} with authoritative self_info —
                        # registry cards overwrite it with wrong role/port (endpoint+1).
                        if name == self.node.node_name:
                            continue
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
                            # Prefer DB self-advertised host over discovery-learned host
                            # (tor advertises 100.74.221.46 via PG; UDP broadcast leaks container IP 172.30.33.13)
                            "host": db_hosts.get(name) or getattr(peer, 'host', '') or existing.get("host", ""),
                            "port": getattr(peer, 'health_port', 8650),
                            "p2p_port": db_p2p_ports.get(name) or getattr(peer, 'p2p_port', 8645),
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

            # ── SSH tunnel connections ────────────────────────────────────
            ssh_tunnel_status = {}
            if hasattr(self.node, '_ssh_tunnel_transport') and self.node.config.ssh_tunnel.enabled:
                try:
                    ssh_tunnel_status = self.node._ssh_tunnel_transport.get_peer_status()
                    for peer_name, ts in ssh_tunnel_status.items():
                        connections.append({
                            "source": self.node.node_name,
                            "target": peer_name,
                            "transport": "ssh_tunnel",
                            "status": "connected" if ts.get("connected") else "disconnected",
                            "local_port": ts.get("local_port"),
                            "uptime_seconds": ts.get("uptime_seconds", 0),
                        })
                except Exception as e:
                    log.warning(f"Topology: SSH tunnel status failed: {e}")

            # ── Peer-originated SSH tunnels (e.g. tor→peers run on the tor node) ──
            # The dashboard host only knows its OWN tunnels; tunnels other nodes originate
            # (tor→morzsa/runa/nova) are invisible here. Fetch each peer's /health in
            # parallel (8s cap — the HAOS container is loaded and 3s flakes) and add their
            # ssh_tunnel edges. Cache the last good state per peer so a brief health spike
            # doesn't blank the edges (they were flapping 3→2→0 before).
            async def _fetch_peer_ssh_tunnels():
                import aiohttp
                # Candidate hosts per peer: registry host first, then static_nodes
                # IPs (LAN+VPN), then the ssh_tunnel control host. HAOS containers
                # advertise a Tailscale IP whose health port is unreachable from
                # outside — their LAN candidate still works.
                peer_urls = {}
                disc = getattr(self.node.config, "discovery", None)
                static_nodes = getattr(disc, "static_nodes", None) or []
                st_cfg = getattr(self.node.config, "ssh_tunnel", None)
                for name, info in nodes.items():
                    if name == self.node.node_name:
                        continue
                    port = info.get("port") or 8650
                    cands = []
                    host = info.get("host") or ""
                    if host and not str(host).startswith("0.0.0.0"):
                        cands.append(host)
                    for sn in static_nodes:
                        try:
                            if (sn.get("name") or "").lower() == str(name).lower():
                                ip = sn.get("ip") or ""
                                if ip and ip not in cands:
                                    cands.append(ip)
                        except Exception:
                            continue
                    try:
                        p = (st_cfg.peers or {}).get(name) if st_cfg else None
                        if p:
                            ip = p.get("ssh_host") or ""
                            if ip and ip not in cands:
                                cands.append(ip)
                    except Exception:
                        pass
                    if cands:
                        peer_urls[name] = [f"http://{h}:{port}/health" for h in cands]
                if not peer_urls:
                    return []

                async def _one(name, urls):
                    for url in urls:
                        try:
                            timeout = aiohttp.ClientTimeout(total=6)
                            async with aiohttp.ClientSession(timeout=timeout) as sess:
                                async with sess.get(url) as resp:
                                    if resp.status != 200:
                                        continue
                                    d = await resp.json(content_type=None)
                                    # Identity check: on multi-agent hosts (HAOS) the
                                    # control-host candidate port may serve a
                                    # DIFFERENT node (mano shares 8650 with tor's
                                    # host). Never attribute another node's health
                                    # to this peer.
                                    if d.get("node") and str(d.get("node")).lower() != str(name).lower():
                                        continue
                                    ts = d.get("ssh_tunnel") or {}
                                    if isinstance(ts, dict) and ts:
                                        self._peer_tunnel_cache = getattr(self, "_peer_tunnel_cache", {})
                                        self._peer_tunnel_cache[name] = ts
                                        return (name, ts)
                        except Exception:
                            continue
                    return None

                async def _ssh_probe(name):
                    """SSH-jump health probe for peers whose health port is not
                    directly reachable (tor: bridge-network container). Uses the
                    peer's ssh_tunnel config; forward_host selects the target
                    inside the SSH endpoint's namespace. Deterministic, no LLM."""
                    import os as _os, json as _json
                    try:
                        if not st_cfg or not (st_cfg.peers or {}).get(name):
                            return None
                        p = st_cfg.peers[name]
                        fwd = p.get("forward_host") or "127.0.0.1"
                        remote_cmd = (
                            "python3 -c 'import urllib.request,sys;sys.stdout.write("
                            f"urllib.request.urlopen(\"http://{fwd}:8650/health\", "
                            "timeout=4).read().decode())' "
                            f"|| wget -qO- -T 4 http://{fwd}:8650/health"
                        )
                        cmd = ["ssh", "-o", "StrictHostKeyChecking=no",
                               "-o", "UserKnownHostsFile=/dev/null",
                               "-o", "BatchMode=yes", "-o", "ConnectTimeout=6"]
                        ident = p.get("identity_file") or getattr(st_cfg, "default_identity_file", "")
                        if ident:
                            cmd += ["-i", _os.path.expanduser(str(ident))]
                        if p.get("ssh_port"):
                            cmd += ["-p", str(p.get("ssh_port"))]
                        user = p.get("ssh_user") or getattr(st_cfg, "default_ssh_user", "") or "root"
                        cmd += [f"{user}@{p.get('ssh_host')}", remote_cmd]
                        proc = await asyncio.create_subprocess_exec(
                            *cmd, stdout=asyncio.subprocess.PIPE,
                            stderr=asyncio.subprocess.DEVNULL)
                        try:
                            out, _ = await asyncio.wait_for(proc.communicate(), timeout=12)
                        except asyncio.TimeoutError:
                            try:
                                proc.kill()
                            except Exception:
                                pass
                            return None
                        if proc.returncode != 0 or not out:
                            return None
                        d = _json.loads(out.decode("utf-8", "replace"))
                        # Identity check — never attribute another node's health
                        if d.get("node") and str(d.get("node")).lower() != str(name).lower():
                            return None
                        ts = d.get("ssh_tunnel") or {}
                        if isinstance(ts, dict) and ts:
                            return ts
                    except Exception as e:
                        log.debug(f"Topology SSH probe failed for {name}: {e}")
                    return None

                gathered = await asyncio.gather(*[_one(n, peer_urls[n]) for n in peer_urls])
                live = [r for r in gathered if r]
                live_names = {name for name, _ in live}
                # Fall back to cached state for peers whose health fetch failed this round
                cache = getattr(self, "_peer_tunnel_cache", {})
                for cname, cts in cache.items():
                    if cname not in live_names:
                        live.append((cname, cts))
                        live_names.add(cname)
                # Probe-sourced results live in their own TTL cache (30s): tor's
                # health port is never directly reachable, so a persistent entry
                # in the main cache would go stale forever.
                pcache = getattr(self, "_probe_tunnel_cache", {})
                now_t = _time.time()
                missing = []
                for n in peer_urls:
                    if n in live_names:
                        continue
                    if n in pcache and now_t - pcache[n][0] < 30:
                        live.append((n, pcache[n][1]))
                        live_names.add(n)
                    else:
                        missing.append(n)
                if missing:
                    probed = await asyncio.gather(*[_ssh_probe(n) for n in missing])
                    for pname, ts in zip(missing, probed):
                        if isinstance(ts, dict) and ts:
                            live.append((pname, ts))
                            live_names.add(pname)
                            pcache[pname] = (now_t, ts)
                    self._probe_tunnel_cache = pcache
                return live

            try:
                peer_tunnel_results = await _fetch_peer_ssh_tunnels()
                for peer_name, ts_dict in peer_tunnel_results:
                    if not isinstance(ts_dict, dict):
                        continue
                    for target_name, tstate in ts_dict.items():
                        if not isinstance(tstate, dict):
                            continue
                        # Include DISCONNECTED peer tunnels too — an invisible
                        # failed tunnel hides bidirectional topology. The
                        # frontend renders them as faint dashed lines.
                        peer_connected = bool(tstate.get("connected"))
                        # Avoid duplicating an edge the local node already reported
                        already = any(
                            c.get("transport") == "ssh_tunnel"
                            and c.get("source") == peer_name
                            and c.get("target") == target_name
                            for c in connections
                        )
                        if not already:
                            connections.append({
                                "source": peer_name,
                                "target": target_name,
                                "transport": "ssh_tunnel",
                                "status": "connected" if peer_connected else "disconnected",
                                "uptime_seconds": tstate.get("uptime_seconds", 0),
                                "retry_count": tstate.get("retry_count", 0),
                            })
            except Exception as e:
                log.debug(f"Topology: peer tunnel fetch failed (non-blocking): {e}")

            # ── MCP end devices (agents talking through this node's bridge) ──
            # Deterministic: reads the shared registry file the MCP bridge writes.
            try:
                from core.mcp_registry import list_clients as _mcp_list
                parent = self.node.node_name
                for c in _mcp_list(parent_node=parent):
                    cname = c["name"]
                    if cname in nodes or cname == parent:
                        continue
                    nodes[cname] = {
                        "name": cname,
                        "role": "end_device",
                        "host": "",
                        "port": 0,
                        "p2p_port": 0,
                        "status": "online" if c.get("online") else "disconnected",
                        "health_score": 1.0 if c.get("online") else 0.0,
                        "capabilities": [],
                        "version": "",
                        "skills": ["mcp_end_device"],
                        "uptime_seconds": 0,
                        "last_seen": c.get("last_seen", 0),
                        "message_count": 0,
                        "is_mcp_end_device": True,
                        "transport_parent": parent,
                    }
                    connections.append({
                        "source": parent,
                        "target": cname,
                        "transport": "mcp",
                        "status": "connected" if c.get("online") else "disconnected",
                    })
            except Exception as e:
                log.debug(f"Topology: MCP end-device add failed (non-blocking): {e}")

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

    # ─── Lab — Project Showcase ─────────────────────────────────────

    async def _lab_page(self, request):
        """GET /lab — Project showcase page with search."""
        from aiohttp import web
        html_path = os.path.join(os.path.dirname(__file__), "lab.html")
        try:
            with open(html_path, "r", encoding="utf-8") as f:
                return web.Response(text=f.read(), content_type="text/html")
        except FileNotFoundError:
            return web.Response(text="<h1>Lab page not found</h1>", status=404)

    async def _skills_page(self, request):
        """GET /skills — Skill marketplace page."""
        from aiohttp import web
        html_path = os.path.join(os.path.dirname(__file__), "skills.html")
        try:
            with open(html_path, "r", encoding="utf-8") as f:
                return web.Response(text=f.read(), content_type="text/html")
        except FileNotFoundError:
            return web.Response(text="<h1>Skills page not found</h1>", status=404)

    async def _api_onboard_node(self, request):
        """POST /api/onboard — Full node onboarding: SSH key exchange, mesh user, auth token.
        
        Body: {
            "node_name": "tor",
            "ssh_pubkey": "ssh-ed25519 AAAA... user@host",
            "use_tailscale": true,
            "pg_host": "192.168.1.30"  // optional, defaults to config
        }
        """
        from aiohttp import web
        import asyncio
        user, err = self._require_auth(request)
        if err:
            return err
        
        try:
            body = await request.json()
        except Exception:
            return web.json_response({"error": "Invalid JSON body"}, status=400)
        
        node_name = body.get("node_name", "").strip()
        ssh_pubkey = body.get("ssh_pubkey", "").strip()
        use_tailscale = body.get("use_tailscale", True)
        pg_host = body.get("pg_host", "192.168.1.30")
        role = body.get("role", "auto")
        auto_approve = body.get("auto_approve", False)
        
        if not node_name:
            return web.json_response({"error": "node_name is required"}, status=400)
        
        try:
            from .bootstrap import onboard_node
            result = await onboard_node(
                node_name=node_name,
                ssh_pubkey=ssh_pubkey,
                pg_host=pg_host,
                use_tailscale=use_tailscale,
                role=role,
                auto_approve=auto_approve,
            )
            return web.json_response(result)
        except Exception as e:
            import logging
            logging.getLogger("a2a_mesh.dashboard").error(f"Onboard error: {e}", exc_info=True)
            return web.json_response({"error": str(e)}, status=500)

    async def _api_onboard_scan(self, request):
        """POST /api/onboard/scan — Scan for new nodes on Tailscale/LAN."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        
        try:
            import subprocess, json as _json
            discovered = []
            
            # 1. Scan Tailscale network for new peers
            r = subprocess.run('tailscale status --json 2>/dev/null', shell=True, capture_output=True, text=True, timeout=10)
            if r.returncode == 0 and r.stdout:
                ts = _json.loads(r.stdout)
                known_nodes = set()
                # Get known mesh nodes from PG
                try:
                    from .async_db import AsyncDB
                    db = AsyncDB()
                    await db.connect()
                    rows = await db.fetch("SELECT node_name, host FROM mesh.mesh_nodes WHERE status = 'active'")
                    for row in rows:
                        known_nodes.add(row['node_name'])
                        known_nodes.add(row['host'])
                    await db.close()
                except Exception:
                    pass
                
                # Parse Tailscale peers
                peers = ts.get('Peer', {})
                for peer_id, peer in peers.items():
                    hostname = peer.get('HostName', '')
                    ips = peer.get('TailscaleIPs', [])
                    if not ips:
                        continue
                    ip = ips[0]
                    # Skip if already in mesh
                    if ip in known_nodes or hostname in known_nodes:
                        continue
                    # Check if it has mesh port open
                    r2 = subprocess.run(f'curl -s --max-time 3 http://{ip}:8650/api/health 2>/dev/null', shell=True, capture_output=True, text=True, timeout=5)
                    if r2.stdout and '"healthy"' in r2.stdout:
                        try:
                            info = _json.loads(r2.stdout)
                            discovered.append({
                                'name': info.get('node', hostname),
                                'host': ip,
                                'platform': info.get('version', 'unknown'),
                                'role': 'router'
                            })
                        except Exception:
                            discovered.append({'name': hostname, 'host': ip, 'platform': 'unknown', 'role': 'router'})
            
            # 2. Also check PG for pending nodes (status = 'pending')
            try:
                from .async_db import AsyncDB
                db = AsyncDB()
                await db.connect()
                rows = await db.fetch("SELECT node_name, host, role FROM mesh.mesh_nodes WHERE status = 'pending'")
                for row in rows:
                    discovered.append({
                        'name': row['node_name'],
                        'host': row['host'] or 'unknown',
                        'platform': 'pending',
                        'role': row['role'] or 'router'
                    })
                await db.close()
            except Exception:
                pass
            
            return web.json_response({"discovered": discovered, "count": len(discovered)})
        except Exception as e:
            import logging
            logging.getLogger("a2a_mesh.dashboard").error(f"Scan error: {e}", exc_info=True)
            return web.json_response({"error": str(e)}, status=500)

    async def _api_onboard_reject(self, request):
        """POST /api/onboard/reject — Reject a pending node."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        
        try:
            body = await request.json()
        except Exception:
            return web.json_response({"error": "Invalid JSON"}, status=400)
        
        node_name = body.get("node_name", "").strip()
        if not node_name:
            return web.json_response({"error": "node_name required"}, status=400)
        
        try:
            from .async_db import AsyncDB
            db = AsyncDB()
            await db.connect()
            await db.execute("UPDATE mesh.mesh_nodes SET status = 'rejected' WHERE node_name = $1", node_name)
            await db.close()
            return web.json_response({"status": "rejected", "node": node_name})
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)

    async def _kanban_page(self, request):
        """GET /kanban — Kanban task management page."""
        from aiohttp import web
        html_path = os.path.join(os.path.dirname(__file__), "kanban.html")
        try:
            with open(html_path, "r", encoding="utf-8") as f:
                return web.Response(text=f.read(), content_type="text/html")
        except FileNotFoundError:
            return web.Response(text="<h1>Kanban page not found</h1>", status=404)

    async def _marveen_page(self, request):
        """GET /marveen — Marveen Engine visual dashboard."""
        from aiohttp import web
        html_path = os.path.join(os.path.dirname(__file__), "marveen.html")
        try:
            with open(html_path, "r", encoding="utf-8") as f:
                return web.Response(text=f.read(), content_type="text/html")
        except FileNotFoundError:
            return web.Response(text="<h1>Marveen page not found</h1>", status=404)

    def _projects_file(self):
        """Get projects JSON file path."""
        import os
        path = os.path.join(os.path.dirname(__file__), "..", "data", "projects.json")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        return path

    def _load_projects(self):
        """Load projects from JSON file."""
        import json, os
        path = self._projects_file()
        try:
            with open(path, "r") as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return []

    def _save_projects(self, projects):
        """Save projects to JSON file and sync to peer nodes."""
        import json, asyncio, logging
        path = self._projects_file()
        with open(path, "w") as f:
            json.dump(projects, f, ensure_ascii=False, indent=2)
        # Best-effort sync to peer nodes via /api/projects/sync endpoint
        try:
            asyncio.ensure_future(self._sync_projects_to_peers(projects))
        except Exception:
            pass  # Don't block save on sync failure

    async def _sync_projects_to_peers(self, projects):
        """Push projects.json to all known peer nodes."""
        import json, aiohttp as aiohttp_lib, logging
        log = logging.getLogger("a2a.lab")
        # Get peers from peer_discovery
        peers = {}
        try:
            node = getattr(self, 'node', None)
            if node and hasattr(node, 'peer_discovery'):
                peers = getattr(node.peer_discovery, '_peers', {})
        except Exception:
            pass
        if not peers:
            return
        data = json.dumps(projects, ensure_ascii=False)
        for name, peer in peers.items():
            host = getattr(peer, 'host', None) or ''
            if not host:
                continue
            # Dashboard port is 8650 on all nodes (not peer.p2p_port which is 8645)
            port = 8650
            url = f"http://{host}:{port}/api/projects/sync"
            try:
                timeout = aiohttp_lib.ClientTimeout(total=5)
                async with aiohttp_lib.ClientSession(timeout=timeout) as session:
                    async with session.post(url, data=data, headers={"Content-Type": "application/json"}) as resp:
                        if resp.status == 200:
                            log.info(f"Projects synced to {name}")
                        else:
                            log.warning(f"Project sync to {name} failed: {resp.status}")
            except Exception as e:
                log.debug(f"Project sync to {name} skipped: {e}")

    async def _api_projects_list(self, request):
        """GET /api/projects — List all projects, optional ?q=search."""
        from aiohttp import web
        import json
        projects = self._load_projects()
        q = request.query.get("q", "").lower()
        if q:
            projects = [p for p in projects if q in p.get("title","").lower() or
                         q in p.get("description","").lower() or
                         q in p.get("category","").lower() or
                         q in " ".join(p.get("tags",[])).lower()]
        return web.json_response({"projects": projects, "count": len(projects)})

    async def _api_projects_create(self, request):
        """POST /api/projects — Create a new project."""
        from aiohttp import web
        import json, uuid, time
        try:
            data = await request.json()
        except Exception:
            return web.json_response({"error": "Invalid JSON"}, status=400)
        if not data.get("title"):
            return web.json_response({"error": "title required"}, status=400)
        pid = str(uuid.uuid4())[:8]
        project = {
            "id": pid,
            "title": data["title"],
            "description": data.get("description", ""),
            "category": data.get("category", "app"),
            "url": data.get("url", ""),
            "links": data.get("links", []),
            "tags": data.get("tags", []),
            "icon": data.get("icon", "📦"),
            "notes": data.get("notes", ""),
            "status": data.get("status", "active"),
            "created_at": time.time(),
            "updated_at": time.time()
        }
        projects = self._load_projects()
        projects.append(project)
        self._save_projects(projects)
        return web.json_response({"status": "ok", "project": project})

    async def _api_projects_update(self, request):
        """PUT /api/projects/{pid} — Update a project."""
        from aiohttp import web
        import time
        pid = request.match_info.get("pid", "")
        try:
            data = await request.json()
        except Exception:
            return web.json_response({"error": "Invalid JSON"}, status=400)
        projects = self._load_projects()
        for p in projects:
            if p["id"] == pid:
                for k in ["title","description","category","url","links","tags","icon","status","notes"]:
                    if k in data:
                        p[k] = data[k]
                p["updated_at"] = time.time()
                self._save_projects(projects)
                return web.json_response({"status": "ok", "project": p})
        return web.json_response({"error": "not found"}, status=404)

    async def _api_projects_delete(self, request):
        """DELETE /api/projects/{pid} — Delete a project."""
        from aiohttp import web
        pid = request.match_info.get("pid", "")
        projects = self._load_projects()
        before = len(projects)
        projects = [p for p in projects if p["id"] != pid]
        if len(projects) == before:
            return web.json_response({"error": "not found"}, status=404)
        self._save_projects(projects)
        return web.json_response({"status": "deleted", "id": pid})

    async def _api_projects_sync(self, request):
        """POST /api/projects/sync — Receive projects from a peer node. Merge by ID."""
        from aiohttp import web
        import json
        try:
            incoming = await request.json()
        except Exception:
            return web.json_response({"error": "Invalid JSON"}, status=400)
        if not isinstance(incoming, list):
            return web.json_response({"error": "Expected array"}, status=400)
        local = self._load_projects()
        local_map = {p["id"]: p for p in local}
        added = 0
        updated = 0
        for p in incoming:
            pid = p.get("id")
            if not pid:
                continue
            if pid in local_map:
                # Merge: update fields from incoming
                local_map[pid].update(p)
                updated += 1
            else:
                local.append(p)
                local_map[pid] = p
                added += 1
        # Save WITHOUT triggering sync (avoid loop)
        path = self._projects_file()
        with open(path, "w") as f:
            json.dump(local, f, ensure_ascii=False, indent=2)
        return web.json_response({"status": "ok", "added": added, "updated": updated, "total": len(local)})

    async def _api_projects_health(self, request):
        """GET /api/projects/health — Ping all project URLs and return status."""
        from aiohttp import web
        import asyncio, aiohttp as aiohttp_lib
        projects = self._load_projects()

        async def check_one(p):
            url = p.get("url", "")
            if not url:
                return {"id": p["id"], "status": "no-url", "code": 0}
            try:
                timeout = aiohttp_lib.ClientTimeout(total=5)
                async with aiohttp_lib.ClientSession(timeout=timeout) as session:
                    async with session.get(url, ssl=False, allow_redirects=True) as resp:
                        return {"id": p["id"], "status": "up" if resp.status < 500 else "down", "code": resp.status}
            except Exception:
                return {"id": p["id"], "status": "down", "code": 0}

        results = await asyncio.gather(*[check_one(p) for p in projects], return_exceptions=False)
        return web.json_response({"results": {r["id"]: r for r in results}})

    async def _api_projects_discover(self, request):
        """GET /api/projects/discover — Scan LAN for common services."""
        from aiohttp import web
        import asyncio, socket, aiohttp as aiohttp_lib

        # Known hosts on the LAN + Tailscale
        hosts = [
            "192.168.1.8",     # Nova (Mac)
            "192.168.1.9",     # Nova alt IP
            "192.168.1.30",    # Morzsa
            "192.168.1.35",    # Synology NAS
            "192.168.1.60",    # Proxmox
            "192.168.1.100",   # Runa (Ubuntu VM)
            "192.168.1.117",   # ESP32-C6 sensor
            "100.75.253.52",   # Nova Tailscale
            "100.65.232.47",   # Morzsa Tailscale
            "100.125.223.24",  # Runa Tailscale
        ]
        # Common service ports with labels
        ports = {
            80: "HTTP", 443: "HTTPS", 3000: "Web App", 3001: "Gitea", 32400: "Plex",
            5000: "Synology DSM", 5001: "Synology HTTPS", 5500: "LibreTranslate",
            6333: "Qdrant", 8080: "Web App", 8090: "IPTV", 8091: "ESPHome MCP",
            8123: "Home Assistant", 8650: "A2A Mesh", 8888: "SearXNG",
            9090: "Prometheus", 9093: "Alertmanager", 3030: "Grafana",
            9120: "HERMEX Dashboard", 9337: "Mesh-LLM", 3131: "Mesh-LLM Console",
            4096: "OpenCode", 3322: "Brain Server", 11434: "Ollama",
            8006: "Proxmox Web UI", 5432: "PostgreSQL",
        }

        async def check_port(host, port, label):
            try:
                fut = asyncio.open_connection(host, port, ssl=False)
                reader, writer = await asyncio.wait_for(fut, timeout=1.5)
                writer.close()
                try: await writer.wait_closed()
                except: pass
                url = f"http://{host}:{port}" if port not in (443, 5001) else f"https://{host}:{port}"
                return {"host": host, "port": port, "label": label, "url": url, "status": "up"}
            except Exception:
                return None

        tasks = []
        for host in hosts:
            for port, label in ports.items():
                tasks.append(check_port(host, port, label))

        results = await asyncio.gather(*tasks, return_exceptions=False)
        found = [r for r in results if r is not None]

        # Check which are already in projects
        existing = self._load_projects()
        existing_urls = set()
        for p in existing:
            existing_urls.add(p.get("url", ""))
            for l in p.get("links", []):
                existing_urls.add(l.get("url", ""))

        new_services = [s for s in found if s["url"] not in existing_urls]
        
        # Auto-add new services to projects.json (permanent, updatable later)
        added = []
        if new_services:
            import time as _time
            for s in new_services:
                project = {
                    "id": f"auto-{int(_time.time()*1000)}-{len(existing)+len(added)}",
                    "icon": "🔌",
                    "title": f"{s['label']} ({s['host']}:{s['port']})",
                    "description": f"Auto-discovered: {s['label']} on {s['host']}:{s['port']}",
                    "category": "other",
                    "url": s["url"],
                    "tags": ["auto-discovered"],
                    "status": "active",
                    "created_at": _time.time(),
                }
                existing.append(project)
                added.append(project)
            self._save_projects(existing)
            log.info(f"Lab auto-discover: added {len(added)} new services to projects.json")
        
        return web.json_response({
            "found": found,
            "new": new_services,
            "added": len(added),
            "existing_count": len(existing) - len(added),
            "new_count": len(new_services)
        })

    # ─── Plugin API ────────────────────────────────────────────────

    # ─── Kanban API ──────────────────────────────────────────────

    def _get_kanban(self):
        """Get or create KanbanManager instance."""
        if not hasattr(self, '_kanban_mgr'):
            from .kanban import KanbanManager
            node_name = getattr(self, 'node_name', 'unknown')
            self._kanban_mgr = KanbanManager(pg_pool=getattr(self, '_pg_pool', None), node_name=node_name)
        return self._kanban_mgr

    async def _api_kanban_boards(self, request):
        """GET /api/kanban — List all boards."""
        from aiohttp import web
        mgr = self._get_kanban()
        return web.json_response({"boards": mgr.get_boards()})

    async def _api_kanban_create_board(self, request):
        """POST /api/kanban — Create a new board."""
        from aiohttp import web
        mgr = self._get_kanban()
        data = await request.json()
        board = mgr.create_board(data.get("title", "New Board"), data.get("columns"))
        return web.json_response(board)

    async def _api_kanban_delete_board(self, request):
        """DELETE /api/kanban/{board_id} — Delete a board."""
        from aiohttp import web
        mgr = self._get_kanban()
        board_id = request.match_info.get("board_id", "")
        if mgr.delete_board(board_id):
            return web.json_response({"status": "deleted"})
        return web.json_response({"error": "not found"}, status=404)

    async def _api_kanban_get_board(self, request):
        """GET /api/kanban/{board_id} — Get a single board with cards."""
        from aiohttp import web
        mgr = self._get_kanban()
        board_id = request.match_info.get("board_id", "")
        board = mgr.get_board(board_id)
        if board:
            return web.json_response(board)
        return web.json_response({"error": "not found"}, status=404)

    async def _api_kanban_get_card_by_id(self, request):
        """GET /api/kanban/cards/{card_id} — Get a single card by ID (searches all boards)."""
        from aiohttp import web
        mgr = self._get_kanban()
        card_id = request.match_info.get("card_id", "")
        for board in mgr.get_boards():
            for card in board.get("cards", []):
                if card.get("id") == card_id:
                    card["board_id"] = board.get("id", "")
                    return web.json_response(card)
        return web.json_response({"error": "card not found"}, status=404)

    async def _api_kanban_add_card(self, request):
        """POST /api/kanban/{board_id}/cards — Add a card to a board."""
        from aiohttp import web
        mgr = self._get_kanban()
        board_id = request.match_info.get("board_id", "")
        data = await request.json()
        card = mgr.add_card(
            board_id, data.get("title", ""),
            column=data.get("column", "todo"),
            description=data.get("description", ""),
            priority=data.get("priority", "medium"),
            assigned_to=data.get("assigned_to", ""),
            parent_id=data.get("parent_id"),
        )
        return web.json_response(card)

    async def _api_kanban_update_card(self, request):
        """PUT /api/kanban/{board_id}/cards/{card_id} — Update a card."""
        from aiohttp import web
        mgr = self._get_kanban()
        board_id = request.match_info.get("board_id", "")
        card_id = request.match_info.get("card_id", "")
        data = await request.json()
        card = mgr.update_card(board_id, card_id, data)
        # Kanban Dispatch: if card moved to in_progress, create delegation + notify assigned agent
        if data.get("column") == "in_progress" and card.get("assigned_to"):
            try:
                node = getattr(self, 'node', None) or getattr(self, '_node_ref', None) or self
                # Create a delegation for this card so the Kanban tracks it
                if hasattr(node, '_pg_pool') and node._pg_pool and not card.get("delegation_task_id"):
                    import uuid
                    task_id = str(uuid.uuid4())
                    await node._pg_pool.execute(
                        """INSERT INTO shared_delegations
                           (task_id, from_agent, to_agent, subject, description, status, priority, kanban_card_id)
                           VALUES ($1, $2, $3, $4, $5, $6, $7, $8)""",
                        task_id, node.node_name, card["assigned_to"],
                        card["title"][:200], card.get("description", "")[:2000],
                        "available", int(card.get("priority_num", 5)), card["id"],
                    )
                    # Store delegation task_id on the card
                    card = mgr.update_card(board_id, card_id, {"delegation_task_id": task_id})
                    log.info(f"Kanban dispatch: created delegation {task_id} for card '{card['title']}' → {card['assigned_to']}")
                if hasattr(node, 'send_a2a_message'):
                    await node.send_a2a_message(
                        to_agent=card["assigned_to"],
                        subject=f"Kanban feladat: {card['title']}",
                        content=f"Kártya '{card['title']}' in_progress státuszba került. Hozzárendelve: {card['assigned_to']}. Leírás: {card.get('description','')}"
                    )
                    log.info(f"Kanban dispatch: notified {card['assigned_to']} about '{card['title']}'")
            except Exception as e:
                log.debug(f"Kanban dispatch skipped: {e}")
        return web.json_response(card)

    async def _api_kanban_delete_card(self, request):
        """DELETE /api/kanban/{board_id}/cards/{card_id} — Delete a card."""
        from aiohttp import web
        mgr = self._get_kanban()
        board_id = request.match_info.get("board_id", "")
        card_id = request.match_info.get("card_id", "")
        if mgr.delete_card(board_id, card_id):
            return web.json_response({"status": "deleted"})
        return web.json_response({"error": "not found"}, status=404)

    async def _api_kanban_breakdown(self, request):
        """POST /api/kanban/{board_id}/cards/{card_id}/breakdown — Break a card into subtasks."""
        from aiohttp import web
        mgr = self._get_kanban()
        board_id = request.match_info.get("board_id", "")
        card_id = request.match_info.get("card_id", "")
        data = await request.json()
        subtasks = data.get("subtasks", [])
        created = await mgr.auto_breakdown(board_id, card_id, subtasks)
        return web.json_response({"created": created})

    async def _api_kanban_approve(self, request):
        """POST /api/kanban/{board_id}/cards/{card_id}/approve — Approve a review card → done."""
        from aiohttp import web
        import os as _os, json as _json, time as _time
        user, err = self._require_auth(request)
        if err:
            return err
        board_id = request.match_info.get("board_id", "")
        card_id = request.match_info.get("card_id", "")
        kanban_path = _os.path.expanduser("~/.hermes/scripts/a2a_mesh/data/kanban.json")
        try:
            with open(kanban_path) as f:
                boards = _json.load(f)
            for board in boards:
                if board.get("id") != board_id:
                    continue
                for card in board.get("cards", []):
                    if card["id"] == card_id:
                        card["column"] = "done"
                        card["approval_required"] = False
                        card["approved_by"] = user.username if hasattr(user, 'username') else "unknown"
                        card["approved_at"] = _time.time()
                        card["updated_at"] = _time.time()
                        with open(kanban_path, 'w') as f:
                            _json.dump(boards, f, indent=2)
                        log.info(f"Kanban approve: card '{card.get('title','')}' approved by {card['approved_by']}")
                        return web.json_response({"status": "approved", "card_id": card_id})
            return web.json_response({"error": "Card not found"}, status=404)
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)

    async def _api_kanban_approve_by_card_id(self, request):
        """POST /api/kanban/cards/{card_id}/approve — Approve by card ID (searches all boards)."""
        from aiohttp import web
        import os as _os, json as _json, time as _time
        user, err = self._require_auth(request)
        if err:
            return err
        card_id = request.match_info.get("card_id", "")
        kanban_path = _os.path.expanduser("~/.hermes/scripts/a2a_mesh/data/kanban.json")
        try:
            with open(kanban_path) as f:
                boards = _json.load(f)
            for board in boards:
                for card in board.get("cards", []):
                    if card["id"] == card_id:
                        card["column"] = "done"
                        card["approval_required"] = False
                        card["approved_by"] = user.username if hasattr(user, 'username') else "unknown"
                        card["approved_at"] = _time.time()
                        card["updated_at"] = _time.time()
                        with open(kanban_path, 'w') as f:
                            _json.dump(boards, f, indent=2)
                        log.info(f"Kanban approve: card '{card.get('title','')}' approved by {card['approved_by']}")
                        return web.json_response({"status": "approved", "card_id": card_id})
            return web.json_response({"error": "Card not found"}, status=404)
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)
    async def _api_kanban_audit(self, request):
        """GET /api/kanban/audit — Audit stale cards across all boards."""
        from aiohttp import web
        mgr = self._get_kanban()
        result = await mgr.audit_stale_cards()
        return web.json_response(result)

    # ─── Plugin API (original) ────────────────────────────────────

    async def _api_precompact_audit(self, request):
        """GET /api/precompact/audit — Audit precompact memories."""
        from aiohttp import web
        from .precompact_hook import precompact_audit
        node_name = getattr(self, 'node_name', 'unknown')
        pg_pool = getattr(self, '_pg_pool', None)
        result = await precompact_audit(pg_pool, node_name=node_name)
        return web.json_response(result)

    async def _api_dream_run(self, request):
        """GET /api/dream — Run dream cycle on demand."""
        from aiohttp import web
        from .dream_engine import run_dream_cycle
        node_name = getattr(self, 'node_name', 'unknown')
        pg_pool = getattr(self, '_pg_pool', None)
        kanban_mgr = None
        if hasattr(self, '_kanban_mgr'):
            kanban_mgr = self._kanban_mgr
        result = await run_dream_cycle(pg_pool, node_name=node_name, kanban_mgr=kanban_mgr)
        return web.json_response(result)

    async def _api_dream_latest(self, request):
        """GET /api/dream/latest — Get latest DREAM.md content."""
        from aiohttp import web
        import os
        dream_path = os.path.join(os.path.dirname(__file__), "..", "data", "DREAM.md")
        try:
            with open(dream_path, "r", encoding="utf-8") as f:
                return web.json_response({"content": f.read(), "exists": True})
        except FileNotFoundError:
            return web.json_response({"content": "", "exists": False})

    async def _api_context_guard(self, request):
        """GET /api/context-guard — Check agent context saturation."""
        from aiohttp import web
        from .context_guard import context_guard_tick
        node_name = getattr(self, 'node_name', 'unknown')
        pg_pool = getattr(self, '_pg_pool', None)
        results = await context_guard_tick(pg_pool, node_name=node_name)
        return web.json_response({"checks": results})

    async def _api_costops_summary(self, request):
        """GET /api/costops/summary — Monthly cost summary."""
        from aiohttp import web
        from .costops import get_monthly_summary
        month = request.query.get("month")
        return web.json_response(get_monthly_summary(month))

    async def _api_costops_budget(self, request):
        """POST /api/costops/budget — Set budget."""
        from aiohttp import web
        from .costops import set_budget
        data = await request.json()
        return web.json_response(set_budget(data.get("category",""), data.get("limit",0)))

    async def _api_costops_alerts(self, request):
        """GET /api/costops/alerts — Check budget alerts."""
        from aiohttp import web
        from .costops import check_budget_alerts
        return web.json_response({"alerts": check_budget_alerts()})

    async def _api_trust_graph(self, request):
        """GET /api/trust — Full trust graph."""
        from aiohttp import web
        from .team_trust import get_trust_graph
        return web.json_response({"graph": get_trust_graph()})

    async def _api_trust_agent(self, request):
        """GET /api/trust/{agent} — Trust report for agent."""
        from aiohttp import web
        from .team_trust import get_agent_trust_report
        agent = request.match_info.get("agent", "")
        return web.json_response(get_agent_trust_report(agent))

    async def _api_trust_set(self, request):
        """POST /api/trust — Set trust level."""
        from aiohttp import web
        from .team_trust import set_trust
        data = await request.json()
        return web.json_response(set_trust(data.get("from",""), data.get("to",""), data.get("level","full")))

    async def _api_prompt_safety_check(self, request):
        """POST /api/prompt-safety/check — Check content safety."""
        from aiohttp import web
        from .prompt_safety import is_safe_for_dispatch, wrap_untrusted
        data = await request.json()
        content = data.get("content", "")
        safe, reason = is_safe_for_dispatch(content)
        return web.json_response({"safe": safe, "reason": reason, "wrapped": wrap_untrusted("api", content) if not safe else None})

    async def _api_model_fallback(self, request):
        """GET /api/model-fallback/{node} — Get model fallback status."""
        from aiohttp import web
        from .model_fallback import get_node_status, check_revert
        node = request.match_info.get("node", "unknown")
        check_revert(node)
        return web.json_response(get_node_status(node))

    async def _api_model_fallback_error(self, request):
        """POST /api/model-fallback/error — Record a model error."""
        from aiohttp import web
        from .model_fallback import record_error
        data = await request.json()
        model = record_error(data.get("node",""), data.get("model",""), data.get("error_type","unknown"))
        return web.json_response({"current_model": model})

    async def _api_pending_retries(self, request):
        """GET /api/pending-retries — Get retry queue stats."""
        from aiohttp import web
        from .pending_retries import get_stats, get_pending
        return web.json_response({"stats": get_stats(), "queue": get_pending()[:20]})

    async def _api_tool_timeouts(self, request):
        """GET /api/tool-timeouts — Get all tool timeout config."""
        from aiohttp import web
        from .tool_timeouts import get_all_timeouts
        return web.json_response(get_all_timeouts())

    async def _api_process_lock(self, request):
        """GET /api/process-lock — Get port lock status."""
        from aiohttp import web
        from .process_lock import get_lock_status
        return web.json_response(get_lock_status())

    async def _api_remote_enroll(self, request):
        """GET /api/remote-enroll — Get enrollment status."""
        from aiohttp import web
        from .remote_enroll import get_enrollment_status
        return web.json_response(get_enrollment_status())

    async def _api_auto_restart(self, request):
        """GET /api/auto-restart — Get restart status for all nodes."""
        from aiohttp import web
        from .auto_restart import get_all_nodes_status
        return web.json_response(get_all_nodes_status())

    async def _api_context_gate(self, request):
        """GET /api/context-gate — Check context saturation."""
        from aiohttp import web
        from .context_gate import get_context_status
        pg_pool = getattr(self.node, '_pg_pool', None) or getattr(self, '_pg_pool', None)
        status = await get_context_status(pg_pool, node=self.node) if pg_pool else {"agents": [], "error": "PG unavailable"}
        return web.json_response(status)

    async def _api_llm_breakdown(self, request):
        """POST /api/llm-breakdown — Break down a task into subtasks.
        If auto_delegate=true, also creates Kanban cards + delegations."""
        from aiohttp import web
        from .llm_breakdown import llm_breakdown, breakdown_and_delegate
        data = await request.json()
        title = data.get("title", "")
        description = data.get("description", "")
        
        if data.get("auto_delegate"):
            # Full pipeline: LLM → Kanban → Delegations
            node = getattr(self, 'node', None)
            result = await breakdown_and_delegate(
                title, description, node=node,
                board_id=data.get("board_id"),
                parent_card_id=data.get("parent_card_id"),
            )
            return web.json_response(result)
        else:
            # Just breakdown, no side effects
            subtasks = await llm_breakdown(title, description)
            return web.json_response({"subtasks": subtasks})

    async def _api_worker_liveness(self, request):
        """GET /api/worker-liveness — Get worker liveness status."""
        from aiohttp import web
        from .worker_liveness import get_all_workers, check_liveness, load_state, save_state, record_heartbeat
        import time as _time
        
        # Auto-register known nodes as workers from registry
        try:
            if hasattr(self, 'node') and hasattr(self.node, 'registry'):
                agents = self.node.registry.list_agents() or []
                for agent_card, health in agents:
                    name = agent_card.name if hasattr(agent_card, 'name') else str(agent_card)
                    hb = health.last_heartbeat if health and hasattr(health, 'last_heartbeat') else 0
                    if hb and hb > 0:
                        record_heartbeat(name, '')
        except:
            pass
        
        issues = check_liveness()
        workers = get_all_workers()
        return web.json_response({"workers": workers, "issues": issues, "count": len(workers)})

    async def _api_stuck_watcher(self, request):
        """GET /api/stuck-watcher — Check for stuck delegations."""
        from aiohttp import web
        from .stuck_watcher import check_stuck
        # In production, fetch from PG
        issues = check_stuck([])
        return web.json_response({"issues": issues})

    async def _api_token_usage(self, request):
        """GET /api/token-usage — Get token usage summary."""
        from aiohttp import web
        from .token_usage import get_summary
        return web.json_response(get_summary())

    async def _api_update_preflight(self, request):
        """GET /api/update-preflight — Run preflight check."""
        from aiohttp import web
        from .update_preflight import run_preflight
        return web.json_response(run_preflight())

    async def _api_store_watcher(self, request):
        """GET /api/store-watcher — Get store inventory + events."""
        from aiohttp import web
        from .store_watcher import get_inventory_summary, get_events
        return web.json_response({"inventory": get_inventory_summary(), "events": get_events(20)})

    async def _api_vault_status(self, request):
        """GET /api/vault — Vault status."""
        from aiohttp import web
        from .vault import get_vault_status
        return web.json_response(get_vault_status())

    async def _api_vault_list(self, request):
        """GET /api/vault/list — List vault entries (no secrets)."""
        from aiohttp import web
        from .vault import list_secrets
        return web.json_response({"entries": list_secrets()})

    async def _api_vault_store(self, request):
        """POST /api/vault/store — Store a secret."""
        from aiohttp import web
        from .vault import store_secret
        data = await request.json()
        result = store_secret(data.get("label", ""), data.get("secret", ""), data.get("type", "generic"))
        return web.json_response(result)

    async def _api_vault_delete(self, request):
        """DELETE /api/vault/{entry_id} — Delete a secret."""
        from aiohttp import web
        from .vault import delete_secret
        entry_id = request.match_info.get("entry_id", "")
        return web.json_response({"deleted": delete_secret(entry_id)})

    # ── Vault Share: per-agent vault hozzáférés + mesh szintű megosztás ──

    def _vault_peers(self) -> list:
        """Known peer node names (peer_discovery alapján, saját nélkül)."""
        peers = []
        try:
            pd = getattr(self.node, "peer_discovery", None)
            if pd:
                all_peers = pd.get_all_peers() if hasattr(pd, "get_all_peers") else (getattr(pd, "_peers", {}) or {})
                for name in (all_peers or {}):
                    if name != self.node.node_name:
                        peers.append(name)
        except Exception:
            pass
        return sorted(set(peers))

    async def _api_vault_mesh(self, request):
        """GET /api/vault/mesh — vault státusz minden node-ról (local + peers)."""
        from aiohttp import web
        from .vault import get_vault_status
        from .vault_share import get_remote_vault_status
        peers = self._vault_peers()
        local = get_vault_status()
        local["node"] = self.node.node_name
        result = {"nodes": {self.node.node_name: {"vault_status": local, "local": True}}}
        if peers:
            remote = await get_remote_vault_status(self.node.router, peers)
            for p, r in remote.items():
                r["local"] = False
                result["nodes"][p] = r
        return web.json_response(result)

    async def _api_vault_remote_list(self, request):
        """GET /api/vault/remote/{node} — adott node vault bejegyzéseinek listája."""
        from aiohttp import web
        from .vault_share import request_from_peer
        node = request.match_info.get("node", "")
        if not node or node == self.node.node_name:
            from .vault import list_secrets
            return web.json_response({"node": self.node.node_name, "local": True,
                                      "entries": list_secrets()})
        resp = await request_from_peer(self.node.router, node, "list")
        if resp is None:
            return web.json_response({"error": f"{node} elérhetetlen vagy nem válaszolt"}, status=504)
        if not resp.get("ok"):
            return web.json_response({"error": resp.get("error", "ismeretlen hiba")}, status=502)
        return web.json_response({"node": node, "local": False, "entries": resp.get("entries", [])})

    async def _api_vault_remote_get(self, request):
        """POST /api/vault/remote/{node}/get {name} — titok lekérése adott node-ról (owner-only)."""
        from aiohttp import web
        from .vault_share import get_remote_secret
        node = request.match_info.get("node", "")
        data = await request.json()
        name = data.get("name", "")
        if not name:
            return web.json_response({"error": "name required"}, status=400)
        if node == self.node.node_name:
            from .vault import get_secret
            value = get_secret(name)
            return web.json_response({"node": self.node.node_name, "local": True,
                                      "name": name, "value": value, "ok": value is not None})
        resp = await get_remote_secret(self.node.router, node, name)
        if resp is None:
            return web.json_response({"error": f"{node} elérhetetlen"}, status=504)
        return web.json_response(resp)

    async def _api_vault_share(self, request):
        """POST /api/vault/share {name, targets, from?} — tétel megosztása cél-node-okra.

        from = forrás node (default: helyi). Távoli forrásnál a titkot először
        lekérjük a forrástól P2P-n, majd közvetlenül a cél vaultokba tároljuk.
        """
        from aiohttp import web
        from .vault_share import share_to_peer, store_to_peer, get_remote_secret
        data = await request.json()
        name = data.get("name", "")
        targets = data.get("targets", [])
        from_node = data.get("from") or self.node.node_name
        if not name or not targets:
            return web.json_response({"error": "name and targets required"}, status=400)
        remote_source = from_node != self.node.node_name
        src_value = None
        if remote_source:
            src = await get_remote_secret(self.node.router, from_node, name)
            if not src or not src.get("ok") or src.get("value") is None:
                return web.json_response(
                    {"error": f"a forrás vaultban ('{from_node}') nincs '{name}' vagy elérhetetlen"},
                    status=502)
            src_value = src.get("value")
        results = {}
        for t in targets:
            if t == self.node.node_name:
                if src_value is not None:
                    from .vault import store_secret
                    r = store_secret(name, src_value)
                    results[t] = {"ok": bool(r.get("stored")), "stored": name if r.get("stored") else None}
                else:
                    results[t] = {"ok": False, "error": "saját node — a tétel már itt van"}
                continue
            if remote_source:
                results[t] = await store_to_peer(self.node.router, t, name, src_value)
            else:
                results[t] = await share_to_peer(self.node.router, t, name)
        return web.json_response({"name": name, "from": from_node, "results": results})

    async def _api_vault_remote_delete(self, request):
        """POST /api/vault/remote/{node}/delete {name} — tétel törlése adott node vaultjából."""
        from aiohttp import web
        from .vault_share import request_from_peer
        from .vault import delete_secret
        node = request.match_info.get("node", "")
        data = await request.json()
        name = data.get("name", "")
        if not name:
            return web.json_response({"error": "name required"}, status=400)
        if node == self.node.node_name:
            deleted = delete_secret(name)
            return web.json_response({"node": node, "local": True, "ok": bool(deleted),
                                      "name": name, "deleted": bool(deleted)})
        resp = await request_from_peer(self.node.router, node, "delete", name)
        if resp is None:
            return web.json_response({"error": f"{node} elérhetetlen"}, status=504)
        return web.json_response(resp)

    async def _api_vault_remote_store(self, request):
        """POST /api/vault/remote/{node}/store {name, value} — tétel mentése adott node vaultjába."""
        from aiohttp import web
        from .vault_share import store_to_peer
        from .vault import store_secret
        node = request.match_info.get("node", "")
        data = await request.json()
        name = data.get("name", "") or data.get("label", "")
        value = data.get("value", "") or data.get("secret", "")
        if not name or not value:
            return web.json_response({"error": "name and value required"}, status=400)
        if node == self.node.node_name:
            r = store_secret(name, value)
            return web.json_response({"node": node, "local": True,
                                      "ok": bool(r.get("stored")), "stored": bool(r.get("stored"))})
        resp = await store_to_peer(self.node.router, node, name, value)
        return web.json_response(resp)

    async def _api_login_throttle(self, request):
        """GET /api/login-throttle — Throttle status."""
        from aiohttp import web
        from .login_throttle import get_throttle_status
        return web.json_response(get_throttle_status())

    async def _api_csrf_status(self, request):
        """GET /api/csrf — CSRF config status."""
        from aiohttp import web
        from .csrf_gate import get_csrf_status
        return web.json_response(get_csrf_status())

    async def _api_channel_health(self, request):
        """GET /api/channel-health — Channel health status."""
        from aiohttp import web
        from .channel_health import get_health_status
        return web.json_response(get_health_status())

    async def _api_federation_status(self, request):
        """GET /api/federation — Federation status (merged with mesh nodes)."""
        from aiohttp import web
        from .federation import manager
        pg_pool = getattr(self.node, "_pg_pool", None)
        node_name = getattr(self.node, "node_name", "nova")
        status = await manager.get_status_with_mesh(pg_pool, node_name)
        return web.json_response(status)

    async def _api_federation_add(self, request):
        """POST /api/federation/peer — Add federated peer."""
        from aiohttp import web
        from .federation import manager
        data = await request.json()
        return web.json_response(manager.add_peer(
            data.get("name", ""), data.get("address", ""),
            data.get("port", 8650), data.get("ssh_tunnel", False)
        ))

    async def _api_federation_remove(self, request):
        """DELETE /api/federation/peer/{name} — Remove peer."""
        from aiohttp import web
        from .federation import manager
        name = request.match_info.get("name", "")
        success = manager.remove_peer(name)
        return web.json_response({"success": success})

    async def _api_federation_connect(self, request):
        """POST /api/federation/connect — Start SSH tunnel."""
        from aiohttp import web
        from .federation import manager
        data = await request.json()
        name = data.get("name", "")
        peer = next((p for p in manager.get_config()["peers"] if p["name"] == name), None)
        if not peer:
            return web.json_response({"error": "Peer not found"}, status=404)
        
        success = manager.bridge.start_tunnel(
            name, peer["address"], peer["port"], 8650
        )
        return web.json_response({"success": success})

    async def _api_federation_discover(self, request):
        """POST /api/federation/discover — LAN auto-discover."""
        from aiohttp import web
        from .federation import manager
        manager.node_name = self.node.node_name
        discovered = await manager.discover_lan()
        return web.json_response({"discovered": discovered})

    async def _api_federation_capabilities(self, request):
        """GET /api/federation/capabilities/{name} — Remote mesh capabilities."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err: return err
        from .federation import manager
        name = request.match_info.get("name", "")
        peer = next((p for p in manager.get_config()["peers"] if p["name"] == name), None)
        if not peer:
            return web.json_response({"error": "Peer not found"}, status=404)
        
        caps = await manager.fetch_capabilities(peer)
        return web.json_response({"capabilities": caps})

    async def _api_federation_trust(self, request):
        """POST /api/federation/trust/{name} — Toggle trust level."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err: return err
        from .federation import manager
        name = request.match_info.get("name", "")
        data = await request.json()
        level = data.get("level", "toggle")
        success, new_level = manager.set_trust(name, level if level != "toggle" else None)
        return web.json_response({"success": success, "trust": new_level})

    async def _api_federation_health(self, request):
        """GET /api/federation/health/{name} — Remote mesh health."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err: return err
        from .federation import manager
        name = request.match_info.get("name", "")
        peer = next((p for p in manager.get_config()["peers"] if p["name"] == name), None)
        if not peer:
            return web.json_response({"error": "Peer not found"}, status=404)
        
        health = await manager.check_health(peer)
        return web.json_response(health)


    async def _api_model_suggest(self, request):
        """GET /api/model-suggest — All model suggestions."""
        from aiohttp import web
        from .model_suggest import get_model_suggestions
        return web.json_response(get_model_suggestions())

    async def _api_model_suggest_node(self, request):
        """GET /api/model-suggest/{node} — Suggestion for a node."""
        from aiohttp import web
        from .model_suggest import get_node_suggestion
        node = request.match_info.get("node", "")
        return web.json_response(get_node_suggestion(node))

    async def _api_voice_status(self, request):
        """GET /api/voice — Voice directive status."""
        from aiohttp import web
        from .voice_directive import get_voice_status
        return web.json_response(get_voice_status())

    async def _api_voice_parse(self, request):
        """POST /api/voice/parse — Parse a voice transcript."""
        from aiohttp import web
        from .voice_directive import parse_voice_directive
        data = await request.json()
        return web.json_response(parse_voice_directive(data.get("text", "")))

    async def _api_inbox_nudge(self, request):
        """GET /api/inbox-nudge — Inbox nudge status."""
        from aiohttp import web
        from .inbox_nudge import get_inbox_status
        return web.json_response(get_inbox_status())

    async def _api_memory_boundary(self, request):
        """GET /api/memory-boundary — Memory boundary status."""
        from aiohttp import web
        from .memory_boundary import get_boundary_status
        return web.json_response(get_boundary_status())

    async def _api_message_router(self, request):
        """GET /api/message-router — Message router status."""
        from aiohttp import web
        from .message_router import get_router_status
        return web.json_response(get_router_status())

    async def _api_team_status(self, request):
        """GET /api/team — Team hierarchy status."""
        from aiohttp import web
        from .agent_team import get_team_status
        return web.json_response(get_team_status())

    async def _api_team_update(self, request):
        """POST /api/team/update — Update a node's team config."""
        from aiohttp import web
        from .agent_team import update_node_role
        data = await request.json()
        result = update_node_role(
            data.get("node", ""),
            data.get("role"),
            data.get("reports_to"),
            data.get("delegates_to"),
            data.get("auto_delegation"),
        )
        return web.json_response(result)

    async def _api_cron_status(self, request):
        """GET /api/cron — Cron scheduler status."""
        from aiohttp import web
        from .cron_scheduler import get_cron_status
        return web.json_response(get_cron_status())

    async def _api_cron_add(self, request):
        """POST /api/cron/add — Add a scheduled task."""
        from aiohttp import web
        from .cron_scheduler import add_task
        data = await request.json()
        result = add_task(data.get("name", ""), data.get("cron", ""), data.get("action", ""), data.get("description", ""))
        return web.json_response(result)

    async def _api_update_checker(self, request):
        """GET /api/update-checker — Git update status."""
        from aiohttp import web
        from .update_checker import get_update_status
        return web.json_response(get_update_status())

    async def _api_update_pull(self, request):
        """POST /api/update-pull — Pull latest from git, deploy to peers, restart."""
        from aiohttp import web
        import subprocess, os
        user, err = self._require_auth(request)
        if err:
            return err
        try:
            mesh_dir = os.path.expanduser("~/.hermes/scripts/a2a_mesh")
            # Step 1: git stash local changes
            subprocess.run(["git", "stash"], cwd=mesh_dir, capture_output=True, text=True, timeout=10)
            # Step 2: git pull
            result = subprocess.run(["git", "pull", "origin", "main"], cwd=mesh_dir, capture_output=True, text=True, timeout=30)
            pull_ok = result.returncode == 0
            pull_output = result.stdout + result.stderr
            # Step 3: Deploy to peers via existing deploy API
            deploy_result = None
            if pull_ok and self.node and hasattr(self.node, 'delegation'):
                try:
                    peers = self.node.peer_discovery.get_all_peers() if hasattr(self.node, 'peer_discovery') else []
                    target_nodes = [p for p in peers if p != self.node.node_name]
                    deploy_results = []
                    for peer_name in target_nodes:
                        remote = "gitea" if peer_name == "morzsa" else "origin"
                        deploy_desc = {"remote": remote, "branch": "main", "action": "pull_restart"}
                        await self.node.delegation.delegate_task(
                            to_agent=peer_name,
                            subject=f"[DEPLOY] git pull + restart",
                            description=deploy_desc,
                            task_type="deploy",
                            priority=8,
                            available=True,
                        )
                        deploy_results.append({"node": peer_name, "status": "delegated"})
                    deploy_result = deploy_results
                except Exception as e:
                    deploy_result = {"error": str(e)}
            # Step 4: Restart Nova
            import platform
            if platform.system() == "Darwin":
                subprocess.run(["launchctl", "stop", "com.hermes.a2a-mesh-node"], capture_output=True, timeout=5)
                subprocess.run(["launchctl", "start", "com.hermes.a2a-mesh-node"], capture_output=True, timeout=5)
            else:
                subprocess.run(["systemctl", "--user", "restart", "a2a-mesh"], capture_output=True, timeout=10)
            return web.json_response({
                "ok": pull_ok,
                "pull_output": pull_output[:500],
                "deploy": deploy_result,
                "restart": "initiated",
            })
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)

    async def _api_network_info(self, request):
        """GET /api/network-info — Network info."""
        from aiohttp import web
        from .network_info import get_network_status
        return web.json_response(get_network_status())

    async def _api_auth_status(self, request):
        """GET /api/auth-status — Password auth status."""
        from aiohttp import web
        from .password_hash import get_auth_status
        return web.json_response(get_auth_status())

    async def _api_sanitize(self, request):
        """GET /api/sanitize — Sanitizer status."""
        from aiohttp import web
        from .sanitize import get_sanitize_status
        return web.json_response(get_sanitize_status())

    async def _api_fleet_status(self, request):
        """GET /api/fleet/status — fleet transfer status."""
        from aiohttp import web
        from .fleet_transfer import get_fleet_status
        return web.json_response(get_fleet_status())

    async def _api_fleet_export(self, request):
        """GET /api/fleet/export — export fleet snapshot."""
        from aiohttp import web
        import os
        from .fleet_transfer import export_fleet, save_export
        include_vault = request.query.get("vault", "false").lower() == "true"
        snapshot = export_fleet(include_vault=include_vault)
        password = request.query.get("password")
        filepath = save_export(snapshot, password=password)
        return web.json_response({
            "exported": True,
            "file": filepath,
            "size": os.path.getsize(filepath),
            "encrypted": bool(password),
        })

    async def _api_fleet_import(self, request):
        """POST /api/fleet/import — import fleet snapshot."""
        from aiohttp import web
        from .fleet_transfer import import_fleet
        try:
            data = await request.json()
        except Exception:
            return web.json_response({"error": "Invalid JSON"}, status=400)
        dry_run = data.get("dry_run", False)
        results = import_fleet(data.get("snapshot", data), dry_run=dry_run)
        return web.json_response(results)

    async def _api_marveen_db_status(self, request):
        """GET /api/marveen-db/status — Marveen DB tables status."""
        from aiohttp import web
        from .marveen_db import get_marveen_db_status
        return web.json_response(await get_marveen_db_status())

    async def _api_watchdog_status(self, request):
        """GET /api/watchdog/status — Channel Monitor Watchdog status."""
        from aiohttp import web
        from .channel_monitor import get_watchdog_status, WatchdogAction
        status = get_watchdog_status()
        return web.json_response({
            "monitored_nodes": status,
            "thresholds": {
                "warning_s": 30,
                "busy_s": 60,
                "stuck_s": 300,
                "frozen_s": 600,
                "dead_s": 1800,
            },
            "policy": {
                "startup_grace_s": 30,
                "restart_grace_s": 60,
                "max_restart_attempts": 5,
                "down_confirm_s": 10,
                "busy_defer_max_s": 600,
            },
        })

    async def _api_governance_rules(self, request):
        """GET /api/governance/rules — List all governance rules."""
        from aiohttp import web
        from .governance import get_gates
        return web.json_response({"rules": get_gates().get_rules()})

    async def _api_governance_audit(self, request):
        """GET /api/governance/audit — Get governance audit log."""
        from aiohttp import web
        from .governance import get_gates
        limit = int(request.query.get("limit", "100"))
        return web.json_response({"audit_log": get_gates().get_audit_log(limit)})

    async def _api_desired_state(self, request):
        """GET /api/desired-state — Desired state reconciler status."""
        from aiohttp import web
        from .desired_state import get_status, ensure_initialized, set_pg_pool, auto_enroll_from_registry
        # Initialize on first call
        pool = getattr(self.node, '_pg_pool', None)
        if pool:
            set_pg_pool(pool)
        await ensure_initialized()
        # Auto-enroll from registry
        known = {}
        try:
            if hasattr(self, 'node') and hasattr(self.node, 'registry'):
                agents = self.node.registry.list_agents() or []
                for agent_card, health in agents:
                    name = agent_card.name if hasattr(agent_card, 'name') else str(agent_card)
                    known[name] = {
                        'last_heartbeat': health.last_heartbeat if health and hasattr(health, 'last_heartbeat') else 0,
                        'status': 'online' if health and hasattr(health, 'status') else 'unknown',
                    }
        except:
            pass
        await auto_enroll_from_registry(known)
        return web.json_response(get_status())

    async def _api_desired_state_add(self, request):
        """POST /api/desired-state/add — Add node to desired state."""
        from aiohttp import web
        from .desired_state import add_desired_node
        try:
            data = await request.json()
            name = data.get("name", "")
            ssh_target = data.get("ssh_target", "")
            ssh_key = data.get("ssh_key", "~/.ssh/id_ed25519_openclaw")
            restart_cmd = data.get("restart_cmd", "")
            if not name:
                return web.json_response({"error": "name required"}, status=400)
            add_desired_node(name, ssh_target, ssh_key, restart_cmd)
            return web.json_response({"ok": True, "node": name})
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)

    async def _api_desired_state_remove(self, request):
        """POST /api/desired-state/remove — Remove node from desired state."""
        from aiohttp import web
        from .desired_state import remove_desired_node
        try:
            data = await request.json()
            name = data.get("name", "")
            if not name:
                return web.json_response({"error": "name required"}, status=400)
            remove_desired_node(name)
            return web.json_response({"ok": True, "node": name, "disabled": True})
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)

    async def _api_process_lock(self, request):
        """GET /api/process-lock — Process lock status."""
        from aiohttp import web
        from .process_lock import get_lock_status
        return web.json_response(get_lock_status())

    async def _api_generate_daily_summary(self, request):
        """POST /api/daily-summary/generate — Generate daily summary from task_runs."""
        from aiohttp import web
        from .marveen_db import generate_daily_summary
        pool = getattr(self.node, '_pg_pool', None) or getattr(self, '_pg_pool', None)
        if not pool:
            return web.json_response({"error": "PG pool not available"}, status=503)
        result = await generate_daily_summary(pool)
        return web.json_response(result)

    async def _api_task_runs(self, request):
        """GET /api/task-runs?agent=Nova&limit=50 — task execution audit trail."""
        from aiohttp import web
        from .marveen_db import get_task_runs
        agent = request.query.get("agent")
        limit = int(request.query.get("limit", "50"))
        return web.json_response({"runs": await get_task_runs(agent, limit)})

    async def _api_kanban_add_comment(self, request):
        """POST /api/kanban/comments — add a comment to a card."""
        from aiohttp import web
        from .marveen_db import add_kanban_comment
        try:
            data = await request.json()
        except Exception:
            return web.json_response({"error": "Invalid JSON"}, status=400)
        comment_id = await add_kanban_comment(data.get("card_id"), data.get("author", "system"), data.get("comment", ""))
        return web.json_response({"id": comment_id})

    async def _api_kanban_get_comments(self, request):
        """GET /api/kanban/comments/{card_id} — get comments for a card."""
        from aiohttp import web
        from .marveen_db import get_kanban_comments
        card_id = int(request.match_info["card_id"])
        return web.json_response({"comments": await get_kanban_comments(card_id)})

    async def _api_kanban_get_events(self, request):
        """GET /api/kanban/events/{card_id} — get event history for a card."""
        from aiohttp import web
        from .marveen_db import get_card_events
        card_id = int(request.match_info["card_id"])
        return web.json_response({"events": await get_card_events(card_id)})

    async def _api_labels_list(self, request):
        """GET /api/labels — list all kanban labels."""
        from aiohttp import web
        from .marveen_db import list_labels
        return web.json_response({"labels": await list_labels()})

    async def _api_labels_create(self, request):
        """POST /api/labels — create a label."""
        from aiohttp import web
        from .marveen_db import create_label
        try:
            data = await request.json()
        except Exception:
            return web.json_response({"error": "Invalid JSON"}, status=400)
        label_id = await create_label(data.get("name", ""), data.get("color", "#6366f1"))
        return web.json_response({"id": label_id})

    async def _api_daily_logs(self, request):
        """GET /api/daily-logs?agent=Nova&days=7 — get daily logs."""
        from aiohttp import web
        from .marveen_db import get_daily_logs
        agent = request.query.get("agent")
        days = int(request.query.get("days", "7"))
        return web.json_response({"logs": await get_daily_logs(agent, days)})

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

    # ── Reflections (Esmefuttatasok) API ──

    async def _api_reflections(self, request):
        """Get reflections from mesh_memory. GET /api/reflections?limit=50&type=stagnation
        
        Query params:
            limit: max results (default 50, max 200)
            type: filter by reflection type (stagnation, consensus, blind_spot, tension, progress)
        """
        from aiohttp import web
        try:
            limit = min(int(request.query.get("limit", "50")), 200)
            type_filter = request.query.get("type", "")

            pool = getattr(self.node, 'pg_pool', None) or getattr(self.node, '_pg_pool', None)
            if not pool or not hasattr(pool, 'is_connected') or not pool.is_connected():
                return web.json_response({"error": "PG not available"}, status=503)

            if type_filter:
                rows = await pool.fetch(
                    """SELECT id, memory_key, memory_value, metadata, created_at
                       FROM mesh.mesh_memory
                       WHERE memory_type = 'reflection'
                         AND metadata::text LIKE $1
                       ORDER BY created_at DESC LIMIT $2""",
                    f'%"{type_filter}"%', limit,
                )
            else:
                rows = await pool.fetch(
                    """SELECT id, memory_key, memory_value, metadata, created_at
                       FROM mesh.mesh_memory
                       WHERE memory_type = 'reflection'
                       ORDER BY created_at DESC LIMIT $1""",
                    limit,
                )

            reflections = []
            for row in rows:
                import json as _json
                meta = _json.loads(row['metadata']) if row['metadata'] else {}
                reflections.append({
                    "id": row['id'],
                    "topic": meta.get('topic', ''),
                    "analysis": row['memory_value'],
                    "types": meta.get('reflection_types', []),
                    "agents": meta.get('agents', []),
                    "msg_count": meta.get('msg_count', 0),
                    "created_at": row['created_at'].isoformat() if row['created_at'] else '',
                })

            return web.json_response({
                "reflections": reflections,
                "count": len(reflections),
            })
        except Exception as e:
            log.error(f"Reflections API error: {e}", exc_info=True)
            return web.json_response({"error": str(e)}, status=500)

    async def _api_reflections_export(self, request):
        """Export all reflections as JSON download. GET /api/reflections/export"""
        from aiohttp import web
        try:
            pool = getattr(self.node, 'pg_pool', None) or getattr(self.node, '_pg_pool', None)
            if not pool or not hasattr(pool, 'is_connected') or not pool.is_connected():
                return web.json_response({"error": "PG not available"}, status=503)

            rows = await pool.fetch(
                """SELECT id, memory_key, memory_value, metadata, created_at
                   FROM mesh.mesh_memory
                   WHERE memory_type = 'reflection'
                   ORDER BY created_at DESC LIMIT 500""",
            )

            import json as _json, time as _time
            reflections = []
            for row in rows:
                meta = _json.loads(row['metadata']) if row['metadata'] else {}
                reflections.append({
                    "id": row['id'],
                    "topic": meta.get('topic', ''),
                    "analysis": row['memory_value'],
                    "types": meta.get('reflection_types', []),
                    "agents": meta.get('agents', []),
                    "msg_count": meta.get('msg_count', 0),
                    "created_at": row['created_at'].isoformat() if row['created_at'] else '',
                })

            export_data = _json.dumps({
                "exportedAt": _time.strftime("%Y-%m-%dT%H:%M:%SZ", _time.gmtime()),
                "source": "nova",
                "count": len(reflections),
                "reflections": reflections,
            }, indent=2, ensure_ascii=False)

            return web.Response(
                text=export_data,
                content_type="application/json",
                headers={
                    "Content-Disposition": f"attachment; filename=reflections_export_{_time.strftime('%Y%m%d_%H%M%S')}.json"
                },
            )
        except Exception as e:
            log.error(f"Reflections export API error: {e}", exc_info=True)
            return web.json_response({"error": str(e)}, status=500)

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

    async def _api_alerts_delegation(self, request):
        """GET /api/alerts/delegation — Delegation + Kanban based alerts.

        Generates alerts from delegation status and Kanban card state:
        - failed delegations → critical
        - stuck in_progress >30min → warning
        - stale review >7days → info
        - available tasks >5min no claim → warning
        """
        from aiohttp import web
        import time as _time
        user, err = self._require_auth(request)
        if err:
            return err

        alerts = []
        now = _time.time()

        # 1. Check delegations
        try:
            pool = getattr(self.node, '_pg_pool', None) or getattr(self, '_pg_pool', None)
            if not pool:
                return web.json_response({"error": "PG pool not available"}, status=503)
            rows = await pool.fetch(
                """SELECT task_id, subject, status, to_agent, from_agent,
                          created_at, kanban_card_id, priority
                   FROM shared_delegations
                   WHERE status IN ('failed', 'available', 'pending', 'running')
                   ORDER BY created_at DESC LIMIT 50"""
            )
            for row in rows:
                r = dict(row)
                # asyncpg returns datetime for timestamps — convert to epoch
                created = r.get("created_at")
                if hasattr(created, 'timestamp'):
                    age = now - created.timestamp()
                else:
                    age = now - float(created or 0)
                r["age_seconds"] = int(age)
                r["age_human"] = _fmt_age(age)

                if r["status"] == "failed":
                    alerts.append({
                        "severity": "critical",
                        "source": "delegation",
                        "task_id": str(r["task_id"]),
                        "title": f"Delegáció FAILED: {r['subject'][:50]}",
                        "message": f"{r['from_agent']} → {r['to_agent']}, {r['age_human']}, kanban={r.get('kanban_card_id') or 'N/A'}",
                        "age": r["age_human"],
                    })
                elif r["status"] == "running" and age > 600:
                    alerts.append({
                        "severity": "warning",
                        "source": "delegation",
                        "task_id": str(r["task_id"]),
                        "title": f"Delegáció beragadva: {r['subject'][:50]}",
                        "message": f"Running {r['age_human']}, {r['from_agent']} → {r['to_agent']}",
                        "age": r["age_human"],
                    })
                elif r["status"] == "available" and age > 300:
                    alerts.append({
                        "severity": "warning",
                        "source": "delegation",
                        "task_id": str(r["task_id"]),
                        "title": f"Available task nem claimelt: {r['subject'][:50]}",
                        "message": f"Available {r['age_human']}, no peer claimed it yet",
                        "age": r["age_human"],
                    })
                elif row["status"] == "pending" and age > 1800:
                    alerts.append({
                        "severity": "warning",
                        "source": "delegation",
                        "task_id": row["task_id"],
                        "title": f"Pending delegáció: {row['subject'][:50]}",
                        "message": f"Pending {r['age_human']}, {row['from_agent']} → {row['to_agent']}",
                        "age": r["age_human"],
                    })
        except Exception as e:
            alerts.append({
                "severity": "critical",
                "source": "system",
                "title": "Delegation alert query error",
                "message": str(e)[:200],
            })

        # 2. Check Kanban cards
        try:
            import json as _json, os as _os
            kanban_path = _os.path.expanduser("~/.hermes/scripts/a2a_mesh/data/kanban.json")
            if _os.path.exists(kanban_path):
                with open(kanban_path) as f:
                    boards = _json.load(f)
                for board in boards:
                    for card in board.get("cards", []):
                        col = card.get("column", "todo")
                        updated = card.get("updated_at", 0)
                        card_age = now - updated if updated else 0
                        dtid = card.get("delegation_task_id", "")

                        if col == "in_progress" and card_age > 1800:
                            alerts.append({
                                "severity": "warning",
                                "source": "kanban",
                                "card_id": card["id"],
                                "title": f"Kanban beragadva: {card['title'][:50]}",
                                "message": f"in_progress {_fmt_age(card_age)}, assigned={card.get('assigned_to','')}, deleg={dtid[:12] or 'N/A'}",
                                "age": _fmt_age(card_age),
                            })
                        elif col == "review" and card_age > 604800:
                            alerts.append({
                                "severity": "info",
                                "source": "kanban",
                                "card_id": card["id"],
                                "title": f"Review túl régi: {card['title'][:50]}",
                                "message": f"review {_fmt_age(card_age)}, deleg={dtid[:12] or 'N/A'}",
                                "age": _fmt_age(card_age),
                            })
                        elif col == "in_progress" and not dtid:
                            alerts.append({
                                "severity": "info",
                                "source": "kanban",
                                "card_id": card["id"],
                                "title": f"Kanban nincs delegálva: {card['title'][:50]}",
                                "message": f"in_progress but no delegation_task_id",
                                "age": _fmt_age(card_age),
                            })
        except Exception as e:
            alerts.append({
                "severity": "warning",
                "source": "system",
                "title": "Kanban alert query error",
                "message": str(e)[:200],
            })

        # Sort by severity (critical first)
        sev_order = {"critical": 0, "warning": 1, "info": 2}
        alerts.sort(key=lambda a: sev_order.get(a["severity"], 3))

        return web.json_response({
            "total": len(alerts),
            "critical": sum(1 for a in alerts if a["severity"] == "critical"),
            "warning": sum(1 for a in alerts if a["severity"] == "warning"),
            "info": sum(1 for a in alerts if a["severity"] == "info"),
            "alerts": alerts,
        })

    # ─── Marveen Insights API ──────────────────────────────────────────

    async def _api_insights_cost(self, request):
        """GET /api/insights/cost — Cost tracking per agent."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        try:
            from .costops import get_monthly_summary, check_budget_alerts
            summary = get_monthly_summary()
            alerts = check_budget_alerts()
            return web.json_response({"summary": summary, "alerts": alerts})
        except Exception as e:
            return web.json_response({"error": str(e), "summary": {}, "alerts": []}, status=500)

    async def _api_insights_inbox(self, request):
        """GET /api/insights/inbox — Inbox nudge status per agent."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        try:
            from .inbox_nudge import get_inbox_status
            status = get_inbox_status()
            return web.json_response(status)
        except Exception as e:
            return web.json_response({"error": str(e), "total_unread": 0, "messages": []}, status=500)

    async def _api_insights_context_gate(self, request):
        """GET /api/insights/context-gate — Context saturation per agent."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        try:
            from .context_gate import get_context_status
            pg_pool = getattr(self.node, '_pg_pool', None)
            status = await get_context_status(pg_pool, node=self.node) if pg_pool else {"agents": [], "error": "PG unavailable"}
            return web.json_response(status)
        except Exception as e:
            return web.json_response({"error": str(e), "agents": []}, status=500)

    async def _api_insights_conversations(self, request):
        """GET /api/insights/conversations/{agent} — Conversation log for agent."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        agent = request.match_info.get("agent", "")
        limit = int(request.query.get("limit", 50))
        try:
            from .marveen_db import get_conversation_log
            logs = await get_conversation_log(agent, limit=limit)
            return web.json_response({"agent": agent, "messages": logs, "count": len(logs)})
        except Exception as e:
            return web.json_response({"error": str(e), "messages": []}, status=500)

    async def _api_insights_dream(self, request):
        """GET /api/insights/dream — Dream Engine status and recent cycles."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        try:
            from .dream_engine import get_dream_status
            pg_pool = getattr(self.node, '_pg_pool', None)
            status = await get_dream_status(pg_pool) if pg_pool else {"enabled": False, "error": "PG unavailable"}
            return web.json_response(status)
        except Exception as e:
            return web.json_response({"error": str(e), "enabled": False}, status=500)

    # ─── Ideas Board (Ötletláda) ─────────────────────────────────────

    async def _api_insights_dream_trigger(self, request):
        """POST /api/insights/dream/trigger — Manually trigger a dream cycle."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        try:
            from .dream_engine import run_dream_cycle
            pg_pool = getattr(self.node, '_pg_pool', None)
            node_name = getattr(self.node, 'node_name', 'unknown')
            if not pg_pool:
                return web.json_response({"error": "PG unavailable"}, status=503)
            results = await run_dream_cycle(pg_pool, node_name)
            return web.json_response({
                "ok": True,
                "timestamp": results.get("timestamp"),
                "buckets": list(results.get("buckets", {}).keys()),
            })
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)

    def _get_pg_pool(self):
        """Get PG pool from node or self."""
        pool = getattr(self, '_pg_pool', None)
        if not pool:
            pool = getattr(self.node, 'pg_pool', None) or getattr(self.node, '_pg_pool', None)
        return pool

    async def _api_ideas_list(self, request):
        """GET /api/ideas — list all ideas with optional filters."""
        from aiohttp import web
        import json as _json
        user, err = self._require_auth(request)
        if err:
            return err
        try:
            pool = self._get_pg_pool()
            if not pool:
                return web.json_response({"error": "PG unavailable", "ideas": []}, status=503)

            status_filter = request.query.get("status", "")
            category_filter = request.query.get("category", "")
            limit = int(request.query.get("limit", 100))

            query = "SELECT * FROM mesh.mesh_ideas"
            conditions = []
            params = []
            idx = 1
            if status_filter:
                conditions.append("status = $" + str(idx))
                params.append(status_filter)
                idx += 1
            if category_filter:
                conditions.append("category = $" + str(idx))
                params.append(category_filter)
                idx += 1
            if conditions:
                query += " WHERE " + " AND ".join(conditions)
            query += " ORDER BY created_at DESC LIMIT $" + str(idx)
            params.append(limit)

            rows = await pool.fetch(query, *params)
            ideas = []
            for r in rows:
                ideas.append({
                    "id": r["idea_id"],
                    "title": r["title"],
                    "description": r["description"],
                    "category": r["category"],
                    "priority": r["priority"],
                    "status": r["status"],
                    "submitted_by": r["submitted_by"],
                    "source_type": r["source_type"],
                    "tags": list(r["tags"]) if r["tags"] else [],
                    "upvotes": r["upvotes"],
                    "downvotes": r["downvotes"],
                    "score": r["upvotes"] - r["downvotes"],
                    "voters": list(r["voters"]) if r["voters"] else [],
                    "assigned_to": r["assigned_to"],
                    "linked_task_id": r["linked_task_id"],
                    "integrated": r["integrated"] if "integrated" in r.keys() else False,
                    "integrated_at": r["integrated_at"].isoformat() if r.get("integrated_at") else None,
                    "integrated_file": r["integrated_file"] if "integrated_file" in r.keys() else None,
                    "created_at": r["created_at"].isoformat() if r["created_at"] else None,
                    "updated_at": r["updated_at"].isoformat() if r["updated_at"] else None,
                })

            # Stats
            stats = {"total": len(ideas), "idea": 0, "approved": 0, "in_progress": 0, "done": 0, "rejected": 0}
            for idea in ideas:
                s = idea["status"]
                if s in stats:
                    stats[s] += 1

            return web.json_response({"ideas": ideas, "stats": stats})
        except Exception as e:
            return web.json_response({"error": str(e), "ideas": [], "stats": {}}, status=500)

    async def _api_ideas_submit(self, request):
        """POST /api/ideas — submit a new idea."""
        from aiohttp import web
        import uuid as _uuid
        user, err = self._require_auth(request)
        if err:
            return err
        try:
            data = await request.json()
            title = (data.get("title") or "").strip()
            if not title:
                return web.json_response({"error": "Title required"}, status=400)
            description = (data.get("description") or "").strip()
            category = (data.get("category") or "general").strip()
            priority = (data.get("priority") or "medium").strip()
            tags = data.get("tags", [])
            if not isinstance(tags, list):
                tags = []
            source_type = (data.get("source_type") or "user").strip()
            submitted_by = (data.get("submitted_by") or getattr(user, 'username', None) or getattr(user, 'name', None) or "user").strip()

            idea_id = "idea_" + _uuid.uuid4().hex[:12]
            pool = self._get_pg_pool()
            if pool:
                await pool.execute(
                    "INSERT INTO mesh.mesh_ideas (idea_id, title, description, category, priority, source_type, submitted_by, tags) "
                    "VALUES ($1, $2, $3, $4, $5, $6, $7, $8::text[])",
                    idea_id, title, description, category, priority, source_type, submitted_by, tags
                )
            else:
                # JSON file fallback when PG unavailable
                import os as _os, json as _json
                ideas_path = _os.path.join(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))), "data", "ideas.json")
                ideas = []
                if _os.path.exists(ideas_path):
                    with open(ideas_path, "r") as _f:
                        try: ideas = _json.loads(_f.read())
                        except: ideas = []
                ideas.append({"id": idea_id, "title": title, "description": description, "category": category, "priority": priority, "source_type": source_type, "submitted_by": submitted_by, "tags": tags, "votes": 0, "status": "open", "created_at": __import__("time").time()})
                with open(ideas_path, "w") as _f:
                    _f.write(_json.dumps(ideas, indent=2))
            return web.json_response({"ok": True, "id": idea_id})
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)

    async def _api_ideas_vote(self, request):
        """POST /api/ideas/{id}/vote — szavazat + determinisztikus szabályok.

        score >= +2 → automatikus elfogadás + megvalósítás
        score <= -2 → automatikus elutasítás
        """
        from aiohttp import web
        from .idea_review import apply_vote_with_rules, make_implement_fn
        user, err = self._require_auth(request)
        if err:
            return err
        try:
            idea_id = request.match_info.get("id", "")
            data = await request.json()
            vote = data.get("vote", "up")
            voter = data.get("voter", getattr(user, 'username', None) or getattr(user, 'name', None) or "user")
            pool = self._get_pg_pool()
            if not pool:
                return web.json_response({"error": "PG unavailable"}, status=503)
            node = getattr(self, 'node', None) or getattr(self, '_node_ref', None) or self
            # EMBERI KAPU: a szavazás csak approved-ig visz — implement_fn nélkül,
            # a beépítés a „Beépítés jóváhagyása" gombbal indul (implement endpoint)
            result = await apply_vote_with_rules(pool, idea_id, voter, vote)
            if not result.get("ok") and result.get("already_voted"):
                return web.json_response(result, status=409)
            if not result.get("ok"):
                return web.json_response(result, status=404)
            return web.json_response(result)
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)

    async def _api_ideas_status(self, request):
        """POST /api/ideas/{id}/status — change idea status."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        try:
            idea_id = request.match_info.get("id", "")
            data = await request.json()
            new_status = (data.get("status") or "").strip()
            valid = {"idea", "approved", "in_progress", "done", "rejected"}
            if new_status not in valid:
                return web.json_response({"error": "Invalid status. Valid: " + ", ".join(valid)}, status=400)
            assigned_to = data.get("assigned_to")
            pool = self._get_pg_pool()
            if not pool:
                return web.json_response({"error": "PG unavailable"}, status=503)

            if assigned_to:
                await pool.execute(
                    "UPDATE mesh.mesh_ideas SET status = $2::varchar, assigned_to = $3, updated_at = NOW(), closed_at = CASE WHEN $2 IN ('done','rejected') THEN NOW() ELSE NULL END WHERE idea_id = $1",
                    idea_id, new_status, assigned_to
                )
            else:
                await pool.execute(
                    "UPDATE mesh.mesh_ideas SET status = $2::varchar, updated_at = NOW(), closed_at = CASE WHEN $2 IN ('done','rejected') THEN NOW() ELSE NULL END WHERE idea_id = $1",
                    idea_id, new_status
                )
            return web.json_response({"ok": True, "status": new_status})
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)

    async def _api_ideas_delete(self, request):
        """DELETE /api/ideas/{id} — delete an idea."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        try:
            idea_id = request.match_info.get("id", "")
            pool = self._get_pg_pool()
            if not pool:
                return web.json_response({"error": "PG unavailable"}, status=503)
            await pool.execute("DELETE FROM mesh.mesh_ideas WHERE idea_id = $1", idea_id)
            return web.json_response({"ok": True})
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)

    async def _api_idea_comments(self, request):
        """GET /api/ideas/{id}/comments — list comments for an idea."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        try:
            idea_id = request.match_info.get("id", "")
            pool = self._get_pg_pool()
            if not pool:
                return web.json_response({"error": "PG unavailable", "comments": []}, status=503)
            # Ensure table exists
            await pool.execute(
                "CREATE TABLE IF NOT EXISTS mesh.mesh_idea_comments ("
                "id SERIAL PRIMARY KEY, idea_id VARCHAR(64) NOT NULL, "
                "author VARCHAR(100) NOT NULL, comment TEXT NOT NULL, "
                "created_at TIMESTAMPTZ DEFAULT NOW())"
            )
            rows = await pool.fetch(
                "SELECT id, idea_id, author, comment, created_at "
                "FROM mesh.mesh_idea_comments WHERE idea_id = $1 ORDER BY created_at ASC",
                idea_id
            )
            comments = []
            for r in rows:
                comments.append({
                    "id": r["id"],
                    "idea_id": r["idea_id"],
                    "author": r["author"],
                    "comment": r["comment"],
                    "created_at": r["created_at"].isoformat() if r["created_at"] else None,
                })
            return web.json_response({"comments": comments, "count": len(comments)})
        except Exception as e:
            return web.json_response({"error": str(e), "comments": []}, status=500)

    async def _api_idea_comment_add(self, request):
        """POST /api/ideas/{id}/comments — add a comment to an idea."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        try:
            idea_id = request.match_info.get("id", "")
            data = await request.json()
            comment_text = (data.get("comment") or "").strip()
            if not comment_text:
                return web.json_response({"error": "Comment text required"}, status=400)
            author = data.get("author") or getattr(user, 'username', None) or getattr(user, 'name', None) or "user"
            pool = self._get_pg_pool()
            if not pool:
                return web.json_response({"error": "PG unavailable"}, status=503)
            await pool.execute(
                "CREATE TABLE IF NOT EXISTS mesh.mesh_idea_comments ("
                "id SERIAL PRIMARY KEY, idea_id VARCHAR(64) NOT NULL, "
                "author VARCHAR(100) NOT NULL, comment TEXT NOT NULL, "
                "created_at TIMESTAMPTZ DEFAULT NOW())"
            )
            await pool.execute(
                "INSERT INTO mesh.mesh_idea_comments (idea_id, author, comment) VALUES ($1, $2, $3)",
                idea_id, author, comment_text
            )
            return web.json_response({"ok": True})
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)

    async def _api_idea_promote_agent(self, request):
        """POST /api/ideas/{id}/promote-agent — promote an approved idea to a mesh agent + Kanban card.
        When an idea gets enough votes (score >= threshold), promote it:
        1. Create a Kanban card for it
        2. Register the idea as a capability/agent in the mesh registry
        3. Notify all nodes about the new agent
        """
        from aiohttp import web
        import uuid as _uuid
        user, err = self._require_auth(request)
        if err:
            return err
        try:
            idea_id = request.match_info.get("id", "")
            pool = self._get_pg_pool()
            if not pool:
                return web.json_response({"error": "PG unavailable"}, status=503)
            # Get idea
            row = await pool.fetchrow(
                "SELECT idea_id, title, description, status, upvotes, downvotes, assigned_to, category "
                "FROM mesh.mesh_ideas WHERE idea_id = $1", idea_id
            )
            if not row:
                return web.json_response({"error": "Idea not found"}, status=404)
            score = row["upvotes"] - row["downvotes"]
            # Promote: update status to approved, create Kanban card, notify mesh
            # 1. Update status to approved
            await pool.execute(
                "UPDATE mesh.mesh_ideas SET status = 'approved', updated_at = NOW() WHERE idea_id = $1",
                idea_id
            )
            # 2. Create Kanban card
            kanban_mgr = getattr(self, '_get_kanban', None)
            if kanban_mgr:
                try:
                    mgr = self._get_kanban()
                    boards = mgr.get_boards()
                    if boards:
                        board_id = boards[0]["id"]
                        mgr.add_card(
                            board_id,
                            title="[" + row["category"] + "] " + row["title"],
                            column="todo",
                            description=row["description"] or "",
                            priority="medium",
                            assigned_to=row["assigned_to"] or "",
                        )
                except Exception as e:
                    log.warning(f"Failed to create Kanban card for idea {idea_id}: {e}")
            # 3. Notify mesh agents
            node = getattr(self, 'node', None) or getattr(self, '_node_ref', None) or self
            if hasattr(node, 'send_direct') or hasattr(node, 'broadcast'):
                try:
                    payload = {
                        "text": "Ötlet elfogadva és Kanban táblára helyezve: " + row["title"],
                        "subject": "Idea promoted: " + row["title"],
                        "idea_id": idea_id,
                        "score": score,
                        "category": row["category"],
                    }
                    if hasattr(node, 'broadcast'):
                        await node.broadcast("a2a_message", payload, priority=5)
                except Exception as e:
                    log.warning(f"Failed to broadcast idea promotion: {e}")
            # 4. EMBERI KAPU: az elfogadás NEM indít automatikus implementációt.
            # Az ötlet approved-ba kerül, a beépítés a „Beépítés jóváhagyása"
            # gombbal (implement endpoint) indul — Zsolt explicit döntése.
            return web.json_response({
                "ok": True,
                "idea_id": idea_id,
                "status": "approved",
                "score": score,
                "kanban_card_created": True,
                "mesh_notified": True,
                "awaiting_build_approval": True,
            })
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)

    async def _implement_idea_internal(self, row, idea_id: str):
        """Közös megvalósítás-logika: approved ötlet → P7 available delegáció + in_progress + Kanban sync.

        v0.42: A delegáció task_type='code_generation' a specifikációval —
        a végrehajtó node az a2a_mesh repóban dolgozik, generált kód
        auto-futtatással. A cél: az ötlet VALÓDI implementációja, nem csak elemzés.
        """
        node = getattr(self, 'node', None) or getattr(self, '_node_ref', None) or self
        delegation = getattr(node, 'delegation', None)
        if not delegation:
            return None
        assigned_to = row["assigned_to"] or ""
        import json as _json_desc
        desc = {
            "type": "code_generation",
            "language": "python",
            "language_hint": "python",
            "description": (
                f"A2A Mesh repó implementáció. Ötlet: {row['title']}\n\n"
                f"Kontextus: {(row['description'] or '')[:3000]}\n\n"
                "A munkakönyvtár az a2a_mesh git repó. A feladat az ötlet tényleges "
                "kód-implementációja: hozz létre vagy módosíts .py fájlokat a repóban, "
                "amik az ötlet funkcionalitását megvalósítják. Generálj futtatható, "
                "önálló Python kódot, ami a repó gyökeréből futtatható."
            ),
            "idea_id": idea_id,
            "source": "otletlada_implement",
            "repo": "a2a_mesh",
            "target": "mesh",
        }
        task_id = await delegation.delegate_task(
            to_agent=assigned_to or "any",
            subject=f"[ötletláda] {row['title']}"[:500],
            description=_json_desc.dumps(desc),
            task_type="code_generation",
            priority=7,
            available=not assigned_to,
        )
        # Ötlet → in_progress
        pool = self._get_pg_pool()
        if pool:
            await pool.execute(
                "UPDATE mesh.mesh_ideas SET status = 'in_progress', assigned_to = COALESCE(NULLIF($2, ''), assigned_to), updated_at = NOW() WHERE idea_id = $1",
                idea_id, assigned_to,
            )
        # Kanban kártya szinkron
        try:
            km = self._get_kanban()
            boards = km.get_boards()
            if boards:
                for b in boards:
                    for c in (b.get("cards") or []):
                        if row["title"][:40] in str(c.get("title", "")):
                            km.update_card(b["id"], c["id"], {
                                "column": "in_progress",
                                "assigned_to": assigned_to or c.get("assigned_to", ""),
                                "delegation_task_id": str(task_id),
                            })
                            break
        except Exception as e:
            log.warning(f"Kanban sync failed for idea {idea_id}: {e}")
        # Mesh értesítés
        try:
            if hasattr(node, 'broadcast'):
                await node.broadcast("a2a_message", {
                    "text": f"🔨 Ötlet megvalósítás indul: {row['title'][:80]} (delegáció {str(task_id)[:8]})",
                    "subject": f"Idea implement: {row['title'][:60]}",
                    "idea_id": idea_id,
                    "task_id": str(task_id),
                }, priority=7)
        except Exception as e:
            log.warning(f"Idea implement broadcast failed: {e}")
        return {"task_id": str(task_id), "assigned_to": assigned_to or "any"}

    async def _api_idea_implement(self, request):
        """POST /api/ideas/{id}/implement — Megvalósítás indítása: approved ötlet → in_progress + available delegáció.

        A koordinátor (ezen node, ha az) létrehozza a delegációt 'available' státusszal,
        így bármelyik agent claimelheti. Az ötlet in_progress-be kerül, a hozzá tartozó
        Kanban kártya (ha van) szinkronban frissül.
        """
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        try:
            idea_id = request.match_info.get("id", "")
            pool = self._get_pg_pool()
            if not pool:
                return web.json_response({"error": "PG unavailable"}, status=503)
            row = await pool.fetchrow(
                "SELECT idea_id, title, description, status, assigned_to, category, priority FROM mesh.mesh_ideas WHERE idea_id = $1",
                idea_id,
            )
            if not row:
                return web.json_response({"error": "Idea not found"}, status=404)
            if row["status"] not in ("approved", "idea"):
                return web.json_response({"error": f"Csak approved ötlet indítható (jelenleg: {row['status']})"}, status=400)

            result = await self._implement_idea_internal(row, idea_id)
            if not result:
                return web.json_response({"error": "Delegation manager nem elérhető"}, status=503)
            return web.json_response({
                "ok": True, "idea_id": idea_id, "status": "in_progress",
                "task_id": result["task_id"], "assigned_to": result["assigned_to"],
            })
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)

    async def _api_ideas_diagnostic_import(self, request):
        """POST /api/ideas/import-diagnostics — import pending diagnostic suggestions as ideas.
        Each diagnostic suggestion with status 'pending' becomes an idea in the board.
        """
        from aiohttp import web
        import uuid as _uuid
        user, err = self._require_auth(request)
        if err:
            return err
        try:
            diagnostics = getattr(self.node, 'diagnostics', None)
            if not diagnostics:
                return web.json_response({"error": "Diagnostics not available"}, status=503)
            pool = self._get_pg_pool()
            if not pool:
                return web.json_response({"error": "PG unavailable"}, status=503)
            # Get all suggestions (get_suggestions doesn't support status filter)
            suggestions = diagnostics.get_suggestions(limit=200)
            # Filter to pending only
            suggestions = [s for s in suggestions if s.status == "pending"]
            imported = []
            for s in suggestions:
                # Check if idea already exists with this title
                existing = await pool.fetchrow(
                    "SELECT idea_id FROM mesh.mesh_ideas WHERE title = $1 AND source_type = 'diagnostic'",
                    s.title
                )
                if existing:
                    continue
                idea_id = "idea_" + _uuid.uuid4().hex[:12]
                priority_map = {"critical": "high", "high": "high", "medium": "medium", "low": "low"}
                await pool.execute(
                    "INSERT INTO mesh.mesh_ideas (idea_id, title, description, category, priority, source_type, submitted_by, tags) "
                    "VALUES ($1, $2, $3, $4, $5, $6, $7, $8::text[])",
                    idea_id,
                    s.title,
                    (s.description or "") + "\n\nJelenlegi: " + str(s.current_value) + "\nJavasolt: " + str(s.suggested_value) + "\nIndoklás: " + str(s.rationale),
                    "diagnostic_" + (s.category or "general"),
                    priority_map.get(s.priority, "medium"),
                    "diagnostic",
                    s.node or "diagnostics",
                    ["diagnostic", s.category or "general"],
                )
                imported.append({"idea_id": idea_id, "title": s.title})
            return web.json_response({"ok": True, "imported": len(imported), "ideas": imported})
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)
    # ==================== PER-USER CHAT SYSTEM ====================

    async def _api_chat_send(self, request):
        """POST /api/chat/send — Send DM from dashboard user to agent."""
        from aiohttp import web
        from .dashboard_chat import handle_chat_send
        user, err = self._require_auth(request)
        if err: return err
        pool = getattr(self.node, "pg_pool", None) or getattr(self.node, "_pg_pool", None)
        if not pool:
            return web.json_response({"error": "DB not available"}, status=503)
        return await handle_chat_send(self.node, request, pool, user)

    async def _api_agent_dm(self, request):
        """POST /api/agent-dm — Agent-to-agent proactive DM via mesh.

        Body: { sender: "nova", recipient: "morzsa", content: "Hello", msg_type: "a2a_message" }
        Uses X-Mesh-Token for auth (mesh-internal).
        """
        from aiohttp import web
        # Auth: X-Mesh-Token OR user auth
        mesh_token = request.headers.get("X-Mesh-Token", "")
        if mesh_token != "mesh-wake-secret-2026":
            user, err = self._require_auth(request)
            if err: return err
        try:
            data = await request.json()
        except Exception:
            return web.json_response({"error": "invalid JSON"}, status=400)

        sender = data.get("sender", "").strip().lower()
        recipient = data.get("recipient", "").strip().lower()
        content = data.get("content", "").strip()
        msg_type = data.get("msg_type", "a2a_message")

        if not sender or not recipient or not content:
            return web.json_response({"error": "sender, recipient, content required"}, status=400)
        if sender == recipient:
            return web.json_response({"error": "cannot DM yourself"}, status=400)

        # Send via mesh direct
        payload = {
            "text": content,
            "subject": content[:80],
            "sender_display": sender,
            "chat_type": "agent_dm",
        }
        try:
            result = await self.node.send_direct(recipient, msg_type, payload, priority=5)
            if result.success:
                log.info(f"📩 Agent DM {sender}→{recipient}: sent via {result.transport}")
                return web.json_response({"ok": True, "transport": result.transport})
            else:
                log.warning(f"📩 Agent DM {sender}→{recipient} failed: {result.error}")
                return web.json_response({"ok": False, "error": result.error}, status=502)
        except Exception as e:
            log.error(f"Agent DM error: {e}", exc_info=True)
            return web.json_response({"error": str(e)}, status=500)

    async def _api_chat_messages(self, request):
        """GET /api/chat/messages?with=morzsa — Chat history with agent."""
        from aiohttp import web
        from .dashboard_chat import handle_chat_messages
        user, err = self._require_auth(request)
        if err: return err
        pool = getattr(self.node, "pg_pool", None) or getattr(self.node, "_pg_pool", None)
        if not pool:
            return web.json_response({"error": "DB not available"}, status=503)
        return await handle_chat_messages(self.node, request, pool, user)

    async def _api_chat_inbox(self, request):
        """GET /api/chat/inbox — Unread DMs for this user."""
        from aiohttp import web
        from .dashboard_chat import handle_chat_inbox
        user, err = self._require_auth(request)
        if err: return err
        pool = getattr(self.node, "pg_pool", None) or getattr(self.node, "_pg_pool", None)
        if not pool:
            return web.json_response({"error": "DB not available"}, status=503)
        return await handle_chat_inbox(self.node, request, pool, user)

    async def _api_chat_mark_read(self, request):
        """POST /api/chat/read — Mark agent messages as read."""
        from aiohttp import web
        from .dashboard_chat import handle_chat_mark_read
        user, err = self._require_auth(request)
        if err: return err
        pool = getattr(self.node, "pg_pool", None) or getattr(self.node, "_pg_pool", None)
        if not pool:
            return web.json_response({"error": "DB not available"}, status=503)
        return await handle_chat_mark_read(self.node, request, pool, user)

    async def _api_chat_contacts(self, request):
        """GET /api/chat/contacts — List agents with unread counts."""
        from aiohttp import web
        from .dashboard_chat import handle_chat_contacts
        user, err = self._require_auth(request)
        if err: return err
        pool = getattr(self.node, "pg_pool", None) or getattr(self.node, "_pg_pool", None)
        if not pool:
            return web.json_response({"error": "DB not available"}, status=503)
        return await handle_chat_contacts(self.node, request, pool, user)
