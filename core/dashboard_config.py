"""Config sync API — shared configuration across mesh nodes.

Stores non-node-specific settings in PG (mesh.mesh_shared_config).
Nodes can pull and apply these on startup or on-demand via POST /api/config/sync.
"""
import asyncio
import json as _json
import time as _time
import logging
log = logging.getLogger("a2a_mesh.dashboard.config")

from aiohttp import web


async def _ensure_table(pg_pool):
    await pg_pool.execute("""
        CREATE TABLE IF NOT EXISTS mesh.mesh_shared_config (
            key TEXT PRIMARY KEY,
            value JSONB NOT NULL,
            updated_at REAL NOT NULL,
            updated_by TEXT
        )
    """)


# ── Whitelist of syncable config keys ──
# Only these keys are synced — node-specific settings (node_name, ports, IPs) are NOT.
SYNCABLE_KEYS = {
    "delegation.expiry_minutes",
    "delegation.auto_renew",
    "delegation.cpu_threshold_p4",
    "delegation.cpu_threshold_p7",
    "discovery.static_nodes",
    "security.tls_enabled",
    "security.mtls_enabled",
    "security.hmac_enabled",
    "security.token_rotation_interval",
    "security.rate_limit_per_minute",
    "monitoring.alert_thresholds",
    "monitoring.dedup_cache_threshold",
    "monitoring.dedup_cleanup_interval",
    "heartbeat.interval",
    "heartbeat.timeout",
    "auto_update.enabled",
    "auto_update.check_interval",
    "auto_update.apply_automatically",
}


class ConfigSyncMixin:
    """Mixed into DashboardServer to provide config sync API endpoints."""

    async def _api_config_shared_get(self, request):
        """GET /api/config/shared — retrieve all shared config values."""
        user, err = self._require_auth(request)
        if err:
            return err
        try:
            pg_pool = getattr(self.node, "_pg_pool", None)
            if not pg_pool:
                return web.json_response({"error": "DB not available"}, status=503)
            await _ensure_table(pg_pool)

            rows = await pg_pool.fetch(
                "SELECT key, value, updated_at, updated_by FROM mesh.mesh_shared_config ORDER BY key"
            )
            config = {}
            for row in rows:
                config[row["key"]] = {
                    "value": row["value"] if isinstance(row["value"], (dict, list)) else _json.loads(row["value"]),
                    "updated_at": row["updated_at"],
                    "updated_by": row["updated_by"],
                }
            return web.json_response({"config": config, "count": len(config)})
        except Exception as e:
            log.error(f"Config shared get error: {e}", exc_info=True)
            return web.json_response({"error": str(e)}, status=500)

    async def _api_config_shared_set(self, request):
        """POST /api/config/shared — set one or more shared config values.

        Body: {"delegation.expiry_minutes": 120, "heartbeat.interval": 15}
        Only whitelisted keys are accepted.
        """
        user, err = self._require_auth(request)
        if err:
            return err
        try:
            body = await request.json()
            if not isinstance(body, dict) or not body:
                return web.json_response({"error": "JSON dict required"}, status=400)

            pg_pool = getattr(self.node, "_pg_pool", None)
            if not pg_pool:
                return web.json_response({"error": "DB not available"}, status=503)
            await _ensure_table(pg_pool)

            now = _time.time()
            updated_by = user.username if user else "unknown"
            accepted = {}
            rejected = {}
            for key, value in body.items():
                if key in SYNCABLE_KEYS:
                    await pg_pool.execute(
                        """INSERT INTO mesh.mesh_shared_config (key, value, updated_at, updated_by)
                           VALUES ($1, $2, $3, $4)
                           ON CONFLICT (key) DO UPDATE
                           SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at, updated_by = EXCLUDED.updated_by""",
                        key, _json.dumps(value), now, updated_by,
                    )
                    accepted[key] = value
                    log.info(f"⚙️  Shared config set: {key}={value} by {updated_by}")
                else:
                    rejected[key] = "not in syncable whitelist"

            return web.json_response({
                "accepted": accepted,
                "rejected": rejected,
                "updated_by": updated_by,
            })
        except Exception as e:
            log.error(f"Config shared set error: {e}", exc_info=True)
            return web.json_response({"error": str(e)}, status=500)

    async def _api_config_sync(self, request):
        """POST /api/config/sync — pull shared config from PG and apply locally.

        Applies whitelisted shared config values to the running node.
        Does NOT modify node-specific settings (node_name, ports, IPs).
        """
        user, err = self._require_auth(request)
        if err:
            return err
        try:
            pg_pool = getattr(self.node, "_pg_pool", None)
            if not pg_pool:
                return web.json_response({"error": "DB not available"}, status=503)
            await _ensure_table(pg_pool)

            rows = await pg_pool.fetch(
                "SELECT key, value FROM mesh.mesh_shared_config"
            )
            if not rows:
                return web.json_response({"applied": 0, "message": "No shared config found"})

            applied = {}
            skipped = {}
            node = self.node

            for row in rows:
                key = row["key"]
                value = row["value"] if isinstance(row["value"], (dict, list)) else _json.loads(row["value"])

                # Apply to running node based on key
                try:
                    if key == "delegation.expiry_minutes":
                        if hasattr(node, "delegation") and hasattr(node.delegation, "default_expiry_minutes"):
                            node.delegation.default_expiry_minutes = int(value)
                        applied[key] = value
                    elif key == "delegation.auto_renew":
                        if hasattr(node, "delegation"):
                            node.delegation.auto_renew_enabled = bool(value)
                        applied[key] = value
                    elif key == "delegation.cpu_threshold_p4":
                        if hasattr(node, "delegation"):
                            node.delegation.cpu_threshold_p4 = int(value)
                        applied[key] = value
                    elif key == "delegation.cpu_threshold_p7":
                        if hasattr(node, "delegation"):
                            node.delegation.cpu_threshold_p7 = int(value)
                        applied[key] = value
                    elif key == "heartbeat.interval":
                        if hasattr(node, "_heartbeat_interval"):
                            node._heartbeat_interval = int(value)
                        applied[key] = value
                    elif key == "heartbeat.timeout":
                        if hasattr(node, "_heartbeat_timeout"):
                            node._heartbeat_timeout = int(value)
                        applied[key] = value
                    elif key == "monitoring.dedup_cache_threshold":
                        if hasattr(node, "dedup"):
                            node.dedup.threshold = int(value)
                        applied[key] = value
                    elif key == "monitoring.dedup_cleanup_interval":
                        if hasattr(node, "dedup"):
                            node.dedup.cleanup_interval = int(value)
                        applied[key] = value
                    elif key == "security.rate_limit_per_minute":
                        if hasattr(node, "message_auth"):
                            node.message_auth.rate_limit = int(value)
                        applied[key] = value
                    elif key == "security.token_rotation_interval":
                        if hasattr(node, "message_auth"):
                            node.message_auth.rotation_interval = int(value)
                        applied[key] = value
                    elif key == "auto_update.enabled":
                        # Auto-update loop indítása/leállítása élőben (v0.48.2)
                        if hasattr(node, "config"):
                            node.config.auto_update.enabled = bool(value)
                        if bool(value) and not any(
                            getattr(t, "get_name", lambda: "")() == "auto-update-loop"
                            for t in getattr(node, "_tasks", [])
                            if hasattr(t, "get_name")
                        ):
                            try:
                                interval = int(getattr(node.config.auto_update, "check_interval", 300))
                                t = asyncio.create_task(node._auto_update_loop(interval))
                                t.set_name("auto-update-loop")
                                node._tasks.append(t)
                                log.info("🔄 Auto-update loop ELINDÍTVA config-sync által")
                            except Exception as ae:
                                log.warning(f"Auto-update loop start failed: {ae}")
                        applied[key] = value
                    elif key == "auto_update.check_interval":
                        if hasattr(node, "config"):
                            node.config.auto_update.check_interval = int(value)
                        applied[key] = value
                    elif key == "auto_update.apply_automatically":
                        # A valódi frissítés-kapcsoló: élőben be/ki (v0.48.2)
                        if hasattr(node, "config"):
                            node.config.auto_update.apply_automatically = bool(value)
                        applied[key] = value
                    else:
                        skipped[key] = "no local handler for this key"
                except Exception as e:
                    skipped[key] = f"apply error: {e}"

            log.info(f"⚙️  Config sync: {len(applied)} applied, {len(skipped)} skipped")
            return web.json_response({
                "applied": applied,
                "skipped": skipped,
                "total": len(rows),
            })
        except Exception as e:
            log.error(f"Config sync error: {e}", exc_info=True)
            return web.json_response({"error": str(e)}, status=500)

    # ── Transport kézi beállítások (v0.48.3) ─────────────────────────────
    # GET /api/config/transports — élő transport státusz + beállítható mezők
    async def _api_config_transports_get(self, request):
        """Élő transport-állapot + config-értékek a Settings panelhez."""
        user, err = self._require_auth(request)
        if err:
            return err
        try:
            from aiohttp import web
            node = self.node
            cfg = getattr(node, "config", None)
            result = {"node": getattr(node, "node_name", "?"), "transports": {}}

            # ── P2P ──
            p2p = getattr(node, "_p2p_transport", None)
            p2p_status = "disabled"
            if p2p:
                try:
                    st = p2p.status() if hasattr(p2p, "status") else {}
                    p2p_status = st.get("status", st.get("state", "unknown")) if isinstance(st, dict) else str(st)
                except Exception:
                    p2p_status = "unknown"
            result["transports"]["p2p"] = {
                "enabled": bool(cfg.p2p.enabled) if cfg else True,
                "listen_port": cfg.p2p.listen_port if cfg else 8645,
                "listen_host": cfg.p2p.listen_host if cfg else "0.0.0.0",
                "advertise_host": (cfg.p2p.advertise_host if cfg else "") or "",
                "status": p2p_status,
                "connected_peers": len(getattr(p2p, "_peers", {}) or {}) if p2p else 0,
            }

            # ── MQTT ──
            mq = getattr(node, "_mqtt_transport", None)
            mqtt_connected = bool(mq and getattr(mq, "_connected", False))
            result["transports"]["mqtt"] = {
                "enabled": bool(cfg.mqtt.enabled) if cfg else False,
                "host": cfg.mqtt.host if cfg else "127.0.0.1",
                "port": cfg.mqtt.port if cfg else 8683,
                "keepalive": cfg.mqtt.keepalive if cfg else 30,
                "status": "connected" if mqtt_connected else ("enabled" if (cfg and cfg.mqtt.enabled) else "disabled"),
            }

            # ── SSH tunnel ──
            tun = getattr(node, "_ssh_tunnel_transport", None)
            peers_info = {}
            if tun:
                try:
                    for name, peer in (getattr(tun, "_tunnels", {}) or {}).items():
                        peers_info[name] = {
                            "connected": bool(getattr(peer, "connected", False)),
                            "ssh_host": getattr(peer, "ssh_host", ""),
                            "ssh_port": getattr(peer, "ssh_port", 22),
                            "local_port": getattr(peer, "local_port", 0),
                            "remote_port": getattr(peer, "remote_port", 8645),
                        }
                except Exception:
                    pass
            result["transports"]["ssh_tunnel"] = {
                "enabled": bool(cfg.ssh_tunnel.enabled) if cfg else False,
                "status": "active" if peers_info else "idle",
                "peers": peers_info,
            }

            # ── PG ──
            pg = getattr(node, "_pg_pool", None)
            pg_ok = bool(pg and getattr(pg, "is_connected", lambda: False)())
            result["transports"]["pg"] = {
                "enabled": True,
                "host": cfg.pg.host if cfg else "",
                "port": cfg.pg.port if cfg else 5432,
                "dbname": cfg.pg.dbname if cfg else "agent_memory",
                "status": "connected" if pg_ok else "disconnected",
            }

            # ── HTTP relay ──
            result["transports"]["http"] = {
                "enabled": bool(cfg.http.url) if cfg else False,
                "url": cfg.http.url if cfg else "",
                "health_url": cfg.http.health_url if cfg else "",
                "timeout": cfg.http.timeout if cfg else 5,
                "status": "configured",
            }

            # ── Transport priority sorrend ──
            result["transport_priority"] = list(cfg.transport_priority) if cfg else []

            return web.json_response(result)
        except Exception as e:
            log.error(f"Transports get error: {e}", exc_info=True)
            return web.json_response({"error": str(e)}, status=500)

    async def _api_config_transport_test(self, request):
        """POST /api/config/transports/test — adott transport ÉLŐ tesztje.
        Body: {"transport": "mqtt", "host": "192.168.1.8", "port": 8683}
              {"transport": "pg", "host": ..., "port": ..., "user": ..., "password": ..., "dbname": ...}
              {"transport": "ssh_tunnel", "host": ..., "port": ..., "user": ...}
              {"transport": "http", "url": ...}
              {"transport": "p2p", "listen_port": ...} (local bind test)
        Determinista probe — nincs LLM, csak hálózati próbálkozás rövid timeouttal.
        """
        user, err = self._require_auth(request)
        if err:
            return err
        try:
            from aiohttp import web
            import socket
            body = await request.json()
            t = body.get("transport", "")
            host = str(body.get("host", ""))
            port = int(body.get("port", 0))
            result = {"transport": t, "ok": False, "detail": ""}

            if t in ("mqtt", "ssh_tunnel", "p2p", "pg"):
                # TCP connect probe — biztonságos, gyors (3s timeout)
                if not host or not port:
                    return web.json_response({"error": "host és port kötelező"}, status=400)
                try:
                    sock = socket.create_connection((host, port), timeout=3)
                    sock.close()
                    result["ok"] = True
                    result["detail"] = f"TCP {host}:{port} elérhető"
                except Exception as e:
                    result["detail"] = f"TCP {host}:{port} SIKERTELEN: {e}"
            elif t == "pg":
                # Teljes PG connect próbálkozás (asyncpg-vel)
                if not host or not port:
                    return web.json_response({"error": "host és port kötelező"}, status=400)
                import asyncpg
                try:
                    conn = await asyncio.wait_for(
                        asyncpg.connect(
                            host=host, port=port,
                            user=body.get("user", "nova"),
                            password=body.get("password", ""),
                            database=body.get("dbname", "agent_memory"),
                            timeout=5,
                        ), timeout=6)
                    ver = await conn.fetchval("SELECT version()")
                    await conn.close()
                    result["ok"] = True
                    result["detail"] = f"PG connect OK: {str(ver)[:60]}"
                except Exception as e:
                    result["detail"] = f"PG connect SIKERTELEN: {e}"
            elif t == "http":
                url = str(body.get("url", ""))
                if not url:
                    return web.json_response({"error": "url kötelező"}, status=400)
                try:
                    import aiohttp
                    async with aiohttp.ClientSession() as sess:
                        async with sess.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                            result["ok"] = resp.status < 500
                            result["detail"] = f"HTTP {resp.status} — {url[:70]}"
                except Exception as e:
                    result["detail"] = f"HTTP SIKERTELEN: {e}"
            else:
                return web.json_response({"error": f"Ismeretlen transport: {t}"}, status=400)

            return web.json_response(result)
        except Exception as e:
            log.error(f"Transport test error: {e}", exc_info=True)
            return web.json_response({"error": str(e)}, status=500)

    async def _api_config_transport_set(self, request):
        """POST /api/config/transports — transport beállítás élő mentése (v0.48.3).

        Body: {"transport": "mqtt", "values": {"host": "192.168.1.8", "port": 8683, "enabled": true}}
        Hatás: futó config frissítése + mesh_config_{node}.yaml persist + MQTT-nél élő reconnect.
        """
        user, err = self._require_auth(request)
        if err:
            return err
        # Csak admin/owner módosíthat transportot
        if user and getattr(user, "role", "") not in ("admin", "owner"):
            return web.json_response({"error": "Admin jog szükséges"}, status=403)
        try:
            from aiohttp import web
            node = self.node
            body = await request.json()
            t = body.get("transport", "")
            values = body.get("values", {})
            if not t or not isinstance(values, dict):
                return web.json_response({"error": "transport és values (dict) kötelező"}, status=400)

            cfg = getattr(node, "config", None)
            if cfg is None:
                return web.json_response({"error": "Config nem elérhető"}, status=503)

            applied = {}
            restart_needed = False

            if t == "mqtt" and hasattr(cfg, "mqtt"):
                for k, v in values.items():
                    if k == "enabled":
                        cfg.mqtt.enabled = bool(v); applied[k] = bool(v)
                    elif k == "host":
                        cfg.mqtt.host = str(v); applied[k] = str(v)
                    elif k == "port":
                        cfg.mqtt.port = int(v); applied[k] = int(v)
                    elif k == "keepalive":
                        cfg.mqtt.keepalive = int(v); applied[k] = int(v)
                restart_needed = True
            elif t == "p2p" and hasattr(cfg, "p2p"):
                for k, v in values.items():
                    if k == "enabled":
                        cfg.p2p.enabled = bool(v); applied[k] = bool(v)
                    elif k == "listen_port":
                        cfg.p2p.listen_port = int(v); applied[k] = int(v)
                    elif k == "listen_host":
                        cfg.p2p.listen_host = str(v); applied[k] = str(v)
                    elif k == "advertise_host":
                        cfg.p2p.advertise_host = str(v); applied[k] = str(v)
                restart_needed = True
            elif t == "pg" and hasattr(cfg, "pg"):
                for k, v in values.items():
                    if k == "host":
                        cfg.pg.host = str(v); applied[k] = str(v)
                    elif k == "port":
                        cfg.pg.port = int(v); applied[k] = int(v)
                    elif k == "dbname":
                        cfg.pg.dbname = str(v); applied[k] = str(v)
                    elif k == "user":
                        cfg.pg.user = str(v); applied[k] = str(v)
                restart_needed = True
            elif t == "ssh_tunnel" and hasattr(cfg, "ssh_tunnel"):
                for k, v in values.items():
                    if k == "enabled":
                        cfg.ssh_tunnel.enabled = bool(v); applied[k] = bool(v)
                restart_needed = True
            elif t == "http" and hasattr(cfg, "http"):
                for k, v in values.items():
                    if k == "url":
                        cfg.http.url = str(v); applied[k] = str(v)
                    elif k == "health_url":
                        cfg.http.health_url = str(v); applied[k] = str(v)
                    elif k == "timeout":
                        cfg.http.timeout = int(v); applied[k] = int(v)
            else:
                return web.json_response({"error": f"Ismeretlen transport: {t}"}, status=400)

            # ── Persist: mesh_config_{node}.yaml frissítése ──
            saved_yaml = ""
            try:
                import os as _os, yaml as _yaml
                ypath = _os.path.expanduser(f"~/.hermes/scripts/a2a_mesh/mesh_config_{node.node_name}.yaml")
                if not _os.path.exists(ypath):
                    ypath = _os.path.expanduser("~/.hermes/scripts/a2a_mesh/mesh_config.yaml")
                if _os.path.exists(ypath):
                    with open(ypath) as f:
                        ydata = _yaml.safe_load(f) or {}
                    ysec = ydata.setdefault(t, {}) if t != "ssh_tunnel" else ydata.setdefault("ssh_tunnel", {})
                    for k, v in applied.items():
                        ysec[k] = v
                    with open(ypath, "w") as f:
                        _yaml.safe_dump(ydata, f, default_flow_style=False, allow_unicode=True)
                    saved_yaml = ypath
            except Exception as ye:
                log.warning(f"Transport yaml persist failed: {ye}")

            # ── MQTT élő reconnect, ha az MQTT-et állítottuk ──
            mqtt_reconnected = False
            if t == "mqtt":
                try:
                    mq = getattr(node, "_mqtt_transport", None)
                    if mq:
                        if hasattr(mq, "stop"):
                            try:
                                await asyncio.wait_for(mq.stop(), timeout=5)
                            except Exception:
                                pass
                        # host/port frissítése a configból
                        if hasattr(mq, "_host"):
                            mq._host = cfg.mqtt.host
                            mq._port = cfg.mqtt.port
                            mq._enabled = cfg.mqtt.enabled
                        if cfg.mqtt.enabled and hasattr(mq, "start"):
                            await mq.start()
                            mqtt_reconnected = bool(getattr(mq, "_connected", False))
                except Exception as re_err:
                    log.warning(f"MQTT reconnect failed: {re_err}")

            return web.json_response({
                "transport": t,
                "applied": applied,
                "saved_yaml": saved_yaml,
                "mqtt_reconnected": mqtt_reconnected,
                "restart_needed": restart_needed and not mqtt_reconnected,
                "message": "Beállítások élőben alkalmazva" if not restart_needed else
                           "Beállítások mentve — teljes érvényesítéshez node restart",
            })
        except Exception as e:
            log.error(f"Transport set error: {e}", exc_info=True)
            return web.json_response({"error": str(e)}, status=500)


    # ── Auto-discovery (v0.48.3): node-ok felismerése a Settings panelhez ──
    async def _api_discovery_scan(self, request):
        """POST /api/config/discovery/scan — determinisztikus node-felderítés.

        Források: (1) PG mesh_nodes (SSOT), (2) MQTT presence retained
        topicok, (3) meglévő peers. Minden találatra TCP health-probe
        (p2p_port + health_port) — az eredmény közvetlenül behúzható a
        transport-beállításokba (host/port javaslat).
        """
        from aiohttp import web
        import asyncio as _aio
        user, err = self._require_auth(request)
        if err:
            return err
        try:
            node = self.node
            node_name = getattr(node, "node_name", "?")
            found = {}

            # 1) PG registry (SSOT)
            pg_pool = getattr(node, "_pg_pool", None)
            if pg_pool and pg_pool.is_connected():
                try:
                    rows = await pg_pool.fetch(
                        """SELECT node_name, host, p2p_port, health_port, transport_info
                           FROM mesh.mesh_nodes
                           WHERE last_heartbeat > NOW() - INTERVAL '10 minutes'"""
                    )
                    for r in rows:
                        n = r["node_name"]
                        if n == node_name:
                            continue
                        ti = {}
                        try:
                            import json as _j
                            ti = _j.loads(r["transport_info"]) if r["transport_info"] else {}
                        except Exception:
                            pass
                        hosts = []
                        if r["host"]:
                            hosts.append(r["host"])
                        # transport_info ssh_hosts lista (élő dial-címek)
                        for h in (ti.get("ssh_hosts") or [])[:3]:
                            if h and h not in hosts:
                                hosts.append(h)
                        found[n] = {
                            "name": n,
                            "hosts": hosts,
                            "p2p_port": int(r["p2p_port"] or 8645),
                            "health_port": int(r["health_port"] or 8650),
                            "source": "pg",
                        }
                except Exception as pe:
                    log.warning(f"Discovery PG scan failed: {pe}")

            # 2) MQTT presence: a2a/nodes/+/status retained (retained payload
            #    offline/online), a host nem szerepel benne — csak a jelenlét
            #    megerősítése; kihagyjuk, a PG a címforrás.
            # 3) Health-probe minden találatra (párhuzamos, 2s timeout)
            async def _probe(entry):
                host_ok = None
                for h in entry["hosts"][:3]:
                    try:
                        reader, writer = await _aio.wait_for(
                            _aio.open_connection(h, entry["health_port"]), timeout=2)
                        writer.close()
                        host_ok = h
                        break
                    except Exception:
                        continue
                entry["reachable_host"] = host_ok
                entry["status"] = "online" if host_ok else "unreachable"

            await _aio.gather(*[_probe(e) for e in found.values()])
            return web.json_response({
                "ok": True,
                "node": node_name,
                "discovered": list(found.values()),
                "count": len(found),
            })
        except Exception as e:
            log.error(f"Discovery scan error: {e}", exc_info=True)
            return web.json_response({"error": str(e)}, status=500)
