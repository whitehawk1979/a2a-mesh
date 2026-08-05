"""Config sync API — shared configuration across mesh nodes.

Stores non-node-specific settings in PG (mesh.mesh_shared_config).
Nodes can pull and apply these on startup or on-demand via POST /api/config/sync.
"""
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