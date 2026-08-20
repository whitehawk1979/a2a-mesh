"""A2A Mesh Plugin — MCP Registry Auto-Advertiser.

Publishes the node's MCP server configuration to the shared_a2a_memory
table on startup and periodically refreshes it, so other nodes can
discover and install MCP servers via the dashboard MCP Registry page.

The published payload (memory_type='mcp_registry') contains:
  {host: str, servers: [{name, enabled, transport, url, command, args, env_keys, has_credentials}]}

Inspired by skill_advertiser_plugin.py's periodic refresh pattern.
"""

import asyncio
import json
import logging
import os
from typing import Any, Dict, List

from a2a_mesh.core.plugin_base import MeshPlugin

log = logging.getLogger("a2a_mesh.plugins.mcp_advertiser")


class McpAdvertiserPlugin(MeshPlugin):
    """Publishes local MCP server configs to the mesh registry."""

    name = "mcp_advertiser"
    version = "1.0.0"

    def __init__(self):
        super().__init__()
        self._refresh_task = None
        self._refresh_interval = 300  # 5 minutes

    async def on_start(self, node):
        """Publish MCP servers on startup (with retry for PG pool readiness)."""
        self.log.info(f"McpAdvertiser plugin starting on {node.node_name}")
        # Delay publish slightly to allow PG pool to connect
        self._refresh_task = asyncio.create_task(self._delayed_start(node))

    async def on_stop(self, node):
        """Clean up on shutdown."""
        if self._refresh_task:
            self._refresh_task.cancel()
            try:
                await self._refresh_task
            except asyncio.CancelledError:
                pass

    async def _delayed_start(self, node):
        """Wait for PG pool to be ready, then publish + start refresh loop."""
        for attempt in range(10):  # 10 attempts, 3s each = 30s max
            await asyncio.sleep(3)
            pg_pool = getattr(node, "_pg_pool", None) or getattr(node, "pg_pool", None)
            if pg_pool and (not hasattr(pg_pool, 'is_closed') or not pg_pool.is_closed()):
                try:
                    await self._publish_mcp_registry(node)
                    break
                except Exception as e:
                    self.log.warning(f"Publish attempt {attempt+1} failed: {e}")
            else:
                self.log.debug(f"Waiting for PG pool (attempt {attempt+1}/10)")
        else:
            self.log.warning("PG pool not ready after 30s — MCP registry will retry on refresh")
        # Start periodic refresh
        await self._refresh_loop(node)

    async def _refresh_loop(self, node):
        """Periodically re-publish MCP configs."""
        while True:
            await asyncio.sleep(self._refresh_interval)
            try:
                await self._publish_mcp_registry(node)
            except Exception as e:
                self.log.warning(f"MCP registry refresh failed: {e}")

    def _collect_local_mcp_servers(self, node) -> List[Dict[str, Any]]:
        """Read MCP server configs from ~/.hermes/config.yaml."""
        servers = []
        config_path = os.path.expanduser("~/.hermes/config.yaml")
        if not os.path.exists(config_path):
            return servers
        try:
            import yaml
            with open(config_path) as f:
                cfg = yaml.safe_load(f) or {}
            mcp_servers = cfg.get("mcp_servers", {}) or {}
            for name, conf in mcp_servers.items():
                if not isinstance(conf, dict):
                    continue
                env = conf.get("env", {})
                headers = conf.get("headers", {})
                servers.append({
                    "name": name,
                    "enabled": conf.get("enabled", True),
                    "transport": "streamable_http" if "url" in conf else "stdio",
                    "url": conf.get("url", ""),
                    "command": conf.get("command", ""),
                    "args": conf.get("args", []) if isinstance(conf.get("args"), list) else str(conf.get("args", "")),
                    "env_keys": list(env.keys()) if isinstance(env, dict) else [],
                    "has_credentials": bool(env or headers),
                })
        except Exception as e:
            self.log.warning(f"Failed to read MCP config: {e}")
        return servers

    async def _publish_mcp_registry(self, node):
        """Publish MCP server list to shared_a2a_memory for cross-node discovery."""
        pg_pool = getattr(node, "_pg_pool", None) or getattr(node, "pg_pool", None)
        if not pg_pool:
            self.log.warning("No PG pool — cannot publish MCP registry")
            return

        servers = self._collect_local_mcp_servers(node)
        if not servers:
            self.log.debug("No MCP servers to publish")
            return

        node_name = node.node_name
        host = getattr(getattr(node, "config", None), "listen_host", "") or ""

        payload = json.dumps({
            "host": host,
            "servers": servers,
        })

        try:
            # Upsert into shared_a2a_memory (memory_type must be 'observation' — CHECK constraint)
            # Delete old entries from this node first, then insert fresh
            await pg_pool.execute(
                "DELETE FROM shared_a2a_memory WHERE sender_agent = $1 AND subject = 'mcp_registry'",
                node_name,
            )
            await pg_pool.execute(
                """INSERT INTO shared_a2a_memory
                   (sender_agent, recipient_agent, memory_type, subject, content, priority, status, created_at)
                   VALUES ($1, 'any', 'observation', 'mcp_registry', $2, 1, 'sent', NOW())""",
                node_name, payload,
            )
            self.log.info(f"Published {len(servers)} MCP servers to mesh registry")
        except Exception as e:
            self.log.warning(f"Failed to publish MCP registry: {e}")