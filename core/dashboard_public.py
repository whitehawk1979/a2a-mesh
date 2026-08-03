"""A2A Mesh Dashboard — Public endpoints mixin.

Public health check and Prometheus metrics endpoints.
No authentication required — designed for monitoring systems.
"""

import time as _time
from aiohttp import web


class DashboardPublicMixin:
    """Public endpoints — health check and Prometheus metrics."""

    async def _api_public_health(self, request):
        """GET /api/health — Public health check, no auth required.
        Returns minimal JSON: node name, running, uptime, peers, version.
        """
        try:
            node = self.node
            pd = getattr(node, 'peer_discovery', None)
            peers = pd.get_stats() if pd else {}
            uptime = int(_time.time() - node._start_time) if getattr(node, '_start_time', None) else 0
            return web.json_response({
                "status": "healthy" if getattr(node, '_running', False) else "unhealthy",
                "node": getattr(node, 'node_name', 'unknown'),
                "running": getattr(node, '_running', False),
                "uptime": uptime,
                "peers": {
                    "known": peers.get('known_peers', 0),
                    "connected": peers.get('connected_peers', 0),
                    "available": peers.get('available_peers', 0),
                },
                "version": getattr(node, '_resolved_version', 'unknown'),
                "timestamp": int(_time.time()),
            })
        except Exception as e:
            return web.json_response({"status": "error", "error": str(e)}, status=500)

    async def _api_prometheus_metrics(self, request):
        """GET /metrics — Prometheus-compatible text format metrics.
        No auth required — designed for scraping by Prometheus/Grafana.
        """
        try:
            node = self.node
            node_name = getattr(node, 'node_name', 'unknown')
            router = getattr(node, 'router', None)
            t_stats = router.get_stats() if router else {}
            pd = getattr(node, 'peer_discovery', None)
            peers = pd.get_stats() if pd else {}
            uptime = int(_time.time() - node._start_time) if getattr(node, '_start_time', None) else 0
            running = 1 if getattr(node, '_running', False) else 0
            lines = [
                "# HELP a2a_mesh_node_up Node running status (1=up, 0=down)",
                "# TYPE a2a_mesh_node_up gauge",
                f'a2a_mesh_node_up{{node="{node_name}"}} {running}',
                "",
                "# HELP a2a_mesh_uptime_seconds Node uptime in seconds",
                "# TYPE a2a_mesh_uptime_seconds gauge",
                f'a2a_mesh_uptime_seconds{{node="{node_name}"}} {uptime}',
                "",
                "# HELP a2a_mesh_peers_connected Number of connected peers",
                "# TYPE a2a_mesh_peers_connected gauge",
                f'a2a_mesh_peers_connected{{node="{node_name}"}} {peers.get("connected_peers", 0)}',
                "",
                "# HELP a2a_mesh_peers_known Number of known peers",
                "# TYPE a2a_mesh_peers_known gauge",
                f'a2a_mesh_peers_known{{node="{node_name}"}} {peers.get("known_peers", 0)}',
                "",
                "# HELP a2a_mesh_messages_sent_total Total messages sent",
                "# TYPE a2a_mesh_messages_sent_total counter",
                f'a2a_mesh_messages_sent_total{{node="{node_name}"}} {t_stats.get("sent", 0)}',
                "",
                "# HELP a2a_mesh_messages_received_total Total messages received",
                "# TYPE a2a_mesh_messages_received_total counter",
                f'a2a_mesh_messages_received_total{{node="{node_name}"}} {t_stats.get("received", 0)}',
                "",
                "# HELP a2a_mesh_messages_forwarded_total Total messages forwarded",
                "# TYPE a2a_mesh_messages_forwarded_total counter",
                f'a2a_mesh_messages_forwarded_total{{node="{node_name}"}} {t_stats.get("forwarded", 0)}',
                "",
                "# HELP a2a_mesh_messages_duplicated_total Total duplicate messages filtered",
                "# TYPE a2a_mesh_messages_duplicated_total counter",
                f'a2a_mesh_messages_duplicated_total{{node="{node_name}"}} {t_stats.get("duplicates", 0)}',
                "",
                "# HELP a2a_mesh_transport_errors_total Total transport errors",
                "# TYPE a2a_mesh_transport_errors_total counter",
                f'a2a_mesh_transport_errors_total{{node="{node_name}"}} {t_stats.get("errors", 0)}',
                "",
                "# HELP a2a_mesh_dedup_cache_size Current dedup cache size",
                "# TYPE a2a_mesh_dedup_cache_size gauge",
                f'a2a_mesh_dedup_cache_size{{node="{node_name}"}} {t_stats.get("dedup", {}).get("size", 0)}',
                "",
            ]
            return web.Response(text="\n".join(lines), content_type="text/plain")
        except Exception as e:
            return web.Response(text=f"# Error: {e}", status=500, content_type="text/plain")