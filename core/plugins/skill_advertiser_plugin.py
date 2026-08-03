"""A2A Mesh Plugin — Skill Auto-Advertiser Example.

This plugin demonstrates the skill marketplace integration:
- Advertises a skill on startup via the mesh_skills API
- Periodically refreshes the advertisement
- Shows how plugins can participate in the skill marketplace

Usage:
    Drop this file in core/plugins/ and restart the node.
    The skill 'data_analysis' will be automatically advertised.
"""

import asyncio
import logging
import time

from ..plugin_base import MeshPlugin

log = logging.getLogger("a2a_mesh.plugins.skill_advertiser")


class SkillAdvertiserPlugin(MeshPlugin):
    """Advertises a data_analysis skill on the mesh marketplace."""

    name = "skill_advertiser"
    version = "1.0.0"

    def __init__(self):
        super().__init__()
        self._refresh_task = None
        self._skill = {
            "skill_name": "data_analysis",
            "display_name": "Data Analysis",
            "description": "Analyze data sets and produce summary statistics, charts, and insights",
            "tags": ["data", "analysis", "statistics", "charts", "pandas"],
            "cost": 0.0,
            "max_concurrent": 2,
        }

    async def on_start(self, node):
        """Advertise skill on startup."""
        self.log.info(f"SkillAdvertiser plugin starting on {node.node_name}")
        await self._advertise_skill(node)
        # Refresh every 5 minutes
        self._refresh_task = asyncio.create_task(self._refresh_loop(node))

    async def on_stop(self, node):
        """Clean up on shutdown."""
        if self._refresh_task:
            self._refresh_task.cancel()
            try:
                await self._refresh_task
            except asyncio.CancelledError:
                pass

    async def _refresh_loop(self, node):
        """Periodically refresh the skill advertisement."""
        while True:
            await asyncio.sleep(300)  # 5 minutes
            try:
                await self._advertise_skill(node)
            except Exception as e:
                self.log.warning(f"Skill refresh failed: {e}")

    async def _advertise_skill(self, node):
        """Advertise the skill via the mesh_skills DB table."""
        pg_pool = getattr(node, '_pg_pool', None)
        if not pg_pool:
            self.log.warning("No PG pool — cannot advertise skill")
            return

        skill_id = f"skill-{node.node_name}-{self._skill['skill_name']}"
        try:
            await pg_pool.execute(
                """INSERT INTO mesh.mesh_skills
                   (skill_id, agent_name, skill_name, display_name,
                    description, tags, cost, max_concurrent, status, updated_at)
                   VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'active', NOW())
                   ON CONFLICT (skill_id)
                   DO UPDATE SET display_name=$4, description=$5, tags=$6,
                                 cost=$7, max_concurrent=$8, status='active',
                                 updated_at=NOW()""",
                skill_id,
                node.node_name,
                self._skill["skill_name"],
                self._skill["display_name"],
                self._skill["description"],
                self._skill["tags"],
                self._skill["cost"],
                self._skill["max_concurrent"],
            )
            self.log.info(f"Skill advertised: {skill_id}")
        except Exception as e:
            self.log.error(f"Failed to advertise skill: {e}")

    async def on_message_received(self, message):
        """Handle incoming messages — respond to data_analysis requests."""
        if hasattr(message, 'content') and isinstance(message.content, dict):
            skill = message.content.get("skill")
            if skill == "data_analysis":
                self.log.info(f"Data analysis request from {message.sender}")
                # In a real plugin, this would do actual analysis
                # For now, just log it