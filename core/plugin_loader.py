"""A2A Mesh Plugin Loader — Dynamic plugin discovery and management.

Discovers, loads, and manages plugins from:
1. Built-in plugins directory: core/plugins/*.py
2. External plugins directory: ~/.hermes/mesh_plugins/*.py
3. Config-specified plugins in mesh_config.yaml

Plugin loading order:
1. Built-in plugins (core/plugins/)
2. External plugins (~/.hermes/mesh_plugins/)
3. Config-specified plugins (mesh_config.yaml)

Each plugin is validated, configured, and registered with the mesh node.
"""

import importlib
import importlib.util
import inspect
import logging
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional, Type

from .plugin_base import MeshPlugin, PluginInfo

log = logging.getLogger("a2a_mesh.plugin_loader")

# Built-in plugins directory (relative to this file)
BUILTIN_PLUGINS_DIR = os.path.join(os.path.dirname(__file__), "plugins")
# External plugins directory (user-configurable)
EXTERNAL_PLUGINS_DIR = os.path.expanduser("~/.hermes/mesh_plugins")


class PluginLoader:
    """Discovers, loads, and manages mesh plugins.

    Usage:
        loader = PluginLoader(node)
        await loader.load_all()
        # ... node runs ...
        await loader.stop_all()
    """

    def __init__(self, node):
        self.node = node
        self.plugins: Dict[str, MeshPlugin] = {}
        self._loaded_modules = []
        self.log = logging.getLogger("a2a_mesh.plugin_loader")

    def discover_plugins(self) -> List[str]:
        """Discover all available plugin files.

        Returns:
            List of plugin file paths (built-in + external)
        """
        plugin_files = []

        # 1. Built-in plugins
        if os.path.isdir(BUILTIN_PLUGINS_DIR):
            for f in sorted(os.listdir(BUILTIN_PLUGINS_DIR)):
                if f.endswith("_plugin.py") and not f.startswith("_"):
                    plugin_files.append(os.path.join(BUILTIN_PLUGINS_DIR, f))

        # 2. External plugins
        if os.path.isdir(EXTERNAL_PLUGINS_DIR):
            for f in sorted(os.listdir(EXTERNAL_PLUGINS_DIR)):
                if f.endswith("_plugin.py") and not f.startswith("_"):
                    plugin_files.append(os.path.join(EXTERNAL_PLUGINS_DIR, f))

        self.log.info(f"Discovered {len(plugin_files)} plugin files")
        return plugin_files

    def load_plugin_from_file(self, filepath: str) -> Optional[MeshPlugin]:
        """Load a single plugin from a Python file.

        The file must contain a class that inherits from MeshPlugin.
        The first such class found will be instantiated.
        """
        module_name = f"a2a_mesh_plugin_{Path(filepath).stem}"

        try:
            # Ensure the parent package is available for absolute imports in plugins
            # This allows plugins to use: from a2a_mesh.core.plugin_base import MeshPlugin
            if "a2a_mesh.core.plugin_base" not in sys.modules:
                # Force import — plugin_loader.py is already inside a2a_mesh.core,
                # so this import will work at module level
                from .plugin_base import MeshPlugin as _MeshPlugin
                sys.modules["a2a_mesh.core.plugin_base"] = sys.modules.get(
                    "a2a_mesh.core.plugin_base",
                    __import__("a2a_mesh.core.plugin_base", fromlist=["MeshPlugin"])
                )

            spec = importlib.util.spec_from_file_location(module_name, filepath)
            if spec is None or spec.loader is None:
                self.log.error(f"Cannot load plugin spec from {filepath}")
                return None

            module = importlib.util.module_from_spec(spec)
            sys.modules[module_name] = module
            spec.loader.exec_module(module)
            self._loaded_modules.append(module_name)

            # Find the first MeshPlugin subclass
            for attr_name in dir(module):
                attr = getattr(module, attr_name)
                if (inspect.isclass(attr)
                        and issubclass(attr, MeshPlugin)
                        and attr is not MeshPlugin):
                    plugin = attr()
                    self.log.info(f"Loaded plugin '{plugin.name}' v{plugin.version} from {filepath}")
                    return plugin

            self.log.warning(f"No MeshPlugin subclass found in {filepath}")
            return None

        except Exception as e:
            self.log.error(f"Failed to load plugin from {filepath}: {e}")
            import traceback
            self.log.debug(traceback.format_exc())
            return None

    async def load_all(self, config: Optional[Dict] = None) -> Dict[str, MeshPlugin]:
        """Discover, load, configure, and start all plugins.

        Args:
            config: Optional dict of plugin_name -> config_dict

        Returns:
            Dict of loaded plugin_name -> plugin instance
        """
        plugin_configs = config or {}
        plugin_files = self.discover_plugins()

        for filepath in plugin_files:
            plugin = self.load_plugin_from_file(filepath)
            if plugin is None:
                continue

            if plugin.name in self.plugins:
                self.log.warning(f"Plugin '{plugin.name}' already loaded, skipping duplicate from {filepath}")
                continue

            # Register with node
            plugin.register(self.node)

            # Apply config
            plugin_config = plugin_configs.get(plugin.name, {})
            plugin.configure(plugin_config)

            # Start the plugin
            try:
                await plugin.on_start()
                self.plugins[plugin.name] = plugin
                self.log.info(f"Plugin '{plugin.name}' v{plugin.version} started successfully")
            except Exception as e:
                self.log.error(f"Failed to start plugin '{plugin.name}': {e}")
                import traceback
                self.log.debug(traceback.format_exc())

        # Announce plugins as skills
        if self.plugins:
            await self._announce_skills()

        return self.plugins

    async def stop_all(self):
        """Stop all loaded plugins."""
        for name, plugin in self.plugins.items():
            try:
                await plugin.on_stop()
                self.log.info(f"Plugin '{name}' stopped")
            except Exception as e:
                self.log.error(f"Error stopping plugin '{name}': {e}")

        # Cleanup loaded modules
        for module_name in self._loaded_modules:
            sys.modules.pop(module_name, None)
        self._loaded_modules.clear()
        self.plugins.clear()

    async def _announce_skills(self):
        """Announce plugin capabilities as mesh skills."""
        skills = []
        for name, plugin in self.plugins.items():
            for cap in plugin.capabilities:
                skills.append({
                    "id": f"{name}_{cap}",
                    "name": cap,
                    "description": f"{plugin.description} — {cap}",
                })

        if skills and hasattr(self.node, 'config'):
            existing_skills = list(self.node.config.skills or [])
            # Merge: add new skills, keep existing
            existing_ids = {s.get('id') if isinstance(s, dict) else s for s in existing_skills}
            for skill in skills:
                if skill['id'] not in existing_ids:
                    existing_skills.append(skill)
            self.node.config.skills = existing_skills
            self.log.info(f"Announced {len(skills)} plugin skills to mesh")

    # ── Hook dispatchers ────────────────────────────────────────

    async def dispatch_message_received(self, message) -> Optional[Any]:
        """Dispatch an incoming message to all plugins.

        Returns the first non-None response from a plugin.
        """
        for name, plugin in self.plugins.items():
            try:
                result = await plugin.on_message_received(message)
                if result is not None:
                    return result
            except Exception as e:
                self.log.error(f"Plugin '{name}' error in on_message_received: {e}")

        return None

    async def dispatch_message_sent(self, message, result):
        """Dispatch a sent message event to all plugins."""
        for name, plugin in self.plugins.items():
            try:
                await plugin.on_message_sent(message, result)
            except Exception as e:
                self.log.error(f"Plugin '{name}' error in on_message_sent: {e}")

    async def dispatch_peer_connected(self, peer_name: str, info: Dict):
        """Dispatch a peer connected event to all plugins."""
        for name, plugin in self.plugins.items():
            try:
                await plugin.on_peer_connected(peer_name, info)
            except Exception as e:
                self.log.error(f"Plugin '{name}' error in on_peer_connected: {e}")

    async def dispatch_peer_disconnected(self, peer_name: str):
        """Dispatch a peer disconnected event to all plugins."""
        for name, plugin in self.plugins.items():
            try:
                await plugin.on_peer_disconnected(peer_name)
            except Exception as e:
                self.log.error(f"Plugin '{name}' error in on_peer_disconnected: {e}")

    async def dispatch_health_change(self, peer_name: str, old_health: float, new_health: float):
        """Dispatch a health change event to all plugins."""
        for name, plugin in self.plugins.items():
            try:
                await plugin.on_health_change(peer_name, old_health, new_health)
            except Exception as e:
                self.log.error(f"Plugin '{name}' error in on_health_change: {e}")

    def get_plugin(self, name: str) -> Optional[MeshPlugin]:
        """Get a loaded plugin by name."""
        return self.plugins.get(name)

    def list_plugins(self) -> List[PluginInfo]:
        """List all loaded plugin info."""
        return [p.info for p in self.plugins.values()]

    def get_status(self) -> Dict:
        """Get status of all loaded plugins."""
        return {
            "total_plugins": len(self.plugins),
            "plugins": {
                name: {
                    "version": p.version,
                    "description": p.description,
                    "capabilities": p.capabilities,
                    "running": p._running,
                    "config": {k: v for k, v in p._config.items()
                               if not k.endswith(('_token', '_secret', '_password', '_key'))},
                }
                for name, p in self.plugins.items()
            },
        }