# A2A Mesh Plugin SDK

## Áttekintés

Az A2A Mesh plugin rendszere lehetővé teszi a mesh node funkcionalitásának kiterjesztését:

- **Gateway bridge-ek** (Telegram, Discord, Slack, WhatsApp)
- **Egyedi üzenetkezelők** (workflow triggerek, értesítések)
- **Transport adapterek** (új kommunikációs csatornák)
- **Discovery metódusok** (mDNS kiterjesztések, custom registry-k)

## Plugin lifecycle

```
1. Discovery  — plugins/ mappa szkennelése *_plugin.py fájlok után
2. Loading    — PluginLoader importálja és példányosítja a plugint
3. Registration — plugin.register(node) — mesh event hook-ok regisztrálása
4. Running    — plugin.start() — plugin működés kezdete
5. Shutdown   — plugin.stop() — tiszta leállás
```

## Hook system

Minden plugin a következő hook-okat implementálhatja:

| Hook | Mikor hívódik | Paraméterek |
|------|-------------|------------|
| `on_start(node)` | Node indulás után | `node`: MeshNode |
| `on_stop(node)` | Node leállás előtt | `node`: MeshNode |
| `on_message_received(message)` | Bejövő üzenet | `message`: A2AMessage |
| `on_message_sent(message, result)` | Kimenő üzenet után | `message`, `result`: SendResult |
| `on_peer_connected(peer_name, info)` | Peer csatlakozás | `peer_name`: str, `info`: dict |
| `on_peer_disconnected(peer_name)` | Peer lekapcsolódás | `peer_name`: str |
| `on_health_change(peer_name, old, new)` | Health státusz változás | `peer_name`, `old`, `new`: str |

## Plugin készítése

### 1. Minimal plugin példa

```python
"""My custom plugin — logs all incoming messages."""
from a2a_mesh.core.plugin_base import MeshPlugin

class MyPlugin(MeshPlugin):
    name = "my_plugin"
    version = "1.0.0"

    def on_start(self, node):
        self.log.info(f"My plugin starting on {node.node_name}")

    async def on_message_received(self, message):
        self.log.info(f"Message from {message.sender}: {message.type}")
```

### 2. Skill advertiser plugin

A pluginok automatikusan hirdethetnek skill-eket a mesh marketplace-en:

```python
class SkillPlugin(MeshPlugin):
    name = "skill_plugin"
    version = "1.0.0"
    
    # Skills that this plugin provides
    skills = [
        {"skill_name": "translation", "display_name": "Translation Service",
         "description": "Translate text between languages",
         "tags": ["translate", "language"], "cost": 0.0}
    ]

    async def on_start(self, node):
        # Skills are auto-advertised by PluginLoader._announce_skills()
        self.log.info(f"Advertising {len(self.skills)} skills")
```

### 3. Message handler plugin

```python
class NotificationPlugin(MeshPlugin):
    name = "notification_plugin"
    version = "1.0.0"

    async def on_message_received(self, message):
        if message.type == "alert":
            # Process alert message
            await self._send_notification(message.content)
            return "processed"  # Return value stops further processing
    
    async def _send_notification(self, content):
        # Custom notification logic
        pass
```

### 4. Gateway bridge plugin

Lásd: `core/plugins/gateway_plugin.py` — teljes Telegram/Discord/Slack bridge implementáció.

## Plugin regisztráció

A pluginok a `core/plugins/` mappába helyezendők, `*_plugin.py` kiterjesztéssel.

```
core/plugins/
├── __init__.py
├── gateway_plugin.py       # Telegram/Discord/Slack bridge
├── health_monitor_plugin.py # Health monitoring
├── notification_plugin.py   # Notification handling
├── skill_advertiser_plugin.py # Skill marketplace auto-advertise
└── task_dispatch_plugin.py  # Task dispatch handling
```

A `PluginLoader.discover_plugins()` automatikusan megtalálja és betölti ezeket.

## Plugin konfiguráció

A pluginok a mesh config YAML-ben konfigurálhatók:

```yaml
plugins:
  enabled:
    - gateway_plugin
    - health_monitor_plugin
    - skill_advertiser_plugin
  config:
    gateway_plugin:
      telegram_bot_token: "your-token"
      discord_webhook_url: "your-url"
```

## Skill marketplace integráció

A pluginok skill-eket hirdethetnek a mesh marketplace-en:

1. **Hirdetés**: Plugin `skills` attribútuma → `mesh.mesh_skills` tábla
2. **Keresés**: `/api/skills/search?q=...`
3. **Delegálás**: `/api/skills/{skill_id}/delegate` → delegation rendszer

## Tesztelés

```bash
# Plugin szintaktikai ellenőrzés
python3 -c "import ast; ast.parse(open('core/plugins/my_plugin.py').read()); print('OK')"

# Plugin betöltés teszt
python3 -c "
from a2a_mesh.core.plugin_loader import PluginLoader
loader = PluginLoader(node=None)
plugins = loader.discover_plugins()
print(f'Found: {plugins}')
"
```

## Best practices

1. **Ne blokkolj**: Minden hook async legyen, ne végezz hosszú szinkron műveleteket
2. **Hibakezelés**: Mindig try/except a hook-okban, ne dobj exception-t
3. **Logging**: Használd `self.log`-ot (logging.Logger)
4. **Cleanup**: `on_stop()`-ban szabadítsd fel az erőforrásokat
5. **Skill hirdetés**: Csak releváns skill-eket hirdess, ne spammeld a marketplace-et