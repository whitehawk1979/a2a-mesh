#!/usr/bin/env python3
"""Create Gitea release for A2A Mesh v0.38.0"""
import json, urllib.request, base64

GITEA_URL = "http://192.168.1.100:3001"
OWNER = "nova"
REPO = "a2a-mesh"
USER = "zsolt"
PASS = "admin1234"

BODY = """## 🎯 Új funkciók

### Broadcast Chat (közös szoba)
- Üzenetek **minden agentnek** eljutnak P2P broadcast-dal
- Minden online agent **válaszol** a közös szobában
- `chat_type` routing: broadcast vs DM megkülönböztetés
- Válaszok `recipient=broadcast` formátumban (general csatornán jelennek meg)

### Wake-Agent Multi-Target
- Broadcast üzenetnél **minden peer** wake-agent hívást kap (HTTP fallback IP-vel)
- `chat_type` meghatározás `message.recipient`-ből (untrusted framing safe)
- `chat_username` + `chat_type` továbbítva a teljes wake-agent láncon

### WebSocket Push
- Új üzenetek **azonnal** megjelennek a general csatornán
- Polling nélküli valós idejű frissítés

## 🔧 Optimalizálások

| Mit | Előtte | Utána |
|-----|--------|-------|
| Wake-agent cooldown | 30s | 5s |
| Auto-ack | 2x (Morzsa+Nova) | 1x (csak címzett) |
| CLI timeout | 90s | 120s |
| Üzenet megjelenítés | Polling | WebSocket push |

## 🐛 Bugfixek

- `SendResult.success` (nem `.status`) — broadcast csendben elbukott
- `_aio` import hiány a broadcast blokkban — wake-agent coroutine nem futott
- `_broadcast_ws` guard — agent-reply 500 hiba (DashboardHandler hiányzó metódus)
- `request.read()` + `request.json()` dupla body read → `json.loads(data)`
- `reply_endpoint` 127.0.0.1 → LAN IP (Morzsa nem érte el a localhostot)
- Chat auto-ack + wake-agent **router.receive() elé** mozgatva (router exception blokkolta)
- Duplikált `com.a2a-mesh.nova` LaunchAgent eltávolítva

## 📦 Függőségek

### Python
```
aiohttp>=3.9.0          # HTTP szerver/kliens, WebSocket
asyncpg>=0.31.0         # AsyncPG PostgreSQL driver
msgpack>=1.0.7          # Bináris üzenet szerializáció
psycopg2-binary>=2.9.9  # Sync PG (fallback, migration)
PyYAML>=6.0             # Config fájlok
zeroconf>=0.130.0       # mDNS peer discovery
```

### Rendszer
```
PostgreSQL 14+ (pgvector)   # Shared mesh database
Tailscale VPN               # P2P connectivity
Hermes Agent                # LLM agent (wake-agent)
```

## 📊 Teszt eredmény

```
id=170 zsolt→broadcast  [chat]: Sziasztok! Teszt 5
id=171 runa→zsolt       [agent_reply]: ✅ Üzenet megkapva!
id=172 morzsa→zsolt     [agent_reply]: ✅ Üzenet megkapva!
id=173 runa→zsolt       [agent_reply]: Szia Zsolt! Teszt 5 megérkezett — Rúna online 🔮
id=174 morzsa→broadcast [agent_reply]: Szia Zsolt! Megjött a Teszt 5 üzeneted! ✅
```

**Mindkét agent válaszolt!** Morzsa broadcast formátumban ✅

## 🏗️ Architektúra

```
Nova (macOS) ←P2P→ Morzsa (Linux) ←P2P→ Runa (Linux)
     ↓                ↓                ↓
  Hermes Agent    Hermes Agent    Hermes Agent
     ↓                ↓                ↓
  A2A Mesh Node  A2A Mesh Node  A2A Mesh Node
     └────────────────┴────────────────┘
                 PostgreSQL (shared)
             192.168.1.30:5432 / agent_memory
                 Schema: mesh
```

## 📁 Fájlok módosítva

- `node.py` — `_trigger_webhook` chat_type extraction, pre-router chat processing
- `core/dashboard_chat.py` — Broadcast handler, wake-agent all peers
- `core/dashboard_agents.py` — agent-reply chat_type routing, reply_body chat_type
- `core/dashboard.js` — WebSocket new_message handler, general channel force-add
- `core/dashboard.py` — Wake-agent cooldown 5s
- `pyproject.toml` — Version 0.38.0
- `README.md` — Teljes újraírás
"""

payload = json.dumps({
    "tag_name": "v0.38.0",
    "target_commitish": "main",
    "name": "A2A Mesh v0.38.0 — Broadcast Chat & Wake-Agent Multi-Target",
    "body": BODY,
    "draft": False,
    "prerelease": False
}).encode()

auth = base64.b64encode(f"{USER}:{PASS}".encode()).decode()
req = urllib.request.Request(
    f"{GITEA_URL}/api/v1/repos/{OWNER}/{REPO}/releases",
    data=payload,
    headers={
        "Content-Type": "application/json",
        "Authorization": f"Basic {auth}"
    },
    method="POST"
)

try:
    with urllib.request.urlopen(req, timeout=15) as resp:
        result = json.loads(resp.read())
        print(f"✅ Release created!")
        print(f"  ID: {result.get('id')}")
        print(f"  Name: {result.get('name')}")
        print(f"  Tag: {result.get('tag_name')}")
        print(f"  URL: {GITEA_URL}/nova/a2a-mesh/releases/tag/v0.38.0")
except Exception as e:
    print(f"❌ Error: {e}")
    if hasattr(e, 'read'):
        print(f"  Response: {e.read().decode()[:500]}")