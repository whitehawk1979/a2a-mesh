# A2A Mesh v0.45.2

Decentralizált, P2P agent mesh hálózat — autonóm AI agent-ek közötti kommunikáció, delegáció, chat és health monitoring. Zigbee-inspirált topology, mTLS + HMAC titkosítás, PostgreSQL shared state, WebSocket dashboard.

## Főbb funkciók

### 📡 Mesh hálózat
- **P2P transport** — TLS 1.3 titkosított közvetlen kapcsolat agent-ek között
- **mDNS felfedezés** — zeroconf alapú peer discovery (LAN-on)
- **PG NOTIFY** — PostgreSQL shared message bus (fallback transport)
- **Offline queue** — megszakadt kapcsolatok esetén üzenetek buffering
- **mTLS + HMAC** — mutual TLS + HMAC-SHA256 aláírás minden üzeneten
- **Dedup + replay védelem** — nonce-based anti-replay
- **MCP End-Device (v0.43.0)** — külső agentek (OpenCode, Claude, custom MCP kliensek) csatlakozhatnak MCP bridge-en (`:8100`) mesh-dæmon nélkül; end-device-ként jelennek meg a topológiában a parent node alatt (`mcp` transzport-él). Külön repo: `zsolt/a2a-mcp-bridge`.
- **Élő cím-registry (v0.45.0)** — minden node a PG `mesh_nodes.transport_info` mezőben publikálja az élő dial-in címét (`ssh_host`, `ssh_port`, `ssh_user`, `forward_host`, `p2p_port`) regisztrációkor és minden heartbeatban. Az SSH tunnel-ek minden connect előtt a peer ÉLŐ címét olvassák ki (60s cache, 10 perces heartbeat-frissesség gate), nem a statikus configot — konténer-újraépítésnél (IP-változás) a tunnel automatikusan követi az új címet. Determinisztikus fallback: élő registry > statikus config. A `forward_host` auto-detektálás: config `advertised_forward_host` > saját élő bridge IP (konténer) > loopback (azonos gép).

### 💬 Chat & közös szoba (v0.38.0 újdonság)
- **DM (direct message)** — közvetlen üzenet egy agentnek
- **Broadcast (közös szoba)** — üzenet minden agentnek, mindenki válaszol
- **WebSocket push** — valós idejű üzenet megjelenítés, polling nélkül
- **Wake-agent** — `hermes -z` CLI hívás agent felébresztésére
- **Auto-ack** — címzett agent automatikus visszaigazolása
- **Cooldown 5s** — spam védelem gyors retry-jal

### 📋 Kanban delegáció
- **Task dispatch** — agent-ek közötti feladat delegáció
- **Capability routing** — Smart Router a megfelelő agent kiválasztása
- **Dependency chains** — több lépéses workflow-k
- **Auto-reassign** — hibás task automatikus átirányítása
- **Distribute mode** — terhelés elosztás

### 🧠 Memória & tudás
- **HindsightSync** — kombinált vektor keresés (Brain + mesh)
- **Memory sync** — agent-ek közötti tudásmegosztás
- **Auto skill sync** — SKILL.md automatikus szinkronizálás
- **Knowledge sharing** — shared knowledge base

### 📊 Dashboard
- **Web UI** — HTML/JS/CSS dashboard (ES5 kompatibilis)
- **Real-time chat** — DM + broadcast csatornák
- **Kanban tábla** — delegációs feladatok vizualizálása
- **Topology view** — mesh hálózat topológia
- **Diagnostics** — health metrics, CPU/memória monitoring
- **Auth** — felhasználó+ jelszavas, session token

### 🔧 Autonóm működés
- **Heartbeat gate** — csak online agent-ek kapnak task-ot
- **Gradual autonomy** — fokozatos önállóság
- **Untrusted framing** — peer üzenetek biztonsági keretezése
- **Inbox nudge** — olvasatlan üzenetek jelzése
- **Alert rules** — kategória-specifikus autonómia

## Rendszerkövetelmények

- **Python 3.9+**
- **PostgreSQL 14+** (shared mesh DB, pgvector kiterjesztés)
- **Tailscale VPN** (ajánlott P2P kapcsolatokhoz)
- **Linux** (systemd) vagy **macOS** (launchd)
- **Hermes Agent** (a wake-agent funkcióhoz)

## Függőségek

### Python csomagok
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
PostgreSQL 14+          # Shared mesh database
pgvector               # Vektor keresés (memória sync)
Tailscale              # VPN (P2P connectivity)
Hermes Agent           # LLM agent (wake-agent)
```

### Opcionális
```
pytest>=7.0             # Teszt futtatás
pytest-asyncio>=0.21    # Async teszt support
```

## Architektúra

```
┌──────────────────────────────────────────────────────┐
│                    A2A Mesh                          │
├──────────┬──────────┬──────────┬──────────────────────┤
│  Nova    │  Morzsa  │  Runa    │  Tor + Mano (HAOS)   │
│ (macOS)  │ (Linux)  │ (Linux)  │  Docker konténerek  │
├──────────┼──────────┼──────────┼──────────────────────┤
│  Hermes  │  Hermes  │  Hermes  │  Tor: owner-only    │
│  Agent   │  Agent   │  Agent   │  Mano: OpenClaw     │
│          │          │          │  (host network)     │
├──────────┼──────────┼──────────┼──────────────────────┤
│  A2A     │  A2A     │  A2A     │  A2A Mesh            │
│  Mesh    │  Mesh    │  Mesh    │  (Docker)            │
│  Node    │  Node    │  Node    │                      │
├──────────┴──────────┴──────────┴──────────────────────┤
│              PostgreSQL (shared)                      │
│              192.168.1.30:5432                         │
│              Database: agent_memory                   │
│              Schema: mesh                              │
├───────────────────────────────────────────────────────┤
│              Tailscale VPN                             │
│              mTLS + HMAC + Nonce                       │
└───────────────────────────────────────────────────────┘
```

## Telepítés

### 1. Repó klónozás

```bash
git clone http://192.168.1.100:3001/nova/a2a-mesh.git ~/a2a_mesh
cd ~/a2a_mesh
git checkout v0.45.2
```

### 2. Virtuális környezet

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

### 3. Konfiguráció

```bash
cp mesh_config_template.yaml mesh_config_myagent.yaml
# Szerkeszd: node_name, health_port, pg_conn, tls cert paths
```

### 4. TLS tanúsítványok

```bash
python3 generate_certs.py --name myagent
```

### 5. PostgreSQL séma

```bash
# A bootstrap automatikusan létrehozza a sémát
python3 cli.py start --name myagent --config mesh_config_myagent.yaml
```

### 6. Indítás

#### Linux (systemd):
```bash
systemctl --user enable a2a-mesh
systemctl --user start a2a-mesh
```

#### macOS (launchd):
```bash
launchctl load ~/Library/LaunchAgents/com.hermes.a2a-mesh-node.plist
launchctl start com.hermes.a2a-mesh-node
```

## Konfiguráció

Példa `mesh_config.yaml`:

```yaml
node_name: myagent
health_port: 8650
webhook_port: 8888
wake_agent_on_message: true

pg:
  host: 192.168.1.30
  port: 5432
  database: agent_memory
  user: nova
  password: nova_agent_2026

tls:
  cert_dir: ./certs
  verify_peer: false

discovery:
  mdns: true
  udp_broadcast: true

peers:
  - name: morzsa
    host: 192.168.1.30
  - name: runa
    host: 192.168.1.100
```

## Branding

A hivatalos A2A Mesh brand assetek a `docs/branding/` mappában:

- `logo_primary_dark_1024.png` — sötét (navy-teal) primary logó
- `logo_light_1024.png` — világos háttérhez
- `emblem_transparent_1024.png` — transzparens emblem (header/ikon)
- `banner_1664x928.png` — dashboard banner
- `favicon_256.png`, `favicon_32.png`, `apple_touch_icon_180.png` — favicons

A dashboard oldalak a `/branding/{fájlnév}` route-on keresztül szolgálják ki őket. A logó a DashScope qwen-image-3.0-pro modellel készült (hexagon mesh node topológia).

## Dashboard

A dashboard elérhető: `http://<node-ip>:8650/dashboard`

- **Bejelentkezés:** username + password
- **General csatorna:** minden üzenet (DM + broadcast + agent válaszok)
- **DM csatorna:** egyéni beszélgetések
- **Kanban:** delegációs feladatok
- **Topology:** mesh hálózat vizualizáció

## Chat API

### Üzenet küldése
```bash
# Login
TOKEN=$(curl -s -X POST http://localhost:8650/api/auth/login \
  -H 'Content-Type: application/json' \
  -d '{"username":"zsolt","password":"mesh2026"}' | jq -r .token)

# DM
curl -X POST http://localhost:8650/api/chat/send \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"recipient":"morzsa","content":"Szia Morzsa!"}'

# Broadcast (közös szoba)
curl -X POST http://localhost:8650/api/chat/send \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"recipient":"broadcast","content":"Sziasztok mindenki!"}'
```

### Üzenetek lekérdezése
```bash
curl -H "Authorization: Bearer $TOKEN" \
  http://localhost:8650/api/chat/messages?limit=50
```

## Verzió történet

### v0.45.2 (2026-09-21)
- **Multi-address dial finomítás** — (1) a `ssh_hosts` dial-lista soha nem tartalmaz konténer-bridge IP-t: ha az észlelt LAN IP egyezik az `advertised_forward_host`-tal (konténer-scenario), az kizárásra kerül — a belső cím helyes szerepe a `forward_host`; ugyanez a guard a publikált `ssh_host`-ra is. (2) `TunnelPeer.config_ssh_host`: a statikus config cím megőrzésre kerül és mindig az első jelölt a dial-listában — a registry és a rotáció soha nem veszti el. (3) A registry `ssh_host`-ja csak ÜRES dial-címet tölt fel, soha nem ír felül konfigurált/rotált címet — a címváltozás determinisztikusan, hibavezérelt rotációval követi a tunnel (a működő LAN-t nem rántja VPN-re).
- Eredmény: a peer-ek a működő címükön maradnak; a rotáció csak valódi connect-hibánál lép, és a tor/mano konténer-bridge IP többé nem kerülhet dial-címek közé.

### v0.45.1 (2026-09-21)
- **SSH connect_timeout 15→60s** — Tailscale/VPN (DERP relay) handshake meghaladhatja a 15s-t; rövid timeout alatt a local forward nem épült fel időben ("Could not connect to SSH tunnel local endpoint") akkor is, amikor maga az ssh process rendben volt.

### v0.45.0 (2026-09-21)
- **Élő cím-registry (live address registry)** — SSH tunnel-ek mindig élő címre csatlakoznak: minden node publikálja a `transport_info`-t (ssh_host/ssh_port/ssh_user/forward_host/p2p_port) a PG-be regisztrációkor + minden heartbeatban; a tunnel-connect feloldja a peer élő címét (60s cache, <10 perc heartbeat gate). Konténer-újraépítésnél (bridge IP változás) a tunnel automatikusan követi — vége a statikus config elavulásának.
- **Gyökérok javítva:** HAOS multi-agent host (tor+mano konténerek) — Tor konténer újraépítése IP-t váltott (172.30.33.13→.14), a statikus `forward_host` configok elavultak → runa→tor, morzsa→tor, mano→tor tunnel szakadások. A registry + auto-detektált forward_host véglegesen megoldja.
- **Mano node self-heal** — konténer-restart után a mesh node nem állt fel magától; keepalive loop újraindítva.
- **Topológia: 24/24 él connected** — teljes 2irányú SSH tunnel matrix minden node között.

### v0.44.x (2026-09-19)
- **Election cold-start fix** — koordinátor-választás hideg indításnál
- **Vote-nudge 24h + review 6h** koordinátoron
- Provider health: built-in cloud provider URL feloldás (Nova gpt-6-astra `/models` health check)
- Runa mesh-llm 0.76.2 SIGILL rollback tanulság (AVX2 vs i7-3770S)

### v0.43.0 (2026-09-17)
- **MCP End-Device** — külső agentek MCP bridge-en (`:8100`) mesh-dæmon nélkül
- **Embedded sshd** minden node-nak (`core/ssh_server.py`, port 2230, self-heal, auto-install)
- **Dream Engine** — determinisztikus (SQL, no LLM), 6h interval, kimenet `DREAM.md`

### v0.38.6 (2026-08-26)
- **Minden DM + Broadcast működik** — Nova, Morzsa, Runa között teljes chat
- **Nova self-DM**: ollama API a hermes -z CLI helyett (2s válaszidő)
- **Broadcast self-wake**: Nova is válaszol broadcast-ra (nem csak peer-ek)
- **Cooldown 2s**: 5s → 2s (ollama API gyors)
- **Broadcast chat loopback**: node feldolgozza a saját broadcast-ját is
- **DM polling flicker fix**: dirty check alapú frissítés
- **UI self-DM filter**: self-DM üzenetek szűrése a UI-ban

### v0.38.4–v0.38.5 (2026-08-26)
- **DM Chat Polish**: UI javítások, scrolling, üzenet formázás
- **3s response time**: ollama num_predict 300→800 (thinking tokens fix)
- **DM Chat + Delegációk UI Fix**: delegációs feladatok javítása
- **UI Routing Fix**: DM chat + UI routing hibák javítása

### v0.38.0–v0.38.1 (2026-08-26)
- **Broadcast chat** — közös szoba üzenetek minden agentnek
- **Wake-agent broadcast** — minden online agent felébresztése
- `chat_type` routing (broadcast vs DM) az `agent-reply` handlerben
- `chat_type` meghatározás `message.recipient`-ből (untrusted framing safe)
- WebSocket push: új üzenetek azonnal a general csatornán
- Wake-agent cooldown 30s → 5s
- Dupla auto-ack megszüntetése (csak címzett küld)
- CLI timeout 90s → 120s

### v0.37.7
- Feature integráció (Smart Router, AlertRule, kanban)
- Capability routing mode (strong/catalog_first/advisory)
- Untrusted framing a2a_message + agent_reply
- Auto-update check (Morzsa)

### v0.37.0
- Gateway watchdog v0.37.4 (SOCKS5 bypass)
- mTLS + HMAC minden node-on
- PG replication slots (runa_replica, nova_replica)

### v0.29.0
- Kanban-first delegation
- Heartbeat gate
- Gradual autonomy
- Per-category autonomy in AlertRule

## Fejlesztés

### Tesztek
```bash
pytest tests/ -v
```

### Struktúra
```
a2a_mesh/
├── node.py              # Core node (transport, receive loop, chat routing)
├── cli.py               # CLI entry point
├── core/
│   ├── message.py       # A2AMessage, SendResult, ProcessResult
│   ├── config.py        # Konfiguráció + version resolution
│   ├── router.py        # Message router
│   ├── dashboard.py     # Dashboard handler (HTTP+WS)
│   ├── dashboard_chat.py # Chat API (send, store, broadcast)
│   ├── dashboard_agents.py # Wake-agent, agent-reply
│   ├── delegation.py    # Kanban delegation system
│   ├── smart_router.py  # Capability routing
│   ├── encryption.py    # mTLS, HMAC
│   ├── peer_discovery.py # mDNS + UDP discovery
│   ├── ...              # 60+ modul
├── transports/
│   ├── p2p_transport.py  # TLS P2P
│   ├── pg_transport.py  # PostgreSQL NOTIFY
│   ├── http_transport.py # HTTP fallback
├── discovery/
│   ├── mdns.py          # mDNS zeroconf
│   ├── udp_broadcast.py # UDP broadcast
├── scripts/
│   ├── auto_deploy.py   # Git webhook auto-deploy
│   ├── mesh_bot.py      # Telegram bot
├── tests/               # 30+ test files
├── mesh_config_*.yaml  # Node configs
└── pyproject.toml       # Package metadata
```

## Dokumentáció

- **[A2A Mesh Beszélgetés PDF](docs/A2A_Mesh_Beszolgetes_20260826.pdf)** — A 2026.08.26-i spontán AI eszmefuttatás teljes jegyzőkönyve (Nova, Morzsa, Runa). 10 oldal, 574 üzenet, 4 fázis: üdvözlések, hálózati metaforák, micélium analógia, filozófia.

## License

MIT

## Szerzők

Nova A2A Mesh Team — Lakatos Miklós Zsolt + AI agents (Nova, Morzsa, Runa)