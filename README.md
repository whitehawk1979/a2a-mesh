# A2A Mesh — Decentralizált Ágens Hálózat

> **Minden agent a saját gépén dolgozik, a saját LLM-ével. A hálózat delegál, koordinál, szinkronizál.**

## A Mesh Lényege

Az A2A Mesh nem egy centralizált szolgáltatás — **egy decentralizált ágens hálózat**, ahol minden node:

1. **Saját gépen fut** — nincs központi szerver, nincs single point of failure
2. **Saját LLM-et használ** — minden node a helyi Ollamájával generál kódot, elemez, dönt
3. **Delegál és fogad feladatokat** — az egyik agent felügyelő, a másik worker, a szerepkörök dinamikusan változnak
4. **Eredményt visszaad** — a worker elkészíti a feladatot, a felügyelő megkapja az eredményt

```
  Nova (felügyelő)          Morzsa (worker)           Runa (worker)
  ┌─────────────┐    ┌─────────────────┐    ┌─────────────────┐
  │ Delegál:    │───►│ LLM generál:    │    │ LLM generál:    │
  │ "Készíts    │    │ glm-5.2:cloud   │    │ glm-5.2:cloud   │
  │  weboldalt" │    │ → HTML/CSS/JS   │    │ → Python script  │
  └──────┬──────┘    └────────┬────────┘    └────────┬────────┘
         │                    │                      │
         │◄───────────────────┘                      │
         │  Eredmény: generated_html_morzsa.html    │
         │                                           │
         │◄──────────────────────────────────────────┘
         │  Eredmény: generated_py_runa.py
         │
  Nova összesíti, validálja, továbbítja a felhasználónak
```

**Ha Runa küld egy feladatot Morzsának** → Runa a felügyelő, Morzsa a worker. A szerepkörök mindig a feladó és végrehajtó között alakulnak ki.

## Architektúra

```
┌─────────────────────────────────────────────────────────┐
│                    A2A Mesh Node                         │
├─────────────────────────────────────────────────────────┤
│  Dashboard (aiohttp)    │  Agent Card (well-known)     │
│  /api/health            │  /api/agent-card              │
│  /api/delegations       │  /api/nodes                   │
│  /api/mesh/topology     │  /api/agents                  │
├─────────────────────────────────────────────────────────┤
│  Delegation Manager     │  LLM Code Generator           │
│  - Task lifecycle       │  - Local Ollama integration    │
│  - Fan-out/claim        │  - Model preference: glm-5.2  │
│  - Result files         │  - Template fallback           │
├─────────────────────────────────────────────────────────┤
│  Smart Router           │  Workflow Coordinator v2      │
│  - health_weighted      │  - FanIn (MERGE/FIRST/VOTE)  │
│  - least_latency        │  - Consensus (ALL/ANY/MAJ)    │
│  - least_load           │  - Budget + Timeout            │
├─────────────────────────────────────────────────────────┤
│  Transports                                             │
│  ├─ P2P Transport (primary, binary framing v1)         │
│  ├─ PG Transport (NOTIFY, fallback)                    │
│  ├─ HTTP/MCP Bridge (external API)                     │
│  └─ BLE Transport (IoT, proximity)                     │
├─────────────────────────────────────────────────────────┤
│  Health Scorer  │  GossipSub  │  Dedup  │  Registry    │
│  (decay/recov)  │  (flood/mesh)│  Cache  │  (skills)    │
└─────────────────────────────────────────────────────────┘
```

## Delegáció Rendszer

A mesh központi funkciója a **feladat-delegáció** — minden node delegálhat feladatot bármelyik másik node-nak:

### Feladat Típusok
| Típus | Leírás | Handler |
|-------|--------|---------|
| `code` | Kódgenerálás (LLM) | `_task_llm_generate` → Ollama, vagy template fallback |
| `generic` | Általános feladat | `_task_code_generation` (kulcsszó-alapú) |
| `research` | Kutatás, elemzés | `_task_system_analysis` |
| `analysis` | Rendszerelemzés | `_task_system_analysis` |
| `monitoring` | Rendszermonitorozás | `_task_html_status` |

### Delegáció Folyamat
```
1. Feladó → POST /api/delegations {to_agent, subject, description, task_type}
2. Koordinátor → PG NOTIFY: delegation_channel
3. Feldolgozó → claim + végrehajtás (LLM vagy template)
4. Eredmény → task result + files (base64)
5. Feladó → GET /api/delegations/{id}/files
```

### LLM Generálás
Minden node a **saját Ollamáját** használja:
- Modell preferencia: `glm-5.2` > `glm-5.1` > `glm-4.7` > `gemma4:31b` > `qwen2.5`
- Ha nincs LLM elérhető → template fallback (sablon-alapú generálás)
- Timeout: 300 másodperc nagy modellekhez

## Skillek

Minden node publikálja a képességeit (skills) a registry-ben:

| Skill | Leírás |
|-------|--------|
| `mesh_send` | Üzenetküldés agentek között |
| `mesh_discover` | Agent felfedezés és képesség-lekérdezés |
| `mesh_health` | Rendszerállapot monitorozás |
| `gdm` | Csoportos döntéshozatal (Group Decision Making) |
| `task_execution` | Delegált feladatok végrehajtása |
| `image_gen` | Képgenerálás szövegből |
| `gateway_bridge` | Külső rendszer híd (Telegram, Discord) |
| `health_monitor` | Riasztások és értesítések |
| `delegation` | Feladat kiosztás és szinkronizáció |
| `task_dispatch` | Feladat szétosztás a meshben |

## API Endpoints

### Dashboard & Monitoring
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/health` | Node health check |
| GET | `/api/agents` | Agent list with skills |
| GET | `/api/nodes` | Node list with transports |
| GET | `/api/mesh/topology` | Full mesh topology |
| GET | `/api/delegations` | Delegation list |
| POST | `/api/delegations` | Create delegation |

### Delegation
| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/delegations` | Create task (to_agent, subject, task_type) |
| GET | `/api/delegations/{id}` | Task status |
| GET | `/api/delegations/{id}/files` | Download result files |
| POST | `/api/delegations/{id}/cancel` | Cancel task |
| POST | `/api/delegations/{id}/claim` | Claim available task |
| GET | `/api/delegations/available` | List available tasks |
| GET | `/api/delegations/stats` | Delegation statistics |

## Quick Start

```bash
# Clone and run
git clone <repo-url> a2a_mesh && cd a2a_mesh
.venv/bin/python cli.py start --name nova --config mesh_config.yaml

# Send a delegation task
curl -X POST http://localhost:8650/api/delegations \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"to_agent":"morzsa","subject":"Generálj Python scriptet","task_type":"code","priority":5}'
```

## Configuration

```yaml
# mesh_config.yaml
node:
  name: nova
  role: router
  version: "0.18.55"

pg:
  host: 192.168.1.30
  port: 5432
  database: agent_memory
  user: nova
  password: "***"

transports:
  p2p:
    enabled: true
    port: 8645
  pg:
    enabled: true
  http:
    enabled: true
    bridge_url: http://192.168.1.30:8199
  ble:
    enabled: true

skills:
  - id: mesh_send
  - id: mesh_discover
  - id: mesh_health
  - id: gdm
  - id: task_execution
  - id: image_gen

plugins:
  task_dispatch:
    enabled: true
    max_concurrent_tasks: 5
```

## Node-ok

| Node | Host | Role | LLM | Skills |
|------|------|------|-----|--------|
| Nova | macOS 192.168.1.8 | Router | qwen2.5, glm-4.7-flash, kimi-k2.5 | 17 |
| Morzsa | Linux 192.168.1.30 | Coordinator | gemma4:31b-cloud, glm-5.2:cloud, qwen2.5 | 16 |
| Runa | Linux 192.168.1.100 | Router | glm-5.2:cloud, gemma4:cloud, qwen2.5 | 17 |
| Lennie | Windows 192.168.1.15 | Agent | (offline) | 0 |

## Verzió Történet

| Version | Highlights |
|---------|------------|
| v0.18.55 | LLM-alapú delegáció: glm-5.2 preferencia, 300s timeout |
| v0.18.54 | LLM task handler: Ollama integráció, template fallback |
| v0.18.53 | Skill normalizálás: dict→{name,id,description} konverzió |
| v0.18.52 | /api/nodes version + skills DB fallback |
| v0.18.47 | Full skill publishing: DB fallback, AgentCard creation |
| v0.18.46 | Topology endpoint 500 fix, asyncpg Record access |
| v0.18.40 | Skill DB tárolás, periodic skills broadcast |
| v0.18.30 | Delegation system: task lifecycle, claim, files |
| v0.18.0 | Dashboard v3: dark theme, glass-morphism, sidebar |
| v0.10.0 | Dashboard v2, mDNS, TLS, static nodes |
| v0.8.0 | StreamMux, BoundedQueue, AgentCard, protocol v0.8.0 |
| v0.1.0 | Project scaffold |

## License

Private — All rights reserved.