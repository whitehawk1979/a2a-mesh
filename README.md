# A2A Mesh — Agent-to-Agent Communication Network

> Decentralized, P2P agent mesh with intelligent routing, health scoring, and workflow orchestration.

## Overview

A2A Mesh enables autonomous AI agents to communicate, coordinate, and execute complex workflows across a decentralized network. Built with reliability-first design (ACK/retry/offline queue) and inspired by [gensyn-ai/axl](https://github.com/gensyn-ai/axl) and [sushaan-k/a2a-mesh](https://github.com/sushaan-k/a2a-mesh).

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                    A2A Mesh Node                     │
├─────────────────────────────────────────────────────┤
│  Dashboard (aiohttp)    │  Agent Card (well-known)  │
│  /api/health/scores     │  /api/router/stats         │
├─────────────────────────────────────────────────────┤
│  Smart Router           │  Workflow Coordinator v2   │
│  - health_weighted      │  - FanIn (MERGE/FIRST/VOTE)│
│  - least_latency        │  - Consensus (ALL/ANY/MAJ) │
│  - least_load           │  - Budget + Timeout        │
├─────────────────────────────────────────────────────┤
│  StreamMux              │  BoundedQueue              │
│  (content routing)      │  (oldest-drop overflow)    │
├─────────────────────────────────────────────────────┤
│  Health Scorer          │  GossipSub Broadcast       │
│  (decay/recovery)       │  (flood≤10, mesh>10)       │
├─────────────────────────────────────────────────────┤
│  Dedup Cache            │  Agent Registry            │
│  (hit/miss tracking)    │  (capabilities, status)    │
├─────────────────────────────────────────────────────┤
│  Transports                                           │
│  ├─ PG Transport (NOTIFY, fallback)                  │
│  └─ P2P Transport (binary framing v1)                │
└─────────────────────────────────────────────────────┘
```

## Features

### Core (v0.1–v0.7)
- **PG Transport**: PostgreSQL NOTIFY + shared mesh_messages table
- **P2P Transport**: Direct TCP with length-prefixed binary framing
- **Dedup Cache**: Message deduplication with hit/miss statistics
- **Agent Registry**: Capability-based agent discovery
- **Smart Router**: Multi-strategy routing (round-robin, least-cost, health-weighted)
- **Auth**: Token-based dashboard + mesh_secret for inter-node
- **Auto-steer**: Priority-based message steering (P10+ interrupt, P7-9 high, P1-6 backlog)

### v0.8.0–v0.8.3 (AXL + sushaan-k inspired)
- **StreamMux**: Content-based stream routing (a2a, mesh_control, file_transfer, task)
- **BoundedQueue**: asyncio.Queue with oldest-drop overflow (maxsize=200)
- **AgentCard**: `/.well-known/agent-card.json` + `/api/agent-card` endpoints
- **Protocol v0.8.0**: `protocol_version` field in messages
- **Connection Semaphore**: Concurrent connection limiting
- **Versioned Binary Framing v1**: `[0x01][4-byte BE len][payload]` — backward compatible
- **GossipSub Broadcast**: Flood mode (≤10 nodes), GossipSub mesh (>10 nodes)
- **Health Scorer**: Decay on failure (0.15+exponential), recovery on success (0.05), latency penalty
- **Workflow DAG Coordinator v2**:
  - FanInStrategy: MERGE, FIRST, VOTE (majority vote)
  - ConsensusMode: ALL, ANY, MAJORITY, QUORUM
  - Fan-out: deepcopy isolation, N parallel copies per task
  - Budget tracking: per-level cost accumulation
  - Workflow timeout: remaining-time tracking

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/health` | Node health check |
| GET | `/.well-known/agent-card.json` | Agent capability card |
| GET | `/api/agent-card` | Agent card (alias) |
| GET | `/api/router/stats` | Router statistics (dedup, queues, gossipsub, health) |
| GET | `/api/health/scores` | All agent health scores |
| POST | `/api/health/record-success/{name}` | Record successful delivery |
| POST | `/api/health/record-failure/{name}` | Record failed delivery |
| POST | `/api/send` | Send message to agent |
| POST | `/api/agent-reply` | Agent reply endpoint |
| GET | `/api/registry` | List registered agents |

## Quick Start

### Option 1: Auto-install (recommended)

```bash
# Clone and run — dependencies install automatically
git clone <repo-url> a2a_mesh && cd a2a_mesh

# Start with PG DSN (easiest — no config editing needed)
python3 cli.py start --name mynode --port 8650 \
  --pg-dsn 'postgresql://nova:mypassword@192.168.1.30:5432/agent_memory'

# Or with A2A_MESH_PG_DSN env var:
export A2A_MESH_PG_DSN='postgresql://nova:mypassword@192.168.1.30:5432/agent_memory'
python3 cli.py start --name mynode --port 8650
# 📦 Missing deps are auto-installed on first run
```

### Option 2: Full install script

```bash
# Clone, install deps, generate certs, guided setup
git clone <repo-url> a2a_mesh && cd a2a_mesh
chmod +x install.sh
./install.sh          # Interactive — creates venv, installs deps, generates certs

# Or non-interactive:
./install.sh --skip-start --no-certs
```

### Option 3: Manual setup

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Start a node
.venv/bin/python3 cli.py start --name nova --port 8650
```

### With TLS encryption

```bash
python3 cli.py start --name nova --port 8650 --tls
# Or with custom certs:
python3 cli.py start --name nova --port 8650 --tls \
  --tls-cert certs/nova.crt --tls-key certs/nova.key --tls-ca certs/a2a-mesh-ca.crt
```

### Send a message
curl -X POST http://localhost:8650/api/send \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{"recipient":"morzsa","type":"task","priority":5,"payload":{"task":"analyze"}}'
```

## Configuration

Environment variables:
- `MESH_SECRET`: Shared secret for inter-node authentication
- `MESH_PG_DSN`: PostgreSQL connection string
- `MESH_PORT`: HTTP port (default: 8650)
- `LOGLEVEL`: Log level (DEBUG, INFO, WARNING)

## Releases

| Version | Date | Highlights |
|---------|------|-----------|
| v0.8.3 | 2026-06-23 | Workflow DAG v2: FanIn, Consensus, Budget, Timeout |
| v0.8.2 | 2026-06-23 | Health Scorer, API endpoints |
| v0.8.1 | 2026-06-23 | Versioned binary framing, GossipSub |
| v0.8.0 | 2026-06-22 | StreamMux, BoundedQueue, AgentCard, protocol v0.8.0 |
| v0.7.0 | 2026-06-21 | Smart Router, Auth, Auto-steer |
| v0.6.0 | 2026-06-20 | Dedup cache, PG transport fixes |
| v0.4.0 | 2026-06-19 | P2P transport, dashboard |
| v0.3.0 | 2026-06-18 | PG NOTIFY transport |
| v0.2.0 | 2026-06-17 | Basic routing, message model |
| v0.1.0 | 2026-06-16 | Project scaffold |

## License

Private — All rights reserved.