# A2A Mesh v0.29.0

Decentralizált, P2P agent mesh hálózat — autonóm agent-ek közötti kommunikáció, delegáció és health monitoring.

## Gyors telepítés

```bash
# 1. Klónozd a repót
git clone http://<gitea>:3001/nova/a2a-mesh.git ~/a2a_mesh
cd ~/a2a_mesh
git checkout v0.29.0

# 2. Futtasd az installert (interaktív)
./install.sh

# Vagy teljesen automatikus:
./install.sh \
  --node nova \
  --host 192.168.1.50 \
  --pg-host 192.168.1.30 \
  --pg-user nova \
  --pg-password 'your_password' \
  --pg-db agent_memory \
  --pg-init
```

Az installer automatikusan:
1. **Python venv** + dependencies telepítés
2. **PostgreSQL schema** inicializálás (`schema_init.sql`)
3. **Config fájl** generálás (`mesh_config_<node>.yaml`)
4. **TLS certifikátumok** generálás
5. **systemd/launchd service** beállítás
6. **Cron job-ok** (watchdog 2min + cleanup 10min)

## Rendszerkövetelmények

- Python 3.9+
- PostgreSQL 14+ (shared mesh DB)
- Tailscale VPN (ajánlott P2P kapcsolatokhoz)
- Linux (systemd) vagy macOS (launchd)

## Architektúra

```
┌──────────────┐     P2P (TLS)     ┌──────────────┐
│   Node A     │◄────────────────►│   Node B     │
│  (Python)    │                  │  (Python)    │
│  Router      │                  │  Router      │
│  HealthScorer│                  │  HealthScorer│
│  Diagnostics │                  │  Diagnostics │
│  Watchdog    │                  │  Watchdog    │
└──────┬───────┘                  └──────┬───────┘
       │                                 │
       └──────────┬──────────────────────┘
                  │
          ┌───────▼───────┐
          │  PostgreSQL   │
          │  agent_memory  │
          │  mesh.* schema │
          └───────────────┘
```

## PostgreSQL Táblák

| Tábla | Leírás |
|-------|--------|
| `mesh.mesh_nodes` | Node regisztráció, heartbeat, provider_status |
| `mesh.mesh_messages` | Agent-ek közötti üzenetek |
| `mesh.mesh_tasks` | Delegált feladatok |
| `mesh.mesh_health_history` | Health score történet (v0.29.0) |
| `mesh.mesh_suggestions` | Diagnostics javaslatok (v0.29.0) |
| `mesh.mesh_events` | Audit log |

## v0.29.0 funkciók

- **Health Score PG Persistence** — 60s-enként mentve, restart utáni helyreállítás
- **Delegation Feedback Loop** — delegáció eredménye → health score
- **Provider Status Integration** — LLM provider állapot → health penalty
- **Gateway Watchdog** — 2perces cron, auto-restart
- **Session Cleanup** — 10perces cron
- **Diagnostics Suggestions** — PG-be persistált config javaslatok
- **mTLS + HMAC** — node-ok közötti titkosítás
- **P2P + PG + HTTP transport** — háromszintű fallback

## Fájlok

```
a2a_mesh/
├── install.sh                    # Full installer
├── schema_init.sql               # PG schema init
├── requirements.txt              # Python dependencies
├── mesh_config_template.yaml      # Config template
├── cli.py                         # CLI entry point
├── cli_mesh.py                    # Mesh management CLI
├── generate_certs.py              # TLS cert generator
├── bootstrap_cli.py               # Lightweight bootstrap
├── node.py                        # Main mesh node
├── core/
│   ├── health_scorer.py           # Health score + PG persistence
│   ├── diagnostics.py             # Config suggestion engine
│   ├── provider_health.py         # LLM provider checks
│   ├── gateway_watchdog.py        # Auto-restart watchdog
│   ├── session_cleanup.py         # Session cleanup
│   ├── router.py                  # Message routing
│   ├── async_db.py                # AsyncPG connection pool
│   ├── peer_discovery.py          # Peer discovery
│   ├── auto_steer.py              # Topology tuning
│   └── ...
├── transports/
│   ├── p2p_transport.py           # TLS P2P transport
│   ├── pg_transport.py            # PostgreSQL NOTIFY transport
│   └── http_transport.py          # HTTP fallback transport
└── certs/                         # TLS certificates (generated)
```

## Indítás

### Linux (systemd):
```bash
systemctl --user start a2a-mesh
journalctl --user -u a2a-mesh -f
```

### macOS (launchd):
```bash
launchctl start com.hermes.a2a-mesh-node
tail -f ~/.hermes/logs/a2a_mesh_node.log
```

### Manual:
```bash
python3 cli.py start --name nova --config mesh_config_nova.yaml
```

## Ellenőrzés

```bash
# Node health
curl http://localhost:8650/health | python3 -m json.tool

# Peers
python3 cli_mesh.py peers

# PG health history
psql -h <pg-host> -U nova -d agent_memory -c \
  "SELECT node_name, health_score, provider_primary FROM mesh.mesh_health_history ORDER BY recorded_at DESC LIMIT 10;"

# Suggestions
psql -h <pg-host> -U nova -d agent_memory -c \
  "SELECT * FROM mesh.mesh_suggestions WHERE status='pending';"
```