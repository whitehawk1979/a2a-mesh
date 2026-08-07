# A2A Mesh v0.29.0

Decentralizált, P2P agent mesh hálózat — autonóm agent-ek közötti kommunikáció, delegáció és health monitoring.

## Rendszerkövetelmények

- Python 3.9+
- PostgreSQL 14+ (shared mesh DB)
- Tailscale VPN (ajánlott P2P kapcsolatokhoz)
- Linux (systemd) vagy macOS (launchd)

---

## Telepítési útmutató

### 1. Repó klónozás

```bash
git clone http://192.168.1.100:3001/nova/a2a-mesh.git ~/a2a_mesh
cd ~/a2a_mesh
git checkout v0.29.0
```

### 2. Installer futtatása

#### Interaktív mód (kérdezget):

```bash
./install.sh
```

#### Automatikus mód (CLI argumentumokkal):

```bash
./install.sh \
  --node nova \
  --host 192.168.1.50 \
  --pg-host 192.168.1.30 \
  --pg-user nova \
  --pg-password 'titkos_jelszo' \
  --pg-db agent_memory \
  --pg-init
```

#### Opciók:

| Opció | Leírás | Default |
|-------|--------|---------|
| `--node NAME` | Node neve (pl. nova, morzsa, runa) | kérdez |
| `--host IP` | Node IP címe (LAN vagy Tailscale) | auto-detect |
| `--pg-host IP` | PostgreSQL host | kérdez |
| `--pg-port PORT` | PostgreSQL port | 5432 |
| `--pg-user USER` | PostgreSQL felhasználó | nova |
| `--pg-password PW` | PostgreSQL jelszó | kérdez |
| `--pg-db NAME` | PostgreSQL adatbázis | agent_memory |
| `--pg-init` | PG schema inicializálás (schema_init.sql) | false |
| `--config FILE` | Config fájl útvonala | mesh_config_\<node\>.yaml |
| `--skip-venv` | System Python használata | false |
| `--skip-certs` | TLS cert generálás kihagyása | false |
| `--skip-service` | systemd/launchd kihagyása | false |
| `--skip-cron` | Cron job-ok kihagyása | false |

### 3. Az installer 7 lépése

| Lépés | Mit csinál |
|-------|-----------|
| 1 | **Python** — Python 3.9+ detektálás, venv létrehozás |
| 2 | **Dependencies** — `pip install -r requirements.txt` (aiohttp, asyncpg, msgpack, PyYAML, zeroconf, cryptography) |
| 3 | **PostgreSQL** — Schema inicializálás (`schema_init.sql`) vagy connection test |
| 4 | **Config** — `mesh_config_<node>.yaml` generálás a template-ből |
| 5 | **TLS Certs** — `certs/` mappa, CA + node cert + key |
| 6 | **Service** — macOS: LaunchAgent, Linux: systemd user service |
| 7 | **Cron** — Watchdog (2min) + Session cleanup (10min) |

### 4. PostgreSQL Schema

Az installer `--pg-init` kapcsolóval automatikusan lefuttatja a `schema_init.sql`-t, ami létrehozza a következő táblákat:

| Tábla | Leírás |
|-------|--------|
| `mesh.mesh_nodes` | Node regisztráció, heartbeat, provider_status (JSONB) |
| `mesh.mesh_messages` | Agent-ek közötti üzenetek (JSONB payload) |
| `mesh.mesh_tasks` | Delegált feladatok (status, result, timestamps) |
| `mesh.mesh_health_history` | Health score történet (v0.29.0) |
| `mesh.mesh_suggestions` | Diagnostics javaslatok (v0.29.0) |
| `mesh.mesh_events` | Audit log |

Kézi inicializálás (ha szükséges):

```bash
psql -h <pg-host> -U nova -d agent_memory -f schema_init.sql
```

### 5. Konfiguráció

Az installer a `mesh_config_template.yaml`-ból generálja a `mesh_config_<node>.yaml` fájlt, helyettesítve a placeholder-eket:

- `__NODE_NAME__` → node neve
- `__NODE_HOST__` → node IP címe
- `__PG_HOST__` → PostgreSQL host
- `__PG_PASSWORD__` → PostgreSQL jelszó

A config tartalmazza:

- **mesh** — node név, transport priority, capabilities, skills
- **network** — host, port-ok, TLS beállítások
- **postgresql** — kapcsolat a shared DB-hez
- **security** — signing key (auto-generated)
- **health_monitor** — health check interval
- **watchdog** — auto-restart beállítások
- **provider_health** — LLM provider (Ollama) státusz
- **learning_loop** — HealthScorer + diagnostics beállítások

Kézi szerkesztés a telepítés után:

```bash
nano ~/a2a_mesh/mesh_config_<node>.yaml
```

### 6. TLS Certifikátumok

Az installer a `generate_certs.py` scripttel generálja a TLS cert-eket:

```
certs/
├── ca.crt          # CA certificate
├── ca.key          # CA private key
├── node.crt        # Node certificate
├── node.key        # Node private key
└── ca.srl          # Serial number file
```

Kézi generálás (ha szükséges):

```bash
python3 generate_certs.py --output certs/
```

### 7. System Service

#### macOS (LaunchAgent):

```bash
# Automatikus (installer futtatja):
launchctl load ~/Library/LaunchAgents/com.hermes.a2a-mesh-node.plist

# Kézi indítás/stop:
launchctl start com.hermes.a2a-mesh-node
launchctl stop com.hermes.a2a-mesh-node

# Logok:
tail -f ~/.hermes/logs/a2a_mesh_node.log
```

#### Linux (systemd):

```bash
# Automatikus (installer futtatja):
systemctl --user enable a2a-mesh
systemctl --user start a2a-mesh

# Kézi indítás/stop:
systemctl --user start a2a-mesh
systemctl --user stop a2a-mesh

# Logok:
journalctl --user -u a2a-mesh -f
```

#### Manuális indítás (service nélkül):

```bash
python3 cli.py start --name nova --config mesh_config_nova.yaml
```

### 8. Cron Job-ok

Az installer automatikusan beállítja:

| Cron | Gyakoriság | Mit csinál |
|------|-----------|------------|
| `gateway_watchdog.py` | 2 perc | Node health endpoint ellenőrzés, auto-restart ha nem válaszol |
| `session_cleanup.py` | 10 perc | Elhagyott session-ök takarítása |

Kézi hozzáadás (ha szükséges):

```bash
crontab -e
# Adj hozzá:
*/2 * * * * /path/to/python3 /path/to/a2a_mesh/core/gateway_watchdog.py --node <name> 2>&1
*/10 * * * * /path/to/python3 /path/to/a2a_mesh/core/session_cleanup.py --node <name> 2>&1
```

### 9. Több node hálózat építése

A mesh mag kialakításához legalább 2 node kell, de optimálisan 3+:

```bash
# Node 1 (Nova — macOS)
./install.sh --node nova --host 100.75.253.52 \
  --pg-host 192.168.1.30 --pg-user nova --pg-password 'pw' --pg-init

# Node 2 (Morzsa — Linux)
./install.sh --node morzsa --host 192.168.1.30 \
  --pg-host 192.168.1.30 --pg-user nova --pg-password 'pw'
  # --pg-init nem kell, már inicializálva

# Node 3 (Runa — Linux)
./install.sh --node runa --host 192.168.1.100 \
  --pg-host 192.168.1.30 --pg-user nova --pg-password 'pw'
```

Minden node ugyanazt a PG adatbázist használja. A node-ok automatikusan felfedezik egymást P2P-n (Tailscale) és PG-n keresztül.

### 10. Ellenőrzés

```bash
# Node health
curl http://localhost:8650/health | python3 -m json.tool

# Peerek listázása
python3 cli_mesh.py peers

# Health history PG-ben
psql -h <pg-host> -U nova -d agent_memory -c \
  "SELECT node_name, health_score, provider_primary FROM mesh.mesh_health_history ORDER BY recorded_at DESC LIMIT 10;"

# Diagnostics suggestions
psql -h <pg-host> -U nova -d agent_memory -c \
  "SELECT * FROM mesh.mesh_suggestions WHERE status='pending';"

# Regisztrált node-ok
psql -h <pg-host> -U nova -d agent_memory -c \
  "SELECT node_name, status, version, provider_status->'primary'->>'status' as primary FROM mesh.mesh_nodes;"
```

### Hibaelhárítás

| Probléma | Megoldás |
|----------|---------|
| Node nem indul | `tail -f ~/.hermes/logs/a2a_mesh_node.log` (macOS) vagy `journalctl --user -u a2a-mesh` (Linux) |
| PG connection failed | Ellenőrizd a jelszót, host-ot, port-ot a config-ban |
| P2P nem connect | Tailscale fut? `tailscale status` |
| Health port nem válaszol | Várj 10-20s indulás után, vagy `launchctl stop/start` |
| Import error | `pip install -r requirements.txt` újra a venv-ben |
| Cert hiányzik | `python3 generate_certs.py --output certs/` |

---

## Architektúra

```
┌──────────────┐     P2P (TLS)     ┌──────────────┐
│   Node A     │◄────────────────►│   Node B     │
│  (Python)    │                  │  (Python)    │
│  Router       │                  │  Router      │
│  HealthScorer│                  │  HealthScorer│
│  Diagnostics  │                  │  Diagnostics │
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
├── install.sh                    # Full installer (7 lépés)
├── schema_init.sql               # PG schema init (6 tábla)
├── requirements.txt              # Python dependencies
├── mesh_config_template.yaml      # Config template
├── README.md                      # Ez a fájl
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