## 2026-08-12 A2A Mesh Status — v0.29.0

### Nova (192.168.1.8 / Mac Pro)
- **Status:** running, router
- **Version:** 0.29.0
- **Uptime:** ~67800s (18.8h)
- **Transports:** PG=True, P2P=True, HTTP=True, BLE=True
- **P2P TLS:** mTLS + TLSv1.3
- **Role:** router (coordinator election: none)
- **Peers:** 2/2 connected (morzsa, runa)
- **Config:** mesh_config_nova.yaml (tls_enabled: true, topology_tuning: enabled)

### Morzsa (192.168.1.30 / OpenClaw)
- **Status:** running, router
- **Version:** 0.29.0
- **Transports:** PG=True, P2P=True, HTTP=True
- **P2P:** connected_s=2725s, RTT=0ms, batch_size=32, frame_version=3
- **Tailscale:** 100.65.232.47 (idle)

### Runa (192.168.1.100 / Ubuntu VM)
- **Status:** running, router
- **Version:** 0.29.0
- **Transports:** PG=True, P2P=True, HTTP=True
- **Monitoring:** Prometheus:9090 + Grafana:3030 + Alertmanager:9093
- **Gitea:** v1.27.0 (port 3001), nginx proxy on 80/443
- **Tailscale:** 100.125.223.24 (idle)
- **Note:** connected_s=0 in peer info (fresh reconnect)

### Infrastructure
- **Brain MCP:** v2.0.0, uptime=341730s (3.95 days)
- **PG:** 192.168.1.30:5432/agent_memory (shared)
- **A2A messages:** 24,828 total, 1,652 unread
- **Heartbeats:** nova (4s), morzsa (13s) — both healthy
- **Shared tables:** shared_a2a_memory, shared_files, shared_steer

### Cron Jobs (Nova)
- `*/2 * * * *` gateway_watchdog.py --node nova
- `*/10 * * * *` session_cleanup.py --node nova

### Launchd Services (Nova)
- com.a2a-mesh.nova (running, PID 724)
- com.a2a-mesh.caddy (running, PID 724)
- com.a2a-mesh.gitea-ssh-proxy (running, PID 739)
- com.hermes.a2a-heartbeat (loaded)
- com.hermes.nova-a2a-watcher (running, PID 748)

### v0.29.0 Features
- Full installer (install.sh — 7-step, interactive + CLI)
- Health score PG persistence (mesh_health_history)
- Delegation feedback loop (success/fail → health score)
- Provider status integration (heartbeat → health scorer)
- Gateway watchdog (cron 2min)
- Session cleanup (cron 10min)
- Diagnostics suggestions PG persistence
- Vector memory search in learning loop
- Compressed P2P frames (v3, zlib)
- Topology auto-tuning (health scorer based)
- Gossipsub (flood_threshold: 6)
- Workflow DAG v3 (conditional branching, retry policy)

### Recent Fixes (Aug 8-11)
- Diagnostics cooldown for restart suggestions + prefix dedup
- HEALTH_CHECK_TIMEOUT 30→90s (eliminates spurious health probe warnings)
- Delegation poll crash loop fix (pg_pool None guard)
- Version field in /api/status + skill_advertiser import fix
- Config sync: gossipsub all nodes, TLS+diagnostic_channel+wake_agent

### Mesh Topology
  nova (0x1E54, router)
    runa (0x622E, router)
    morzsa (0xE984, router)

### Open Issues (Gitea)
- #2: Health endpoint fails on lennie node (v0.13.0) — stale, lennie offline
- #3: Lennie diagnostic report — stale, lennie offline

### Known Limitations
- Runa SSH not accessible from Nova (pubkey auth not configured)
- Gitea nginx X-Forwarded-Proto fix pending (needs SSH access to Runa)
- mesh_config.yaml contains runa config (not used by Nova, but confusing)
- Lennie node offline (Windows, last seen 6+ days ago via Tailscale)
