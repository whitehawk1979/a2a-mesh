## 2026-09-11 A2A Mesh Status — v0.42.3

Generated: 2026-09-11 07:25 (automated reggeli check)

### Nova (192.168.1.8 — MacBook, localhost)
- **Status:** running, router, addr 0x1E54
- **Version:** v0.42.3-9-gbab8ae1 (git SSOT, main@556bb6c)
- **Transports:** PG=✅ P2P=✅ HTTP=✅ BLE=✅
- **Messages:** 122 sent / 1348 received, 1 failed ACK (Tor restart körül)
- **SSH tunnelek:** morzsa, runa, tor(×2 HAOS) — mind él

### Morzsa (192.168.1.30 — PG primary)
- **Status:** running, router, addr 0xE984, parent=nova
- **Version:** v0.42.3-9-gbab8ae1 ✅ sync
- **Transports:** PG=✅ P2P=✅ HTTP=✅ BLE=❌
- **PG:** 192.168.1.30:5432 active, mesh_node_health mind 5 node fresh

### Runa (192.168.1.100)
- **Status:** running, router, addr 0x622E
- **Version:** v0.42.3-9-gbab8ae1 ✅ sync
- **Transports:** PG=✅ P2P=✅ HTTP=✅ BLE=❌
- **Gitea:** nginx proxy OK, X-Forwarded-Proto beállítva, :3001/:80 egyaránt 200
- **mesh-llm:** v0.72.1 (AVX build), :9337 él, 1 modell (qwen2.5-3b)
- **SSH:** zsolt@192.168.1.100 id_ed25519_openclaw kulccsal OK

### Tor + Mano (192.168.1.43 — HAOS)
- **Tor:** running, root, addr 0x77A9, uptime ~35min (03:33 restart, magából helyreállt), PG=✅ P2P=✅ HTTP=❌
- **Mano:** running, addr 0x1EAD, uptime ~12.9h, PG=✅ P2P=✅ HTTP=✅, 74 failed ACK (tor restart vihar)
- **Konténer:** app_0a6523c6_hermes_agent, cli a .venv/bin/python3-val futtatható (host python3 nem!)

### Mesh-wide
- **Node restart 06:25-06:36:** Hermes Agent auto-update (122 commit) — koordinált, nem hiba
- **Transport priority:** p2p → ssh_tunnel → pg_notify → http (P2P-first)
- **TLS P2P:** mTLS + TLSv1.3 minden node-on
- **mDNS:** discovery/mdns.py aktív (_a2a._tcp), mDNS + static discovery
- **Topology tuning:** topology_tuner.py + health_scorer.py aktív (enabled, promote 0.9 / demote 0.3)
- **Cron job-ok:** 17/17 enabled, nova-heartbeat OK (silent=success), update-check: nincs új verzió
- **DLQ:** 0 pending
