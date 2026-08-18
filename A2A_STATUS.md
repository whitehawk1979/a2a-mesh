## 2026-08-18 A2A Mesh Status — v0.29.0

Generated: 2026-08-18 07:10 (automated reggeli check)

### Nova (192.168.1.8 — localhost)
- **Status:** running, router
- **Version:** v0.29.0-88-g2a19489 (88 commits ahead of tag)
- **Uptime:** ~54min (started 06:57 AM)
- **Transports:** P2P=True, PG=True, HTTP=True, SSH-tunnel=True
- **P2P TLS:** mTLS + TLSv1.3
- **Peers:** 2/2 connected
  - morzsa: p2p=✓ pg=✓ http=✓ — SSH tunnel heartbeat ACK confirmed
  - runa: p2p=✓ pg=✓ http=✓ — SSH tunnel heartbeat ACK confirmed
- **Discovery:** PG discovery cycle ~30s, 0 new peers, 2 known total
- **Heartbeat:** interval=60s, Brain MCP age=4s ✅

### Morzsa (192.168.1.30)
- **Status:** running, router
- **Version:** 0.29.0
- **Uptime:** ~431s (7.2min — recently restarted)
- **Transports:** PG=True, P2P=True, HTTP=True, SSH-tunnel=True
- **P2P TLS:** mTLS + TLSv1.3
- **Peers:** 2/2 connected
  - nova: p2p=✓ pg=✓ http=✓
  - runa: p2p=✓ pg=✓ http=✓
- **Heartbeat:** Brain MCP age=49s ✅

### Runa (192.168.1.100)
- **Status:** P2P active (heartbeat ACK confirmed), Gitea API v1.27.0 alive on :3001
- **Transports:** P2P=✓ (via SSH tunnel + direct), PG=✓ (shared), HTTP health port 8650
- **P2P TLS:** mTLS + TLSv1.3
- **Peers:** 2/2 connected (nova + morzsa, confirmed by P2P ACK log)
- **Note:** SSH to Runa timed out — host may have firewall/SSH config issue, but mesh traffic flows via P2P + HTTP
- **Gitea:** v1.27.0 on :3001 — nginx proxy NOT yet deployed (X-Forwarded-Proto fix pending)

### Mesh-wide
- **Transport priority:** p2p (primary), pg_notify (fallback), http (last resort) ✅
- **P2P shortcut logic:** implemented in router.py — direct peer connection skips PG entirely
- **mDNS:** code implemented (discovery/mdns.py), zeroconf installed, config enabled=true
  - No mDNS log entries seen — may be running silently or not initialized at startup
- **TLS P2P:** mTLS + TLSv1.3 enabled on all nodes ✅
- **Topology tuning:** enabled (check_interval=300s, auto_apply=false) ✅
- **Health Scorer:** active — records success/failure per transport/recipient
- **DLQ:** 0
- **Latest release:** v0.29.0 (no newer tag available, 88 commits ahead on main)
- **Cron jobs (Nova):** gateway_watchdog (2min), session_cleanup (10min) — both active

### Development tasks (from Zsolt roadmap)
- [x] P2P-first design — p2p is primary transport, P2P shortcut bypasses PG for direct peers
- [x] TLS P2P — mTLS + TLSv1.3 enabled on all nodes
- [x] Topology auto-tuning — health scorer based, enabled
- [~] mDNS auto-discovery — code ready, zeroconf installed, needs runtime verification
- [ ] Gitea nginx X-Forwarded-Proto fix on Runa — config ready in deploy/, SSH to Runa timed out, cannot deploy
- [x] Runa mesh v0.29.0 (no newer version available)
