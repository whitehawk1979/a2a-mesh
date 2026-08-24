## 2026-08-23 A2A Mesh Status — v0.37.4

Generated: 2026-08-23 07:05 (automated reggeli check)

### Nova (192.168.1.8 — localhost)
- **Status:** running, router
- **Version:** v0.37.4
- **Uptime:** ~25116s (7h)
- **Transports:** P2P=True, PG=True, HTTP=True, BLE=True, SSH-tunnel=True
- **P2P TLS:** mTLS + TLSv1.3, tls_verify_peer=true
- **Peers:** 2/2 connected
  - morzsa: p2p=✓ pg=✓ http=✓
  - runa: p2p=✓ pg=✓ http=✓

### Morzsa (192.168.1.30)
- **Status:** running, router
- **Version:** v0.37.4
- **Uptime:** ~24655s (6.8h)
- **Transports:** P2P=True, PG=True, HTTP=True, SSH-tunnel=True
- **P2P TLS:** mTLS + TLSv1.3, tls_verify_peer=true
- **Peers:** 2/2 connected
  - nova: p2p=✓ pg=✓ http=✓
  - runa: p2p=✓ pg=✓ http=✓

### Runa (192.168.1.100)
- **Status:** running, router
- **Version:** v0.37.4
- **Uptime:** ~17600s (4.9h)
- **Transports:** P2P=True, PG=True, HTTP=True, SSH-tunnel=True
- **P2P TLS:** mTLS + TLSv1.3, tls_verify_peer=true
- **Peers:** 2/2 connected
  - nova: p2p=✓ pg=✓ http=✓
  - morzsa: p2p=✓ pg=✓ http=✓
- **Gitea:** v1.27.0 on :3001 — nginx proxy deployed (X-Forwarded-Proto fix)
- **SSH:** Intermittently unreachable — mesh unaffected

### Mesh-wide
- **Transport priority:** p2p → pg_notify → http (P2P-first)
- **TLS P2P:** mTLS + TLSv1.3 on all nodes
- **mDNS:** zeroconf v0.150.0 installed, UDP listening
- **Topology tuning:** enabled (auto_apply=false)
- **Health Scorer:** active on all nodes
- **DLQ:** 0
