## 2026-08-15 A2A Mesh Status — v0.29.0

Generated: 2026-08-15 07:15 (automated reggeli check)

### Nova (localhost)
- **Status:** running, router
- **Version:** 0.29.0
- **Uptime:** ~4288s (1.2h)
- **Transports:** PG=True, P2P=True, HTTP=True
- **P2P TLS:** mTLS + TLSv1.3
- **Peers:** 2/2 connected
  - morzsa: p2p=True pg=True http=True ver=0.29.0
  - runa: p2p=True pg=True http=True ver=0.29.0
- **Health Scorer:**
  - runa: score=1.0 requests=0 failures=0
  - morzsa: score=1.0 requests=25 failures=1

### Morzsa (192.168.1.30)
- **Status:** running, router
- **Version:** 0.29.0
- **Uptime:** ~3962s (1.1h)
- **Transports:** PG=True, P2P=True, HTTP=True
- **P2P TLS:** mTLS + TLSv1.3
- **Peers:** 2/2 connected
  - nova: p2p=True pg=True http=True ver=0.29.0
  - runa: p2p=True pg=True http=True ver=0.29.0
- **Health Scorer:**
  - nova: score=1.0 requests=0 failures=0
  - morzsa: score=1.0 requests=19 failures=1
  - runa: score=1.0 requests=0 failures=0

### Runa (192.168.1.100)
- **Status:** running, router
- **Version:** 0.29.0
- **Uptime:** ~3957s (1.1h)
- **Transports:** PG=True, P2P=True, HTTP=True
- **P2P TLS:** mTLS + TLSv1.3
- **Peers:** 2/2 connected
  - nova: p2p=True pg=True http=True ver=0.29.0
  - morzsa: p2p=True pg=True http=True ver=0.29.0
- **Health Scorer:**
  - nova: score=1.0 requests=0 failures=0
  - morzsa: score=1.0 requests=25 failures=1
  - runa: score=1.0 requests=0 failures=0

### Mesh-wide
- **Transport priority:** p2p (primary), pg_notify, http
- **mDNS:** code implemented, zeroconf v0.150.0 installed, auto-enabled on non-Docker hosts
- **Topology tuning:** enabled (check_interval=300s, auto_apply=false)
- **DLQ:** 0
- **Diagnostic suggestions:** 0 (1111 stale cleaned in last review)
- **Gitea:** v1.27.0 on Runa:3001, nginx X-Forwarded-Proto config ready (deploy/nginx-gitea.conf)
- **Latest release:** v0.29.0 (no newer version available)

### Development tasks (from Zsolt roadmap)
- [x] P2P-first design — p2p is primary transport
- [x] TLS P2P — mTLS + TLSv1.3 enabled on all nodes
- [x] Topology auto-tuning — health scorer based, enabled
- [~] mDNS auto-discovery — code ready, needs runtime verification
- [ ] Gitea nginx X-Forwarded-Proto fix on Runa — config ready, needs deployment
- [x] Runa mesh v0.29.0 (no newer version available)
