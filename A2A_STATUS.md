## 2026-08-04 A2A Mesh Status — v0.23.0

### Nova (192.168.1.8 / Mac Pro)
- **Status:** running, coordinator
- **Version:** 0.22.0 → 0.23.0 (restart needed)
- **Transports:** PG=True, P2P=True, HTTP=True, BLE=True
- **P2P TLS:** mTLS + HMAC-SHA256, TLSv1.3
- **Role:** coordinator

### Morzsa (192.168.1.30 / OpenClaw)
- **Status:** running, router
- **Version:** 0.22.0 → 0.23.0 (auto-update)
- **Transports:** PG=True, P2P=True, HTTP=True, BLE=False

### Runa (192.168.1.100 / Linux)
- **Status:** running, router
- **Version:** 0.22.0 → 0.23.0 (auto-update)
- **Transports:** PG=True, P2P=True, HTTP=True
- **Monitoring:** Prometheus:9090 + Grafana:3030 + Alertmanager:9093

### v0.23.0 Újdonságok
- **Telegram alerting:** Prometheus Alertmanager → webhook → Telegram
- **Node restart CLI:** `a2a restart <nova|morzsa|runa|all>`
- **Dashboard alert panel:** 🚨 tab, Prometheus alerts real-time
- **Plugin SDK dokumentáció:** docs/PLUGIN_SDK.md
- **Mesh backup/restore CLI:** `a2a backup -o <dir>` / `a2a restore <path>`
- **Detached HEAD fix:** auto-updater main ágon marad
- **Tesztek javítva:** 502 passed, 0 failed

### Monitoring
- Prometheus: 3/3 target UP, 8 alert rules, 3 groups
- Grafana: v13.1.1, 2 dashboard, auto-provisioned
- Alertmanager: webhook → Telegram (port 9091 on Runa)

### Mesh Topology
  nova (0x1E54, coordinator)
    runa (0x622E, router)
    morzsa (0xE984, router)

### Testing
- 502 tests passed, 0 failed, 2 skipped
- 510 total test cases