## 2026-07-23 A2A Mesh Status

### Nova (192.168.1.8 / Mac Pro)
- **Status:** running, coordinator
- **Version:** 0.18.91
- **Transports:** PG=True, P2P=True, HTTP=True, BLE=True
- **P2P TLS:** Config ready (tls_enabled=true in config, requires restart)
- **P2P Peers:** morzsa, runa, lennie (all connected)
- **Role:** coordinator (assigned 2026-07-23)

### Morzsa (192.168.1.30 / OpenClaw)
- **Status:** running, router
- **Version:** 0.18.92 (auto-updated)
- **Transports:** PG=True, P2P=True, HTTP=True, BLE=False
- **P2P TLS:** Config ready (requires restart)
- **P2P Peers:** nova, runa (lennie: backoff)
- **Health:** WARNING high load (13.97), swap 7.9G/8G

### Runa (192.168.1.100 / Linux)
- **Status:** running, router
- **Version:** 0.18.91
- **Transports:** PG=True, P2P=True, HTTP=True
- **P2P TLS:** Config ready (requires restart)
- **P2P Peers:** nova, morzsa

### Lennie (100.121.92.95 / Lenovo)
- **Status:** active but health endpoint unreachable
- **Transports:** P2P=True, PG=True, HTTP=False
- **Health:** WARNING CPU 100%, Memory 97.5%

### Mesh Topology
  nova (0x1E54, coordinator)
    lennie (0x0000, end_device)
    runa (0x622E, router)
    morzsa (0xE984, router)
