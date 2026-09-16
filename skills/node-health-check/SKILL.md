---
name: node-health-check
description: Mesh node egeszseg ellenorzes: CPU, memoria, disk, service status, Tailscale connectivity.
tags: [monitoring, health, devops, diagnostics]
---

# Node Health Check

Gyors egeszseg ellenorzes mesh node-okon.

## Node-ok
- Nova: macOS (local)
- Morzsa: Linux 192.168.1.30 (openclaw@)
- Runa: Linux 192.168.1.100 (zsolt@)

## Ellenorzendo metrikak
1. **Service status**: `systemctl --user status a2a-mesh` (Linux) / `launchctl list | grep hermes` (macOS)
2. **Disk**: `df -h /` - Runa 360G, figyelni ha >85%
3. **CPU/Mem**: `top -bn1 | head -5` (Linux) / `top -l1 -n0` (macOS)
4. **Tailscale**: `tailscale status` - minden node erheto?
5. **Mesh port**: `curl -s http://localhost:8650/api/status` - API valaszol?

## Proxmox VM
- 192.168.1.60 root/2009December16
- Runa VM ID=130, qm guest exec: `qm guest exec 130 ...`
- Disk expand: qm resize + growpart + pvresize + lvextend + resize2fs

## WoL
- MikroTik router: `/tool wol interface=bridge mac=MAC`
