#!/bin/bash
# a2a-mesh tor keepalive — container-restart tolerant monitor.
#
# What survives nothing on a container restart (both are re-launched here):
#   1. embedded sshd on :2222  (peers' outbound SSH -> tor)
#   2. reverse SSH tunnel tor->morzsa (18645/18650) — morzsa tailscale is
#      userspace/dead, so this is morzsa's ONLY path to tor P2P/health.
#
# What is NOT container-scoped (managed on the HAOS HOST, survives container
# restart, must be re-added after a HOST reboot / tailscale re-init):
#   - iptables DNAT  tailscale-IP:2222 -> container, inserted at position 3
#     (BEFORE the tailscale catch-all rule). The container has no iptables,
#     so this script cannot manage it; it is logged as a host-side reminder.
#
# Idempotent (lock file). Loop 5s. Log: /config/a2a-mesh-keepalive.log.
#
# v2 2026-09-02: added ensure_mesh_node() — restarts the tor a2a-mesh node if
# it died or if /health shows pg=false (HOME env bug left it PG-less after the
# container restart on 2026-09-01). Also fixes the zombie '-L' tunnel flood
# that exhausted morzsa sshd MaxStartups.

LOCK=/tmp/tor_keepalive.lock
LOG=/config/a2a-mesh-keepalive.log
MORZSA_HOST=192.168.1.30
TUNNEL_IDENTITY=/config/.ssh/id_ed25519

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" >> "$LOG"; }

# idempotent guard — if a live instance holds the lock, exit
if [ -e "$LOCK" ] && kill -0 "$(cat "$LOCK" 2>/dev/null)" 2>/dev/null; then
  exit 0
fi
echo $$ > "$LOCK"
log "keepalive started (pid $$)"

ensure_sshd() {
  if ! ss -ltn 2>/dev/null | grep -q ':2222 '; then
    log "ACTION: sshd :2222 down -> starting"
    mkdir -p /run/sshd
    /usr/sbin/sshd -p 2222 2>>"$LOG" && log "sshd :2222 started" || log "ERROR: sshd start failed"
  fi
}

ensure_morzsa_tunnel() {
  # no local tunnel process -> start
  if ! ps aux | grep -E 'R 127.0.0.1:18645' | grep -v grep >/dev/null; then
    log "ACTION: morzsa reverse tunnel down -> starting (LAN :22)"
    start_tunnel
    return
  fi
  # zombie detection: process alive but the remote listener is gone
  # (a -R forward that failed once leaves the ssh process alive but dead)
  if ! ssh -i "$TUNNEL_IDENTITY" -o BatchMode=yes -o ConnectTimeout=8 \
       -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
       "openclaw@$MORZSA_HOST" 'ss -ltn 2>/dev/null | grep -q ":18645 " && ss -ltn 2>/dev/null | grep -q ":18222 "' 2>/dev/null; then
    log "ACTION: tunnel process alive but remote listener missing (18645/18222) -> killing + restarting"
    pkill -f 'R 127.0.0.1:18645' 2>/dev/null
    pkill -f 'R 127.0.0.1:18222' 2>/dev/null
    sleep 1
    start_tunnel
  fi
}

start_tunnel() {
  setsid nohup ssh -N \
    -R 127.0.0.1:18645:127.0.0.1:8645 \
    -R 127.0.0.1:18650:127.0.0.1:8650 \
    -R 127.0.0.1:18222:127.0.0.1:2222 \
    -p 22 \
    -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
    -o ServerAliveInterval=30 -o ServerAliveCountMax=3 \
    -o ExitOnForwardFailure=no -o BatchMode=yes \
    -i "$TUNNEL_IDENTITY" \
    "openclaw@$MORZSA_HOST" >> "$LOG" 2>&1 &
}


ensure_mesh_node() {
  # restart the a2a-mesh node if the process is gone or PG transport is down
  if ! ps aux | grep -E 'cli\.py start --name tor' | grep -v grep >/dev/null; then
    log "ACTION: tor mesh node down -> starting"
    ( cd /config/a2a-mesh && setsid nohup env HOME=/config \
      ./.venv/bin/python3 ./cli.py start --name tor --config ./mesh_config_haos.yaml \
      >> data/tor_node.log 2>&1 & )
    return
  fi
  # PG transport health gate (prevents silent pg=false zombie nodes)
  PG_OK=$(curl -s -m 5 http://127.0.0.1:8650/health 2>/dev/null | grep -o '"pg": *[a-z]*' | head -1)
  if [ -n "$PG_OK" ] && ! echo "$PG_OK" | grep -q true; then
    log "ACTION: tor mesh node pg=false -> restarting"
    pkill -f 'cli\.py start --name tor' 2>/dev/null
    sleep 3
    ( cd /config/a2a-mesh && setsid nohup env HOME=/config \
      ./.venv/bin/python3 ./cli.py start --name tor --config ./mesh_config_haos.yaml \
      >> data/tor_node.log 2>&1 & )
  fi
}

while true; do
  ensure_sshd
  ensure_morzsa_tunnel
  ensure_mesh_node
  sleep 5
done
