#!/bin/bash
# A2A Mesh DB Retention Script — runs daily via cron
# Cleans up old data from mesh DB to prevent unbounded growth
#
# Retention policy:
#   mesh_messages:       7 days (delivered/read/acknowledged/sent)
#   mesh_messages ack:  48 hours (transport confirmations; wake-storm 2026-09-04
#                        generated 67K self-acks / 1.5 GB TOAST in hours)
#   mesh_messages heartbeat: 24 hours (99% of table volume; 4 nodes x 30s = 3GB/day at 7d equilibrium)
#   mesh_debug_logs:     3 days (all levels)
#   mesh_suggestions:    7 days (superseded status only)
#   shared_dlq:          7 days (processed entries)
#   shared_a2a_memory:   7 days (read/acknowledged/sent/delivered/archived)
#   mesh_health_history: 7 days (all entries)
#
# Usage: bash scripts/db_retention.sh
# Cron:  0 4 * * * bash ~/.hermes/scripts/a2a_mesh/scripts/db_retention.sh >> ~/.hermes/logs/mesh_retention.log 2>&1

set -euo pipefail

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
}

# PG_HOST: Tailscale IP has priority — the macOS Local Network (TCC) permission blocks the unsigned Homebrew psql towards LAN IPs (192.168.1.x), while the utun5 interface is exempt.
# 2026-08-30: the daily cron has been silently failing since its setup due to this!
# 2026-09-01: tailscaled died on Morzsa (LXC, missing /dev/net/tun after Proxmox kernel update) -> TS IP unreachable, psql hung 60s+ with NO connect timeout.
#   Fix: try each host with pg_isready (5s probe), fall back to LAN 192.168.1.30. PGCONNECT_TIMEOUT caps every psql call at 10s.
PG_PORT="${PG_PORT:-5432}"
PG_DB="${PG_DB:-agent_memory}"
PG_USER="${PG_USER:-nova}"
export PGPASSWORD="${PGPASSWORD:-nova_agent_2026}"
export PGCONNECT_TIMEOUT="${PGCONNECT_TIMEOUT:-10}"

PG_HOSTS=("${PG_HOST:-100.65.232.47}" "192.168.1.30")
PG_HOST=""
for _h in "${PG_HOSTS[@]}"; do
    if pg_isready -h "$_h" -p "$PG_PORT" -t 5 >/dev/null 2>&1; then
        PG_HOST="$_h"
        break
    fi
done
if [[ -z "$PG_HOST" ]]; then
    log "ERROR: no reachable PG host (${PG_HOSTS[*]}) — aborting."
    exit 1
fi
log "Using PG host: $PG_HOST"


log "=== A2A Mesh DB Retention ==="

# 0. Delete old heartbeats (>24 hours) — they are 99% of mesh_messages volume
#    4 nodes x 30s heartbeat = ~11.5K rows/day/node; 7d retention let the table grow to 12GB.
# 0. Ensure retention index (idempotent, fast after first cleanup)
# 2026-09-03: CREATE INDEX IF NOT EXISTS acquires ShareLock on the table even when the index
# already exists — today it queued behind a VACUUM and blocked ALL mesh INSERTs for 8+ minutes
# (heartbeats stalled on Tor/Morzsa). Guard: only run CREATE INDEX when it's actually missing.
log "Ensuring retention index on mesh_messages (msg_type, created_at)..."
IDX_EXISTS=$(psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DB" -t -A -c "SELECT 1 FROM pg_indexes WHERE schemaname='mesh' AND tablename='mesh_messages' AND indexname='idx_mesh_messages_retention';" 2>/dev/null)
if [[ "$IDX_EXISTS" == "1" ]]; then
    log "  retention index already present — skipping CREATE INDEX (avoids ShareLock blocking mesh traffic)"
else
    psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DB" -c "CREATE INDEX IF NOT EXISTS idx_mesh_messages_retention ON mesh.mesh_messages (msg_type, created_at);" 2>&1 | head -1
fi

log "Cleaning mesh_messages heartbeats (>24h)..."
DELETED=$(psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DB" -t -c "
    DELETE FROM mesh.mesh_messages 
    WHERE msg_type = 'heartbeat' 
    AND created_at < now() - interval '24 hours';
" 2>&1 | head -1)
log "  mesh_messages heartbeats: $DELETED rows deleted"

# 0b. Same for diagnostic noise types (>48h)
log "Cleaning mesh_messages skills/diagnostic noise (>48h)..."
DELETED=$(psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DB" -t -c "
    DELETE FROM mesh.mesh_messages 
    WHERE msg_type IN ('skills_announcement', 'diagnostic_report', 'config_suggestion')
    AND created_at < now() - interval '48 hours';
" 2>&1 | head -1)
log "  mesh_messages skills/diag noise: $DELETED rows deleted"

# 1. Delete old mesh messages (>7 days)
log "Cleaning mesh_messages (>7 days)..."
DELETED=$(psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DB" -t -c "
    DELETE FROM mesh.mesh_messages 
    WHERE created_at < now() - interval '7 days' 
    AND status IN ('sent', 'delivered', 'read', 'acknowledged');
" 2>&1 | head -1)
log "  mesh_messages: $DELETED rows deleted"

# 1b. Delete old ACK messages (>48h, any status) — pure transport-layer confirmations.
#     AckTracker is in-memory only; after 48h an undelivered ack has no consumer.
#     Without this rule a wake-storm balloons the table (see header note).
log "Cleaning mesh_messages acks (>48h)..."
DELETED=$(psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DB" -t -c "
    DELETE FROM mesh.mesh_messages 
    WHERE msg_type = 'ack' 
    AND created_at < now() - interval '48 hours';
" 2>&1 | head -1)
log "  mesh_messages acks: $DELETED rows deleted"

# 2. Delete old debug logs (>3 days)
log "Cleaning mesh_debug_logs (>3 days)..."
DELETED=$(psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DB" -t -c "
    DELETE FROM mesh.mesh_debug_logs 
    WHERE created_at < now() - interval '3 days';
" 2>&1 | head -1)
log "  mesh_debug_logs: $DELETED rows deleted"

# 3. Delete superseded suggestions (>7 days)
log "Cleaning mesh_suggestions (superseded >7 days)..."
DELETED=$(psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DB" -t -c "
    DELETE FROM mesh.mesh_suggestions 
    WHERE status = 'superseded' 
    AND updated_at < now() - interval '7 days';
" 2>&1 | head -1)
log "  mesh_suggestions superseded: $DELETED rows deleted"

# 3b. Delete completed suggestions (>7 days) — auto-resolved suggestions
# accumulate indefinitely without this (observed: 445 completed rows, mostly
# disk-usage churn noise). Completed = issue auto-resolved, safe to purge.
log "Cleaning mesh_suggestions (completed >7 days)..."
DELETED=$(psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DB" -t -c "
    DELETE FROM mesh.mesh_suggestions 
    WHERE status = 'completed' 
    AND updated_at < now() - interval '7 days';
" 2>&1 | head -1)
log "  mesh_suggestions completed: $DELETED rows deleted"

# 4. Delete old DLQ entries (>7 days)
log "Cleaning shared_dlq (>7 days)..."
DELETED=$(psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DB" -t -c "
    DELETE FROM shared_dlq 
    WHERE created_at < now() - interval '7 days'
    AND status IN ('processed', 'expired', 'discarded');
" 2>&1 | head -1)
log "  shared_dlq: $DELETED rows deleted"

# 5. Delete old shared_context (>30 days)
log "Cleaning shared_context (>30 days)..."
DELETED=$(psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DB" -t -c "
    DELETE FROM shared_context 
    WHERE updated_at < now() - interval '30 days'
    AND (expires_at IS NULL OR expires_at < now());
" 2>&1 | head -1)
log "  shared_context: $DELETED rows deleted"

# 5b. Delete old shared_a2a_memory (>7 days, read/acknowledged/sent/delivered/archived)
log "Cleaning shared_a2a_memory (>7 days)..."
DELETED=$(psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DB" -t -c "
    DELETE FROM shared_a2a_memory 
    WHERE created_at < now() - interval '7 days'
    AND status IN ('sent', 'delivered', 'read', 'acknowledged', 'archived');
" 2>&1 | head -1)
log "  shared_a2a_memory: $DELETED rows deleted"

# 5c. Delete old mesh_health_history (>7 days)
log "Cleaning mesh_health_history (>7 days)..."
DELETED=$(psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DB" -t -c "
    DELETE FROM mesh.mesh_health_history 
    WHERE recorded_at < now() - interval '7 days';
" 2>&1 | head -1)
log "  mesh_health_history: $DELETED rows deleted"

# 6. Vacuum analyze (non-blocking, doesn't lock table)
log "Running VACUUM ANALYZE..."
psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DB" -c "VACUUM ANALYZE mesh.mesh_messages;" 2>&1 | head -1
psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DB" -c "VACUUM ANALYZE shared_a2a_memory;" 2>&1 | head -1
psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DB" -c "VACUUM ANALYZE mesh.mesh_health_history;" 2>&1 | head -1
log "  VACUUM ANALYZE done"

# 7. Report table sizes
log "Current table sizes:"
psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DB" -c "
    SELECT relname, n_live_tup as rows, pg_size_pretty(pg_total_relation_size(relid)) as size
    FROM pg_stat_user_tables 
    WHERE relname LIKE 'mesh%' OR relname LIKE 'shared%'
    ORDER BY pg_total_relation_size(relid) DESC
    LIMIT 10;
" 2>&1

log "=== Retention complete ==="