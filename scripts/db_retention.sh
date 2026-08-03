#!/bin/bash
# A2A Mesh DB Retention Script — runs daily via cron
# Cleans up old data from mesh DB to prevent unbounded growth
#
# Retention policy:
#   mesh_messages:   7 days (delivered/read/acknowledged/sent)
#   mesh_debug_logs:  3 days (all levels)
#   mesh_suggestions: 7 days (superseded status only)
#   shared_dlq:       7 days (processed entries)
#
# Usage: bash scripts/db_retention.sh
# Cron:  0 4 * * * bash ~/.hermes/scripts/a2a_mesh/scripts/db_retention.sh >> ~/.hermes/logs/mesh_retention.log 2>&1

set -euo pipefail

PG_HOST="${PG_HOST:-192.168.1.30}"
PG_PORT="${PG_PORT:-5432}"
PG_DB="${PG_DB:-agent_memory}"
PG_USER="${PG_USER:-nova}"
export PGPASSWORD="${PGPASSWORD:-nova_agent_2026}"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
}

log "=== A2A Mesh DB Retention ==="

# 1. Delete old mesh messages (>7 days)
log "Cleaning mesh_messages (>7 days)..."
DELETED=$(psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DB" -t -c "
    DELETE FROM mesh.mesh_messages 
    WHERE created_at < now() - interval '7 days' 
    AND status IN ('sent', 'delivered', 'read', 'acknowledged');
" 2>&1 | head -1)
log "  mesh_messages: $DELETED rows deleted"

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
    DELETE FROM mesh_suggestions 
    WHERE status = 'superseded' 
    AND updated_at < now() - interval '7 days';
" 2>&1 | head -1)
log "  mesh_suggestions: $DELETED rows deleted"

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
    WHERE created_at < now() - interval '30 days';
" 2>&1 | head -1)
log "  shared_context: $DELETED rows deleted"

# 6. Vacuum analyze (non-blocking, doesn't lock table)
log "Running VACUUM ANALYZE on mesh_messages..."
psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DB" -c "VACUUM ANALYZE mesh.mesh_messages;" 2>&1 | head -1
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