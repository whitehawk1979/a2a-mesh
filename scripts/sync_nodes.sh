#!/bin/bash
# A2A Mesh Node Sync Script — syncs code to Morzsa and Runa
# Usage: bash scripts/sync_nodes.sh [--restart]
#
# Syncs core/, transports/, discovery/, cli.py, node.py, pyproject.toml
# Does NOT overwrite: mesh_config_*.yaml, certs/, local_store_*.db, incoming_files/

# No set -e: a failure on one node must not abort sync to the other

RESTART=false
[[ "$1" == "--restart" ]] && RESTART=true

MORZSA_HOST="openclaw@192.168.1.30"
MORZSA_PASS="2009December16"
RUNA_HOST="zsolt@192.168.1.100"
RUNA_KEY="$HOME/.ssh/id_ed25519_openclaw"   # key auth: password auth times out on Runa
SSH_OPTS="-o StrictHostKeyChecking=no -o ConnectTimeout=10"

LOCAL_DIR="$(cd "$(dirname "$0")/.." && pwd)"
MORZSA_REMOTE_DIR="a2a_mesh"        # Morzsa runs from ~/a2a_mesh/
RUNA_REMOTE_DIR="a2a_mesh"           # Runa runs from ~/a2a_mesh/ (systemd)

echo "=== A2A Mesh Node Sync ==="
echo "Local: $LOCAL_DIR"
echo "Target: $REMOTE_DIR"
echo "Restart: $RESTART"
echo ""

# Files/dirs to sync
SYNC_PATHS=(
    "core/"
    "transports/"
    "discovery/"
    "cli.py"
    "node.py"
    "pyproject.toml"
    "scripts/"
)

# Files to exclude (node-specific config, local state)
EXCLUDES=(
    "--exclude=mesh_config_*.yaml"
    "--exclude=mesh_config.yaml"
    "--exclude=certs/"
    "--exclude=local_store_*.db"
    "--exclude=local_store_*.db-shm"
    "--exclude=local_store_*.db-wal"
    "--exclude=incoming_files/"
    "--exclude=__pycache__/"
    "--exclude=.venv/"
    "--exclude=.git/"
    "--exclude=backups/"
    "--exclude=a2a_mesh.db"
    "--exclude=*.bak"
    "--exclude=*.bak.*"
)

# ── Sync to Morzsa ──
echo "📡 Syncing to Morzsa ($MORZSA_HOST)..."
for path in "${SYNC_PATHS[@]}"; do
    if [ -e "$LOCAL_DIR/$path" ]; then
        sshpass -p "$MORZSA_PASS" rsync -az --delete \
            "${EXCLUDES[@]}" \
            "$LOCAL_DIR/$path" \
            "$MORZSA_HOST:$MORZSA_REMOTE_DIR/$path" 2>/dev/null && echo "  ✅ $path" || echo "  ⚠️  $path (failed)"
    fi
done

if $RESTART; then
    echo "🔄 Restarting Morzsa mesh node..."
    sshpass -p "$MORZSA_PASS" ssh $SSH_OPTS "$MORZSA_HOST" \
        "systemctl --user restart a2a-mesh 2>/dev/null || (pkill -f '[c]li.py start' 2>/dev/null; sleep 1; cd ~/$MORZSA_REMOTE_DIR && nohup .venv/bin/python3 cli.py start --name morzsa --port 8650 --config ~/.hermes/mesh_config.yaml > /dev/null 2>&1 &)" 2>/dev/null
    echo "  ✅ Morzsa restarted"
fi

# ── Sync to Runa ──
echo "📡 Syncing to Runa ($RUNA_HOST)..."
for path in "${SYNC_PATHS[@]}"; do
    if [ -e "$LOCAL_DIR/$path" ]; then
        rsync -az --delete -e "ssh -i $RUNA_KEY -o BatchMode=yes -o StrictHostKeyChecking=no -o ConnectTimeout=10" \
            "${EXCLUDES[@]}" \
            "$LOCAL_DIR/$path" \
            "$RUNA_HOST:$RUNA_REMOTE_DIR/$path" 2>/dev/null && echo "  ✅ $path" || echo "  ⚠️  $path (failed)"
    fi
done

if $RESTART; then
    echo "🔄 Restarting Runa mesh node..."
    ssh -i "$RUNA_KEY" -o BatchMode=yes $SSH_OPTS "$RUNA_HOST" \
        "systemctl --user restart a2a-mesh.service 2>/dev/null || (pkill -f 'cli.py start' 2>/dev/null; sleep 1; cd ~/$RUNA_REMOTE_DIR && nohup .venv/bin/python3 cli.py start --name runa --port 8650 > /dev/null 2>&1 &)" 2>/dev/null
    echo "  ✅ Runa restarted"
fi

echo ""
echo "=== Sync complete ==="