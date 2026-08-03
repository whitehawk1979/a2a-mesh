#!/bin/bash
# A2A Mesh Chaos Testing — node kill + recovery verification
#
# Tests mesh resilience by:
# 1. Stopping a node's mesh process
# 2. Verifying other nodes detect it as down
# 3. Restarting the node
# 4. Verifying it reconnects and peers恢复
#
# Usage: bash scripts/chaos_test.sh [node_name]
# Default: tests morzsa (safest to kill — Nova has launchd auto-restart)

set -euo pipefail

NODES=(
    "nova:localhost:8650"
    "morzsa:192.168.1.30:8650"
    "runa:192.168.1.100:8650"
)

# SSH credentials
MORZSA_HOST="openclaw@192.168.1.30"
MORZSA_PASS="2009December16"
RUNA_HOST="zsolt@192.168.1.100"
RUNA_PASS="2009December16"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log() { echo -e "[$(date +%H:%M:%S)] $1"; }
ok() { log "${GREEN}✅ $1${NC}"; }
fail() { log "${RED}❌ $1${NC}"; }
warn() { log "${YELLOW}⚠️  $1${NC}"; }

# Check if a node is healthy
check_health() {
    local name=$1
    local url=$2
    local result=$(curl -s --connect-timeout 5 "${url}/api/health" 2>/dev/null)
    if echo "$result" | grep -q '"healthy"'; then
        echo "up"
    else
        echo "down"
    fi
}

# Get peer count
get_peers() {
    local url=$1
    curl -s --connect-timeout 5 "${url}/api/health" 2>/dev/null | python3 -c "
import sys,json
try:
    d=json.load(sys.stdin)
    p=d.get('peers',{})
    print(f\"{p.get('connected',0)}/{p.get('known',0)}\")
except:
    print('0/0')
" 2>/dev/null
}

# Kill a node's mesh process
kill_node() {
    local target=$1
    case $target in
        morzsa)
            sshpass -p "$MORZSA_PASS" ssh -o ConnectTimeout=10 -o StrictHostKeyChecking=no \
                "$MORZSA_HOST" 'pkill -f "cli.py start"' 2>/dev/null
            ;;
        runa)
            sshpass -p "$RUNA_PASS" ssh -o ConnectTimeout=10 -o StrictHostKeyChecking=no \
                "$RUNA_HOST" 'systemctl --user stop a2a-mesh.service' 2>/dev/null
            ;;
        nova)
            warn "Refusing to kill Nova (this script runs on Nova)"
            return 1
            ;;
    esac
}

# Restart a node's mesh process
restart_node() {
    local target=$1
    case $target in
        morzsa)
            sshpass -p "$MORZSA_PASS" ssh -o ConnectTimeout=10 -o StrictHostKeyChecking=no \
                "$MORZSA_HOST" \
                'cd ~/a2a_mesh && .venv/bin/python3 cli.py start --name morzsa --port 8650 --config ~/.hermes/mesh_config.yaml &' 2>/dev/null
            ;;
        runa)
            sshpass -p "$RUNA_PASS" ssh -o ConnectTimeout=10 -o StrictHostKeyChecking=no \
                "$RUNA_HOST" 'systemctl --user start a2a-mesh.service' 2>/dev/null
            ;;
        nova)
            pkill -f 'cli.py start' 2>/dev/null
            # launchd auto-restarts
            ;;
    esac
}

# Main test
TARGET=${1:-morzsa}
log "=== A2A Mesh Chaos Test: kill $TARGET ==="

# Step 1: Pre-test status
log "Step 1: Pre-test status"
for entry in "${NODES[@]}"; do
    IFS=':' read -r name ip port <<< "$entry"
    if [ "$ip" = "localhost" ]; then
        url="http://localhost:${port}"
    else
        url="http://${ip}:${port}"
    fi
    status=$(check_health "$name" "$url")
    peers=$(get_peers "$url")
    log "  $name: $status (peers=$peers)"
done

# Step 2: Kill target
log "Step 2: Killing $TARGET..."
kill_node "$target" || exit 1
sleep 5

# Step 3: Verify target is down
log "Step 3: Verifying $TARGET is down..."
target_url=""
for entry in "${NODES[@]}"; do
    IFS=':' read -r name ip port <<< "$entry"
    if [ "$name" = "$TARGET" ]; then
        if [ "$ip" = "localhost" ]; then
            target_url="http://localhost:${port}"
        else
            target_url="http://${ip}:${port}"
        fi
    fi
done

status=$(check_health "$TARGET" "$target_url")
if [ "$status" = "down" ]; then
    ok "$TARGET is down"
else
    fail "$TARGET is still up — kill failed"
fi

# Step 4: Verify other nodes still healthy
log "Step 4: Checking other nodes..."
for entry in "${NODES[@]}"; do
    IFS=':' read -r name ip port <<< "$entry"
    [ "$name" = "$TARGET" ] && continue
    if [ "$ip" = "localhost" ]; then
        url="http://localhost:${port}"
    else
        url="http://${ip}:${port}"
    fi
    status=$(check_health "$name" "$url")
    peers=$(get_peers "$url")
    if [ "$status" = "up" ]; then
        ok "$name still up (peers=$peers)"
    else
        fail "$name is down — cascade failure!"
    fi
done

# Step 5: Restart target
log "Step 5: Restarting $TARGET..."
restart_node "$TARGET"
sleep 20

# Step 6: Verify recovery
log "Step 6: Verifying $TARGET recovery..."
status=$(check_health "$TARGET" "$target_url")
if [ "$status" = "up" ]; then
    peers=$(get_peers "$target_url")
    ok "$TARGET recovered (peers=$peers)"
else
    fail "$TARGET did not recover"
    exit 1
fi

# Step 7: Final status
log "Step 7: Final status"
all_ok=true
for entry in "${NODES[@]}"; do
    IFS=':' read -r name ip port <<< "$entry"
    if [ "$ip" = "localhost" ]; then
        url="http://localhost:${port}"
    else
        url="http://${ip}:${port}"
    fi
    status=$(check_health "$name" "$url")
    peers=$(get_peers "$url")
    log "  $name: $status (peers=$peers)"
    [ "$status" != "up" ] && all_ok=false
done

if $all_ok; then
    ok "=== Chaos test PASSED — all nodes healthy ==="
else
    fail "=== Chaos test FAILED — some nodes still down ==="
    exit 1
fi