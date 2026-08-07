#!/usr/bin/env bash
# ════════════════════════════════════════════════════════════════
#  A2A Mesh v0.29.0 — Full Installer
#  Creates a complete, functional mesh node from scratch.
#
#  Usage:
#    ./install.sh                                    # Interactive
#    ./install.sh --node nova --host 192.168.1.50 \
#                --pg-host 192.168.1.30 --pg-user nova \
#                --pg-password 'secret' --pg-db agent_memory
#    curl -fsSL http://<gitea>:3001/nova/a2a-mesh/raw/branch/main/install.sh | bash
# ════════════════════════════════════════════════════════════════
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="${SCRIPT_DIR}/.venv"
VERSION="0.29.0"

# Colors
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; NC='\033[0m'
info()  { echo -e "${GREEN}[INFO]${NC} $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC} $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*"; }
step()  { echo -e "\n${BLUE}═══ $* ═══${NC}"; }

# ─── Parse Args ──────────────────────────────────────────────────
NODE_NAME=""
NODE_HOST=""
PG_HOST=""
PG_PORT="5432"
PG_USER="nova"
PG_PASSWORD=""
PG_DB="agent_memory"
PG_INIT=false
SKIP_VENV=false
SKIP_CERTS=false
SKIP_SERVICE=false
SKIP_CRON=false
CONFIG_FILE=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --node)        NODE_NAME="$2"; shift 2 ;;
        --host)        NODE_HOST="$2"; shift 2 ;;
        --pg-host)    PG_HOST="$2"; shift 2 ;;
        --pg-port)    PG_PORT="$2"; shift 2 ;;
        --pg-user)    PG_USER="$2"; shift 2 ;;
        --pg-password) PG_PASSWORD="$2"; shift 2 ;;
        --pg-db)      PG_DB="$2"; shift 2 ;;
        --pg-init)    PG_INIT=true; shift ;;
        --config)     CONFIG_FILE="$2"; shift 2 ;;
        --skip-venv)  SKIP_VENV=true; shift ;;
        --skip-certs) SKIP_CERTS=true; shift ;;
        --skip-service) SKIP_SERVICE=true; shift ;;
        --skip-cron)  SKIP_CRON=true; shift ;;
        -h|--help)
            echo "A2A Mesh v${VERSION} — Full Installer"
            echo ""
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Required (or interactive):"
            echo "  --node NAME         Node name (e.g. nova, morzsa, runa)"
            echo "  --host IP           Node host IP (LAN or Tailscale)"
            echo "  --pg-host IP        PostgreSQL host"
            echo "  --pg-password PW    PostgreSQL password"
            echo ""
            echo "Optional:"
            echo "  --pg-port PORT      PostgreSQL port (default: 5432)"
            echo "  --pg-user USER      PostgreSQL user (default: nova)"
            echo "  --pg-db NAME        PostgreSQL database (default: agent_memory)"
            echo "  --pg-init           Initialize PG schema (schema_init.sql)"
            echo "  --config FILE       Config file path (default: mesh_config_<node>.yaml)"
            echo "  --skip-venv         Use system Python"
            echo "  --skip-certs        Skip TLS cert generation"
            echo "  --skip-service      Skip systemd/launchd service setup"
            echo "  --skip-cron         Skip watchdog/cleanup cron setup"
            exit 0 ;;
        *) warn "Unknown arg: $1"; shift ;;
    esac
done

# ─── Interactive Prompts ────────────────────────────────────────
if [[ -z "$NODE_NAME" ]]; then
    read -rp "$(echo -e '${GREEN}Node name (e.g. nova, morzsa, runa):${NC} ')" NODE_NAME
    [[ -z "$NODE_NAME" ]] && error "Node name required" && exit 1
fi
if [[ -z "$NODE_HOST" ]]; then
    # Auto-detect: try Tailscale first, then LAN IP
    AUTO_HOST=""
    if command -v tailscale &>/dev/null; then
        AUTO_HOST=$(tailscale ip -4 2>/dev/null || true)
    fi
    if [[ -z "$AUTO_HOST" ]]; then
        AUTO_HOST=$(hostname -I 2>/dev/null | awk '{print $1}' || ipconfig getifaddr en0 2>/dev/null || true)
    fi
    read -rp "$(echo -e "${GREEN}Node host IP [${AUTO_HOST}]:${NC} ')" NODE_HOST
    NODE_HOST="${NODE_HOST:-$AUTO_HOST}"
fi
if [[ -z "$PG_HOST" ]]; then
    read -rp "$(echo -e '${GREEN}PostgreSQL host [192.168.1.30]:${NC} ')" PG_HOST
    PG_HOST="${PG_HOST:-192.168.1.30}"
fi
if [[ -z "$PG_PASSWORD" ]]; then
    read -rp "$(echo -e '${GREEN}PostgreSQL password:${NC} ')" -s PG_PASSWORD
    echo
    [[ -z "$PG_PASSWORD" ]] && error "PG password required" && exit 1
fi

CONFIG_FILE="${CONFIG_FILE:-${SCRIPT_DIR}/mesh_config_${NODE_NAME}.yaml}"

# ─── Step 1: Python ──────────────────────────────────────────────
step "1/7 — Python"
if command -v python3 &>/dev/null; then
    PYTHON="python3"
elif command -v python &>/dev/null; then
    PYTHON="python"
else
    error "Python 3.9+ required. Install from https://python.org"
    exit 1
fi
PY_VER=$($PYTHON -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
info "Found Python $PY_VER"

if [[ "$(uname)" == "Darwin" ]]; then
    if ! $PYTHON -c "import sys; sys.exit(0 if sys.version_info >= (3, 9) else 1)" 2>/dev/null; then
        for P in /opt/homebrew/bin/python3 /usr/local/bin/python3; do
            [[ -f "$P" ]] && PYTHON="$P" && info "Using $P" && break
        done
    fi
fi

# ─── Step 2: Venv + Dependencies ─────────────────────────────────
step "2/7 — Virtual Environment + Dependencies"
if [[ "$SKIP_VENV" == "false" ]]; then
    if [[ ! -d "$VENV_DIR" ]]; then
        info "Creating venv..."
        $PYTHON -m venv "$VENV_DIR"
    fi
    PYTHON="${VENV_DIR}/bin/python"
    PIP="${VENV_DIR}/bin/pip"
else
    PIP="$PYTHON -m pip"
fi
info "Installing dependencies..."
$PIP install --upgrade pip --quiet 2>/dev/null || true
$PIP install -r "${SCRIPT_DIR}/requirements.txt" --quiet 2>/dev/null || {
    warn "pip install failed, trying --user..."
    $PIP install -r "${SCRIPT_DIR}/requirements.txt" --user --quiet
}
info "Verifying imports..."
$PYTHON -c "
import aiohttp, msgpack, asyncpg, yaml
print(f'  aiohttp:  {aiohttp.__version__}')
print(f'  asyncpg:  {asyncpg.__version__}')
print(f'  msgpack:  {msgpack.version}')
print(f'  PyYAML:   {yaml.__version__}')
" || { error "Import verification failed!"; exit 1; }
info "Dependencies OK ✅"

# ─── Step 3: PostgreSQL Schema ──────────────────────────────────
step "3/7 — PostgreSQL Schema"
if [[ "$PG_INIT" == "true" ]]; then
    info "Initializing PG schema on ${PG_HOST}:${PG_PORT}/${PG_DB}..."
    PGPASSWORD="$PG_PASSWORD" psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DB" \
        -f "${SCRIPT_DIR}/schema_init.sql" 2>&1 | grep -v "^$" || {
        warn "PG schema init failed (maybe already exists?). Continuing..."
    }
    info "PG schema initialized ✅"
else
    info "Skipping PG init (use --pg-init to initialize schema)"
    # Quick connection test
    $PYTHON -c "
import asyncio, asyncpg
async def test():
    try:
        conn = await asyncpg.connect(
            host='${PG_HOST}', port=${PG_PORT},
            user='${PG_USER}', password='${PG_PASSWORD}',
            database='${PG_DB}', timeout=5
        )
        ver = await conn.fetchval('SELECT version()')
        print(f'  PG: {ver[:50]}...')
        await conn.close()
        return True
    except Exception as e:
        print(f'  PG connection failed: {e}')
        return False
result = asyncio.run(test())
exit(0 if result else 1)
" 2>&1 && info "PG connection OK ✅" || warn "PG connection failed — node will retry on start"
fi

# ─── Step 4: Config Generation ──────────────────────────────────
step "4/7 — Configuration"
info "Generating config: ${CONFIG_FILE}"
# Start from template
cp "${SCRIPT_DIR}/mesh_config_template.yaml" "$CONFIG_FILE"
# Replace placeholders
sed -i.bak "s/__NODE_NAME__/${NODE_NAME}/g" "$CONFIG_FILE"
sed -i.bak "s/__NODE_HOST__/${NODE_HOST}/g" "$CONFIG_FILE"
sed -i.bak "s/__PG_HOST__/${PG_HOST}/g" "$CONFIG_FILE"
sed -i.bak "s/__PG_PASSWORD__/${PG_PASSWORD}/g" "$CONFIG_FILE"
rm -f "${CONFIG_FILE}.bak"
info "Config generated ✅"

# ─── Step 5: TLS Certificates ────────────────────────────────────
step "5/7 — TLS Certificates"
if [[ "$SKIP_CERTS" == "false" ]]; then
    CERTS_DIR="${SCRIPT_DIR}/certs"
    if [[ ! -f "$CERTS_DIR/ca.crt" ]]; then
        info "Generating TLS certificates..."
        $PYTHON "${SCRIPT_DIR}/generate_certs.py" --output "$CERTS_DIR" 2>/dev/null || {
            warn "Cert generation failed — run manually: python generate_certs.py --output certs/"
        }
    else
        info "TLS certificates already exist ✅"
    fi
else
    info "Skipping cert generation"
fi

# ─── Step 6: Service (systemd / launchd) ─────────────────────────
step "6/7 — System Service"
OS_TYPE="$(uname)"
if [[ "$SKIP_SERVICE" == "false" ]]; then
    if [[ "$OS_TYPE" == "Darwin" ]]; then
        # ─── macOS: LaunchAgent ───
        PLIST_PATH="$HOME/Library/LaunchAgents/com.hermes.a2a-mesh-node.plist"
        mkdir -p "$(dirname "$PLIST_PATH")"
        cat > "$PLIST_PATH" << PLIST_EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.hermes.a2a-mesh-node</string>
    <key>ProgramArguments</key>
    <array>
        <string>${PYTHON}</string>
        <string>${SCRIPT_DIR}/cli.py</string>
        <string>start</string>
        <string>--name</string>
        <string>${NODE_NAME}</string>
        <string>--config</string>
        <string>${CONFIG_FILE}</string>
    </array>
    <key>WorkingDirectory</key>
    <string>${SCRIPT_DIR}</string>
    <key>StandardOutPath</key>
    <string>${HOME}/.hermes/logs/a2a_mesh_node.log</string>
    <key>StandardErrorPath</key>
    <string>${HOME}/.hermes/logs/a2a_mesh_node.log</string>
    <key>KeepAlive</key>
    <true/>
    <key>RunAtLoad</key>
    <true/>
    <key>EnvironmentVariables</key>
    <dict>
        <key>PYTHONPATH</key>
        <string>${HOME}</string>
    </dict>
</dict>
</plist>
PLIST_EOF
        mkdir -p "${HOME}/.hermes/logs"
        launchctl unload "$PLIST_PATH" 2>/dev/null || true
        launchctl load "$PLIST_PATH" 2>/dev/null
        info "LaunchAgent installed: ${PLIST_PATH} ✅"
        info "Start: launchctl start com.hermes.a2a-mesh-node"
        info "Stop:  launchctl stop com.hermes.a2a-mesh-node"
        info "Logs:  tail -f ~/.hermes/logs/a2a_mesh_node.log"

    elif [[ "$OS_TYPE" == "Linux" ]]; then
        # ─── Linux: systemd user service ───
        SERVICE_DIR="$HOME/.config/systemd/user"
        SERVICE_PATH="${SERVICE_DIR}/a2a-mesh.service"
        mkdir -p "$SERVICE_DIR"
        cat > "$SERVICE_PATH" << SVC_EOF
[Unit]
Description=A2A Mesh Node v${VERSION}
After=network.target

[Service]
Type=simple
WorkingDirectory=${SCRIPT_DIR}
ExecStart=${PYTHON} ${SCRIPT_DIR}/cli.py start --name ${NODE_NAME} --config ${CONFIG_FILE}
Restart=always
RestartSec=5
Environment=PYTHONPATH=${HOME}
Environment=PYTHONUNBUFFERED=1

[Install]
WantedBy=default.target
SVC_EOF
        systemctl --user daemon-reload
        systemctl --user enable a2a-mesh 2>/dev/null
        info "systemd service installed: ${SERVICE_PATH} ✅"
        info "Start: systemctl --user start a2a-mesh"
        info "Stop:  systemctl --user stop a2a-mesh"
        info "Logs:  journalctl --user -u a2a-mesh -f"
    fi
else
    info "Skipping service setup"
fi

# ─── Step 7: Cron Jobs (watchdog + cleanup) ──────────────────────
step "7/7 — Cron Jobs"
if [[ "$SKIP_CRON" == "false" ]]; then
    # Read existing crontab
    CRON_EXISTING=$(crontab -l 2>/dev/null || true)
    CRON_NEW="$CRON_EXISTING"

    # Watchdog — every 2 minutes
    WATCHDOG_CMD="*/2 * * * * ${PYTHON} ${SCRIPT_DIR}/core/gateway_watchdog.py --node ${NODE_NAME} 2>&1"
    if echo "$CRON_EXISTING" | grep -q "gateway_watchdog.py.*${NODE_NAME}"; then
        info "Watchdog cron already exists ✅"
    else
        CRON_NEW="${CRON_NEW}${WATCHDOG_CMD}
"
        info "Added watchdog cron (2min) ✅"
    fi

    # Session cleanup — every 10 minutes
    CLEANUP_CMD="*/10 * * * * ${PYTHON} ${SCRIPT_DIR}/core/session_cleanup.py --node ${NODE_NAME} 2>&1"
    if echo "$CRON_EXISTING" | grep -q "session_cleanup.py.*${NODE_NAME}"; then
        info "Cleanup cron already exists ✅"
    else
        CRON_NEW="${CRON_NEW}${CLEANUP_CMD}
"
        info "Added cleanup cron (10min) ✅"
    fi

    # Write crontab if changed
    if [[ "$CRON_NEW" != "$CRON_EXISTING" ]]; then
        echo "$CRON_NEW" | crontab - 2>/dev/null || warn "Failed to write crontab — add manually"
    fi
else
    info "Skipping cron setup"
fi

# ─── Summary ─────────────────────────────────────────────────────
echo ""
info "══════════════════════════════════════════════════════════"
info "  A2A Mesh v${VERSION} — Installation Complete ✅"
info "══════════════════════════════════════════════════════════"
echo ""
info "  Node:     ${NODE_NAME} (${NODE_HOST})"
info "  Python:   $($PYTHON --version 2>&1)"
info "  Venv:     ${VENV_DIR}"
info "  Config:   ${CONFIG_FILE}"
info "  PG:       ${PG_HOST}:${PG_PORT}/${PG_DB}"
echo ""
info "  Start the node:"
if [[ "$OS_TYPE" == "Darwin" ]]; then
    info "    launchctl start com.hermes.a2a-mesh-node"
else
    info "    systemctl --user start a2a-mesh"
fi
echo ""
info "  Verify:"
info "    curl http://localhost:8650/health"
info "    ${PYTHON} ${SCRIPT_DIR}/cli_mesh.py peers"
echo ""
info "  Logs:"
if [[ "$OS_TYPE" == "Darwin" ]]; then
    info "    tail -f ~/.hermes/logs/a2a_mesh_node.log"
else
    info "    journalctl --user -u a2a-mesh -f"
fi
echo ""
warn "  Edit ${CONFIG_FILE} to customize capabilities, skills, providers."