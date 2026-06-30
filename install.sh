#!/usr/bin/env bash
# A2A Mesh — Auto-install script
# Usage: curl -fsSL https://<gitea>/nova/a2a-mesh/raw/branch/main/install.sh | bash
#   or: ./install.sh [--dev] [--skip-venv] [--config FILE]
#
# Installs Python dependencies, creates venv if needed, generates certs,
# and optionally starts the mesh node.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="${SCRIPT_DIR}/.venv"
REQUIREMENTS="${SCRIPT_DIR}/requirements.txt"
CONFIG_FILE="${1:-${A2A_CONFIG:-${SCRIPT_DIR}/../.hermes/mesh_config.yaml}}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

info()  { echo -e "${GREEN}[INFO]${NC} $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC} $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*"; }

# ─── Parse Args ───
SKIP_VENV=false
DEV_MODE=false
GENERATE_CERTS=true
SKIP_START=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --skip-venv)  SKIP_VENV=true; shift ;;
        --dev)        DEV_MODE=true; shift ;;
        --no-certs)   GENERATE_CERTS=false; shift ;;
        --skip-start) SKIP_START=true; shift ;;
        --config)    CONFIG_FILE="$2"; shift 2 ;;
        -h|--help)
            echo "Usage: $0 [--skip-venv] [--dev] [--no-certs] [--skip-start] [--config FILE]"
            echo ""
            echo "  --skip-venv    Use system Python instead of venv"
            echo "  --dev          Install dev dependencies (pytest, etc.)"
            echo "  --no-certs     Skip certificate generation"
            echo "  --skip-start   Don't prompt to start after install"
            echo "  --config FILE  Path to mesh_config.yaml"
            exit 0 ;;
        *) warn "Unknown arg: $1"; shift ;;
    esac
done

# ─── Check Python ───
info "Checking Python..."
if command -v python3 &>/dev/null; then
    PYTHON="python3"
elif command -v python &>/dev/null; then
    PYTHON="python"
else
    error "Python 3.9+ is required. Install it from https://python.org"
    exit 1
fi

PY_VERSION=$($PYTHON -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
info "Found Python $PY_VERSION"

if [[ "$(uname)" == "Darwin" ]]; then
    # macOS: check for Homebrew Python if system Python is too old
    if $PYTHON -c "import sys; sys.exit(0 if sys.version_info >= (3, 9) else 1)" 2>/dev/null; then
        info "Python version OK (>= 3.9)"
    else
        warn "System Python too old. Trying Homebrew..."
        if [[ -f /opt/homebrew/bin/python3 ]]; then
            PYTHON="/opt/homebrew/bin/python3"
            info "Using Homebrew Python: $($PYTHON --version)"
        elif [[ -f /usr/local/bin/python3 ]]; then
            PYTHON="/usr/local/bin/python3"
            info "Using Intel Homebrew Python: $($PYTHON --version)"
        else
            error "No Python 3.9+ found. Install via Homebrew: brew install python"
            exit 1
        fi
    fi
fi

# ─── Create Venv ───
if [[ "$SKIP_VENV" == "false" ]]; then
    if [[ ! -d "$VENV_DIR" ]]; then
        info "Creating virtual environment..."
        $PYTHON -m venv "$VENV_DIR"
    fi
    PYTHON="${VENV_DIR}/bin/python"
    PIP="${VENV_DIR}/bin/pip"
    info "Using venv Python: $($PYTHON --version)"
else
    PIP="$PYTHON -m pip"
fi

# ─── Install Dependencies ───
info "Installing dependencies..."
$PIP install --upgrade pip --quiet 2>/dev/null || true
$PIP install -r "$REQUIREMENTS" --quiet 2>/dev/null || {
    warn "pip install failed, trying with --user..."
    $PIP install -r "$REQUIREMENTS" --user --quiet
}

if [[ "$DEV_MODE" == "true" ]]; then
    info "Installing dev dependencies..."
    $PIP install pytest pytest-asyncio --quiet 2>/dev/null || true
fi

info "Dependencies installed ✅"

# ─── Verify Imports ───
info "Verifying imports..."
$PYTHON -c "
import aiohttp, msgpack, psycopg2, yaml, zeroconf
print(f'  aiohttp:    {aiohttp.__version__}')
print(f'  msgpack:    {msgpack.version}')
print(f'  psycopg2:   {psycopg2.__version__}')
print(f'  PyYAML:     {yaml.__version__}')
print(f'  zeroconf:   {zeroconf.__version__}')
" || {
    error "Import verification failed! Check the error messages above."
    exit 1
}

# ─── Generate Certs ───
if [[ "$GENERATE_CERTS" == "true" ]]; then
    CERTS_DIR="${SCRIPT_DIR}/certs"
    if [[ ! -f "$CERTS_DIR/ca.crt" ]]; then
        info "Generating TLS certificates..."
        $PYTHON "${SCRIPT_DIR}/generate_certs.py" --output "$CERTS_DIR" 2>/dev/null || {
            warn "Certificate generation failed. You may need to generate them manually."
            warn "Run: python generate_certs.py --output certs/"
        }
    else
        info "TLS certificates already exist ✅"
    fi
fi

# ─── Summary ───
echo ""
info "═══════════════════════════════════════════"
info "  A2A Mesh v0.10.1 — Installation Complete"
info "═════════════════════════════════════════════"
echo ""
info "  Python:    $($PYTHON --version 2>&1)"
info "  Venv:      ${VENV_DIR}"
info "  Config:    ${CONFIG_FILE}"
info "  Certs:     ${SCRIPT_DIR}/certs/"
echo ""
info "  Quick start:"
info "    $PYTHON ${SCRIPT_DIR}/cli.py start --name <node_name> --port 8650"
echo ""
info "  With TLS:"
info "    $PYTHON ${SCRIPT_DIR}/cli.py start --name <node_name> --port 8650 --tls"
echo ""

# ─── Optional Start ───
if [[ "$SKIP_START" == "false" && -t 0 ]]; then
    read -rp "$(echo -e '${GREEN}Start mesh node now? [y/N]${NC} ')" -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        read -rp "Node name [nova]: " NODE_NAME
        NODE_NAME="${NODE_NAME:-nova}"
        read -rp "Port [8650]: " PORT
        PORT="${PORT:-8650}"
        read -rp "Enable TLS? [Y/n]: " TLS_REPLY
        TLS_FLAG=""
        if [[ ! "$TLS_REPLY" =~ ^[Nn]$ ]]; then
            TLS_FLAG="--tls"
        fi
        info "Starting mesh node: $NODE_NAME on port $PORT"
        $PYTHON "${SCRIPT_DIR}/cli.py" start --name "$NODE_NAME" --port "$PORT" $TLS_FLAG
    fi
fi