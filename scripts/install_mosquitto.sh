#!/bin/bash
# ============================================================
# A2A Mesh — Mosquitto Broker Installer (macOS)
# ============================================================
# Telepíti és launchd alá helyezi a mosquitto broker-t a mesh
# MQTT transportjához (transports.mqtt, port 8683).
#
# Használat:
#   bash scripts/install_mosquitto.sh            # normál brew-út
#   bash scripts/install_mosquitto.sh --offline  # kézi bottle (raw.github halott)
#
# Az --offline út: a Homebrew cache-ben lévő bottle-ból telepít
#   (ghcr.io CDN), @@HOMEBREW_PREFIX@@ relocáció install_name_tool-lel,
#   majd ad-hoc codesign (macOS SIGKILL a módosított bináris ellen).
# ============================================================
set -euo pipefail

BREW="$(command -v brew || true)"
PREFIX="${BREW:+$(dirname "$(dirname "$BREW")")}"
PREFIX="${PREFIX:-/usr/local}"
PORT="${A2A_MQTT_PORT:-8683}"
CONF_DIR="${HOME}/.hermes/mqtt"
LOG_DIR="${HOME}/.hermes/logs"
PLIST="${HOME}/Library/LaunchAgents/com.hermes.mosquitto.plist"

echo "== A2A Mesh mosquitto installer =="
echo "   prefix: ${PREFIX}"

# --- 1) konfiguráció ------------------------------------------------
mkdir -p "${CONF_DIR}" "${LOG_DIR}" "${CONF_DIR}/data"
if [ ! -f "${CONF_DIR}/mosquitto.conf" ]; then
  cat > "${CONF_DIR}/mosquitto.conf" <<EOF
# A2A Mesh MQTT broker (v0.47.0)
listener ${PORT} 0.0.0.0
allow_anonymous true
persistence true
persistence_location ${CONF_DIR}/data/
log_dest file ${LOG_DIR}/mosquitto-launchd.err.log
EOF
  echo "   config: ${CONF_DIR}/mosquitto.conf (port ${PORT})"
else
  echo "   config: már létezik — nem bántom"
fi

# --- 2) bináris ------------------------------------------------------
BIN="$(ls -1 "${PREFIX}"/Cellar/mosquitto/*/sbin/mosquitto 2>/dev/null | head -1 || true)"

if [ -z "${BIN}" ] && [ -n "${BREW}" ]; then
  echo "   brew install mosquitto…"
  brew install mosquitto >/dev/null 2>&1 || true
  BIN="$(ls -1 "${PREFIX}"/Cellar/mosquitto/*/sbin/mosquitto 2>/dev/null | head -1 || true)"
fi

# --- offline / kézi bottle-út ---------------------------------------
if [ -z "${BIN}" ] && [ "${1:-}" = "--offline" ]; then
  BOTTLE="$(ls -1 "${HOME}"/Library/Caches/Homebrew/downloads/*--mosquitto--*.tar.gz 2>/dev/null | head -1 || true)"
  if [ -n "${BOTTLE}" ]; then
    echo "   offline: ${BOTTLE}"
    mkdir -p "${PREFIX}/Cellar"
    tar -xzf "${BOTTLE}" -C "${PREFIX}/Cellar/"
    VER_DIR="$(ls -1d "${PREFIX}"/Cellar/mosquitto/*/ | head -1)"
    BIN="${VER_DIR}sbin/mosquitto"
    # @@HOMEBREW_PREFIX@@ → valós prefix (brew-hű relocáció) + ad-hoc sign
    for f in "${VER_DIR}sbin/mosquitto" "${VER_DIR}bin/mosquitto_pub" "${VER_DIR}bin/mosquitto_sub"; do
      [ -f "$f" ] || continue
      otool -L "$f" 2>/dev/null | awk '{print $1}' | grep '^@@HOMEBREW_PREFIX@@' | while read -r lib; do
        install_name_tool -change "$lib" "${PREFIX}${lib#\@\@HOMEBREW_PREFIX\@\@}" "$f"
      done
      codesign -f -s - "$f" 2>/dev/null || true
    done
  fi
fi

if [ -z "${BIN}" ]; then
  echo "HIBA: mosquitto bináris nem áll elő." >&2
  echo "  1) brew install mosquitto  — vagy —" >&2
  echo "  2) bash $0 --offline (cache-bottle)" >&2
  exit 1
fi
echo "   bináris: ${BIN}"
"${BIN}" --version | head -1

# --- 3) LaunchAgent --------------------------------------------------
cat > "${PLIST}" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.hermes.mosquitto</string>
    <key>ProgramArguments</key>
    <array>
        <string>${BIN}</string>
        <string>-c</string>
        <string>${CONF_DIR}/mosquitto.conf</string>
    </array>
    <key>KeepAlive</key>
    <true/>
    <key>RunAtLoad</key>
    <true/>
    <key>StandardOutPath</key>
    <string>${LOG_DIR}/mosquitto-launchd.out.log</string>
    <key>StandardErrorPath</key>
    <string>${LOG_DIR}/mosquitto-launchd.err.log</string>
</dict>
</plist>
EOF
plutil -lint "${PLIST}" >/dev/null && echo "   plist OK: ${PLIST}"

echo
echo "Kész. Indítás KÜLÖN Terminálból (launchctl a gateway-ben guardolt):"
echo "  launchctl bootstrap gui/\$(id -u) ${PLIST}"
echo "Ellenőrzés:"
echo "  lsof -iTCP:${PORT} -sTCP:LISTEN"
echo "Ezután a node-configba: transports.mqtt.enabled: true"