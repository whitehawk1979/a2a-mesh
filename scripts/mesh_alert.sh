#!/bin/bash
# A2A Mesh Alert Watchdog — node down → Telegram alert
# Runs every 5 minutes via cron
# Only alerts on STATE CHANGE (up→down, down→up) to avoid spam

NODES=(
    "nova:192.168.1.8:8650"
    "morzsa:192.168.1.30:8650"
    "runa:192.168.1.100:8650"
)

STATE_FILE="/tmp/a2a-mesh-alert-state.json"
BOT_TOKEN="${A2A_TELEGRAM_BOT_TOKEN:-}"
CHAT_ID="${A2A_TELEGRAM_CHAT_ID:-7796035659}"  # Zsolt DM by default

# Initialize state file
if [ ! -f "$STATE_FILE" ]; then
    echo '{}' > "$STATE_FILE"
fi

send_telegram() {
    local msg="$1"
    # Use hermes send CLI (doesn't need separate bot token)
    if command -v hermes &> /dev/null; then
        hermes send telegram:${CHAT_ID} "${msg}" 2>/dev/null && return
    fi
    # Fallback: direct Telegram Bot API
    if [ -n "$BOT_TOKEN" ]; then
        curl -s -X POST "https://api.telegram.org/bot${BOT_TOKEN}/sendMessage" \
            -d "chat_id=${CHAT_ID}" \
            -d "text=${msg}" \
            -d "parse_mode=HTML" > /dev/null 2>&1
    else
        echo "[ALERT] No bot token or hermes CLI, logging only: $msg"
    fi
}

for entry in "${NODES[@]}"; do
    name=$(echo "$entry" | cut -d: -f1)
    host=$(echo "$entry" | cut -d: -f2)
    port=$(echo "$entry" | cut -d: -f3)
    
    # Health check (3s timeout)
    response=$(curl -s --connect-timeout 3 --max-time 8 "http://${host}:${port}/api/health" 2>/dev/null)
    
    if echo "$response" | grep -q '"healthy"'; then
        current_state="up"
        uptime=$(echo "$response" | python3 -c "import sys,json; print(json.load(sys.stdin).get('uptime',0))" 2>/dev/null || echo "?")
        peers=$(echo "$response" | python3 -c "import sys,json; d=json.load(sys.stdin); p=d.get('peers',{}); print(f\"{p.get('connected',0)}/{p.get('known',0)}\")" 2>/dev/null || echo "?")
        detail="uptime=${uptime}s peers=${peers}"
    else
        current_state="down"
        detail="unreachable"
    fi
    
    # Read previous state
    prev_state=$(python3 -c "import json; d=json.load(open('$STATE_FILE')); print(d.get('$name','up'))" 2>/dev/null || echo "up")
    
    # Alert only on state change
    if [ "$current_state" != "$prev_state" ]; then
        if [ "$current_state" = "down" ]; then
            send_telegram "🔴 <b>A2A Mesh Alert</b>
Node <b>${name}</b> is <b>DOWN</b>
Host: ${host}:${port}
Time: $(date '+%Y-%m-%d %H:%M:%S')"
            echo "[ALERT] $name went DOWN"
        else
            send_telegram "🟢 <b>A2A Mesh Alert</b>
Node <b>${name}</b> is back <b>UP</b>
Host: ${host}:${port}
Detail: ${detail}
Time: $(date '+%Y-%m-%d %H:%M:%S')"
            echo "[ALERT] $name came back UP ($detail)"
        fi
    else
        # Silent when OK — no stdout means no delivery (watchdog pattern)
        :
    fi
    
    # Update state
    python3 -c "import json; d=json.load(open('$STATE_FILE')); d['$name']='$current_state'; json.dump(d, open('$STATE_FILE','w'))" 2>/dev/null
done