#!/bin/bash
# Nova Mesh Watchdog — restart if health check fails
cd /Users/zsolt/.hermes/scripts/a2a_mesh
HEALTH=$(curl -sf http://127.0.0.1:8650/health 2>/dev/null)
if [ -z "$HEALTH" ]; then
  echo "Nova not responding, restarting..."
  pkill -f "python.*cli.py" 2>/dev/null
  sleep 2
  nohup .venv/bin/python cli.py start --name nova --config mesh_config_nova.yaml > /dev/null 2>&1 &
  echo "Nova restarted"
else
  UPTIME=$(echo "$HEALTH" | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'OK uptime={d.get(\"uptime_seconds\",0):.0f}s')" 2>/dev/null || echo "OK")
  echo "Nova healthy: $UPTIME"
fi