#!/bin/sh
# mesh_node_keepalive.sh — determinisztikus HAOS keepalive (LLM nélkül)
#
# A HAOS konténerekben (tor, mano) nincs supervisor a mesh-node processzre:
# ha a node leáll (pkill, crash, OOM), semmi nem indítja újra → a kétirányú
# SSH tunnelek megszakadnak. Ez a script 30s-enként ellenőrzi és újraindítja.
#
# Használat (docker exec -d a HAOS hostról):
#   docker exec -d <konténer> sh -c '/bin/sh /config/a2a_mesh/mesh_node_keepalive.sh /config/a2a_mesh <node> <config.yaml>'
# Példa (mano):
#   docker exec -d app_17e0cc66_openclaw_assistant sh -c '/bin/sh /config/a2a_mesh/mesh_node_keepalive.sh /config/a2a_mesh mano mesh_config_mano.yaml'
# Példa (tor):
#   docker exec -d app_0a6523c6_hermes_agent sh -c '/bin/sh /config/a2a-mesh/mesh_node_keepalive.sh /config/a2a-mesh tor mesh_config_haos.yaml'
MESH_DIR="$1"; NODE_NAME="$2"; CONFIG="$3"
while true; do
  if ! pgrep -f "cli.py start --name $NODE_NAME" >/dev/null 2>&1; then
    cd "$MESH_DIR" 2>/dev/null || exit 1
    nohup ./.venv/bin/python3 cli.py start --name "$NODE_NAME" --config "$CONFIG" >/dev/null 2>&1 &
  fi
  sleep 30
done