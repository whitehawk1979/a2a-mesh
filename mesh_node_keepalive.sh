#!/bin/sh
# mesh_node_keepalive.sh — determinisztikus HAOS keepalive (LLM nelkul)
# 30s-enkent ellenorzi a node processzt, ujrainditja ha leallt.
MESH_DIR="$1"; NODE_NAME="$2"; CONFIG="$3"
while true; do
  if ! pgrep -f "cli.py start --name $NODE_NAME" >/dev/null 2>&1; then
    cd "$MESH_DIR" 2>/dev/null || exit 1
    nohup ./.venv/bin/python3 cli.py start --name "$NODE_NAME" --config "$CONFIG" >/dev/null 2>&1 &
  fi
  sleep 30
done
