#!/bin/bash
# Load/Reload the A2A Mesh Node LaunchAgent
# Run this from a SEPARATE terminal (not inside Hermes gateway)
#
# Usage: bash load_launchagent.sh

PLIST="$HOME/Library/LaunchAgents/com.hermes.a2a-mesh-node.plist"
LABEL="com.hermes.a2a-mesh-node"

echo "=== A2A Mesh Node LaunchAgent Manager ==="

# Unload if already loaded
if launchctl list | grep -q "$LABEL"; then
    echo "Unloading existing $LABEL..."
    launchctl unload "$PLIST" 2>/dev/null
    sleep 2
fi

# Load the LaunchAgent
echo "Loading $LABEL..."
launchctl load "$PLIST"

sleep 3

# Verify
if launchctl list | grep -q "$LABEL"; then
    PID=$(launchctl list | grep "$LABEL" | awk '{print $1}')
    echo "✅ $LABEL loaded successfully (PID: $PID)"
else
    echo "❌ Failed to load $LABEL"
    echo "Check logs: tail -20 ~/.hermes/logs/a2a_mesh_node.log"
fi