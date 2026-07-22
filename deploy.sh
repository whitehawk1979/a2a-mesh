#!/bin/bash
# A2A Mesh Deploy Script
# Usage: ./deploy.sh [tag]
# If tag is specified, checks out that tag. Otherwise uses current main.
# Each node uses its own config file (mesh_config_<node>.yaml)
set -e
cd "$(dirname "$0")"

TAG="${1:-}"
BRANCH="main"

echo "=== A2A Mesh Deploy ==="

# Detect node name from hostname or NODE_NAME env var
NODE_NAME="${NODE_NAME:-}"
if [ -z "$NODE_NAME" ]; then
    case "$(hostname)" in
        *morzsa*|*Morzsa*) NODE_NAME="morzsa" ;;
        *runa*|*Runa*) NODE_NAME="runa" ;;
        *lennie*|*Lennie*) NODE_NAME="lennie" ;;
        *) NODE_NAME="nova" ;;  # Default for Mac Pro
    esac
fi

# Find the right config file
CONFIG_FILE="mesh_config_${NODE_NAME}.yaml"
if [ ! -f "$CONFIG_FILE" ]; then
    echo "Warning: $CONFIG_FILE not found, falling back to mesh_config.yaml"
    CONFIG_FILE="mesh_config.yaml"
fi

echo "Node: $NODE_NAME, Config: $CONFIG_FILE"

# Stash any local changes
git stash --quiet 2>/dev/null || true

# Make sure we're on main (NEVER detached HEAD for config)
git checkout "$BRANCH"
git pull gitea "$BRANCH" --ff-only 2>/dev/null || git pull gitea "$BRANCH"

# If a tag is specified, check it out
if [ -n "$TAG" ]; then
    echo "Checking out tag: $TAG"
    git checkout "$TAG"
fi

echo "Current HEAD: $(git log --oneline -1)"
echo "Using config: $CONFIG_FILE"
echo "Deploy done."