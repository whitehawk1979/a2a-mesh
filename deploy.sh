#!/bin/bash
# A2A Mesh Deploy Script
# Usage: ./deploy.sh [tag]
# If tag is specified, checks out that tag. Otherwise uses current main.
set -e
cd "$(dirname "$0")"

TAG="${1:-}"
BRANCH="main"

echo "=== A2A Mesh Deploy ==="

# Stash any local changes
git stash --quiet 2>/dev/null || true

# Make sure we're on main
git checkout "$BRANCH"
git pull gitea "$BRANCH" --ff-only 2>/dev/null || git pull gitea "$BRANCH"

# If a tag is specified, check it out
if [ -n "$TAG" ]; then
    echo "Checking out tag: $TAG"
    git checkout "$TAG"
fi

echo "Current HEAD: $(git log --oneline -1)"
echo "Deploy done."