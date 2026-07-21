#!/bin/bash
cd ~/a2a_mesh
git stash -q 2>/dev/null
git pull gitea main
systemctl --user restart a2a-mesh
echo 'Synced and restarted'
