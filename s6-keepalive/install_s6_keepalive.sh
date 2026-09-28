#!/bin/sh
set -e
mkdir -p /etc/s6-overlay/s6-rc.d/user/contents.d
cp -r /config/a2a-mesh/s6-keepalive/a2a-mesh-keepalive /etc/s6-overlay/s6-rc.d/
touch /etc/s6-overlay/s6-rc.d/user/contents.d/a2a-mesh-keepalive
echo "OK — következő konténer-restarttól az s6 automatikusan indítja."
