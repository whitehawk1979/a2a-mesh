#!/bin/bash
# tor-keepalive-watchdog.sh — HAOS tor mesh node keepalive watchdog (cron 2 min)
# Biztosítja, hogy a tor_keepalive.sh loop fusson; ha halott, újraindítja.
# A tor node processzt maga a loop felügyeli (ensure_mesh_node).
# Determinisztikus: nincs LLM, csak pgrep + start. Sikeres futás → néma exit 0.
# Telepítési hely a HAOS konténerben: /config/.hermes/scripts/ (perzisztens /config
# mount — túlél konténer-restartot és image-frissítést is).
# Regisztráció: Hermes cron jobs.json (no_agent, deliver: local, */2 * * * *).

LOG=/config/a2a-mesh-keepalive.log
KA=/config/a2a-mesh/tor_keepalive.sh

# Ha a loop fut -> nincs teendo (silent success)
if pgrep -f "tor_keepalive.sh" >/dev/null 2>&1; then
    exit 0
fi

echo "[$(date +%F_%T)] WATCHDOG: tor_keepalive loop halott -> ujrainditas" >> $LOG
if [ -f $KA ]; then
    setsid nohup bash $KA >> /config/a2a-mesh-keepalive-watchdog.out 2>&1 &
    echo "[$(date +%F_%T)] WATCHDOG: loop ujrainditva (pid $!)" >> $LOG
else
    echo "[$(date +%F_%T)] WATCHDOG: HIBA — $KA nem letezik!" >> $LOG
    exit 1
fi