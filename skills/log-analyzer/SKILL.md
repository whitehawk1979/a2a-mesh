---
name: log-analyzer
description: Hibakeresés és log analízis A2A mesh node-okon. errors.log, agent.log, gateway.log, state.db FTS.
tags: [debugging, logs, error-analysis, troubleshooting]
---

# Log Analyzer

Mesh node hibakeresés logok elemzésével.

## Log források (prioritási sorrend)
1. **errors.log** — ELSŐ! Itt vannak a valódi traceback-ek (ProxyError, APIConnectionError)
2. **agent.log** — Részletes agent műveletek
3. **gateway.log** — Csak INFO szint (nem túl hasznos debug-hoz)

## Tipikus hibák
- **ProxyError / APIConnectionError** → LLM provider nem elérhető
- **Transient agent failure** → errors.log-ban a valódi ok
- **state.db FTS bloat** → Drop triggers+vtables, VACUUM (fts_cleanup.py)

## Debug flow
1. `ssh user@host 'tail -100 ~/.hermes/logs/errors.log'`
2. Keresd meg a traceback-et
3. Ellenőrizd a gateway routing: state.db gateway_routing + sessions.json
4. Ha model override probléma: mindkét helyet töröld

## state.db FTS fix
- FTS triggers+vtables bloátolják a state.db-t (226MB→1.1MB)
- `fts_cleanup.py` weekly cron (Sun 3am) megelőzi
- auto_prune=true, retention=7d/30d
