---
name: log-analyzer
description: Hibakereses es log analizis A2A mesh node-okon. errors.log, agent.log, gateway.log, state.db FTS.
tags: [debugging, logs, error-analysis, troubleshooting]
---

# Log Analyzer

Mesh node hibakereses logok elemzesevel.

## Log forrasok (prioritasi sorrend)
1. **errors.log** - ELSO! Itt vannak a valodi traceback-ek (ProxyError, APIConnectionError)
2. **agent.log** - Reszletes agent muveletek
3. **gateway.log** - Csak INFO szint (nem tul hasznos debug-hoz)

## Tipikus hibak
- **ProxyError / APIConnectionError** -> LLM provider nem erheto el
- **Transient agent failure** -> errors.log-ban a valodi ok
- **state.db FTS bloat** -> Drop triggers+vtables, VACUUM (fts_cleanup.py)

## Debug flow
1. `ssh user@host 'tail -100 ~/.hermes/logs/errors.log'`
2. Keresd meg a traceback-et
3. Ellenorizd a gateway routing: state.db gateway_routing + sessions.json
4. Ha model override problema: mindket helyet torold

## state.db FTS fix
- FTS triggers+vtables bloatoljak a state.db-t (226MB->1.1MB)
- `fts_cleanup.py` weekly cron (Sun 3am) megeloz
- auto_prune=true, retention=7d/30d
