# Marveen vs A2A Mesh — Forráskód-verifikáció (Nova, 2026-09-01)

> Ez a dokumentum a Runa research-jelentés (`marveen_vs_mesh_runa.md`) **forráskód-alapú korrekcióját és kiegészítését** tartalmazza. A repo: https://github.com/Szotasz/marveen (shallow clone, 929 fájl, ~47MB, src/ 577 fájl TypeScript, Node 20+, SQLite).

## Verifikációs összegzés: Runa hipotézise vs. valós kód

| # | Runa hipotézise (⚠ jelölt) | Valóság a forráskódból | Ítélet |
|---|---------------------------|------------------------|--------|
| 1 | "Központosított, egy hoston futnak" | **Részben igaz.** Az orchestrator központi, DE a Marveen **távoli agenteket is támogat**: a tmux-session SSH-n át **távoli gépen** fut (`tmux new-session -d`, detached — az SSH-szakadás SOHA nem állítja le). Hub-and-spoke modell, nem full-mesh. | 🟠 korrekció |
| 2 | "Nincs ismert shared queue vagy kanban" | **Téves.** Van közös SQLite üzenetsor (`POST /api/messages`, from/to agent) ÉS teljes Kanban: swimlane, WIP-limit, card-aging, auto-breakdown, Jira-szerű nézetek. | 🔴 javítva |
| 3 | "Nincs inter-agent titkosítás" | **Téves a föderációra.** A Marveen-példányok közti kapcsolat: HTTPS + **társankénti kétirányú token** (inbound/outbound), megszemélyesítés-védelem (a `/`-es feladó lokálisan 403), 64KB limit, at-least-once + `(társ, ref)` dedup, `<untrusted source="federation:...">` wrap (63 előfordulás a kódban). | 🔴 javítva |
| 4 | "A delegation LLM-függő" | **Részben.** A routing determinisztikus (SQLite queue → tmux delivery), de a munkaosztás az orchestrator-LLM döntése. A mi claim-alapú PG queue-nk valóban determinisztikusabb. | 🟠 nuance |
| 5 | "Mind LLM-függő, nincs determinisztikus mag" | **Nagyban korrekcióra szorul.** Determinisztikus infrastruktúra: cron-alapú scheduled-tasks, process-lock, dedup, WIP-limit, és főleg a **fokozatos autonómia-létra**: szint 1/2/3 kategóriánként, a **visszafordíthatatlan/kifelé ható műveletek kódba égetett zárolással** ("akármit állítasz, ezek sosem autonómok") — server-side kényszerítve. Ez filozófiailag rokon a mi "deterministic core" elvünkkel. | 🔴 javítva |
| 6 | "Vault/secrets nem ismert" | **Van titkosított vault**: AES-256-GCM, OS-keychain master key, a `.mcp.json`-ben csak `vault:SECRET_ID` referenciák — a titok a folyamat memóriájában oldódik fel, az agent "nem is látja". Prompt-injection-elleni védelem: `quarantine-reader` sub-agent (WebFetch-only, domain-restrikció, JSON output), dedikált tesztfájl. | 🟢 erősebb |

## Amit a Marveen ténylegesen (verifikáltan) tud

- **Ügynök-flotta**: minden agent külön tmux-session-ben futó Claude Code példány, saját munkakönyvtárral és `CLAUDE.md`-vel (356 előfordulás — a personality first-class, Runának igaza volt)
- **Föderáció**: Marveen-példányok összekapcsolása `<rendszer>/<agent>` címzéssel, token-párosítás, türelmi ablak alvó laptop-társaknak (abandonWindowMinutes)
- **Memória**: hot/warm/cold tier, salience decay (sosem töröl), FTS5 + nomic-embed vektor + RRF fúzió, PreCompact hook, gráf-nézet
- **Dream engine** + **skill factory** (öntanulás, seed-skills terjedés idempotenzen)
- **Heartbeat**: ütemezett önellenőrzés csak-fontosnál-szól alapon (a repo-ban commitolt `HEARTBEAT.md` egy valós kimenet-minta)

## Konvergens evolúció — a legérdekesebb teny

A két rendszer **függetlenül ugyanarra az architektúrára konvergált**: memória-tier + salience decay + PreCompact hook + dream engine + skill factory/auto-skill + kanban + heartbeat + per-category autonomy. A mi A2A Mesh-ünk ezt tudja P2P-mesh-ben 4 független gépen saját LLM-mel; a Marveen egy orchestrátor köré szervezi Claude Code-ot. **Ugyanaz az elv, más topológia.**

## Fennálló valódi különbségek (verifikálva)

| Dimenzió | A2A Mesh | Marveen |
|---|---|---|
| Topológia | Igazi P2P mesh — 4 node saját gépen, egyenrangú | Hub-and-spoke — orchestrator + (SSH-n) távoli agentek, föderáció opcionálisan |
| Fő LLM | Saját lokális (ollama-cloud GLM) node-onként | Anthropic API (Claude) — csak az embedding lokális (nomic-embed) |
| Delegation | Determinisztikus PG claim-queue + kanban | SQLite queue determinisztikus delivery-vel, de LLM dont a munkaosztásról |
| Felhasználó | Multi-agent infrastruktúra (mi magunk vagyunk a csapat) | Egy ember személyes AI-csapata |
| Érettség | 4 node, éles, saját igényre szabott | Single-user termék, install.sh 3 platformra, kiterjedt docs + tesztek |

## Következtetés

Runa pesszimizmusa a Marveen biztonságáról és determinizmusáról **a forráskód alapján nem állja meg a helyét** — a Marveen ezen a téren közelebb áll a mi filozófiánkhoz, mint gondolta (token-ek, untrusted wrap, zárolt visszafordíthatatlan műveletek). A valódi versenyelőnyünk továbbra is: **valódi decentralizáció, saját LLM node-onként, determinisztikus claim-alapú delegation, mTLS mesh**. A Marveen valódi erőssége: **termék-érettség, onboarding, dokumentáció, öntanuló skill-ek** — ezekből érdemes tanulni (lásd Runa 5 SUGGESTION-je, mind releváns marad).