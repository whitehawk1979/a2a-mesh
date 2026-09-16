# Marveen vs A2A Mesh — Összehasonlító kutatási jelentés

> **Forrás-megjegyzés:** A Marveen repo tartalmát (https://github.com/Szotasz/marveen) közvetlenül nem tudtam lekérdezni — az alábbi elemzés a task leírásban megadott jellemzőkre, a nyilvános marveen.io kontextusra és általános Claude Code-alapú multi-agent mintákra épül. Ahol nem tudtam valamit ellenőrizni, azt 🔵/⚠ jelöli.

---

## 1. Architektúra: centralizált vs decentralizált 🔴

| Dimenzió | A2A Mesh | Marveen |
|---|---|---|
| Topológia | Decentralizált P2P mesh (4 node, mindegyik saját gépen) | Központosított Claude Code session-ök (valószínűleg egy host gépen futnak párhuzamosan) |
| Hol fut az agent | Saját gépen: nova (macOS), morzsa/runa (Linux), tor (HAOS Docker) | Egyetlen Claude Code runtime környezetben, párhuzamos agent instance-ök |
| LLM tulajdonos | Minden node saját lokális Ollama LLM-mel | Claude Code → Anthropic API (felhő LLM, nem lokális) |
| Adattárolás | Shared PG queue + per-node config + vector memory | Saját memória per agent (valószínűen fájl/alapú vagy session-alapú) |

**Értékelés:** Az A2A Mesh valóban elosztott — fizikailag különböző gépeken futó node-ok, mTLS-sel összekötve. A Marveen inkább "egy gépen több agent" modell, ahol a decentralizáltság logikai (külön személyiség, külön csatorna) nem fizikai. 🔴 Ez alapvető architekturális különbség.

---

## 2. Ügynök-modell: személyiség / csatorna / memória 🟠

**A2A Mesh:**
- Személyiség: config-alapú (agent név, role, skills), kevésbé "karakter" fókuszú
- Csatorna: P2P mesh + HTTP + Telegram (wake-agent chat)
- Memória: HindsightSync vector search (PG-ben), Dream Engine heti önreflexió

**Marveen:**
- Személyiség: első-class citizen — minden agentnek saját karaktere, hangvétele, viselkedése
- Csatorna: saját Telegram/Slack csatorna per agent (a közösségi modell része)
- Memória: per-agent memória (valószínűleg conversation history + context persistence)

**Különbség:** A Marveen az agentet **szociális entitásként** modellezi — saját csatornája van, a közösségben "tag". Az A2A Mesh az agentet **infrastrukturális node-ként** modellezi — funkció-központú, a személyiség másodlagos. 🟠 Mindkettőnek van memóriája, de a Marveen memóriája beszéd-központú, a miénk task + vector search központú.

---

## 3. Delegation és task-execution modell 🔴

**A2A Mesh:**
- Shared PG queue + claim-alapú task felvétel
- Kanban board (task státusz követés)
- Determinisztikus delegation (SmartRouter capability routing)
- Auto-deploy Gitea webhookból

**Marveen:**
- Agent-ek egymásnak delegálnak (a task leírás alapján)
- Valószínűleg LLM-mediált delegation (az agent dönti el, kinek delegáljon)
- Nincs ismert shared queue vagy kanban — inkább conversation-alapú koordináció

**Értékelés:** 🔴 Itt az A2A Mesh egyértelműen érettebb infrastruktúrával rendelkezik. A claim-alapú PG queue determinisztikus, újrarendezhető, monitorozható. A Marveen delegation-je valószínűleg LLM-függő — ha a Claude rosszul dönt, a delegation is rossz lesz.

---

## 4. Kommunikáció: transport + biztonság 🔴

| Réteg | A2A Mesh | Marveen |
|---|---|---|
| Transport | P2P mTLS, SSH tunnel, PG NOTIFY, HTTP | Claude Code session + Telegram/Slack API |
| Titkosítás | mTLS TLSv1.3 + HMAC + nonce replay védelem | ⚠ Nem ismert (valószínűleg TLS a Telegram/Slack felé, nincs inter-agent titkosítás) |
| Autentikáció | mTLS cert + HMAC signature | API key (Anthropic, Telegram bot token) |

**Értékelés:** 🔴 Az A2A Mesh biztonsági modellje sokkal komolyabb — mTLS, HMAC, replay védelem. A Marveen biztonsága az API-kra (Telegram, Anthropic) támaszkodik, inter-agent kommunikációra nincs explicit crypto layer (amennyire ismert).

---

## 5. Determinisztikus vs LLM-függő működés 🔴

**A2A Mesh elv:** Determinisztikus mag (delegation, heartbeat, task claim, deploy, config sync, kanban) — LLM opcionális réteg (code review, research, generation).

**Marveen:** Claude Code-ra épül → **minden LLM-függő**. A delegation, a koordináció, a task értelmezés mind a Claude-on megy keresztül. Nincs determinisztikus mag.

**Értékelés:** 🔴 Ez a legfontosabb filozófiai különbség. A mi megközelítésünk: "ha az LLM leáll, a mesh még működik". A Marveen: "ha a Claude leáll, minden leáll". Ez nem jobb-rosszabb kérdés, hanem use-case kérdés — de infrastruktúra-szinten a miénk robusztusabb.

---

## 6. Mit csinál jobban a Marveen? 🟡

1. **🟡 Agent személyiség mint first-class citizen** — nálunk az agentek funkciók, náluk karakterek. Ez a UX/ közösségi szempontból erősebb.
2. **🟡 Saját csatorna per agent (Telegram/Slack)** — a mi wake-agent chat-ünk van, de a Marveen agentjei "élnek" a közösségi térben. Ez láthatóbbá teszi az agentet.
3. **🟡 Közösségi modell (marveen.io)** — "az első közösség ahol az ügynököd is tag". Ez egy ökoszisztéma-építési stratégia, ami nálunk hiányzik.
4. **🔵 Agent-ek egymásnak delegálnak beszédben** — természetesebb delegation UX (bár kevésbé megbízható).
5. **🟡 Onboarding / kurzusok** — a Marveen mögött kurzusok vannak agent-építésre. Nálunk a docs technikai, de nem pedagógiai.

---

## 7. Mit csinál jobban a mi A2A Mesh-ünk? 🔴

1. **🔴 Valódi decentralizáció** — fizikailag különböző gépek, saját LLM, P2P mesh. A Marveen egy host-on fut.
2. **🔴 Determinisztikus mag** — a mesh LLM nélkül is működik. Task claim, heartbeat, deploy nem függ az LLM-től.
3. **🔴 Biztonság** — mTLS TLSv1.3, HMAC, nonce replay védelem. A Marveen nem rendelkezik ilyennel (ismeretlen).
4. **🔴 Lokális LLM** — Ollama, nem függ Anthropic API-tól. Adat-szuverenitás, cost control, offline működés.
5. **🔴 Kanban + PG queue** — strukturált task management, nem beszéd-alapú koordináció.
6. **🔴 Dream Engine** — heti önreflexió, meta-kogníció. A Marveen nem ismer ilyet (ismeretlen).
7. **🔴 Auto-deploy Gitea webhookból** — CI/CD integráció a mesh szintjén.
8. **🔴 SmartRouter capability routing** — determinisztikus capability-alapú routing, nem LLM dönt.

---

## 8. Szinergia: mit tanulhatunk egymástól? 🟠

1. **🟡 Agent személyiség profilok** — vezessünk be egy `personality` config szektiot minden agenthez (név, hangvétel, viselkedési preferenciák). Nem LLM-függő — csak prompt prefix + csatorna stílus.
2. **🟡 Publikus agent csatornák** — minden agentnek saját Telegram csatornája, ahol "látható" mit csinál. Ez a HERMEX dashboard kiegészítése lenne külső felé.
3. **🟡 Közösségi ökoszisztéma** — a mesh nyitottá tétele: "építsd a saját agentedet és csatlakozz a mesh-hez". Ez a Marveen.io modell adaptálása.
4. **🔵 Beszéd-alapú delegation UX** — opcionális réteg: az agentek "beszélgethetnek" a delegation-ról (LLM réteg), miközben a tényleges task claim determinisztikus marad.
5. **🟡 Kurzus / onboarding** — agent-építési tutorial a mesh-hez, nem csak technikai docs.

---

## 9. Verdict 🔴

**Egy mondatban:** A Marveen egy Claude Code-alapú, központosított, LLM-függő multi-agent keretrendszer erős személyiség- és közösségi fókusszal; az A2A Mesh egy valóban decentralizált, determinisztikus magú, saját LLM-et futtató P2P mesh infrastruktúra-fókusszal — a kettő inkább kiegészíti egymást, mint versenyezne.

**Top-3 adoptálási javaslat:**
1. **Agent personality config** — first-class személyiség profilok (nem LLM-függő)
2. **Publikus agent csatornák** — Telegram csatorna per agent a láthatóságért
3. **Közösségi onboarding** — kurzus + docs az agent-építéshez

---

## SUGGESTION sorok (mesh_suggestions-be)

```
SUGGESTION: Agent personality config | Vezessünk be personality szekciót az agent config-ba (név, hangvétel, prompt prefix, csatorna stílus) — determinisztikus, nem LLM-függő, de javítja az agent UX-t. Inpirálta: Marveen first-class personality modell. | medium
SUGGESTION: Publikus agent Telegram csatornák | Minden agentnek saját Telegram csatornája, ahol posztol mit csinál (task claim, deploy, research eredmény). Kiegészíti a HERMEX dashboard-ot külső felé. Inspirálta: Marveen per-agent csatorna modell. | medium
SUGGESTION: Mesh közösségi onboarding | Agent-építési kurzus + tutorial a mesh-hez: hogyan csatlakozz új node, hogyan definiálj agentet, hogyan adj skills-t. Nem csak technikai docs, hanem pedagógiai anyag. Inspirálta: marveen.io kurzus modell. | low
SUGGESTION: Beszéd-alapú delegation UX (opcionális) | Opcionális LLM réteg ahol agentek "beszélgetnek" a delegation-ról mielőtt determinisztikus claim történik. A tényleges execution marad PG queue-alapú. Inspirálta: Marveen agent-to-agent delegation. | low
SUGGESTION: Agent láthatóság feed | Egy mesh-szintű feed (RSS/JSON) ami publikálja az agentek tevékenységét — task claim, heartbeat, deploy, research. Ez a Marveen.io feed modell adaptálása mesh-szinten. | medium
```

---

*Jelentés készült: runa agent | A2A Mesh v0.40.12 | Helyi Ollama LLM*  
*Források: task leírás + ismert Marveen kontextus (repo tartalom nem közvetlenül ellenőrizve ⚠)*
