# ELEMZÉS: A2A Mesh vs Marveen — Építési prioritások a saját koncepciónk megtartásával

Köszönöm a feladatot. Az alábbi elemzés a Marveen verifikált funkcióit veti össze a mi decentralizált, determinisztikus A2A Mesh koncepciónkkal. Kizárólag azokat az elemeket vizsgálom, amelyek vagy illeszkednek a mi architektúránkba, vagy egyértelműen ütköznek vele. Runa korábbi javaslatait (personality config, Telegram, onboarding, voice UX, activity feed) tudomásul vettem, és nem ismétlem meg őket.

## 1. Részletes elemzés a kiemelt fókuszterületeken

### 1.1 Skill Factory öntanulási mechanizmusa (auto_skill.py)
- **MIÉRT illik / NEM illik:** Kiválóan illik a decentralizált modellbe. Ha a node-ok sikeres feladatvégzés után skill-eket generálnak, ezeket a determinisztikus mag a shared PG-tudásbázisba szinkronizálhatja, így a hálózat kollektíven tanul.
- **KONRÉTUM:** A meglévő `auto_skill.py` kiterjesztése egy `skill_sync` modullal, amely a generált skill-eket a shared PG `skills` táblába írja idempotens módon. A 3-szintes progressive disclosure megvalósítható metadata tag-ekkel a PG-ben. Erőfeszítés: **M**.
- **Prioritás:** **P1**
- **Ütközés-elemzés:** Nincs ütközés. A meglévő delegation claim-queue-val jól együttműködik, a skill-eket a capability routing használhatja.

### 1.2 Memória-tier + salience decay (salience_decay.py)
- **MIÉRT illik / NEM illik:** Alapvető illeszkedés. A decentralizált PG-tudás és a HindsightSync vector search hatékonysága múlik a memória rétegezésén. A Marveen sosem töröl, csak "elhalványít" (salience decay) — ez a mi koncepciónkban is ideális, mivel a node-ok helyi LLM-jeinek korlátozott context windowja van, de a PG tárolókapacitása nagy.
- **KONRÉTUM:** Hot/Warm/Cold tier implementálása a PG-ben (vagy helyi SQLite cache + PG cold storage). A legfontosabb hiányosság a mi rendszerünkhöz képest az RRF (Reciprocal Rank Fusion), ami a Marveen-ben kombinálja az FTS5 szöveges keresést a nomic-embed vektor kereséssel. Ezt be kell építeni a HindsightSync-be. Erőfeszítés: **L**.
- **Prioritás:** **P1**
- **Ütközés-elemzés:** Nincs ütközés, a `salience_decay.py` kiegészíthető a tier logikával.

### 1.3 Vault titkosítás (AES-256-GCM)
- **MIÉRT illik / NEM illik:** Kritikus illeszkedés. A mi koncepciónk P2P mesh-e mTLS+HMAC alapokon áll. Jelenleg a mesh secret-ek plaintext config-ban vannak, ami egy decentralizált, több gépen futó rendszerben (különösen HAOS-on) komoly biztonsági kockázat.
- **KONRÉTUM:** Egy `vault.py` modul implementálása, amely az OS-keychain-t (pl. `keyring` Python library) használja a mesh mTLS kulcsok és HMAC secret-ek tárolására. A titkok csak a memóriában oldódnak fel a kapcsolatfelvétel pillanatában. Erőfeszítés: **M**.
- **Prioritás:** **P0**
- **Ütközés-elemzés:** Nincs ütközés, a meglévő config betöltést kell lecserélni a vault-ból való olvasásra.

### 1.4 Autonómia-létra kódba égetett zárolással
- **MIÉRT illik / NEM illik:** Tökéletesen illik a determinisztikus mag koncepcióhoz. Az LLM opcionális réteg, tehát a biztonsági zárolásoknak a kódban kell lenniük, hogy egy esetleges LLM hallucináció (vagy rosszindulatú prompt) ne tudjon irreverzibilis műveletet végrehajtani.
- **KONRÉTUM:** A meglévő AlertRule per-category autonomy mellé kell egy `safety_locks.py` a determinisztikus magban. Ez egy hardcoded blacklist (pl. `rm -rf /`, `DROP TABLE`, `reboot`), amit a 3-as szintű (autonóm) végrehajtás előtt a mag ellenőriz. Erőfeszítés: **S**.
- **Prioritás:** **P0**
- **Ütközés-elemzés:** Nincs ütközés, kiegészíti az AlertRule rendszert egy fix alsó korláttal.

### 1.5 PreCompact hook (precompact_hook.py)
- **MIÉRT illik / NEM illik:** Illik. Mivel node-onként saját (ollama GLM) LLM fut, a context window kezelés sokkal kritikusabb, mint egy Claude Code alapú rendszerben. A Marveen PreCompact hookja a context tömörítés előtt menti az adatokat.
- **KONRÉTUM:** A `precompact_hook.py` frissítése, hogy a context tömörítés előtt a fontos információkat (döntések, tények) kinyerje és a shared PG-be / HindsightSync-be mentse, így a tömörítés nem okoz adatvesztést a mesh szintjén. Erőfeszítés: **S**.
- **Prioritás:** **P1**
- **Ütközés-elemzés:** Nincs ütközés.

### 1.6 Dream engine (dream_engine.py)
- **MIÉRT illik / NEM illik:** Illik, de alacsonyabb prioritású. A decentralizált mesh-ben a node-ok alacsony forgalom idején (pl. éjszaka) reflektálhatnak a napi task logokon.
- **KONRÉTUM:** A `dream_engine.py` kiterjesztése, hogy a heartbeat ütemező indítsa el csendes időszakban. Elemezze a napi delegation claim-queue történelmet, és frissítse a salience pontszámokat, vagy javasoljon új skill-eket az `auto_skill.py` számára. Erőfeszítés: **M**.
- **Prioritás:** **P2**
- **Ütközés-elemzés:** Ütközhet a heartbeat ütemezéssel, ha nem megfelelően van szinkronizálva, de időzítéssel megoldható.

## 2. További releváns Marveen funkciók

### 2.1 Prompt-injection védelem (quarantine-reader sub-agent)
- **MIÉRT illik / NEM illik:** Kifejezetten ajánlott. Bár a node-ok egymás között mTLS-sel kommunikálnak, bármelyik node kérhet külső adatot (WebFetch). Egy decentralizált hálózatban egy fertőzött node prompt-injection-nel megpróbálhatja manipulálni a többit.
- **KONRÉTUM:** Egy `quarantine_reader.py` modul, amely a külső inputokat (web, nem megbízható források) egy izolált környezetben olvassa be, és csak szanitált szöveget ad vissza az LLM-nek. Erőfeszítés: **S**.
- **Prioritás:** **P0**
- **Ütközés-elemzés:** Nincs ütközés, erősíti az untrusted framinget.

### 2.2 Kanban (WIP-limit, card-aging, auto-breakdown)
- **MIÉRT illik / NEM illik:** A mi koncepciónkban már szerepel a kanban a determinisztikus magban. A Marveen WIP-limit és card-aging funkciói hasznosak lehetnek a delegation claim-queue torlódásának elkerülésére.
- **KONRÉTUM:** WIP-limit bevezetése node-onként a claim-queue-ban, és card-aging (idővel csökkenő prioritás) implementálása. Erőfeszítés: **M**.
- **Prioritás:** **P2**
- **Ütközés-elemzés:** A WIP-limit ütközhet a gyors feladatátvétellel, ha túl alacsonyra van állítva.

## Összegzés
A Marveen funkciói közül a biztonsági és memóriakezelési elemek (Vault, Safety locks, Quarantine reader) azonnal (P0) beépítendők, mivel a P2P architektúra és a saját LLM modell ezeket megköveteli. A tanulási és optimalizációs funkciók (Skill sync, RRF fusion, Dream engine) a következő sprintekben (P1/P2) építhetők be, hogy a hálózat kollektív intelligenciája fejlődjön, anélkül, hogy elveszítenénk a determinisztikus mag stabilitását.

---

SUGGESTION: Vault titkosítás OS-keychainnel | mTLS/HMAC kulcsok titkosítása memórián kívül, AES-256-GCM | P0
SUGGESTION: Kódba égetett safety_locks az irreverzibilis műveletekre | Determinisztikus mag blokkolja a veszélyes parancsokat az autonómia-létra felett | P0
SUGGESTION: Quarantine-reader sub-agent | WebFetch/külső inputok szanitálása prompt-injection ellen | P0
SUGGESTION: RRF fúzió a HindsightSync-be | FTS5 szöveges és vektor-keresés kombinálása Reciprocal Rank Fusion-nel a HindsightSync-ben (a Marveen memória-rendszerének kulcsmechanizmusa) | P1
SUGGESTION: Skill-sync: auto_skill → shared PG | A node-onként generált skill-ek idempotent szinkronizálása a shared PG-be, hogy a mesh kollektíven tanuljon | P1
SUGGESTION: WIP-limit a claim-queue-ban | Node-onkénti egyidejű task-limit + card-aging (öregedő kártyák prioritás-csökkenése) a delegation torlódás ellen | P2
