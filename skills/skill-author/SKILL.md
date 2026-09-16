---
name: skill-author
description: Uj SKILL.md fajlok szerzoje. Elemzi a sikeres task-okat es strukturalt skill-eket hoz letre a mesh szamara.
tags: [skill, authoring, auto-skill, meta]
---

# Skill Author

Uj skillek letrehozasa a mesh agentek szamara.

## Mikor generalj skill-t?
- Task 3+ tool call-lal es sikeres eredmennyel
- Error recovery utan sikeres befejezes
- Nincs meg skill ami lefedi ezt a feladat tipust

## SKILL.md struktura
```
---
name: skill-name
description: Rovid leiras
tags: [tag1, tag2]
---

# Skill Name

## Trigger Conditions
- Mikor hasznaljuk?

## Steps
1. Szamozott lepesek

## Pitfalls
- Gyakori hibak es elkerulesuk
```

## Publikalas
1. Mentsd a SKILL.md fajlt a skills/ konyvtarba
2. Publikald a mesh-be: POST /api/skills/{skill_id}/publish
3. Broadcast: POST /api/skills/broadcast (sync mas node-okkal)

## Szabalyok
- Mindig praktikus es tomor
- Alapozd a valodi tapasztalaton (task notes, result)
- Magyarázd el a pitfalls-t
