---
name: marveen-integration
description: Auto-generated from successful task. Trigger: Marveen feature integration tasks.
---

# Marveen funkciók integrálása A2A Mesh-be

Auto-generated skill from a successful task execution by nova.

## Steps
1. Analizáld a Marveen feature-setet
2. Identifikáld mely funkciók illeszkednek a mesh architektúrába
3. Implementáld a backend módosításokat
4. Hozd létre a frontend HTML-t
5. Teszteld API szinten
6. Deployolja a módosításokat peer node-okra

## Pitfalls
- SSH connection refused on Morzsa — wait and retry
- PG max_connections exhaustion — terminate idle connections
- Token timing on separate HTML pages — use a2a_token primary
