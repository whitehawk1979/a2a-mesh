## 2026-09-21 Runa mesh-llm frissítési kísérlet (0.72.1 → 0.76.2) — SIGILL rollback

**Eredmény:** NEM frissíthető. A v0.76.2 x86_64 CPU bundle AVX2-t igényel, Runa CPU (i7-3770S, Ivy Bridge) csak AVX1-et támogat → native runtime SIGILL (core-dump) crash-loop induláskor.
A v0.72.1 binary visszaállítva backupból (`mesh-llm.bak-0.72.1`), szolgáltatás verifikálva él (health OK, chat inference OK).

**Tanulság a Runa infra számára:**
- i7-3770S = AVX1-only → bármely új CPU-bundle frissítés előtt: `grep -c avx2 /proc/cpuinfo` check
- A 0.76.2 binary nvidia-smi detektálásnál CUDA runtime-ot próbál letölteni (1.3GB /tmp-be) — GPU-nál --device cpu force kell
- Mesh-LLM v0.76.2 (2026-09-14): durable KV prefix cache, SafeTensors direct load, iteration-level scheduler — de x86_64 CPU build AVX2+

**Visszaállítva:** ~/.local/bin/mesh-llm.bak-0.72.1 → mesh-llm; start.sh eredeti (GPU device sor vissza)
