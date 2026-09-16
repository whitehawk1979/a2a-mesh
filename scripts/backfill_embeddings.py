#!/usr/bin/env python3
"""Backfill NULL embeddings in mesh.mesh_memory.

Reads rows with embedding IS NULL (engramm/capsule/skill_seed/reflection),
creates nomic-embed-text vectors via the morzsa Ollama endpoint, and writes
them back in batches. Safe to re-run — skips rows that already have vectors.

Usage: python3 backfill_embeddings.py [batch_limit]
"""
import asyncio
import json
import os
import sys
import time

import asyncpg

import aiohttp

PG_DSN = "postgresql://nova:nova_agent_2026@192.168.1.30:5432/agent_memory"
OLLAMA_URL = os.environ.get("OLLAMA_URL", "http://127.0.0.1:11434")
EMBED_MODEL = "nomic-embed-text"

BATCH = int(sys.argv[1]) if len(sys.argv) > 1 else 500


async def embed(session, text: str):
    # nomic-embed-text context ~2048 tokens (~1800 chars HU) — 500-as hiba túl hosszú promptnál
    payload = {"model": EMBED_MODEL, "prompt": text[:1800]}
    try:
        async with session.post(f"{OLLAMA_URL}/api/embeddings", json=payload,
                                timeout=aiohttp.ClientTimeout(total=60)) as resp:
            if resp.status == 200:
                data = await resp.json()
                return data.get("embedding") or None
            else:
                print(f"  embed HTTP {resp.status}: {(await resp.text())[:100]}")
    except Exception as e:
        print(f"  embed error: {type(e).__name__}: {e}")
    return None


async def main():
    import aiohttp
    conn = await asyncpg.connect(PG_DSN)
    rows = await conn.fetch(
        """SELECT id, memory_type, memory_key, memory_value
           FROM mesh.mesh_memory
           WHERE embedding IS NULL
             AND memory_type IN ('engramm', 'capsule', 'skill_seed')
           ORDER BY id LIMIT $1""",
        BATCH,
    )
    print(f"NULL embedding rows to fix: {len(rows)}")

    fixed = 0
    failed = 0
    async with aiohttp.ClientSession() as session:
        for r in rows:
            # Embed text: key + value (the same text the create path uses)
            text = f"{r['memory_key']}\n{r['memory_value']}"
            vec = await embed(session, text)
            if vec:
                vec_str = "[" + ",".join(str(x) for x in vec) + "]"
                await conn.execute(
                    "UPDATE mesh.mesh_memory SET embedding = $1::vector WHERE id = $2",
                    vec_str, r['id'],
                )
                fixed += 1
                if fixed % 50 == 0:
                    print(f"  {fixed}/{len(rows)} fixed...")
            else:
                failed += 1

    await conn.close()
    print(f"DONE: fixed={fixed}, failed={failed}")


if __name__ == "__main__":
    asyncio.run(main())