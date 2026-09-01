"""Hindsight integration — save mesh delegation results to long-term memory.

When a delegation completes, the result is saved to Hindsight for later
context injection. This allows agents to "remember" past delegations and
use that knowledge in future tasks.

Usage:
    from core.hindsight_sync import HindsightSync
    hs = HindsightSync(node)
    await hs.save_delegation_result(task_row)
    context = await hs.get_context_for_prompt(subject)
"""
import json
import logging
import time
from typing import Optional, Dict, Any, List

log = logging.getLogger("mesh.hindsight_sync")


class HindsightSync:
    """Sync mesh delegation results to Hindsight long-term memory."""

    def __init__(self, node):
        self.node = node  # May be None — Brain host uses hardcoded fallback
        self._pg_pool = None
        self._enabled = True  # Always enabled — hindsight is just memory, non-fatal

    def set_pg_pool(self, pool):
        """Set PG pool for direct DB access."""
        self._pg_pool = pool

    async def save_delegation_result(self, task_row: dict) -> bool:
        """Save a completed delegation result to Hindsight via PG.

        Called when a delegation task completes. Stores the result
        in mesh.mesh_memory for later retrieval by context injection.
        """
        if not self._enabled or not self._pg_pool:
            return False

        try:
            task_id = task_row.get("task_id", "")
            from_agent = task_row.get("from_agent", "")
            to_agent = task_row.get("assigned_agent") or task_row.get("to_agent", "")
            subject = task_row.get("subject", "")
            result = task_row.get("result", "")
            status = task_row.get("status", "completed")

            if not result or not subject:
                return False

            # Store in mesh.mesh_memory table (create if not exists)
            await self._pg_pool.execute("""
                CREATE TABLE IF NOT EXISTS mesh.mesh_memory (
                    id SERIAL PRIMARY KEY,
                    memory_key TEXT NOT NULL,
                    memory_value TEXT NOT NULL,
                    source_agent TEXT NOT NULL,
                    target_agent TEXT,
                    memory_type TEXT DEFAULT 'delegation_result',
                    priority INTEGER DEFAULT 5,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    metadata JSONB DEFAULT '{}'
                )
            """)

            # Insert delegation result
            metadata = {
                "task_id": str(task_id),
                "from_agent": from_agent,
                "to_agent": to_agent,
                "status": status,
            }

            await self._pg_pool.execute("""
                INSERT INTO mesh.mesh_memory 
                    (memory_key, memory_value, source_agent, target_agent, memory_type, priority, metadata)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
            """, f"delegation:{str(task_id)[:8]}", result[:5000], from_agent, to_agent,
                 "delegation_result", 5, json.dumps(metadata))

            log.info(f"Saved delegation result to mesh_memory: {subject[:50]} ({str(task_id)[:8]})")

            # Generate embedding for this entry via Brain server (non-fatal)
            try:
                import urllib.request
                import urllib.parse
                try:
                    brain_host = getattr(self.node.config, 'brain_host', None) or '192.168.1.8'
                    brain_port = getattr(self.node.config, 'brain_port', 3322)
                except Exception:
                    brain_host = '192.168.1.8'
                    brain_port = 3322
                embed_text = f"{subject}: {result[:1000]}"
                data = json.dumps({"id": None, "text": embed_text[:2000]}).encode()
                # Get the last inserted ID
                row = await self._pg_pool.fetchval(
                    "SELECT id FROM mesh.mesh_memory ORDER BY id DESC LIMIT 1"
                )
                if row:
                    data = json.dumps({"id": row, "text": embed_text[:2000]}).encode()
                    url = f"http://{brain_host}:{brain_port}/mesh/memory/embed"
                    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"}, method='POST')
                    urllib.request.urlopen(req, timeout=10)
                    log.info(f"Generated embedding for mesh_memory id={row}")
            except Exception as emb_err:
                log.debug(f"Embedding generation failed (non-fatal): {emb_err}")

            return True

        except Exception as e:
            log.error(f"Error saving delegation result to Hindsight: {e}")
            return False

    @staticmethod
    def _rrf_fuse(ranked_lists: List[Tuple[str, List[Dict]]], k: int = 60) -> List[Dict]:
        """Reciprocal Rank Fusion (Marveen memory-system pattern).

        Fuses multiple ranked result lists into one. score(d) = Σ 1/(k + rank_i(d))
        where rank_i is the item's 1-based position in list i. Standard k=60.

        Each item dict must carry a unique "rrf_key"; the fused list is sorted
        by fused score, desc, and each item gains "rrf_score" + "rrf_sources".
        """
        import hashlib as _hl
        scores: Dict[str, Dict] = {}
        for list_name, items in ranked_lists:
            for pos, item in enumerate(items, start=1):
                raw_key = item.get("rrf_key") or (
                    item.get("memory_value") or item.get("content") or item.get("text") or ""
                )
                key = _hl.md5(str(raw_key).encode("utf-8", "replace")).hexdigest()
                entry = scores.setdefault(key, {"item": item, "rrf_score": 0.0, "rrf_sources": []})
                entry["rrf_score"] += 1.0 / (k + pos)
                if list_name not in entry["rrf_sources"]:
                    entry["rrf_sources"].append(list_name)
        fused = sorted(scores.values(), key=lambda e: e["rrf_score"], reverse=True)
        out = []
        for e in fused:
            item = dict(e["item"])
            item["rrf_score"] = round(e["rrf_score"], 5)
            item["rrf_sources"] = e["rrf_sources"]
            out.append(item)
        return out

    async def get_context_for_prompt(self, subject: str, limit: int = 5) -> str:
        """Retrieve relevant memory context for a given subject.

        Marveen-inspired hybrid recall: vector search (Brain pgvector, mesh +
        agent corpora) AND keyword search (PG ILIKE) run in PARALLEL, then the
        ranked lists are fused with Reciprocal Rank Fusion — so a memory that
        ranks high in BOTH semantic and lexical spaces surfaces first.
        """
        if not self._enabled or not self._pg_pool:
            return ""

        # ── RRF hybrid recall: vector + keyword in parallel, then fuse ──
        ranked: List[Tuple[str, List[Dict]]] = []

        # Keyword search first (PG, local — cheap and independent of Brain)
        kw_items: List[Dict] = []
        try:
            rows = await self._pg_pool.fetch("""
                SELECT memory_value, source_agent, target_agent, created_at
                FROM mesh.mesh_memory
                WHERE memory_type = 'delegation_result'
                  AND (memory_key ILIKE $1 OR memory_value ILIKE $1)
                ORDER BY created_at DESC
                LIMIT $2
            """, f"%{subject[:50]}%", limit)
            for r in rows:
                kw_items.append({
                    "rrf_key": f"kw:{str(r.get('memory_value', ''))[:100]}",
                    "memory_value": r.get("memory_value") or "",
                    "source_agent": r.get("source_agent") or "?",
                    "created_at": str(r.get("created_at", ""))[:19],
                    "similarity": None,
                })
        except Exception as e:
            log.debug(f"RRF keyword leg failed: {e}")
        if kw_items:
            ranked.append(("keyword", kw_items))

        # Vector searches (Brain server) — same corpora as before
        try:
            import urllib.request
            import urllib.parse
            try:
                brain_host = getattr(self.node.config, 'brain_host', None) or '192.168.1.8'
                brain_port = getattr(self.node.config, 'brain_port', 3322)
            except Exception:
                brain_host = '192.168.1.8'
                brain_port = 3322

            # Search mesh_memory (delegation results)
            mesh_results = []
            try:
                url = f"http://{brain_host}:{brain_port}/mesh/memory/vector?query={urllib.parse.quote(subject)}&limit={limit}"
                req = urllib.request.Request(url, method='GET')
                resp = urllib.request.urlopen(req, timeout=5)
                data = json.loads(resp.read())
                mesh_results = data.get("results", [])
            except Exception:
                pass

            # Search agent_memory (Nova personal memory — knowledge, decisions, etc.)
            agent_results = []
            try:
                url2 = f"http://{brain_host}:{brain_port}/memory/vector?query={urllib.parse.quote(subject)}&limit={limit}"
                req2 = urllib.request.Request(url2, method='GET')
                resp2 = urllib.request.urlopen(req2, timeout=5)
                data2 = json.loads(resp2.read())
                agent_results = data2.get("results", [])
            except Exception:
                pass

            if mesh_results:
                ranked.append(("vector_mesh", mesh_results))
            if agent_results:
                ranked.append(("vector_agent", agent_results))
        except Exception as vec_err:
            log.debug(f"Vector search failed: {vec_err}")

        if not ranked:
            return ""

        # Fuse with RRF, then render
        fused = self._rrf_fuse(ranked)[: limit * 2]

        all_lines = [f"=== Hybrid recall (RRF: {', '.join(n for n, _ in ranked)}) ==="]
        for item in fused:
            srcs = "+".join(item.get("rrf_sources", []))
            score = item.get("rrf_score", 0)
            if item.get("memory_value") is not None:
                # keyword / vector_mesh style entry
                ts = str(item.get("created_at", ""))[:19]
                sender = item.get("source_agent") or item.get("source_agent", "?")
                value = (item.get("memory_value") or "")[:200]
                sim = item.get("similarity")
                sim_s = f" sim={sim}" if sim is not None else ""
                all_lines.append(f"[{ts}] {sender} ({srcs}{sim_s}, rrf={score}): {value}")
            elif item.get("content") is not None or item.get("title") is not None:
                # vector_agent style entry
                cat = item.get("category", "?")
                title = (item.get("title") or "")[:60]
                content = (item.get("content") or "")[:150]
                all_lines.append(f"[{cat}] ({srcs}, rrf={score}): {title} — {content}")

        context = "\n".join(all_lines)
        log.info(f"RRF recall: {len(fused)} fused items from {len(ranked)} lists for '{subject[:30]}'")
        return context

    async def get_recent_memories(self, limit: int = 20) -> List[Dict]:
        """Get recent delegation results from mesh_memory."""
        if not self._enabled or not self._pg_pool:
            return []

        try:
            rows = await self._pg_pool.fetch("""
                SELECT memory_key, memory_value, source_agent, target_agent, 
                       memory_type, created_at, metadata
                FROM mesh.mesh_memory
                ORDER BY created_at DESC
                LIMIT $1
            """, limit)

            return [dict(r) for r in rows] if rows else []

        except Exception as e:
            log.error(f"Error getting recent memories: {e}")
            return []