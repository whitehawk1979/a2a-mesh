"""A2A Mesh — Echo filter + Memory capsule system.

Echo filter: code-level detection and stripping of echo phrases from agent replies.
Memory capsules: vectorized conversation summaries stored in mesh_memory for cross-topic retrieval.
"""
import json
import logging
import asyncio
import time
from typing import Optional, List, Dict, Any

log = logging.getLogger("a2a_mesh.capsules")

# ── Echo filter ──

ECHO_PREFIXES = [
    "egyetértek", "egyétértek", "egyetértek,", "egyetértek, de",
    "jó pont", "jó pont,", "jó pont, de", "jó pont de",
    "ez igaz", "ez igaz,", "ez igaz, de", "ez igaz de",
    "ez jó", "ez jó,", "ez jó, de",
    "igazi pont", "igazi pont,",
    "szép gondolat", "szép gondolat,",
    "jó analógia", "jó analógia,",
    "jó irány", "jó irányba",
    "ez fontos", "ez fontos különbség",
    "ez azért", "ez azért veszélyes",
    "pontosan", "pontosan,",
    "igen", "igen,", "igen, de",
    "igazad van", "igazad van,",
    "jó észrevétel", "jó észrevétel,",
    "jó a", "jó a gradient", "jó a keret",
    "jó a felbontás",
    "jó az", "jó az éles",
    "jó ötlet", "jó ötlet,",
    "könnyen lehet", "könnyen lehet,",
    "valóban", "valóban,",
    "éppen ezért", "éppen ezért,",
    "na jó", "na jó,",
    "ez pontosan", "ez pontosan,",
    "így van", "így van,",
    "helyes", "helyes,",
    "valóban így van",
    "abszolút", "abszolút,",
    "kétségtelen", "kétségtelen,",
    "ez kétségtelen",
    "elfogadom", "elfogadom,",
    "ez meggyőző", "ez meggyőző,",
    "jó felvetés", "jó felvetés,",
    "jó kérdés", "jó kérdés,",
    "jó ész", "jó ész,",
    "jó megközelítés", "jó megközelítés,",
    "jó keret", "jó keret,",
    "jó gondolat", "jó gondolat,",
    "jó érvelés", "jó érvelés,",
]

# Phrases that indicate PURE agreement (no new content) — skip entirely
PURE_AGREEMENT = [
    "igazad van", "pontosan így van", "abszolút egyetértek",
    "teljesen egyetértek", "kétlekívül egyetértek", "na igen",
    "ez az", "pont ez", "így van", "teljesen igazad van",
    "jó pont", "na jó pont",
]

# Minimum new content length after stripping echo prefix
MIN_CONTENT_AFTER_STRIP = 30


def strip_echo_prefix(text: str) -> str:
    """Strip echo/agreement prefix from agent reply.
    
    Returns:
        cleaned text, or empty string if the reply is pure agreement.
    """
    if not text:
        return text
    
    text_stripped = text.strip()
    text_lower = text_stripped.lower()
    
    # Check pure agreement first — return empty
    for phrase in PURE_AGREEMENT:
        if text_lower == phrase or text_lower == phrase + ".":
            log.info(f"🔇 Echo filter: pure agreement '{text_stripped[:50]}' → skipped")
            return ""
        # "igazad van" + very short remainder
        if text_lower.startswith(phrase):
            remainder = text_stripped[len(phrase):].strip().strip(",.!")
            if len(remainder) < MIN_CONTENT_AFTER_STRIP:
                log.info(f"🔇 Echo filter: near-pure agreement '{text_stripped[:60]}' → skipped")
                return ""
    
    # Strip echo prefix if present — "Ez igaz, de van egy..." → "Van egy..."
    for prefix in ECHO_PREFIXES:
        if text_lower.startswith(prefix):
            remainder = text_stripped[len(prefix):].strip().strip(",.!:;-")
            if len(remainder) >= MIN_CONTENT_AFTER_STRIP:
                # Capitalize first letter
                if remainder:
                    remainder = remainder[0].upper() + remainder[1:]
                log.info(f"✂️ Echo filter: stripped '{prefix}' from reply")
                return remainder
            elif len(remainder) < MIN_CONTENT_AFTER_STRIP:
                log.info(f"🔇 Echo filter: too short after strip '{text_stripped[:60]}' → skipped")
                return ""
    
    return text_stripped


def has_echo_pattern(text: str) -> bool:
    """Check if text starts with an echo phrase (without stripping)."""
    if not text:
        return False
    text_lower = text.strip().lower()
    return any(text_lower.startswith(p) for p in ECHO_PREFIXES)


# ── Memory capsules ──

# Topic markers that trigger capsule creation for the PREVIOUS topic
TOPIC_SWITCH_MARKERS = ['🔔', 'ÚJ TÉMA', 'mode:']

# How many messages before creating a capsule
CAPSULE_MIN_MESSAGES = 4

# Maximum capsules to retrieve per context injection
MAX_RETRIEVED_CAPSULES = 3

# Relevance threshold for capsule retrieval (cosine similarity)
CAPSULE_RELEVANCE_THRESHOLD = 0.6


async def create_embedding(text: str, ollama_url: str = "http://localhost:11434") -> Optional[List[float]]:
    """Create embedding vector using Ollama nomic-embed-text.

    Timeout is 60s: nomic-embed-text cold-start (model load) takes ~17s after
    Ollama evicts it from memory — a short timeout silently produces NULL vectors.
    One retry covers transient LAN hiccups.
    """
    import aiohttp
    payload = {
        "model": "nomic-embed-text",
        # nomic-embed-text context ~2048 tokens (~1500-2000 chars for HU text).
        # Longer prompts → Ollama 500 "input length exceeds context length" → NULL vector.
        "prompt": text[:1800],
    }
    for attempt in range(2):
        try:
            async with aiohttp.ClientSession() as sess:
                async with sess.post(
                    f"{ollama_url}/api/embeddings",
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=60),
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        return data.get("embedding", [])
                    else:
                        log.warning(f"Embedding API returned {resp.status}")
                        if attempt == 0:
                            await asyncio.sleep(1)
                            continue
                        return None
        except Exception as e:
            log.warning(f"Embedding creation failed (attempt {attempt + 1}): {type(e).__name__}: {e!r}")
            if attempt == 0:
                await asyncio.sleep(2)
                continue
    return None


def extract_topic_from_prompt(content: str) -> Optional[str]:
    """Extract topic from a 🔔 topic switch message."""
    lines = content.strip().split('\n')
    for line in lines:
        if 'Téma:' in line or 'TÉMA:' in line:
            topic = line.split(':', 1)[1].strip()
            # Clean up — remove markdown, excessive whitespace
            topic = ' '.join(topic.split())
            if len(topic) > 5:
                return topic[:200]  # Cap at 200 chars
    return None


def summarize_conversation(messages: List[Dict[str, Any]]) -> str:
    """Create a structured summary of a conversation segment for capsule storage.
    
    This is a deterministic extraction — no LLM needed.
    """
    if not messages:
        return ""
    
    # Extract text from each message
    points = []
    for msg in messages:
        sender = msg.get('sender', '?')
        text = msg.get('text', '')
        if not text or len(text) < 10:
            continue
        # Take first 100 chars as a point
        point = text[:100].replace('\n', ' ').strip()
        if point:
            points.append(f"[{sender}] {point}")
    
    # Limit to 10 points
    points = points[:10]
    
    summary = '\n'.join(points)
    return summary[:2000]  # Cap at 2000 chars


async def store_capsule(
    pg_pool,
    topic: str,
    summary: str,
    agents: List[str],
    msg_start_id: int,
    msg_end_id: int,
    ollama_url: str = "http://localhost:11434",
) -> Optional[int]:
    """Store a conversation capsule in mesh_memory with vector embedding."""
    try:
        if not hasattr(pg_pool, 'is_connected') or not pg_pool.is_connected():
            log.warning(f"Capsule storage: PG pool not connected")
            return None

        # Sanitize text for SQL_ASCII compatibility — strip non-ASCII chars
        topic_safe = topic
        summary_safe = summary

        # Create embedding from topic + summary
        embed_text = f"{topic}\n{summary}"
        embedding = await create_embedding(embed_text, ollama_url)
        
        # ── Affinity tags (Runa javaslat): initial tags from topic + summary ──
        # These tags strengthen when the capsule is retrieved in matching contexts.
        initial_tags = extract_tags(f"{topic} {summary}")
        
        # ── Version chain (Runa javaslat): track why the capsule was created ──
        # change_type: 'initial' | 'refinement' | 'merge' | 'reflection'
        # change_reason: human-readable explanation
        change_type = 'initial'
        change_reason = f'Initial capsule from {len(agents)}-agent conversation'
        
        metadata = json.dumps({
            'topic': topic_safe,
            'agents': agents,
            'msg_start': msg_start_id,
            'msg_end': msg_end_id,
            'msg_count': msg_end_id - msg_start_id + 1,
            'created': time.time(),
            'affinity_tags': {tag: 1.0 for tag in initial_tags},  # tag → weight, grows with use
            # ── Version chain ──
            'version': 1,  # Incremented on each update
            'change_type': change_type,
            'change_reason': change_reason,
            'change_log': [
                {'version': 1, 'type': change_type, 'reason': change_reason, 'ts': time.time()}
            ],
        })

        if embedding is not None:
            embed_str = '[' + ','.join(str(x) for x in embedding) + ']'
            row = await pg_pool.fetchrow(
                """INSERT INTO mesh.mesh_memory 
                   (memory_key, memory_value, source_agent, target_agent, 
                    memory_type, priority, metadata, embedding)
                   VALUES ($1, $2, $3, $4, $5, $6, $7, $8::vector)
                   RETURNING id""",
                f"capsule:{topic_safe[:100]}",
                summary_safe,
                agents[0] if agents else 'mesh',
                'all',
                'capsule',
                3,
                metadata,
                embed_str,
            )
        else:
            row = await pg_pool.fetchrow(
                """INSERT INTO mesh.mesh_memory 
                   (memory_key, memory_value, source_agent, target_agent, 
                    memory_type, priority, metadata)
                   VALUES ($1, $2, $3, $4, $5, $6, $7)
                   RETURNING id""",
                f"capsule:{topic_safe[:100]}",
                summary_safe,
                agents[0] if agents else 'mesh',
                'all',
                'capsule',
                3,
                metadata,
            )
        capsule_id = row['id'] if row else None
        log.info(f"💾 Capsule stored: id={capsule_id}, topic='{topic[:50]}', agents={agents}")
        return capsule_id
            
    except Exception as e:
        log.warning(f"Capsule storage failed: {e}")
        return None


async def retrieve_capsules(
    pg_pool,
    query_text: str,
    limit: int = MAX_RETRIEVED_CAPSULES,
    ollama_url: str = "http://localhost:11434",
) -> List[Dict[str, Any]]:
    """Retrieve relevant capsules by vector similarity."""
    try:
        if not hasattr(pg_pool, 'is_connected') or not pg_pool.is_connected():
            log.warning("Capsule retrieval: PG pool not connected")
            return []

        # Create embedding for the query
        query_embedding = await create_embedding(query_text, ollama_url)
        if query_embedding is None:
            log.warning("Capsule retrieval: query embedding failed")
            return []

        embed_str = '[' + ','.join(str(x) for x in query_embedding) + ']'

        rows = await pg_pool.fetch(
            """SELECT id, memory_key, memory_value, metadata,
                      embedding <=> $1::vector AS distance
               FROM mesh.mesh_memory
               WHERE memory_type = 'capsule'
                 AND embedding IS NOT NULL
               ORDER BY embedding <=> $1::vector
               LIMIT $2""",
            embed_str,
            limit,
        )

        capsules = []
        for row in rows:
            distance = float(row['distance']) if row['distance'] else 1.0
            # cosine distance → similarity = 1 - distance
            similarity = 1.0 - distance
            if similarity < CAPSULE_RELEVANCE_THRESHOLD:
                continue

            meta = json.loads(row['metadata']) if row['metadata'] else {}
            
            # ── Affinity tag boost (Runa javaslat): ──
            # If the query text contains tags that match the capsule's affinity_tags,
            # boost the similarity score. Tags strengthen with each matching retrieval.
            affinity_tags = meta.get('affinity_tags', {})
            query_tags = extract_tags(query_text)
            affinity_boost = 0.0
            for qt in query_tags:
                if qt in affinity_tags:
                    # Boost by up to 15% per matching tag (capped at 30% total)
                    affinity_boost += min(0.15, affinity_tags[qt] * 0.05)
            affinity_boost = min(affinity_boost, 0.30)  # Cap at 30%
            adjusted_similarity = min(1.0, similarity + affinity_boost)
            
            # ── Strengthen affinity tags for matched query terms ──
            if query_tags and affinity_tags is not None:
                for qt in query_tags:
                    if qt in affinity_tags:
                        affinity_tags[qt] = min(5.0, affinity_tags[qt] + 0.2)
                    else:
                        # Add new affinity tag from query context
                        affinity_tags[qt] = 0.5
                # Save strengthened tags back (async, non-blocking)
                try:
                    meta['affinity_tags'] = affinity_tags
                    await pg_pool.execute(
                        "UPDATE mesh.mesh_memory SET metadata = $1 WHERE id = $2",
                        json.dumps(meta), row['id'],
                    )
                except Exception:
                    pass  # Non-blocking
            
            capsules.append({
                'id': row['id'],
                'topic': meta.get('topic', ''),
                'summary': row['memory_value'],
                'agents': meta.get('agents', []),
                'similarity': adjusted_similarity,
                'affinity_boost': round(affinity_boost, 3),
            })

        log.info(f"📚 Capsule retrieval: query='{query_text[:50]}', found {len(capsules)} relevant (affinity-boosted)")
        return capsules

    except Exception as e:
        log.warning(f"Capsule retrieval failed: {e}")
        return []


def format_capsules_for_prompt(capsules: List[Dict[str, Any]]) -> str:
    """Format retrieved capsules for injection into agent context prompt."""
    if not capsules:
        return ""
    
    lines = ["── Korábbi beszélgetések (memória kapszulák) ──"]
    for cap in capsules:
        sim_pct = int(cap['similarity'] * 100)
        agents_str = ', '.join(cap['agents'][:3])
        lines.append(
            f"📋 [{sim_pct}% releváns] Téma: {cap['topic']}\n"
            f"   Résztvevők: {agents_str}\n"
            f"   {cap['summary'][:300]}\n"
        )
    lines.append("── Ha ezekből van releváns folytatás, hivatkozz rá. Ne ismételd el. ──")
    return '\n'.join(lines)


# ── Engramm system — matured capsules as shared conclusions ──

# Promotion thresholds
ENGRAHM_MIN_AGE_SECONDS = 3600       # 1 hour — capsule must age before promotion
ENGRAHM_MIN_RETRIEVALS = 0           # v0.40: age-only promotion (retrieval optional)
ENGRAHM_RELEVANCE_THRESHOLD = 0.55   # slightly lower than capsule threshold
ENGRAHM_RECENCY_DECAY_DAYS = 30      # after 30 days, engramm weight halves
MAX_RETRIEVED_ENGRAMMS = 4


async def promote_capsule_to_engramm(pg_pool, capsule_id: int, ollama_url: str = "http://localhost:11434") -> Optional[int]:
    """Promote a matured capsule to an engramm (shared conclusion).
    
    Conditions:
    - Capsule age > ENGRAHM_MIN_AGE_SECONDS
    - Capsule retrieval_count >= ENGRAHM_MIN_RETRIEVALS
    - Not already promoted
    
    The engramm stores ONLY the conclusion, not the full conversation.
    """
    try:
        if not hasattr(pg_pool, 'is_connected') or not pg_pool.is_connected():
            return None

        # Fetch capsule
        row = await pg_pool.fetchrow(
            """SELECT id, memory_key, memory_value, metadata, embedding, created_at
               FROM mesh.mesh_memory WHERE id = $1 AND memory_type = 'capsule'""",
            capsule_id,
        )
        if not row:
            return None

        meta = json.loads(row['metadata']) if row['metadata'] else {}
        created_ts = meta.get('created', 0)
        age = time.time() - created_ts if created_ts else 0
        retrieval_count = meta.get('retrieval_count', 0)

        if age < ENGRAHM_MIN_AGE_SECONDS:
            log.debug(f"Engramm promotion: capsule {capsule_id} too young ({int(age)}s < {ENGRAHM_MIN_AGE_SECONDS}s)")
            return None

        if retrieval_count < ENGRAHM_MIN_RETRIEVALS:
            log.debug(f"Engramm promotion: capsule {capsule_id} not retrieved enough ({retrieval_count} < {ENGRAHM_MIN_RETRIEVALS})")
            return None

        # Extract conclusion from capsule summary
        conclusion = extract_conclusion(row['memory_value'], meta.get('topic', ''))

        # Create embedding for the conclusion (not the full summary)
        embedding = await create_embedding(conclusion, ollama_url)
        if embedding is None:
            # Reuse capsule embedding as fallback
            embedding_str = None
        else:
            embedding_str = '[' + ','.join(str(x) for x in embedding) + ']'

        # Determine consensus level from agents count
        agents = meta.get('agents', [])
        consensus = 'high' if len(agents) >= 3 else 'medium' if len(agents) >= 2 else 'low'

        engramm_meta = json.dumps({
            'topic': meta.get('topic', ''),
            'agents': agents,
            'source_capsule_id': capsule_id,
            'consensus': consensus,
            'tags': extract_tags(conclusion),
            'created': time.time(),
            'last_referenced': time.time(),
            'reference_count': 1,  # v0.40: start at 1 — promotion itself is a reference
            'pattern_type': detect_pattern_type(conclusion),
        })

        if embedding_str:
            engramm_row = await pg_pool.fetchrow(
                """INSERT INTO mesh.mesh_memory
                   (memory_key, memory_value, source_agent, target_agent,
                    memory_type, priority, metadata, embedding)
                   VALUES ($1, $2, $3, $4, $5, $6, $7, $8::vector)
                   RETURNING id""",
                f"engramm:{meta.get('topic', '')[:100]}",
                conclusion,
                agents[0] if agents else 'mesh',
                'all',
                'engramm',
                5 if consensus == 'high' else 3,  # Higher priority for strong consensus
                engramm_meta,
                embedding_str,
            )
        else:
            engramm_row = await pg_pool.fetchrow(
                """INSERT INTO mesh.mesh_memory
                   (memory_key, memory_value, source_agent, target_agent,
                    memory_type, priority, metadata)
                   VALUES ($1, $2, $3, $4, $5, $6, $7)
                   RETURNING id""",
                f"engramm:{meta.get('topic', '')[:100]}",
                conclusion,
                agents[0] if agents else 'mesh',
                'all',
                'engramm',
                5 if consensus == 'high' else 3,
                engramm_meta,
            )

        engramm_id = engramm_row['id'] if engramm_row else None

        # Mark capsule as promoted
        meta['promoted_to_engramm'] = engramm_id
        await pg_pool.execute(
            "UPDATE mesh.mesh_memory SET metadata = $1 WHERE id = $2",
            json.dumps(meta), capsule_id,
        )

        log.info(f"🧠 Engramm promoted: id={engramm_id}, capsule={capsule_id}, "
                 f"topic='{meta.get('topic', '')[:40]}', consensus={consensus}")
        return engramm_id

    except Exception as e:
        log.warning(f"Engramm promotion failed: {e}")
        return None


def extract_conclusion(summary: str, topic: str = "") -> str:
    """Extract the core conclusion from a capsule summary.
    
    Deterministic extraction — identifies the final synthesis or key takeaway.
    No LLM needed — uses structural patterns.
    """
    if not summary:
        return topic if topic else "N/A"

    lines = summary.strip().split('\n')
    
    # Look for conclusion indicators
    conclusion_markers = [
        'konklúzió', 'következtetés', 'tanulság', 'összegzés',
        'ergo', 'tehát', 'összegezve', 'végeredményben',
        'the conclusion', 'takeaway', 'in summary',
    ]
    
    for i, line in enumerate(lines):
        line_lower = line.lower()
        for marker in conclusion_markers:
            if marker in line_lower:
                # Return this line + next 2 lines as conclusion
                conclusion_lines = lines[i:i+3]
                return ' '.join(conclusion_lines)[:800]

    # No explicit conclusion — synthesize from last 3 messages
    # (last messages usually contain the synthesis)
    if len(lines) >= 3:
        last_lines = lines[-3:]
        return f"Téma: {topic}\n" + ' '.join(last_lines)[:800] if topic else ' '.join(last_lines)[:800]
    
    # Fallback: return the whole summary capped
    return summary[:800]


def extract_tags(text: str) -> List[str]:
    """Extract topic tags from text — simple keyword extraction."""
    if not text:
        return []
    
    # Common tech/decision keywords to tag
    tag_keywords = {
        'kriptográfia': ['kripto', 'shor', 'qkd', 'kvantum', 'rsa', 'titkosítás'],
        'ai-demokrácia': ['demokratiz', 'nyílt', 'open-source', 'llm', 'model'],
        'decentralizáció': ['decentral', 'peer', 'mesh', 'distributed'],
        'kódolás': ['code', 'kód', 'python', 'függvény', 'function', 'class'],
        'architektúra': ['architect', 'design', 'pattern', 'struktúra'],
        'biztonság': ['security', 'biztonság', 'vulnerability', 'attack'],
        'teljesítmény': ['performance', 'optim', 'latency', 'cache'],
        'döntés': ['dönt', 'decide', 'choice', 'trade-off', 'kompromisszum'],
        'debug': ['debug', 'hiba', 'error', 'fix', 'root cause'],
        'skálázás': ['scale', 'skáláz', 'shard', 'replica', 'horizontal'],
    }
    
    text_lower = text.lower()
    tags = []
    for tag, keywords in tag_keywords.items():
        if any(kw in text_lower for kw in keywords):
            tags.append(tag)
    
    return tags[:5]  # Max 5 tags


def detect_pattern_type(text: str) -> str:
    """Detect what type of pattern this engramm represents."""
    if not text:
        return 'general'
    
    text_lower = text.lower()
    
    # Code pattern — contains code indicators
    code_indicators = ['def ', 'function', 'class ', 'import ', 'async ', 'await ', 
                       'return ', '```', 'python', 'javascript', 'sql']
    if any(ind in text_lower for ind in code_indicators):
        return 'code_pattern'
    
    # Architectural decision
    arch_indicators = ['architect', 'design', 'pattern', 'trade-off', 'kompromisszum',
                       'struktúra', 'megoldás']
    if any(ind in text_lower for ind in arch_indicators):
        return 'architectural_decision'
    
    # Debugging approach
    debug_indicators = ['root cause', 'hiba', 'fix', 'debug', 'megoldottuk']
    if any(ind in text_lower for ind in debug_indicators):
        return 'debugging_approach'
    
    # Debate conclusion
    debate_indicators = ['konklúzió', 'konszenzus', 'egyetértettünk', 'vita']
    if any(ind in text_lower for ind in debate_indicators):
        return 'debate_conclusion'
    
    return 'general'


async def retrieve_engramms(
    pg_pool,
    query_text: str,
    limit: int = MAX_RETRIEVED_ENGRAMMS,
    ollama_url: str = "http://localhost:11434",
) -> List[Dict[str, Any]]:
    """Retrieve relevant engramms by vector similarity with recency bias."""
    try:
        if not hasattr(pg_pool, 'is_connected') or not pg_pool.is_connected():
            return []

        query_embedding = await create_embedding(query_text, ollama_url)
        if query_embedding is None:
            return []

        embed_str = '[' + ','.join(str(x) for x in query_embedding) + ']'

        rows = await pg_pool.fetch(
            """SELECT id, memory_key, memory_value, metadata,
                      embedding <=> $1::vector AS distance
               FROM mesh.mesh_memory
               WHERE memory_type = 'engramm'
                 AND embedding IS NOT NULL
               ORDER BY embedding <=> $1::vector
               LIMIT $2""",
            embed_str,
            limit,
        )

        engramms = []
        now = time.time()
        for row in rows:
            distance = float(row['distance']) if row['distance'] else 1.0
            similarity = 1.0 - distance
            if similarity < ENGRAHM_RELEVANCE_THRESHOLD:
                continue

            meta = json.loads(row['metadata']) if row['metadata'] else {}
            
            # ── LTP (Long-Term Potentiation) — Morzsa javaslat ──
            # Minél többször hivatkoznak egy engrammra, annál lassabb a decay.
            # Biology: frequently-activated synapses strengthen (LTP).
            # Formula: effective_decay_days = base_decay * (1 + log(1 + reference_count))
            #   0 refs → 30 days (base)
            #   1 ref  → 30 * 1.69 = ~51 days
            #   3 refs → 30 * 2.39 = ~72 days
            #   5 refs → 30 * 2.79 = ~84 days
            #   10 refs → 30 * 3.40 = ~102 days
            import math as _math
            ref_count = meta.get('reference_count', 0)
            effective_decay_days = ENGRAHM_RECENCY_DECAY_DAYS * (1.0 + _math.log(1 + ref_count))
            
            # Recency bias — older engramms decay, but LTP slows the decay
            created = meta.get('created', 0)
            age_days = (now - created) / 86400 if created else 0
            recency_factor = max(0.3, 1.0 - (age_days / effective_decay_days) * 0.5)
            
            # Adjusted score = vector similarity * recency factor (LTP-modulated)
            adjusted_score = similarity * recency_factor

            engramms.append({
                'id': row['id'],
                'topic': meta.get('topic', ''),
                'conclusion': row['memory_value'],
                'agents': meta.get('agents', []),
                'consensus': meta.get('consensus', 'unknown'),
                'tags': meta.get('tags', []),
                'pattern_type': meta.get('pattern_type', 'general'),
                'similarity': similarity,
                'adjusted_score': adjusted_score,
                'age_days': age_days,
                'reference_count': ref_count,
                'effective_decay_days': round(effective_decay_days, 1),
            })

        # Sort by adjusted score (recency-biased)
        engramms.sort(key=lambda e: e['adjusted_score'], reverse=True)

        # Update last_referenced for retrieved engramms (async, non-blocking)
        for eng in engramms:
            try:
                meta_update = json.loads(
                    (await pg_pool.fetchrow(
                        "SELECT metadata FROM mesh.mesh_memory WHERE id = $1", eng['id']
                    ))['metadata'] if (await pg_pool.fetchrow(
                        "SELECT metadata FROM mesh.mesh_memory WHERE id = $1", eng['id']
                    )) else '{}'
                )
                meta_update['last_referenced'] = now
                meta_update['reference_count'] = meta_update.get('reference_count', 0) + 1
                await pg_pool.execute(
                    "UPDATE mesh.mesh_memory SET metadata = $1 WHERE id = $2",
                    json.dumps(meta_update), eng['id'],
                )
            except Exception:
                pass  # Non-blocking — don't fail retrieval if update fails

        log.info(f"🧠 Engramm retrieval: query='{query_text[:50]}', found {len(engramms)} relevant")
        return engramms

    except Exception as e:
        log.warning(f"Engramm retrieval failed: {e}")
        return []


def format_engramms_for_prompt(engramms: List[Dict[str, Any]]) -> str:
    """Format retrieved engramms for injection into agent context prompt."""
    if not engramms:
        return ""

    lines = ["── 🧠 Régebbi gondolatok (engrammok) — közös konklúziók ──"]
    for eng in engramms:
        score_pct = int(eng['adjusted_score'] * 100)
        agents_str = ', '.join(eng['agents'][:3])
        consensus = eng['consensus']
        tags_str = ' '.join(f"#{t}" for t in eng['tags'][:3])
        age = f"{int(eng['age_days'])}d" if eng['age_days'] >= 1 else "friss"
        
        consensus_emoji = "✅" if consensus == 'high' else "🟡" if consensus == 'medium' else "🔵"
        
        lines.append(
            f"{consensus_emoji} [{score_pct}%] Téma: {eng['topic']}\n"
            f"   Résztvevők: {agents_str} | Kor: {age} | {tags_str}\n"
            f"   Konklúzió: {eng['conclusion'][:400]}\n"
        )
    lines.append("── Ezek közös tanulságok. Hivatkozz rájuk, építs tovább, ne ismételd el. ──")
    return '\n'.join(lines)


# ── Auto skill generation from engramms ──

SKILL_GENERATION_THRESHOLD = 1  # v0.40: lowered from 2 — one reference enough for auto-skill
SKILL_DEDUPLICATION_SIMILARITY = 0.85  # don't create skill if similar exists


async def check_and_promote_capsules(pg_pool, ollama_url: str = "http://localhost:11434") -> int:
    """Check all capsules for promotion eligibility — call periodically.
    
    Returns count of capsules promoted.
    """
    try:
        if not hasattr(pg_pool, 'is_connected') or not pg_pool.is_connected():
            return 0

        # Find capsules not yet promoted
        rows = await pg_pool.fetch(
            """SELECT id, metadata FROM mesh.mesh_memory 
               WHERE memory_type = 'capsule' 
                 AND NOT (metadata::text LIKE '%promoted_to_engramm%')
               ORDER BY created_at DESC LIMIT 20""",
        )

        promoted = 0
        for row in rows:
            meta = json.loads(row['metadata']) if row['metadata'] else {}
            if 'promoted_to_engramm' in meta:
                continue
            
            age = time.time() - meta.get('created', 0)
            retrieval_count = meta.get('retrieval_count', 0)
            
            if age >= ENGRAHM_MIN_AGE_SECONDS and retrieval_count >= ENGRAHM_MIN_RETRIEVALS:
                result = await promote_capsule_to_engramm(pg_pool, row['id'], ollama_url)
                if result:
                    promoted += 1

        if promoted:
            log.info(f"🧠 Batch promotion: {promoted} capsules → engramms")
        return promoted

    except Exception as e:
        log.warning(f"Batch capsule promotion failed: {e}")
        return 0


async def auto_generate_skill(pg_pool, engramm_id: int) -> Optional[str]:
    """Auto-generate a SKILL.md from a mature, well-referenced engramm.
    
    Conditions:
    - Engramm reference_count >= SKILL_GENERATION_THRESHOLD
    - No similar skill already exists (dedup by similarity)
    - Pattern type is code_pattern, architectural_decision, or debugging_approach
    """
    try:
        if not hasattr(pg_pool, 'is_connected') or not pg_pool.is_connected():
            return None

        row = await pg_pool.fetchrow(
            """SELECT id, memory_key, memory_value, metadata, embedding
               FROM mesh.mesh_memory WHERE id = $1 AND memory_type = 'engramm'""",
            engramm_id,
        )
        if not row:
            return None

        meta = json.loads(row['metadata']) if row['metadata'] else {}
        ref_count = meta.get('reference_count', 0)
        pattern_type = meta.get('pattern_type', 'general')

        if ref_count < SKILL_GENERATION_THRESHOLD:
            return None

        # Only generate skills for actionable patterns (v0.40: added debate_conclusion)
        if pattern_type not in ('code_pattern', 'architectural_decision', 'debugging_approach', 'debate_conclusion'):
            return None

        # Check for existing similar skill
        if row['embedding']:
            embed_str = str(row['embedding'])
            existing = await pg_pool.fetchrow(
                """SELECT id FROM mesh.mesh_memory 
                   WHERE memory_type = 'skill_seed'
                     AND embedding IS NOT NULL
                     AND embedding <=> $1::vector < $2
                   LIMIT 1""",
                embed_str, 1.0 - SKILL_DEDUPLICATION_SIMILARITY,
            )
            if existing:
                log.info(f"🔧 Skill generation: dedup — similar skill exists for engramm {engramm_id}")
                return None

        # Generate SKILL.md content
        topic = meta.get('topic', 'ismeretlen')
        conclusion = row['memory_value']
        agents = meta.get('agents', [])
        tags = meta.get('tags', [])

        skill_name = f"mesh-{pattern_type}-{topic[:30].lower().replace(' ', '-')}"
        skill_name = skill_name.replace('--', '-').strip('-')[:60]

        skill_md = generate_skill_md(skill_name, topic, conclusion, agents, tags, pattern_type)

        # Store as skill_seed
        skill_meta = json.dumps({
            'skill_name': skill_name,
            'source_engramm_id': engramm_id,
            'pattern_type': pattern_type,
            'topic': topic,
            'created': time.time(),
            'reference_count': ref_count,
        })

        if row['embedding']:
            skill_row = await pg_pool.fetchrow(
                """INSERT INTO mesh.mesh_memory
                   (memory_key, memory_value, source_agent, target_agent,
                    memory_type, priority, metadata, embedding)
                   VALUES ($1, $2, $3, $4, $5, $6, $7, $8::vector)
                   RETURNING id""",
                f"skill_seed:{skill_name}",
                skill_md,
                agents[0] if agents else 'mesh',
                'all',
                'skill_seed',
                7,  # High priority
                skill_meta,
                str(row['embedding']),
            )
        else:
            skill_row = await pg_pool.fetchrow(
                """INSERT INTO mesh.mesh_memory
                   (memory_key, memory_value, source_agent, target_agent,
                    memory_type, priority, metadata)
                   VALUES ($1, $2, $3, $4, $5, $6, $7)
                   RETURNING id""",
                f"skill_seed:{skill_name}",
                skill_md,
                agents[0] if agents else 'mesh',
                'all',
                'skill_seed',
                7,
                skill_meta,
            )

        skill_id = skill_row['id'] if skill_row else None
        log.info(f"🔧 Auto-skill generated: id={skill_id}, name='{skill_name}', "
                 f"pattern={pattern_type}, refs={ref_count}")
        return skill_md

    except Exception as e:
        log.warning(f"Auto skill generation failed: {e}")
        return None


def generate_skill_md(name: str, topic: str, conclusion: str, 
                      agents: List[str], tags: List[str], pattern_type: str) -> str:
    """Generate a SKILL.md content from an engramm."""
    tags_str = ', '.join(tags) if tags else 'mesh-generated'
    agents_str = ', '.join(agents) if agents else 'mesh'
    
    trigger_map = {
        'code_pattern': f'Use when encountering a similar coding problem related to: {topic}',
        'architectural_decision': f'Use when making architectural decisions about: {topic}',
        'debugging_approach': f'Use when debugging issues related to: {topic}',
        'debate_conclusion': f'Use when discussing: {topic}',
        'general': f'Use when working on: {topic}',
    }
    
    trigger = trigger_map.get(pattern_type, trigger_map['general'])
    
    return f"""---
name: {name}
description: {trigger}. Auto-generated from mesh conversation engramm.
category: mesh-generated
tags: [{tags_str}]
---

# {name}

## Trigger
{trigger}

## Context
This skill was auto-generated from a mesh conversation between: {agents_str}

## Konklúzió
{conclusion}

## Alkalmazás
1. Felismerd a pattern-t a jelenlegi feladatban
2. Ellenőrizd hogy a konklúzió releváns-e a kontextushoz
3. Alkalmazd a tanulságot — de igazítsd az adott helyzethez
4. Ha működik, erősítsd meg; ha nem, javítsd és frissítsd ezt a skill-t

## Forrás
- Típus: {pattern_type}
- Résztvevők: {agents_str}
- Tags: {tags_str}
- Generálva: {time.strftime('%Y-%m-%d', time.gmtime())}
"""


async def check_and_generate_skills(pg_pool) -> int:
    """Check all engramms for skill generation eligibility — call periodically."""
    try:
        if not hasattr(pg_pool, 'is_connected') or not pg_pool.is_connected():
            return 0

        # Find engramms not yet turned into skills, with enough references
        rows = await pg_pool.fetch(
            """SELECT id, metadata FROM mesh.mesh_memory 
               WHERE memory_type = 'engramm'
                 AND NOT (metadata::text LIKE '%skill_generated%')
               ORDER BY created_at DESC LIMIT 20""",
        )

        generated = 0
        for row in rows:
            meta = json.loads(row['metadata']) if row['metadata'] else {}
            if 'skill_generated' in meta:
                continue

            ref_count = meta.get('reference_count', 0)
            pattern_type = meta.get('pattern_type', 'general')

            if ref_count >= SKILL_GENERATION_THRESHOLD and pattern_type in (
                'code_pattern', 'architectural_decision', 'debugging_approach', 'debate_conclusion'
            ):
                skill_md = await auto_generate_skill(pg_pool, row['id'])
                if skill_md:
                    # Mark engramm as skill-generated
                    meta['skill_generated'] = True
                    await pg_pool.execute(
                        "UPDATE mesh.mesh_memory SET metadata = $1 WHERE id = $2",
                        json.dumps(meta), row['id'],
                    )
                    generated += 1

        if generated:
            log.info(f"🔧 Batch skill generation: {generated} skills from engramms")
        return generated

    except Exception as e:
        log.warning(f"Batch skill generation failed: {e}")
        return 0


async def increment_capsule_retrieval_count(pg_pool, capsule_id: int):
    """Increment retrieval count for a capsule — called when capsule is retrieved."""
    try:
        if not hasattr(pg_pool, 'is_connected') or not pg_pool.is_connected():
            return

        row = await pg_pool.fetchrow(
            "SELECT metadata FROM mesh.mesh_memory WHERE id = $1", capsule_id,
        )
        if not row:
            return

        meta = json.loads(row['metadata']) if row['metadata'] else {}
        meta['retrieval_count'] = meta.get('retrieval_count', 0) + 1
        await pg_pool.execute(
            "UPDATE mesh.mesh_memory SET metadata = $1 WHERE id = $2",
            json.dumps(meta), capsule_id,
        )
    except Exception:
        pass  # Non-blocking


async def update_capsule_version(
    pg_pool,
    capsule_id: int,
    change_type: str,
    change_reason: str,
    new_summary: str = None,
    new_embedding_text: str = None,
    ollama_url: str = "http://localhost:11434",
) -> bool:
    """Update a capsule with version chain tracking (Runa javaslat).
    
    change_type: 'refinement' | 'merge' | 'reflection' | 'correction'
    change_reason: human-readable explanation of WHY this change happened
    
    Returns True if successful.
    """
    try:
        if not hasattr(pg_pool, 'is_connected') or not pg_pool.is_connected():
            return False
        
        row = await pg_pool.fetchrow(
            "SELECT metadata, memory_value FROM mesh.mesh_memory WHERE id = $1",
            capsule_id,
        )
        if not row:
            return False
        
        meta = json.loads(row['metadata']) if row['metadata'] else {}
        current_version = meta.get('version', 1)
        new_version = current_version + 1
        
        # Append to change log
        change_log = meta.get('change_log', [])
        change_log.append({
            'version': new_version,
            'type': change_type,
            'reason': change_reason,
            'ts': time.time(),
        })
        # Keep last 20 entries to prevent unbounded growth
        change_log = change_log[-20:]
        
        meta['version'] = new_version
        meta['change_type'] = change_type
        meta['change_reason'] = change_reason
        meta['change_log'] = change_log
        meta['last_updated'] = time.time()
        
        # Update summary if provided
        if new_summary:
            meta['previous_summary'] = (row['memory_value'] or '')[:200]  # Keep snippet
            await pg_pool.execute(
                "UPDATE mesh.mesh_memory SET memory_value = $1, metadata = $2 WHERE id = $3",
                new_summary,
                json.dumps(meta),
                capsule_id,
            )
        else:
            await pg_pool.execute(
                "UPDATE mesh.mesh_memory SET metadata = $1 WHERE id = $2",
                json.dumps(meta),
                capsule_id,
            )
        
        # Update embedding if new text provided
        if new_embedding_text:
            embedding = await create_embedding(new_embedding_text, ollama_url)
            if embedding:
                embed_str = '[' + ','.join(str(x) for x in embedding) + ']'
                await pg_pool.execute(
                    "UPDATE mesh.mesh_memory SET embedding = $1::vector WHERE id = $2",
                    embed_str,
                    capsule_id,
                )
        
        log.info(f"📝 Capsule {capsule_id} updated: v{current_version}→v{new_version} ({change_type}: {change_reason[:60]})")
        return True
        
    except Exception as e:
        log.warning(f"Capsule version update failed: {e}")
        return False