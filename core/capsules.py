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
    """Create embedding vector using Ollama nomic-embed-text."""
    try:
        import aiohttp
        async with aiohttp.ClientSession() as sess:
            payload = {
                "model": "nomic-embed-text",
                "prompt": text[:8000],  # nomic-embed-text has a context limit
            }
            async with sess.post(
                f"{ollama_url}/api/embeddings",
                json=payload,
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data.get("embedding", [])
                else:
                    log.warning(f"Embedding API returned {resp.status}")
                    return None
    except Exception as e:
        log.warning(f"Embedding creation failed: {e}")
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
        # Create embedding from topic + summary
        embed_text = f"{topic}\n{summary}"
        embedding = await create_embedding(embed_text, ollama_url)
        
        if embedding is None:
            log.warning(f"Capsule storage: embedding failed for topic '{topic[:50]}'")
            # Store without embedding — still useful for keyword search
            embedding = None
        
        # Insert into mesh_memory
        async with pg_pool.acquire() as conn:
            if embedding is not None:
                # Convert embedding to pgvector format
                embed_str = '[' + ','.join(str(x) for x in embedding) + ']'
                row = await conn.fetchrow(
                    """INSERT INTO mesh.mesh_memory 
                       (memory_key, memory_value, source_agent, target_agent, 
                        memory_type, priority, metadata, embedding)
                       VALUES ($1, $2, $3, $4, $5, $6, $7, $8::vector)
                       RETURNING id""",
                    f"capsule:{topic[:100]}",
                    summary,
                    agents[0] if agents else 'mesh',
                    'all',
                    'capsule',
                    3,  # Medium priority
                    json.dumps({
                        'topic': topic,
                        'agents': agents,
                        'msg_start': msg_start_id,
                        'msg_end': msg_end_id,
                        'msg_count': msg_end_id - msg_start_id + 1,
                        'created': time.time(),
                    }),
                    embed_str,
                )
            else:
                row = await conn.fetchrow(
                    """INSERT INTO mesh.mesh_memory 
                       (memory_key, memory_value, source_agent, target_agent, 
                        memory_type, priority, metadata)
                       VALUES ($1, $2, $3, $4, $5, $6, $7)
                       RETURNING id""",
                    f"capsule:{topic[:100]}",
                    summary,
                    agents[0] if agents else 'mesh',
                    'all',
                    'capsule',
                    3,
                    json.dumps({
                        'topic': topic,
                        'agents': agents,
                        'msg_start': msg_start_id,
                        'msg_end': msg_end_id,
                        'msg_count': msg_end_id - msg_start_id + 1,
                        'created': time.time(),
                    }),
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
        # Create embedding for the query
        query_embedding = await create_embedding(query_text, ollama_url)
        if query_embedding is None:
            log.warning("Capsule retrieval: query embedding failed")
            return []
        
        embed_str = '[' + ','.join(str(x) for x in query_embedding) + ']'
        
        async with pg_pool.acquire() as conn:
            rows = await conn.fetch(
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
                capsules.append({
                    'id': row['id'],
                    'topic': meta.get('topic', ''),
                    'summary': row['memory_value'],
                    'agents': meta.get('agents', []),
                    'similarity': similarity,
                })
            
            log.info(f"📚 Capsule retrieval: query='{query_text[:50]}', found {len(capsules)} relevant")
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