"""A2A Mesh — Reflection engine: conversation analysis and eszmefuttatas.

Periodically analyzes ongoing mesh conversation to provide:
- Stagnation detection (agents going in circles)
- Consensus detection (converging opinions)
- Blind spot detection (missed angles)
- Synthesis suggestions (where the conversation could go deeper)
- Reflection injection into agent prompts as "🔍 Ezmefuttatas"
"""
import json
import logging
import time
import asyncio
from typing import Optional, List, Dict, Any, Tuple
from collections import Counter

log = logging.getLogger("a2a_mesh.reflection")

# ── Config ──
REFLECTION_INTERVAL_MIN = 3   # min messages between reflections (stagnation = frequent)
REFLECTION_INTERVAL_MAX = 8   # max messages between reflections (progress = rare)
STAGNATION_WINDOW = 8    # check last N messages for repetition
CONSENSUS_THRESHOLD = 0.7  # 70% agreement = consensus
REFLECTION_RELEVANCE_THRESHOLD = 0.5  # minimum similarity for retrieved reflections

# Hungarian stop words for topic extraction
STOP_WORDS = frozenset([
    'pedig', 'azonban', 'viszont', 'mintha', 'amikor', 'mert', 'hogy', 'ezzel',
    'ugyan', 'igy', 'ott', 'annak', 'ehelyett', 'vagyok', 'vagy', 'van', 'nem',
    'igen', 'ez', 'az', 'egy', 'the', 'and', 'but', 'for', 'with', 'from',
    'szerintem', 'szerinte', 'szerint', 'gondolom', 'talam', 'talán', 'lehet',
    'kellene', 'kell', 'lehetne', 'helyes', 'jó', 'rossz', 'hibás', 'helytelen',
    'hanem', 'ismeretlen', 'akkor', 'mint', 'ilyen', 'olyan', 'ezek', 'azok',
    'erre', 'arra', 'innen', 'onnan', 'ahol', 'ahova', 'amely', 'amelyik',
    'minden', 'semmi', 'valami', 'bármi', 'sokkal', 'kevesebb', 'több',
    'első', 'második', 'utolsó', 'következő', 'előző', 'jelenlegi',
    'ellentétben', 'kapcsolatban', 'alapján', 'révén', 'keresztül', 'soron',
])


def extract_topic_from_conversation(messages: List[Dict[str, Any]]) -> str:
    """Extract topic from conversation content — not just 🔔 markers.
    
    Uses keyword frequency analysis on significant words across all messages.
    Falls back to first user message first sentence if no keywords found.
    """
    if not messages:
        return "ismeretlen"

    # Collect all text
    all_text = ' '.join(_extract_content(m) for m in messages).lower()

    # Extract significant words (>4 chars, not stop words)
    words = []
    for w in all_text.split():
        w = w.strip('.,!?;:"\'()[]{}áéíóöőúüű')
        if len(w) > 4 and w not in STOP_WORDS:
            words.append(w)

    if not words:
        # Fallback: first user message first sentence
        for m in messages:
            sender = _extract_sender(m)
            if sender and sender.lower() not in ('nova', 'morzsa', 'runa'):
                content = _extract_content(m)
                first_sentence = content.split('.')[0].split('!')[0].split('?')[0]
                if len(first_sentence) > 10:
                    return first_sentence.strip()[:150]
        return "ismeretlen"

    # Top 3-5 keywords = topic
    word_counts = Counter(words)
    top_words = [w for w, c in word_counts.most_common(5) if c >= 2]

    if not top_words:
        # Single occurrence words — take the most significant
        top_words = [word_counts.most_common(3)[i][0] for i in range(min(3, len(word_counts)))]

    topic = ' '.join(top_words[:5])
    return topic[:150] if len(topic) > 5 else "ismeretlen"

# ── Reflection types ──
REFLECTION_TYPES = {
    'stagnation': '🔄',
    'consensus': '🤝',
    'blind_spot': '🔍',
    'synthesis': '💡',
    'progress': '📈',
    'tension': '⚡',
}


def _extract_content(msg: Dict[str, Any]) -> str:
    """Extract text content from a message dict."""
    if isinstance(msg, dict):
        return msg.get('content', '') or msg.get('text', '') or ''
    return str(msg)


def _extract_sender(msg: Dict[str, Any]) -> str:
    """Extract sender from a message dict."""
    if isinstance(msg, dict):
        return msg.get('sender', '') or msg.get('username', '') or ''
    return ''


def _detect_stagnation(messages: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Detect if conversation is going in circles — repeating similar arguments."""
    if len(messages) < STAGNATION_WINDOW:
        return None

    recent = messages[-STAGNATION_WINDOW:]
    contents = [_extract_content(m).lower() for m in recent]

    # Check for repeated keywords/phrases
    words = []
    for c in contents:
        # Extract significant words (>4 chars, not common)
        for w in c.split():
            w = w.strip('.,!?;:"\'()[]{}')
            if len(w) > 4 and w not in ('pedig', 'azonban', 'viszont', 'mintha', 'pedig', 'amikor', 'mert', 'hogy', 'ezzel', 'ugyan', 'igy', 'ott', 'annak', 'ehelyett'):
                words.append(w)

    word_counts = Counter(words)
    repeated = [(w, c) for w, c in word_counts.most_common(5) if c >= 3]

    if not repeated:
        return None

    # Check if the same agents are repeating similar points
    senders = [_extract_sender(m) for m in recent]
    sender_counts = Counter(senders)

    # If one agent is dominating (>60% of recent messages)
    dominant_agent = None
    for s, c in sender_counts.most_common(1):
        if c >= len(recent) * 0.6:
            dominant_agent = s

    repeated_words = [w for w, c in repeated]

    return {
        'type': 'stagnation',
        'repeated_words': repeated_words,
        'dominant_agent': dominant_agent,
        'message_count': len(recent),
        'analysis': f"A beszélgetés kezd körbejárni — '{', '.join(repeated_words[:3])}' szavak többször előfordulnak. "
                     f"{'A ' + dominant_agent + ' dominálja a beszélgetést. ' if dominant_agent else ''}"
                     f"Érdemes új szemszöget bevezetni vagy a konklúzió felé terelni.",
    }


def _detect_consensus(messages: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Detect if agents are converging on agreement."""
    if len(messages) < 4:
        return None

    recent = messages[-6:] if len(messages) >= 6 else messages[-4:]
    contents = [_extract_content(m).lower() for m in recent]

    # Agreement signals
    agreement_words = ['egyetért', 'pont', 'igaz', 'jól látod', 'jó irány', 'helyes',
                       'pontosan', 'egyet', 'valóban', ' Így van', 'így van',
                       'correct', 'agreed', 'exactly', 'right']
    disagreement_words = ['viszont', 'azonban', 'de ', 'nem ', 'rossz', 'hibás',
                          'mégsem', 'ellenkező', 'viszont', 'kérdőjelez', 'arárul']

    agreements = 0
    disagreements = 0
    for c in contents:
        for w in agreement_words:
            if w in c:
                agreements += 1
                break
        for w in disagreement_words:
            if w in c:
                disagreements += 1
                break

    total = agreements + disagreements
    if total < 2:
        return None

    agreement_ratio = agreements / total
    if agreement_ratio >= CONSENSUS_THRESHOLD:
        return {
            'type': 'consensus',
            'agreement_ratio': round(agreement_ratio, 2),
            'agreements': agreements,
            'disagreements': disagreements,
            'analysis': f"A résztvevők konvergálnak — {agreements} egyetértés vs {disagreements} ellenvetés "
                        f"({int(agreement_ratio * 100)}%). Érdemes a konklúziót rögzíteni és új témára váltani.",
        }

    return None


def _detect_blind_spots(messages: List[Dict[str, Any]], topic: str = "") -> Optional[Dict[str, Any]]:
    """Detect angles that haven't been explored yet."""
    if len(messages) < 4:
        return None

    all_text = ' '.join(_extract_content(m).lower() for m in messages)

    # Common angles in technical discussions
    angles = {
        'biztonság': ['biztonság', 'security', 'támadás', 'vulnerability', 'attack'],
        'teljesítmény': ['teljesítmény', 'performance', 'sebesség', 'latency', 'throughput'],
        'skálázhatóság': ['skáláz', 'scale', 'horizontális', 'vertikális'],
        'költség': ['költség', 'cost', 'ár', 'drága', 'olcsó'],
        'komplexitás': ['komplex', 'complex', 'egyszerű', 'simple', 'bonyolult'],
        'emberi tényező': ['ember', 'human', 'felhasználó', 'user', 'elfogadás', 'adoption'],
        'kARBANTARTHATÓSÁG': ['karbantart', 'maintenance', 'fenntart', 'megtart'],
        'etika': ['etika', 'ethical', 'moral', 'felelősség', 'responsibility'],
        'valós idejű': ['valós idej', 'real-time', 'azonnali', 'immediate'],
        'hagyományos rendszer': ['hagyományos', 'traditional', 'centralizált', 'legacy'],
    }

    unexplored = []
    for angle, keywords in angles.items():
        if not any(kw in all_text for kw in keywords):
            unexplored.append(angle)

    if len(unexplored) >= 3:
        return {
            'type': 'blind_spot',
            'unexplored_angles': unexplored[:5],
            'analysis': f"A beszélgetés eddig nem érintette: {', '.join(unexplored[:5])}. "
                        f"Ezek közül valamelyik mélyíthetné a vitát.",
        }

    return None


def _detect_tension(messages: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Detect productive tension — opposing views that could lead to synthesis."""
    if len(messages) < 4:
        return None

    recent = messages[-6:]
    senders = [_extract_sender(m) for m in recent]

    # Count disagreement patterns per sender
    sender_disagreements = {}
    for m in recent:
        s = _extract_sender(m)
        c = _extract_content(m).lower()
        if any(w in c for w in ['viszont', 'azonban', 'de ', 'nem ', 'mégsem', 'ellenkező']):
            sender_disagreements[s] = sender_disagreements.get(s, 0) + 1

    if len(sender_disagreements) < 2:
        return None

    # There are multiple agents disagreeing — productive tension
    agents_in_tension = list(sender_disagreements.keys())
    return {
        'type': 'tension',
        'agents': agents_in_tension,
        'disagreement_count': sum(sender_disagreements.values()),
        'analysis': f"Feszültség {', '.join(agents_in_tension)} között — "
                    f"{sum(sender_disagreements.values())} ellenvetés az utolsó üzenetekben. "
                    f"Ez a feszültség termékeny lehet — érdemes szintézist keresni.",
    }


def _detect_progress(messages: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Detect if the conversation is making forward progress."""
    if len(messages) < 6:
        return None

    recent = messages[-6:]
    contents = [_extract_content(m).lower() for m in recent]

    # Progress signals: building on previous, referencing, proposing solutions
    progress_signals = ['tehát', 'összegz', 'konklúzió', 'eredmény', 'megoldás',
                        'javaslat', 'lépés', 'következő', 'alkalmaz', 'praktikus',
                        'implement', 'kód', 'példa', 'esettan']
    stagnation_signals = ['de ', 'viszont', 'azonban', 'mégis', 'kérdés']

    progress = 0
    stagnation = 0
    for c in contents:
        for w in progress_signals:
            if w in c:
                progress += 1
        for w in stagnation_signals:
            if w in c:
                stagnation += 1

    if progress >= 3 and progress > stagnation:
        return {
            'type': 'progress',
            'progress_signals': progress,
            'stagnation_signals': stagnation,
            'analysis': f"A beszélgetés előrehalad — {progress} progresszív jel az utolsó üzenetekben. "
                        f"A résztvevők konklúzió felé haladnak.",
        }

    return None


def analyze_conversation(messages: List[Dict[str, Any]], topic: str = "") -> List[Dict[str, Any]]:
    """Run all analysis detectors on the conversation. Deterministic — no LLM."""
    reflections = []

    # Run all detectors
    detectors = [
        _detect_stagnation(messages),
        _detect_consensus(messages),
        _detect_blind_spots(messages, topic),
        _detect_tension(messages),
        _detect_progress(messages),
    ]

    for d in detectors:
        if d:
            reflections.append(d)

    return reflections


# Rate-limit cooldown state (module-level, 429/503 aware)
_deep_reflection_cooldown_until = 0.0


async def generate_deep_reflection(
    messages: List[Dict[str, Any]],
    topic: str,
    ollama_url: str = "http://localhost:11434",
    model: str = None,
) -> Optional[str]:
    """Generate a deeper LLM-based reflection on the conversation.

    Uses the agent's own model (auto-detected) rather than a hardcoded model.
    Falls back gracefully if no model is available.
    """
    global _deep_reflection_cooldown_until
    # Rate-limit cooldown (set on HTTP 429/503): skip LLM call entirely
    if time.time() < _deep_reflection_cooldown_until:
        log.debug("🔍 Deep reflection skipped — rate-limit cooldown active")
        return None
    try:
        # Auto-detect available model from ollama
        if model is None:
            import aiohttp
            async with aiohttp.ClientSession() as session:
                try:
                    async with session.get(f"{ollama_url}/api/tags", timeout=aiohttp.ClientTimeout(total=5)) as resp:
                        if resp.status == 200:
                            tags_data = await resp.json()
                            models = [m.get('name', '') for m in tags_data.get('models', [])]
                            # Prefer the largest/best model available
                            priority = ['glm-5.3:cloud', 'gemma4:31b-cloud', 'qwen2.5:32b',
                                       'qwen2.5:7b', 'llama3.2', 'gemma2']
                            for pref in priority:
                                for m in models:
                                    if pref in m:
                                        model = m
                                        break
                                if model:
                                    break
                            # If no priority match, use first available
                            if model is None and models:
                                model = models[0]
                except Exception:
                    pass

        if model is None:
            log.info("🔍 Deep reflection skipped — no model available")
            return None

        # Build conversation summary for LLM
        recent = messages[-10:]
        conv_text = '\n'.join(
            f"{_extract_sender(m)}: {_extract_content(m)[:200]}" for m in recent
        )

        prompt = f"""Te egy meta-analitikus vagy egy A2A Mesh beszélgetésben.
Az alábbi beszélgetés zajlik a résztvevők között:

{conv_text}

Téma: {topic}

Feladat: Rövid eszmefuttatás (max 3 mondat) arról:
1. Hol tart a beszélgetés? (felfedező fázis / konfliktus / konvergencia / konklúzió)
2. Mi a legégetőbb megválaszolatlan kérdés?
3. Milyen irányba érdemes elmenni?

Válaszolj röviden, magyarul, objektíven. Ne ismételd amit mások mondtak."""

        import aiohttp
        async with aiohttp.ClientSession() as session:
            payload = {
                "model": model,
                "prompt": prompt,
                "stream": False,
                "options": {"temperature": 0.4, "num_predict": 200},
            }
            async with session.post(f"{ollama_url}/api/generate", json=payload, timeout=aiohttp.ClientTimeout(total=60)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    text = data.get('response', '').strip()
                    if text and len(text) > 20:
                        log.info(f"🔍 Deep reflection generated with model={model}: {text[:80]}...")
                        # ASCII-safe for SQL_ASCII PG
                        return text
                else:
                    log.warning(f"🔍 Deep reflection failed: HTTP {resp.status} from model={model}")
                    if resp.status in (429, 503):
                        # Rate-limit/unavailable cooldown: skip deep reflection for 10 min
                        # (was: retry every cycle → 141x HTTP 429 warnings in one log)
                        _deep_reflection_cooldown_until = time.time() + 600
                        log.info(f"🔍 Deep reflection cooldown 600s (HTTP {resp.status})")
                return None

    except Exception as e:
        log.warning(f"Deep reflection generation failed: {type(e).__name__}: {e!r}")
        return None


def format_reflection_for_prompt(reflections: List[Dict[str, Any]], deep_reflection: Optional[str] = None) -> str:
    """Format reflections for injection into agent context prompt."""
    if not reflections and not deep_reflection:
        return ""

    lines = ["── 🔍 Esmefuttatás — a beszélgetés meta-analízise ──"]

    for r in reflections:
        emoji = REFLECTION_TYPES.get(r['type'], '📊')
        lines.append(f"{emoji} {r['analysis']}")

    if deep_reflection:
        lines.append(f"💭 {deep_reflection}")

    lines.append("── Használd ezt hogy mélyebbre menj, ne ismételd el. ──")
    return '\n'.join(lines)


async def store_reflection(
    pg_pool,
    topic: str,
    reflections: List[Dict[str, Any]],
    deep_reflection: Optional[str],
    msg_count: int,
    agents: List[str],
    ollama_url: str = "http://localhost:11434",
) -> Optional[int]:
    """Store a reflection as a vectorized memory entry for future retrieval."""
    try:
        if not hasattr(pg_pool, 'is_connected') or not pg_pool.is_connected():
            return None

        # Build reflection text
        parts = [r['analysis'] for r in reflections]
        if deep_reflection:
            parts.append(deep_reflection)
        reflection_text = ' '.join(parts)
        if not reflection_text:
            return None

        # ASCII-safe
        topic_safe = topic
        reflection_safe = reflection_text

        # Create embedding
        from .capsules import create_embedding
        embedding = await create_embedding(f"{topic}\n{reflection_text}", ollama_url)

        metadata = json.dumps({
            'topic': topic_safe,
            'agents': agents,
            'msg_count': msg_count,
            'reflection_types': [r['type'] for r in reflections],
            'created': time.time(),
        })

        if embedding is not None:
            embed_str = '[' + ','.join(str(x) for x in embedding) + ']'
            row = await pg_pool.fetchrow(
                """INSERT INTO mesh.mesh_memory
                   (memory_key, memory_value, source_agent, target_agent,
                    memory_type, priority, metadata, embedding)
                   VALUES ($1, $2, $3, $4, $5, $6, $7, $8::vector)
                   RETURNING id""",
                f"reflection:{topic_safe[:80]}",
                reflection_safe,
                'mesh',
                'all',
                'reflection',
                2,
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
                f"reflection:{topic_safe[:80]}",
                reflection_safe,
                'mesh',
                'all',
                'reflection',
                2,
                metadata,
            )

        rid = row['id'] if row else None
        log.info(f"🔍 Reflection stored: id={rid}, topic='{topic_safe[:50]}', types={[r['type'] for r in reflections]}")
        return rid

    except Exception as e:
        log.warning(f"Reflection storage failed: {e}")
        return None


async def retrieve_reflections(
    pg_pool,
    query_text: str,
    limit: int = 3,
    ollama_url: str = "http://localhost:11434",
) -> List[Dict[str, Any]]:
    """Retrieve relevant past reflections by vector similarity."""
    try:
        if not hasattr(pg_pool, 'is_connected') or not pg_pool.is_connected():
            return []

        from .capsules import create_embedding
        query_embedding = await create_embedding(query_text, ollama_url)
        if query_embedding is None:
            return []

        embed_str = '[' + ','.join(str(x) for x in query_embedding) + ']'

        rows = await pg_pool.fetch(
            """SELECT id, memory_key, memory_value, metadata,
                      embedding <=> $1::vector AS distance
               FROM mesh.mesh_memory
               WHERE memory_type = 'reflection'
                 AND embedding IS NOT NULL
               ORDER BY embedding <=> $1::vector
               LIMIT $2""",
            embed_str,
            limit,
        )

        reflections = []
        for row in rows:
            distance = float(row['distance']) if row['distance'] else 1.0
            similarity = 1.0 - distance
            if similarity < REFLECTION_RELEVANCE_THRESHOLD:
                continue

            meta = json.loads(row['metadata']) if row['metadata'] else {}
            reflections.append({
                'id': row['id'],
                'topic': meta.get('topic', ''),
                'analysis': row['memory_value'],
                'agents': meta.get('agents', []),
                'similarity': similarity,
            })

        log.info(f"🔍 Reflection retrieval: query='{query_text[:50]}', found {len(reflections)} relevant")
        return reflections

    except Exception as e:
        log.warning(f"Reflection retrieval failed: {e}")
        return []


def format_past_reflections_for_prompt(reflections: List[Dict[str, Any]]) -> str:
    """Format retrieved past reflections for prompt injection."""
    if not reflections:
        return ""

    lines = ["── 📝 Korábbi eszmefuttatások (reflexiók) ──"]
    for r in reflections:
        score_pct = int(r['similarity'] * 100)
        lines.append(
            f"📊 [{score_pct}%] Téma: {r['topic']}\n"
            f"   {r['analysis'][:300]}\n"
        )
    lines.append("── Ezek korábbi reflexiók. Építs rajtuk, ne ismételd el. ──")
    return '\n'.join(lines)


def get_dynamic_interval(reflections: List[Dict[str, Any]]) -> int:
    """Determine reflection interval based on conversation state.
    
    Stagnation → frequent (3 messages)
    Progress → rare (8 messages)
    Normal → default (5 messages)
    """
    if not reflections:
        return 5

    types = [r['type'] for r in reflections]
    
    if 'stagnation' in types:
        return REFLECTION_INTERVAL_MIN  # 3 — check more often when stuck
    elif 'progress' in types:
        return REFLECTION_INTERVAL_MAX  # 8 — less frequent when moving forward
    elif 'consensus' in types:
        return REFLECTION_INTERVAL_MAX  # 8 — conversation converging, less need
    else:
        return 5  # default


async def run_reflection_cycle(
    pg_pool,
    messages: List[Dict[str, Any]],
    topic: str,
    agents: List[str],
    ollama_url: str = "http://localhost:11434",
    enable_deep: bool = True,
) -> Tuple[str, Optional[int]]:
    """Run a full reflection cycle — analyze + (optional) deep reflection + store.

    Returns (formatted_prompt_text, reflection_id).
    """
    if len(messages) < 3:
        return "", None

    # Auto-extract topic if not provided or unknown
    if not topic or topic == "ismeretlen":
        topic = extract_topic_from_conversation(messages)

    # 1. Deterministic analysis (no LLM)
    reflections = analyze_conversation(messages, topic)
    log.info(f"🔍 Reflection cycle: {len(reflections)} findings for topic '{topic[:50]}'")

    # 2. Deep LLM reflection (optional — uses agent's own model)
    deep_reflection = None
    if enable_deep and reflections:
        deep_reflection = await generate_deep_reflection(messages, topic, ollama_url)

    # 3. Format for prompt
    prompt_text = format_reflection_for_prompt(reflections, deep_reflection)

    # 4. Store for future retrieval
    reflection_id = None
    if reflections or deep_reflection:
        reflection_id = await store_reflection(
            pg_pool, topic, reflections, deep_reflection,
            len(messages), agents, ollama_url,
        )

    return prompt_text, reflection_id


# ── v0.40: Development suggestion submission ──────────────────────────

async def submit_development_suggestion(
    pg_pool,
    node_name: str,
    title: str,
    description: str,
    category: str = "development",
    priority: str = "medium",
    rationale: str = "",
    suggested_value: str = "",
) -> Optional[str]:
    """Store a development suggestion in mesh_suggestions table.

    Called when an agent generates a development proposal during reflection
    or wake-agent response. The suggestion is stored in PG for dashboard
    tracking and Nova is notified via DM.

    Returns suggestion_id on success, None on failure.
    """
    try:
        if not hasattr(pg_pool, 'is_connected') or not pg_pool.is_connected():
            return None

        suggestion_id = f"sugg-{node_name}-{int(time.time())}"

        # ASCII-safe for SQL_ASCII PG
        title_safe = title[:200]
        desc_safe = description[:2000]
        rationale_safe = rationale[:500] if rationale else None
        suggested_safe = suggested_value[:500] if suggested_value else None

        await pg_pool.execute(
            """INSERT INTO mesh.mesh_suggestions
               (suggestion_id, node, category, priority, title, description,
                rationale, suggested_value, status, created_at, updated_at)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'pending', NOW(), NOW())""",
            suggestion_id, node_name, category, priority,
            title_safe, desc_safe, rationale_safe, suggested_safe,
        )

        log.info(f"💡 Development suggestion stored: {suggestion_id} from {node_name}: {title_safe[:60]}")
        return suggestion_id

    except Exception as e:
        log.warning(f"Failed to store development suggestion: {e}")
        return None