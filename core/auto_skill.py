"""
Auto Skill-Factory — automatically generates SKILL.md files from successful delegations.

Trigger conditions:
  - Task completed successfully
  - Task had 5+ tool calls OR had error-recovery (retry after failure)
  - No existing skill covers this task type

The generated skill is saved to ~/.hermes/skills/auto/ and registered in the mesh.
This module is OPTIONAL — it uses LLM to generate the skill content, so it's
a non-deterministic layer on top of the deterministic mesh core.
"""

import os
import re
import time
import json
import asyncio
import logging

log = logging.getLogger("auto_skill")

# Minimum tool calls to trigger skill generation
MIN_TOOL_CALLS = 5
# Max skills to auto-generate per day (prevent runaway)
MAX_PER_DAY = 10
# Skills directory
SKILLS_DIR = os.path.expanduser("~/.hermes/skills/auto")
# Track generated skills per day for rate limiting
_daily_count = 0
_daily_reset_ts = 0


def _sanitize_name(name: str) -> str:
    """Convert a task subject to a valid skill name."""
    # Remove brackets, special chars, lowercase, hyphenate
    name = re.sub(r'[\[\]\(\){}]', '', name)
    name = re.sub(r'[^a-zA-Z0-9\s-]', '', name)
    name = name.strip().lower().replace(' ', '-')
    name = re.sub(r'-+', '-', name)
    return name[:64] if name else "auto-task"


def _count_tool_calls(task: dict) -> int:
    """Estimate tool call count from task notes and result."""
    count = 0
    notes = task.get("notes", [])
    if isinstance(notes, list):
        for note in notes:
            text = note.get("text", "") if isinstance(note, dict) else str(note)
            # Count lines that mention tool usage
            if any(kw in text.lower() for kw in ["tool", "exec", "terminal", "patch", "write_file", "search", "fetch"]):
                count += 1
    # Also count from result text
    result = task.get("result", "")
    if isinstance(result, str):
        count += result.lower().count("[tool]") + result.lower().count("tool_call")
    return count


def _had_retry(task: dict) -> bool:
    """Check if task had error-recovery (retry after failure)."""
    notes = task.get("notes", [])
    if isinstance(notes, list):
        for note in notes:
            text = note.get("text", "") if isinstance(note, dict) else str(note)
            if "retry" in text.lower() or "recovery" in text.lower() or "error" in text.lower():
                return True
    return False


def _existing_skill_covers(subject: str, task_type: str) -> bool:
    """Check if a skill already exists that covers this task type."""
    sanitized = _sanitize_name(subject)
    # Check local FS
    if os.path.isdir(SKILLS_DIR):
        for entry in os.listdir(SKILLS_DIR):
            if entry == sanitized:
                return True
    return False


async def maybe_generate_skill(task: dict, node_name: str, llm_generate=None):
    """
    Check if a completed task should trigger auto-skill generation.
    Marveen-style: considers tool calls, error recovery, task complexity,
    and rate limiting. Generates SKILL.md + registers in PG mesh_skills.
    
    Args:
        task: The completed task dict
        node_name: Name of the node that executed the task
        llm_generate: Optional async callable(text) -> str for LLM generation.
                      If None, uses a template-based approach (no LLM).
    
    Returns:
        str or None: The skill name if generated, None if skipped.
    """
    global _daily_count, _daily_reset_ts
    try:
        # Rate limiting: reset daily counter
        now_ts = time.time()
        if now_ts - _daily_reset_ts > 86400:  # 24h
            _daily_count = 0
            _daily_reset_ts = now_ts
        if _daily_count >= MAX_PER_DAY:
            log.debug(f"Auto-skip: daily limit reached ({_daily_count}/{MAX_PER_DAY})")
            return None
        
        subject = task.get("subject", "")
        if not subject or len(subject) < 3:
            return None
            
        task_type = "generic"
        desc = task.get("description", "{}")
        if isinstance(desc, str):
            try:
                d = json.loads(desc)
                task_type = d.get("type", "generic")
            except Exception:
                pass
        
        # Check trigger conditions
        tool_calls = _count_tool_calls(task)
        had_retry = _had_retry(task)
        
        # Marveen: skip trivial tasks
        if tool_calls < MIN_TOOL_CALLS and not had_retry:
            log.debug(f"Auto-skill skip: {subject} (only {tool_calls} tool calls, no retry)")
            return None
        
        if _existing_skill_covers(subject, task_type):
            log.debug(f"Auto-skill skip: {subject} (skill already exists)")
            return None
        
        skill_name = _sanitize_name(subject)
        skill_dir = os.path.join(SKILLS_DIR, skill_name)
        
        # Create skill directory
        os.makedirs(skill_dir, exist_ok=True)
        
        # Build skill content
        result_text = task.get("result", "")
        if isinstance(result_text, str):
            result_text = result_text[:2000]
        
        notes_text = ""
        notes = task.get("notes", [])
        if isinstance(notes, list):
            notes_text = "\n".join(
                n.get("text", str(n)) if isinstance(n, dict) else str(n)
                for n in notes[-10:]  # last 10 notes
            )[:2000]
        
        # Use LLM if available, otherwise template
        if llm_generate:
            prompt = f"""Generate a SKILL.md file for a successful task that was completed by an AI agent.

Task subject: {subject}
Task type: {task_type}
Agent: {node_name}
Tool calls: {tool_calls}
Had retry/recovery: {had_retry}

Task result summary:
{result_text}

Task notes (timeline):
{notes_text}

Create a concise SKILL.md with:
1. YAML frontmatter (name, description, trigger conditions)
2. Numbered steps to reproduce this task
3. Key decisions and approaches that worked
4. Pitfalls section (what went wrong, what to avoid)

Keep it practical and concise. Output only the SKILL.md content."""
            try:
                skill_content = await asyncio.wait_for(llm_generate(prompt), timeout=30)
            except Exception as e:
                log.warning(f"LLM skill generation failed, using template: {e}")
                skill_content = _template_skill(subject, task_type, node_name, tool_calls, had_retry, result_text, notes_text)
        else:
            skill_content = _template_skill(subject, task_type, node_name, tool_calls, had_retry, result_text, notes_text)
        
        # Write SKILL.md
        skill_path = os.path.join(skill_dir, "SKILL.md")
        with open(skill_path, "w") as f:
            f.write(skill_content)
        
        # ── Skill-sync (P1): publish the generated skill to the shared PG so ALL
        # nodes can pull it (mesh_skill_files — same table /api/skills/publish uses).
        # Idempotent (ON CONFLICT update); silent failure — local skill still works.
        try:
            import asyncpg as _apg
            import asyncio as _aio
            _pg_dsn_env = os.environ.get("A2A_MESH_PG_DSN", "")
            async def _publish():
                conn = None
                try:
                    if _pg_dsn_env:
                        conn = await _apg.connect(_pg_dsn_env)
                    else:
                        conn = await _apg.connect(
                            host=os.environ.get("A2A_PG_HOST", "192.168.1.30"),
                            port=int(os.environ.get("A2A_PG_PORT", "5432")),
                            user=os.environ.get("A2A_PG_USER", "nova"),
                            password=os.environ.get("A2A_PG_PASSWORD", ""),
                            database=os.environ.get("A2A_PG_DBNAME", "agent_memory"),
                        )
                    await conn.execute("""
                        CREATE TABLE IF NOT EXISTS mesh.mesh_skill_files (
                            skill_id TEXT NOT NULL,
                            filename TEXT NOT NULL,
                            content TEXT NOT NULL,
                            updated_at REAL NOT NULL,
                            PRIMARY KEY (skill_id, filename)
                        )
                    """)
                    await conn.execute(
                        """INSERT INTO mesh.mesh_skill_files (skill_id, filename, content, updated_at)
                           VALUES ($1, 'SKILL.md', $2, $3)
                           ON CONFLICT (skill_id, filename)
                           DO UPDATE SET content = EXCLUDED.content, updated_at = EXCLUDED.updated_at""",
                        f"auto-{skill_name}", skill_content, time.time(),
                    )
                    return True
                except Exception as pg_err:
                    log.debug(f"Skill-sync publish failed (non-fatal): {pg_err}")
                    return False
                finally:
                    if conn:
                        await conn.close()
            published = await _publish()
            if published:
                log.info(f"📦 Skill-sync: 'auto-{skill_name}' published to shared PG (all nodes can pull)")
        except Exception as sync_err:
            log.debug(f"Skill-sync module error (non-fatal): {sync_err}")
        
        log.info(f"✅ Auto-skill generated: {skill_name} ({tool_calls} tool calls, retry={had_retry})")
        _daily_count += 1
        return skill_name
        
    except Exception as e:
        log.error(f"Auto-skill generation error: {e}")
        return None


def _template_skill(subject: str, task_type: str, node_name: str, 
                    tool_calls: int, had_retry: bool, 
                    result_text: str, notes_text: str) -> str:
    """Generate a template-based SKILL.md without LLM."""
    retry_note = "- Error recovery was needed — check retry logic" if had_retry else "- No errors encountered"
    return f"""---
name: {_sanitize_name(subject)}
description: Auto-generated from successful delegation by {node_name}. Trigger: {task_type} tasks similar to "{subject}".
---

# {_sanitize_name(subject)}

Auto-generated skill from a successful task execution by {node_name}.

## Trigger Conditions
- Task type: `{task_type}`
- Subject pattern: "{subject}"
- Required tool calls: {tool_calls}
- Error recovery: {"yes" if had_retry else "no"}

## Steps

1. Receive task with subject "{subject}"
2. Analyze context and requirements from task description
3. Execute required operations (estimated {tool_calls} tool calls)
4. {"Handle errors with retry/recovery logic" if had_retry else "Execute linearly"}
5. Verify results match expected outcome
6. Report completion

## Task Result
```
{result_text[:500]}
```

## Task Notes (Timeline)
```
{notes_text[:500]}
```

## Pitfalls
{retry_note}
- This is an auto-generated skill — review and refine manually
- Generated on {time.strftime("%Y-%m-%d %H:%M:%S")} by {node_name}
- Rate limit: {_daily_count}/{MAX_PER_DAY} skills today
"""