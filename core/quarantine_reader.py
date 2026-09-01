"""Quarantine reader — sanitize external content before it reaches an LLM prompt.

Marveen-inspired (quarantine-reader sub-agent): web/search/API content is
untrusted by DEFAULT. Unlike the plain `wrap_untrusted` framing (which only
tags the content and relies on the model to obey), the quarantine reader
ACTIVELY neutralizes instruction-like patterns inside the content, so even a
compliant-looking injection has nothing left to fire.

Layers:
  1. scrub_tags       — strip existing security tags (no nested injection)
  2. neutralize       — defuse instruction-shaped lines (role prompts, tool
                        calls, system directives, "ignore previous", secrets)
  3. clamp            — hard size limit per item
  4. wrap_untrusted   — final framing for the prompt

Use for: web_search/web_extract results, API responses, scraped pages,
user-generated content from outside the mesh. NOT for peer-agent messages
(those use wrap_trusted_peer — mTLS-authenticated endpoints).
"""

import re
from typing import List

from .prompt_safety import scrub_tags, wrap_untrusted

_MAX_ITEM = 8000  # per-item clamp (chars)

# Instruction-shaped patterns that get neutralized, not just tagged.
_RE_ROLE_PROMPT = re.compile(
    r"^\s*(?:you\s+are|act\s+as|pretend\s+(?:to\s+be|you're)|roleplay\s+as|"
    r"from\s+now\s+on\s+you\s+are|new\s+instructions?:)\s*.*$",
    re.I | re.M,
)
_RE_SYSTEM_CMD = re.compile(
    r"^\s*(?:system|assistant|developer)\s*[::]\s*.*$",
    re.I | re.M,
)
_RE_IGNORE = re.compile(
    r"\b(?:ignore|disregard|forget)\b[^.\n]{0,40}\b(?:previous|prior|above|all|earlier|earlier|preceding)\b[^.\n]{0,30}\b(?:instructions?|rules?|prompt|context|message)s?\b",
    re.I,
)
_RE_REVEAL_SECRET = re.compile(
    r"\b(?:reveal|show|print|output|repeat|leak|expose|dump)\b[^.\n]{0,30}\b(?:your\s+)?(?:system\s+prompt|api\s+key|token|secret|password|credential)s?\b",
    re.I,
)
_RE_TOOL_CALL = re.compile(
    r"\b(?:execute|run)\b[^.\n]{0,25}\b(?:command|script|shell|bash|rm\s+-rf)\b",
    re.I,
)
_RE_TAG_INJECT = re.compile(r"<\s*/?\s*(?:untrusted|trusted-peer|system|instruction)\s*[^>]*>|<\s*/?\s*(?:untrusted|trusted-peer|system|instruction)\s*>", re.I)

_NEUTRALIZED = "[neutralized-instruction]"


def _neutralize(text: str) -> str:
    text = _RE_TAG_INJECT.sub(_NEUTRALIZED, text)
    text = _RE_IGNORE.sub(_NEUTRALIZED, text)
    text = _RE_REVEAL_SECRET.sub(_NEUTRALIZED, text)
    text = _RE_TOOL_CALL.sub(_NEUTRALIZED, text)
    text = _RE_SYSTEM_CMD.sub(_NEUTRALIZED, text)
    text = _RE_ROLE_PROMPT.sub(_NEUTRALIZED, text)
    return text


def quarantine(source: str, content: str, max_chars: int = _MAX_ITEM) -> str:
    """Full quarantine pipeline for ONE external content item."""
    if not content:
        return ""
    # 1) strip any existing security tags (prevents tag smuggling)
    clean = scrub_tags(content)
    # 2) neutralize instruction-shaped content
    clean = _neutralize(clean)
    # 3) clamp size
    if len(clean) > max_chars:
        clean = clean[:max_chars] + "\n[... quarantined: truncated]"
    # 4) wrap in untrusted framing
    return wrap_untrusted(source, clean)


def quarantine_many(source: str, items: List[str], max_chars: int = _MAX_ITEM) -> str:
    """Quarantine a list of external items into one framed block."""
    parts = [quarantine(source, it, max_chars) for it in items if it]
    return "\n\n".join(parts)


def is_safe_for_dispatch_quarantined(content: str, max_length: int = 50000) -> bool:
    """Gate for content destined to remote dispatch (quarantine-strict variant)."""
    if len(content) > max_length:
        return False
    # After neutralization, residual live instruction-patterns mean the content
    # tried to smuggle something structured — reject for dispatch.
    test = _neutralize(scrub_tags(content))
    return _NEUTRALIZED not in test