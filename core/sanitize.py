"""
Sanitize — input sanitization for A2A Mesh.

Inspired by Marveen's sanitize:
  - NFD + combining-mark strip for Hungarian accents
  - Agent name sanitizer: lowercase, NFD, strip accents, keep [a-z0-9-]
  - Path sanitizer: prevent directory traversal

For A2A Mesh:
  - Sanitize node names, task names, user input
  - Hungarian accent handling (é→e, á→a, etc.)
  - Path traversal prevention
"""

import re
import unicodedata
import os
from pathlib import Path

SAFE_NAME_RE = re.compile(r"[^a-z0-9-]")


def sanitize_name(raw):
    """Sanitize a name: lowercase, NFD, strip accents, keep [a-z0-9-].
    
    'Mézsör' → 'mezsor'
    'Nova Mesh!' → 'nova-mesh'
    """
    if not raw:
        return ""
    result = raw.strip().lower()
    result = unicodedata.normalize("NFD", result)
    result = "".join(c for c in result if unicodedata.category(c) != "Mn")
    result = SAFE_NAME_RE.sub("-", result)
    result = re.sub(r"-+", "-", result)
    result = result.strip("-")
    return result[:50]


def sanitize_path(raw_path, base_dir=None):
    """Sanitize a file path to prevent directory traversal.
    
    '/etc/passwd' → rejected if base_dir set
    '../../../etc' → rejected
    """
    if not raw_path:
        return None
    
    # Resolve and normalize
    path = os.path.normpath(raw_path)
    
    # Block absolute paths if base_dir is set
    if base_dir:
        base = os.path.abspath(base_dir)
        full = os.path.abspath(os.path.join(base, raw_path))
        if not full.startswith(base):
            return None
        return full
    
    # Block obvious traversal
    if ".." in path:
        return None
    
    return path


def sanitize_text(raw, max_length=500):
    """Sanitize general text input.
    
    - Strip control characters
    - Limit length
    - Keep newlines
    """
    if not raw:
        return ""
    result = raw[:max_length]
    result = "".join(c for c in result if c == "\n" or c == "\t" or ord(c) >= 32)
    return result.strip()


def sanitize_cron_expr(raw):
    """Sanitize a cron expression.
    
    Only allow: digits, *, /, -, ,, and spaces
    """
    if not raw:
        return None
    result = re.sub(r"[^0-9*/,\-\s]", "", raw)
    parts = result.split()
    if len(parts) != 5:
        return None
    return " ".join(parts)


def get_sanitize_status():
    """Get sanitizer status for dashboard."""
    test_cases = [
        ("Mézsör", sanitize_name("Mézsör")),
        ("Nova Mesh!", sanitize_name("Nova Mesh!")),
        ("Morzsa_2026", sanitize_name("Morzsa_2026")),
    ]
    return {
        "name_sanitizer": True,
        "path_sanitizer": True,
        "text_sanitizer": True,
        "cron_sanitizer": True,
        "test_cases": [
            {"input": inp, "output": out}
            for inp, out in test_cases
        ],
    }