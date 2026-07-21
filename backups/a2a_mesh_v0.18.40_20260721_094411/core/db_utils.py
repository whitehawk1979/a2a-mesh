"""A2A Mesh DB Utilities — Helpers for database operations.

SQL_ASCII-compatible JSON serialization and other PG helpers.
"""

import json as _json


def ascii_json(obj, default=None) -> str:
    """json.dumps that produces pure ASCII output compatible with SQL_ASCII databases.
    
    json.dumps(ensure_ascii=True) still produces \\uXXXX escapes for non-ASCII chars,
    which PostgreSQL rejects when the database encoding is SQL_ASCII. This function
    converts common Unicode characters to ASCII equivalents before serialization.
    
    Args:
        obj: Object to serialize.
        default: Optional default function for non-serializable objects (like json.dumps default).
    """
    def _sanitize_strings(o):
        """Recursively replace non-ASCII chars in all strings."""
        if isinstance(o, str):
            # Replace common Unicode chars with ASCII equivalents
            o = o.replace('\u2014', '-')   # em-dash → dash
            o = o.replace('\u2013', '-')   # en-dash → dash  
            o = o.replace('\u2018', "'")   # left single quote
            o = o.replace('\u2019', "'")   # right single quote
            o = o.replace('\u201c', '"')   # left double quote
            o = o.replace('\u201d', '"')   # right double quote
            o = o.replace('\u2026', '...') # ellipsis
            o = o.replace('\u00d7', 'x')   # multiplication sign
            o = o.replace('\u2192', '->')   # right arrow
            o = o.replace('\u2190', '<-')   # left arrow
            # Remove any remaining non-ASCII
            o = o.encode('ascii', 'replace').decode('ascii')
            return o
        elif isinstance(o, dict):
            return {k: _sanitize_strings(v) for k, v in o.items()}
        elif isinstance(o, (list, tuple)):
            return [_sanitize_strings(item) for item in o]
        return o
    
    return _json.dumps(_sanitize_strings(obj), ensure_ascii=True, default=default or str)
