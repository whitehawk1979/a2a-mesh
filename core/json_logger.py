"""
Structured JSON logging for A2A mesh.

Provides:
- JSONFormatter: outputs each log record as a JSON line
- setup_json_logging(): configures all a2a_mesh loggers with JSON formatting
- Trace-ID propagation via LogRecord attributes
"""

import json
import logging
import time
from datetime import datetime, timezone
from typing import Optional


# Thread-local storage for trace-id propagation
import threading
_trace_context = threading.local()


def set_trace_id(trace_id: str):
    """Set trace-ID for the current thread (propagated to all log records)."""
    _trace_context.trace_id = trace_id


def get_trace_id() -> Optional[str]:
    """Get current thread's trace-ID."""
    return getattr(_trace_context, 'trace_id', None)


def clear_trace_id():
    """Clear trace-ID for the current thread."""
    _trace_context.trace_id = None


class TraceIdFilter(logging.Filter):
    """Logging filter that injects trace_id into every log record."""
    
    def filter(self, record):
        record.trace_id = getattr(_trace_context, 'trace_id', None) or ''
        record.node_name = getattr(_trace_context, 'node_name', None) or ''
        return True


class JSONFormatter(logging.Formatter):
    """Format log records as structured JSON lines.
    
    Output format:
    {
        "ts": "2026-07-24T07:30:00.123Z",
        "level": "INFO",
        "logger": "a2a_mesh.node",
        "msg": "Message text",
        "trace_id": "trace-nova-abc12345",
        "node": "nova",
        "func": "start",
        "line": 42
    }
    """
    
    # Fields that are always included
    REQUIRED_FIELDS = ('ts', 'level', 'logger', 'msg')
    
    # Extra fields from LogRecord to include if present
    EXTRA_FIELDS = ('trace_id', 'node_name', 'task_id', 'peer', 'duration_ms')
    
    def format(self, record):
        log_entry = {
            'ts': datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            'level': record.levelname,
            'logger': record.name,
            'msg': record.getMessage(),
        }
        
        # Add trace context
        trace_id = getattr(record, 'trace_id', None)
        if trace_id:
            log_entry['trace_id'] = trace_id
        
        node_name = getattr(record, 'node_name', None)
        if node_name:
            log_entry['node'] = node_name
        
        # Add location info for DEBUG and above
        if record.levelno >= logging.DEBUG:
            log_entry['func'] = record.funcName
            log_entry['line'] = record.lineno
        
        # Add extra fields
        for field in ('task_id', 'peer', 'duration_ms'):
            val = getattr(record, field, None)
            if val is not None:
                log_entry[field] = val
        
        # Add exception info
        if record.exc_info and record.exc_info[0]:
            log_entry['exception'] = self.formatException(record.exc_info)
        
        return json.dumps(log_entry, ensure_ascii=False)


class HumanFormatter(logging.Formatter):
    """Human-readable format with optional trace-id.
    
    Format: 2026-07-24 07:30:00 [a2a_mesh.node] [trace-nova-abc12] INFO: Message
    """
    
    COLORS = {
        'DEBUG': '\033[36m',     # Cyan
        'INFO': '\033[32m',      # Green
        'WARNING': '\033[33m',   # Yellow
        'ERROR': '\033[31m',     # Red
        'CRITICAL': '\033[35m',  # Magenta
    }
    RESET = '\033[0m'
    
    def __init__(self, use_color=True):
        super().__init__()
        self.use_color = use_color
    
    def format(self, record):
        # Base format
        ts = datetime.fromtimestamp(record.created, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        trace_id = getattr(record, 'trace_id', None)
        trace_part = f' [{trace_id}]' if trace_id else ''
        
        msg = f'{ts} [{record.name}]{trace_part} {record.levelname}: {record.getMessage()}'
        
        if self.use_color:
            color = self.COLORS.get(record.levelname, self.RESET)
            msg = f'{color}{msg}{self.RESET}'
        
        if record.exc_info and record.exc_info[0]:
            msg += '\n' + self.formatException(record.exc_info)
        
        return msg


def setup_json_logging(node_name: str = '', log_file: str = None, json_mode: str = 'auto'):
    """Configure structured logging for all a2a_mesh loggers.
    
    Args:
        node_name: Name of this mesh node (injected into every log record)
        log_file: Optional log file path
        json_mode: 'json' (always JSON), 'human' (always human), 'auto' (JSON to file, human to console)
    """
    # Set node_name in thread-local context
    _trace_context.node_name = node_name
    
    # Create the trace filter
    trace_filter = TraceIdFilter()
    
    # Determine formatters
    if json_mode == 'json':
        console_formatter = JSONFormatter()
        file_formatter = JSONFormatter()
    elif json_mode == 'human':
        console_formatter = HumanFormatter(use_color=True)
        file_formatter = HumanFormatter(use_color=False)
    else:  # auto: JSON to file, human to console
        console_formatter = HumanFormatter(use_color=True)
        file_formatter = JSONFormatter()
    
    # Configure root logger — this catches all a2a.delegation, a2a_mesh.*, etc.
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    # Remove existing handlers to avoid duplicates
    for h in list(root.handlers):
        root.removeHandler(h)
    
    # Console handler on root (human-readable, catches everything)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(console_formatter)
    console_handler.addFilter(trace_filter)
    root.addHandler(console_handler)
    
    # File handler on root (JSON, catches everything)
    if log_file:
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(file_formatter)
        file_handler.addFilter(trace_filter)
        root.addHandler(file_handler)
    
    # Also configure a2a_mesh logger specifically (propagation will send to root)
    mesh_logger = logging.getLogger('a2a_mesh')
    mesh_logger.setLevel(logging.DEBUG)
    mesh_logger.addFilter(trace_filter)
    # Don't add handlers here — propagation sends to root
    
    # Also set node_name on any future log records from this node
    if node_name:
        _trace_context.node_name = node_name


def log_with_trace(logger, level, msg, trace_id=None, **kwargs):
    """Log a message with an explicit trace-ID.
    
    Usage:
        log_with_trace(log, logging.INFO, "Processing task", trace_id="trace-nova-abc12")
    """
    if trace_id:
        old = getattr(_trace_context, 'trace_id', None)
        _trace_context.trace_id = trace_id
        try:
            logger.log(level, msg, **kwargs)
        finally:
            _trace_context.trace_id = old
    else:
        logger.log(level, msg, **kwargs)