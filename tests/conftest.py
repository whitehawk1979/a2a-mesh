"""Global test configuration — isolate A2A env vars to prevent PG connections in tests."""
import os

# Environment variables that should NOT leak into tests (cause PG timeouts, etc.)
_A2A_ENV_VARS = [
    "A2A_PG_HOST", "A2A_PG_PORT", "A2A_PG_DBNAME", "A2A_PG_USER", "A2A_PG_PASSWORD",
    "A2A_MESH_PG_DSN", "A2A_HTTP_URL", "A2A_HEALTH_URL",
    "WEBHOOK_PORT", "WEBHOOK_SECRET", "A2A_TELEGRAM_CHAT_ID",
]

# Remove A2A env vars IMMEDIATELY at import time, before any test module imports core.auth
_saved_env = {}
for _key in _A2A_ENV_VARS:
    if _key in os.environ:
        _saved_env[_key] = os.environ.pop(_key)


def pytest_sessionfinish(session, exitstatus):
    """Restore A2A env vars after the test session."""
    os.environ.update(_saved_env)
