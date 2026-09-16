"""
CSRF Origin Gate — protect state-changing HTTP requests.

Inspired by Marveen's csrf-origin:
  - Block non-safe requests (POST/PUT/DELETE/PATCH) with foreign Origin
  - SameSite=Strict cookie is primary defense; this is second layer
  - Allow same-origin and bearer-token requests

For A2A Mesh:
  - aiohttp middleware
  - Check Origin header on state-changing methods
  - Allow if Origin matches host or if Bearer token present
"""

import logging

log = logging.getLogger("csrf")

SAFE_METHODS = {"GET", "HEAD", "OPTIONS"}


def is_origin_allowed(request_origin, host):
    """Check if origin matches host."""
    if not request_origin:
        return True  # No origin header = non-browser request (curl, etc.)
    
    # Parse origin: scheme://host:port
    try:
        from urllib.parse import urlparse
        parsed = urlparse(request_origin)
        origin_host = parsed.hostname or ""
        origin_port = parsed.port
        
        # Parse our host
        our_host = host.split(":")[0] if ":" in host else host
        
        # Allow if hostname matches
        if origin_host == our_host or origin_host == "localhost" or origin_host == "127.0.0.1":
            return True
        
        # Allow Tailscale IPs (100.x.x.x)
        if origin_host.startswith("100."):
            return True
            
    except Exception:
        pass
    
    return False


def check_csrf(request):
    """Check CSRF for a request. Returns (ok, reason)."""
    method = request.method.upper()
    
    # Safe methods always pass
    if method in SAFE_METHODS:
        return True, None
    
    # Bearer token requests are not CSRF (they're API calls)
    auth = request.headers.get("Authorization", "")
    if auth.startswith("Bearer "):
        return True, None
    
    # Check Origin header
    origin = request.headers.get("Origin", "")
    host = request.host
    
    if is_origin_allowed(origin, host):
        return True, None
    
    log.warning(f"CSRF blocked: {method} from {origin} to host {host}")
    return False, f"Foreign origin: {origin}"


def get_csrf_status():
    """Get CSRF config status."""
    return {
        "enabled": True,
        "safe_methods": list(SAFE_METHODS),
        "bearer_exempt": True,
        "same_site_cookie": "Strict",
        "tailscale_allowed": True,
    }