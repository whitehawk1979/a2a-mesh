#!/usr/bin/env python3
"""A2A Mesh Delegation Bridge — Hermes delegate_task integration.

Usage:
    # Create a delegation and wait for result
    python bridge_delegate.py create --to morzsa --subject "Health check" --type monitoring --priority 5
    
    # Create and poll for result (blocks until complete or timeout)
    python bridge_delegate.py delegate --to morzsa --subject "Health check" --type monitoring --timeout 120
    
    # Check status of existing delegation
    python bridge_delegate.py status --task-id <uuid>
    
    # Get result files
    python bridge_delegate.py files --task-id <uuid>
    
    # Download result file
    python bridge_delegate.py download --task-id <uuid>
    
    # List all delegations
    python bridge_delegate.py list [--status completed] [--limit 10]
    
    # Cancel a delegation
    python bridge_delegate.py cancel --task-id <uuid>
"""
import argparse
import json
import sys
import time
import urllib.request
import urllib.error

DEFAULT_HOST = "http://127.0.0.1:8650"
DEFAULT_USER = "zsolt"
DEFAULT_PASS = "mesh2026"


class BridgeClient:
    """Simple HTTP client for A2A Mesh Delegation API."""
    
    def __init__(self, host: str = DEFAULT_HOST, user: str = DEFAULT_USER, password: str = DEFAULT_PASS):
        self.host = host.rstrip("/")
        self.user = user
        self.password = password
        self._token = None
    
    def _login(self) -> str:
        """Authenticate and get JWT token."""
        url = f"{self.host}/api/auth/login"
        data = json.dumps({"username": self.user, "password": self.password}).encode()
        req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req) as resp:
            result = json.loads(resp.read())
            self._token = result.get("token", "")
            return self._token
    
    def _headers(self) -> dict:
        if not self._token:
            self._login()
        return {"Authorization": f"Bearer {self._token}", "Content-Type": "application/json"}
    
    def _request(self, method: str, path: str, data: dict = None, _retries: int = 2) -> dict:
        """Make an authenticated API request with retry on 429/5xx."""
        url = f"{self.host}{path}"
        body = json.dumps(data).encode() if data else None
        req = urllib.request.Request(url, data=body, headers=self._headers(), method=method)
        try:
            with urllib.request.urlopen(req) as resp:
                return json.loads(resp.read())
        except urllib.error.HTTPError as e:
            error_body = e.read().decode() if e.fp else str(e)
            # Retry on 429 (rate limit) or 5xx (server error) with exponential backoff
            if e.code in (429, 500, 502, 503) and _retries > 0:
                wait = 2 ** (3 - _retries)  # 1s, 2s, 4s backoff
                time.sleep(wait)
                # Force re-login on auth errors
                if e.code == 429:
                    self._token = None
                return self._request(method, path, data, _retries=_retries - 1)
            try:
                return json.loads(error_body)
            except json.JSONDecodeError:
                return {"error": error_body, "status": e.code}
    
    def create(self, to_agent: str, subject: str, task_type: str = "generic",
               priority: int = 5, description: str = "", context: dict = None,
               available: bool = False, timeout_minutes: int = 30, fan_out: int = 0,
               max_retries: int = 2) -> dict:
        """Create a new delegation. If fan_out > 0, creates N identical tasks."""
        data = {
            "to_agent": to_agent,
            "subject": subject,
            "task_type": task_type,
            "priority": priority,
            "description": description,
            "timeout_minutes": timeout_minutes,
            "max_retries": max_retries,
        }
        if context:
            data["context"] = context
        if available or to_agent == "any":
            data["available"] = True
            data["to_agent"] = "any"
        if fan_out > 0:
            data["fan_out"] = fan_out
        return self._request("POST", "/api/delegations", data)
    
    def status(self, task_id: str) -> dict:
        """Get delegation status."""
        return self._request("GET", f"/api/delegations/{task_id}")
    
    def files(self, task_id: str) -> dict:
        """Get result files for a delegation."""
        return self._request("GET", f"/api/delegations/{task_id}/files")
    
    def download(self, task_id: str) -> str:
        """Download result file content."""
        url = f"{self.host}/api/delegations/{task_id}/files?download=1"
        req = urllib.request.Request(url, headers=self._headers())
        with urllib.request.urlopen(req) as resp:
            return resp.read().decode()
    
    def list_delegations(self, status: str = None, limit: int = 10, 
                          agent: str = None, task_type: str = None) -> dict:
        """List delegations with optional filters."""
        params = [f"limit={limit}"]
        if status:
            params.append(f"status={status}")
        if agent:
            params.append(f"agent={agent}")
        if task_type:
            params.append(f"task_type={task_type}")
        return self._request("GET", f"/api/delegations?{'&'.join(params)}")
    
    def cancel(self, task_id: str) -> dict:
        """Cancel a delegation."""
        return self._request("POST", f"/api/delegations/{task_id}/cancel", {})
    
    def delegate(self, to_agent: str, subject: str, task_type: str = "generic",
                 priority: int = 5, description: str = "", context: dict = None,
                 timeout: int = 120, poll_interval: int = 5, available: bool = False) -> dict:
        """Create a delegation and wait for result (blocking)."""
        # Create
        result = self.create(
            to_agent=to_agent, subject=subject, task_type=task_type,
            priority=priority, description=description, context=context,
            available=available,
        )
        if "error" in result:
            return result
        
        task_id = result.get("task_id")
        if not task_id:
            return {"error": "No task_id returned", "raw": result}
        
        print(f"Task created: {task_id}", file=sys.stderr)
        print(f"Waiting for completion (timeout: {timeout}s)...", file=sys.stderr)
        
        # Poll for result
        start = time.time()
        while time.time() - start < timeout:
            status = self.status(task_id)
            s = status.get("status", "unknown")
            
            if s in ("completed", "failed", "cancelled"):
                # Try to get files if completed
                if s == "completed":
                    try:
                        files = self.files(task_id)
                        status["files"] = files
                    except Exception:
                        pass
                return status
            
            time.sleep(poll_interval)
        
        return {"error": f"Timeout after {timeout}s", "task_id": task_id, "status": "timeout"}


def main():
    parser = argparse.ArgumentParser(description="A2A Mesh Delegation Bridge")
    parser.add_argument("command", choices=["create", "delegate", "status", "files", "download", "list", "cancel"])
    parser.add_argument("--to", help="Target agent name (morzsa, runa, any)")
    parser.add_argument("--subject", help="Task subject")
    parser.add_argument("--type", default="generic", help="Task type (monitoring, research, code, analysis, generic)")
    parser.add_argument("--priority", type=int, default=5, help="Priority 1-10")
    parser.add_argument("--description", default="", help="Task description")
    parser.add_argument("--context", default=None, help="JSON context data")
    parser.add_argument("--task-id", help="Task UUID")
    parser.add_argument("--status", help="Filter by status (completed, failed, pending, etc.)")
    parser.add_argument("--limit", type=int, default=10, help="Max results")
    parser.add_argument("--timeout", type=int, default=120, help="Timeout in seconds for delegate command")
    parser.add_argument("--poll-interval", type=int, default=5, help="Poll interval in seconds")
    parser.add_argument("--available", action="store_true", help="Make task available for any agent to claim")
    parser.add_argument("--fan-out", type=int, default=0, help="Create N identical tasks (first to complete wins, others cancelled)")
    parser.add_argument("--wait-all", action="store_true", help="For fan-out: wait for ALL tasks to complete and show aggregated results")
    parser.add_argument("--max-retries", type=int, default=2, help="Max retry attempts for failed tasks (default: 2)")
    parser.add_argument("--host", default=DEFAULT_HOST, help="A2A Mesh host URL")
    parser.add_argument("--user", default=DEFAULT_USER, help="Auth username")
    parser.add_argument("--password", default=DEFAULT_PASS, help="Auth password")
    
    args = parser.parse_args()
    client = BridgeClient(host=args.host, user=args.user, password=args.password)
    
    if args.command == "create":
        if not args.subject:
            print("Error: --subject required", file=sys.stderr)
            sys.exit(1)
        to = args.to or "any"
        ctx = json.loads(args.context) if args.context else None
        result = client.create(
            to_agent=to, subject=args.subject, task_type=args.type,
            priority=args.priority, description=args.description,
            context=ctx, available=args.available or to == "any",
            fan_out=getattr(args, 'fan_out', 0) or 0,
            max_retries=args.max_retries,
        )
        # fan_out returns dict with task_ids
        task_ids = []
        if isinstance(result, dict) and "task_ids" in result:
            task_ids = result["task_ids"]
            if args.wait_all and task_ids:
                print(f"Waiting for {len(task_ids)} fan-out tasks to complete...", file=sys.stderr)
                aggregated = []
                for tid in task_ids:
                    start = time.time()
                    remaining = args.timeout
                    while remaining > 0:
                        s = client.status(tid)
                        status = s.get("status", "unknown")
                        if status in ("completed", "failed", "cancelled"):
                            aggregated.append(s)
                            break
                        time.sleep(args.poll_interval)
                        remaining = args.timeout - (time.time() - start)
                    else:
                        aggregated.append({"task_id": tid, "status": "timeout"})
                # Summarize
                completed = [a for a in aggregated if a.get("status") == "completed"]
                failed = [a for a in aggregated if a.get("status") != "completed"]
                print(f"\nFan-out results: {len(completed)} completed, {len(failed)} failed/timeout", file=sys.stderr)
                print(json.dumps(aggregated, indent=2, ensure_ascii=False))
            else:
                print(json.dumps(result, indent=2, ensure_ascii=False))
        else:
            print(json.dumps(result, indent=2, ensure_ascii=False))
    
    elif args.command == "delegate":
        if not args.subject:
            print("Error: --subject required", file=sys.stderr)
            sys.exit(1)
        to = args.to or "any"
        ctx = json.loads(args.context) if args.context else None
        result = client.delegate(
            to_agent=to, subject=args.subject, task_type=args.type,
            priority=args.priority, description=args.description,
            context=ctx, timeout=args.timeout,
            poll_interval=args.poll_interval, available=args.available or to == "any",
        )
        print(json.dumps(result, indent=2))
    
    elif args.command == "status":
        if not args.task_id:
            print("Error: --task-id required", file=sys.stderr)
            sys.exit(1)
        result = client.status(args.task_id)
        print(json.dumps(result, indent=2))
    
    elif args.command == "files":
        if not args.task_id:
            print("Error: --task-id required", file=sys.stderr)
            sys.exit(1)
        result = client.files(args.task_id)
        print(json.dumps(result, indent=2))
    
    elif args.command == "download":
        if not args.task_id:
            print("Error: --task-id required", file=sys.stderr)
            sys.exit(1)
        content = client.download(args.task_id)
        print(content)
    
    elif args.command == "list":
        result = client.list_delegations(status=args.status, limit=args.limit)
        print(json.dumps(result, indent=2))
    
    elif args.command == "cancel":
        if not args.task_id:
            print("Error: --task-id required", file=sys.stderr)
            sys.exit(1)
        result = client.cancel(args.task_id)
        print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
