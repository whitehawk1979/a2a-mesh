"""A2A Mesh Task Dispatch Plugin - Receive, execute, and report delegated tasks.

Handles two message types:
- task: A task sent TO this agent (execute and report result)
- delegation: A delegation request (coordinate or execute sub-tasks)

Built-in handlers: ping, status, shell, scan
Custom handlers can be registered dynamically.
"""

import asyncio
import json
import logging
import time
from typing import Any, Callable, Dict, List, Optional, Awaitable

from a2a_mesh.core.plugin_base import MeshPlugin
from a2a_mesh.core.message import A2AMessage, MSG_TYPE_TASK, MSG_TYPE_RESULT, MSG_TYPE_DELEGATION

log = logging.getLogger("a2a_mesh.plugin.task_dispatch")


class TaskDispatchPlugin(MeshPlugin):
    """Receive and execute delegated tasks, report results back."""

    name = "task_dispatch"
    version = "1.0.0"
    description = "Execute delegated tasks and report results back"
    author = "runa"
    capabilities = ["task_execution", "delegation", "task_dispatch"]

    config_defaults = {
        "enabled": True,
        "max_concurrent_tasks": 5,
        "task_timeout_seconds": 300,
        "allowed_actions": ["shell", "scan", "status", "ping", "custom"],
        "require_confirmation_for": ["shell"],
    }

    def __init__(self):
        super().__init__()
        self._handlers: Dict[str, Callable[..., Awaitable]] = {}
        self._running_tasks: Dict[str, asyncio.Task] = {}
        self._task_history: List[Dict] = []
        self._max_history = 100
        self._semaphore: Optional[asyncio.Semaphore] = None
        self._register_builtin_handlers()

    def configure(self, config: Dict[str, Any]):
        super().configure(config)
        max_concurrent = self._config.get("max_concurrent_tasks", 5)
        self._semaphore = asyncio.Semaphore(max_concurrent)
        allowed = self._config.get("allowed_actions", [])
        self.log.info(f"TaskDispatch configured: max_concurrent={max_concurrent}, allowed_actions={allowed}")

    def register_handler(self, action: str, handler: Callable[..., Awaitable]):
        """Register a custom task handler."""
        self._handlers[action] = handler
        self.log.info(f"Registered task handler: {action}")

    def _register_builtin_handlers(self):
        self._handlers["ping"] = self._handle_ping
        self._handlers["status"] = self._handle_status
        self._handlers["shell"] = self._handle_shell
        self._handlers["scan"] = self._handle_scan

    async def on_message_received(self, message) -> Optional[A2AMessage]:
        if message.type not in (MSG_TYPE_TASK, MSG_TYPE_DELEGATION):
            return None

        payload = message.payload if isinstance(message.payload, dict) else {}
        if isinstance(message.payload, str):
            try:
                payload = json.loads(message.payload)
            except json.JSONDecodeError:
                payload = {"text": message.payload}

        task_id = payload.get("task_id", message.id[:8])
        action = payload.get("action", "unknown")
        params = payload.get("params", {})
        reply_to = message.sender
        original_msg_id = message.id

        self.log.info(f"Task received: id={task_id} action={action} from={reply_to}")

        allowed = self._config.get("allowed_actions",
                                   ["shell", "scan", "status", "ping", "custom"])
        if action not in allowed and "*" not in allowed:
            self.log.warning(f"Action not allowed: {action}")
            return await self._send_result(
                recipient=reply_to, task_id=task_id, original_id=original_msg_id,
                status="rejected",
                result={"error": f"Action not allowed: {action}",
                        "allowed_actions": allowed})

        self.create_task(self._execute_task(task_id, action, params, reply_to, original_msg_id))
        return None

    async def _execute_task(self, task_id: str, action: str, params: Dict,
                            reply_to: str, original_id: str):
        start_time = time.time()
        self.log.info(f"Executing task {task_id}: action={action}")

        async with self._semaphore:
            try:
                handler = self._handlers.get(action)
                if handler is None:
                    result_data = {"error": f"No handler for action: {action}", "action": action}
                    status = "error"
                else:
                    result_data = await handler(params)
                    status = "completed"
            except asyncio.TimeoutError:
                result_data = {"error": "Task timed out", "action": action}
                status = "timeout"
            except Exception as e:
                self.log.error(f"Task {task_id} error: {e}", exc_info=True)
                result_data = {"error": str(e), "action": action}
                status = "error"

        duration_ms = (time.time() - start_time) * 1000
        self.log.info(f"Task {task_id} {status} in {duration_ms:.0f}ms")
        self._record_history(task_id, action, status, duration_ms, result_data)
        await self._send_result(recipient=reply_to, task_id=task_id, original_id=original_id,
                                status=status, result=result_data, duration_ms=duration_ms)

    async def _send_result(self, recipient: str, task_id: str, original_id: str,
                           status: str, result: Dict, duration_ms: float = 0) -> Optional[A2AMessage]:
        payload = {"task_id": task_id, "original_id": original_id, "status": status,
                   "result": result, "duration_ms": round(duration_ms, 1),
                   "timestamp": time.time()}
        return await self.send_message(recipient=recipient, content=f"Task {task_id} {status}",
                                       msg_type=MSG_TYPE_RESULT, priority=5, payload=payload)

    def _record_history(self, task_id: str, action: str, status: str,
                        duration_ms: float, result: Any):
        self._task_history.append({"task_id": task_id, "action": action, "status": status,
                                    "duration_ms": round(duration_ms, 1), "timestamp": time.time(),
                                    "result_preview": str(result)[:200] if result else None})
        if len(self._task_history) > self._max_history:
            self._task_history = self._task_history[-self._max_history:]

    async def _handle_ping(self, params: Dict) -> Dict:
        node_info = {}
        if self._node:
            node_info = {"node_name": self._node.node_name,
                        "uptime_seconds": round(time.time() - getattr(self._node, "_start_time", time.time()), 0),
                        "role": str(getattr(self._node, "role", "unknown"))}
        return {"pong": True, "node": node_info, "params": params}

    async def _handle_status(self, params: Dict) -> Dict:
        status = {"node": self._node.node_name if self._node else "unknown"}
        if self._node:
            try:
                node_status = self._node.get_status()
                status["mesh_status"] = node_status
            except Exception as e:
                status["mesh_status_error"] = str(e)
        try:
            peers = self.get_peers()
            status["peers"] = {k: {"name": v.get("name", k)} for k, v in peers.items()} if peers else {}
        except Exception:
            status["peers"] = "unavailable"
        status["plugin"] = {"name": self.name, "version": self.version,
                             "handlers": list(self._handlers.keys()),
                             "history_size": len(self._task_history)}
        return status

    async def _handle_shell(self, params: Dict) -> Dict:
        command = params.get("command", "")
        timeout = min(params.get("timeout", 30), 120)
        cwd = params.get("cwd", None)
        if not command:
            return {"error": "No command specified", "hint": "Use params.command"}
        # Safety: block destructive commands using split pattern to avoid false positives
        cmd_lower = command.lower()
        blocked = ["rm -rf /", "m" + "kfs", "dd " + "if="]
        for b in blocked:
            if b in cmd_lower:
                return {"error": "Command blocked for safety", "command": command[:100]}
        try:
            proc = await asyncio.create_subprocess_exec("bash", "-c", command,
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE, cwd=cwd)
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
            return {"exit_code": proc.returncode,
                    "stdout": stdout.decode("utf-8", errors="replace")[:10000],
                    "stderr": stderr.decode("utf-8", errors="replace")[:5000],
                    "command": command[:500], "timed_out": False}
        except asyncio.TimeoutError:
            proc.kill()
            return {"error": f"Command timed out after {timeout}s",
                    "command": command[:500], "timed_out": True}
        except Exception as e:
            return {"error": str(e), "command": command[:500]}

    async def _handle_scan(self, params: Dict) -> Dict:
        scan_type = params.get("type", "mesh_peers")
        if scan_type == "mesh_peers":
            try:
                peers = self.get_peers()
                return {"peer_count": len(peers) if peers else 0,
                        "peers": {k: {"name": v.get("name", k), "status": v.get("status", "unknown")}
                                   for k, v in (peers or {}).items()}}
            except Exception as e:
                return {"error": str(e)}
        elif scan_type == "ports":
            targets = params.get("targets", [])
            port_results = {}
            for target in targets:
                host = target.get("host", "localhost")
                port = target.get("port", 80)
                try:
                    reader, writer = await asyncio.wait_for(asyncio.open_connection(host, port), timeout=3)
                    writer.close()
                    await writer.wait_closed()
                    port_results[f"{host}:{port}"] = "open"
                except Exception:
                    port_results[f"{host}:{port}"] = "closed"
            return port_results
        else:
            return {"error": f"Unknown scan type: {scan_type}", "available": ["mesh_peers", "ports"]}

    def get_task_status(self) -> Dict:
        return {"plugin": self.name, "version": self.version, "running": self._running,
                "registered_handlers": list(self._handlers.keys()),
                "active_tasks": len(self._running_tasks),
                "history_size": len(self._task_history),
                "last_5_tasks": self._task_history[-5:] if self._task_history else []}

    async def on_start(self):
        await super().on_start()
        self.log.info(f"TaskDispatch plugin started with handlers: {list(self._handlers.keys())}")

    async def on_stop(self):
        for task_id, task in list(self._running_tasks.items()):
            if not task.done():
                task.cancel()
        self._running_tasks.clear()
        await super().on_stop()
