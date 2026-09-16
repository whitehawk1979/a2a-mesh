# Federation — inter-mesh federation for connecting separate mesh clusters.
# Inspired by Marveen's federation system.

import time
import logging
import json
import os
import asyncio
import aiohttp
import subprocess
from typing import Dict, List, Optional, Any

log = logging.getLogger("federation")

FEDERATION_CONFIG = os.path.expanduser("~/.hermes/scripts/a2a_mesh/data/federation.json")

class FederationBridge:
    """Handles SSH tunnels to remote meshes."""
    def __init__(self):
        self.tunnels: Dict[str, subprocess.Popen] = {}

    def start_tunnel(self, peer_name: str, remote_host: str, remote_port: int, local_port: int = 8650):
        """Starts an SSH tunnel to the remote mesh."""
        # Simple SSH tunnel command: ssh -L local_port:localhost:remote_port user@remote_host -N
        cmd = ["ssh", "-L", f"{local_port}:localhost:{remote_port}", remote_host, "-N"]
        try:
            proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            self.tunnels[peer_name] = proc
            log.info(f"SSH tunnel started for {peer_name} ({remote_host})")
            return True
        except Exception as e:
            log.error(f"Failed to start SSH tunnel for {peer_name}: {e}")
            return False

    def stop_tunnel(self, peer_name: str):
        """Stops the SSH tunnel for a peer."""
        proc = self.tunnels.pop(peer_name, None)
        if proc:
            proc.terminate()
            log.info(f"SSH tunnel stopped for {peer_name}")

    def stop_all(self):
        """Stop all active tunnels."""
        for name in list(self.tunnels.keys()):
            self.stop_tunnel(name)

class FederationManager:
    """Manages federation peers, health, and capabilities."""
    def __init__(self):
        self.bridge = FederationBridge()
        self.health_status = {}
        self.node_name = "nova"  # default, overridden by dashboard

    def get_config(self) -> dict:
        try:
            with open(FEDERATION_CONFIG, "r") as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return {"peers": [], "enabled": False}

    def save_config(self, config: dict):
        os.makedirs(os.path.dirname(FEDERATION_CONFIG), exist_ok=True)
        with open(FEDERATION_CONFIG, "w") as f:
            json.dump(config, f, indent=2)

    def add_peer(self, name: str, address: str, port: int = 8650, ssh_tunnel: bool = False):
        config = self.get_config()
        # Check if peer already exists — preserve trust level
        existing = next((p for p in config["peers"] if p["name"] == name), None)
        if existing:
            # Update address/port but keep trust and other metadata
            existing["address"] = address
            existing["port"] = port
            existing["ssh_tunnel"] = ssh_tunnel
            self.save_config(config)
            return existing
        peer = {
            "name": name,
            "address": address,
            "port": port,
            "ssh_tunnel": ssh_tunnel,
            "trust": "discovered", # trusted, untrusted, discovered
            "added_at": time.time(),
            "status": "unknown",
            "capabilities": [],
            "last_seen": 0
        }
        config["peers"].append(peer)
        self.save_config(config)
        return peer

    def remove_peer(self, name: str):
        config = self.get_config()
        peers_before = len(config["peers"])
        config["peers"] = [p for p in config["peers"] if p["name"] != name]
        self.bridge.stop_tunnel(name)
        self.save_config(config)
        return len(config["peers"]) < peers_before

    def set_trust(self, name: str, level: str = None):
        config = self.get_config()
        for p in config["peers"]:
            if p["name"] == name:
                # Toggle if no explicit level given
                if level is None or level == "toggle":
                    p["trust"] = "trusted" if p.get("trust", "discovered") != "trusted" else "untrusted"
                else:
                    p["trust"] = level
                self.save_config(config)
                return True, p["trust"]
        return False, None

    async def discover_lan(self, port: int = 8650) -> List[dict]:
        """Scans local network for mesh nodes on specified port."""
        discovered = []
        import socket
        try:
            # Get local IP reliably (not 127.0.0.1)
            local_ip = None
            try:
                s_test = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                s_test.connect(("8.8.8.8", 80))
                local_ip = s_test.getsockname()[0]
                s_test.close()
            except Exception:
                pass
            if not local_ip or local_ip.startswith("127."):
                local_ip = "192.168.1.8"  # fallback Nova LAN IP
            
            subnet = ".".join(local_ip.split(".")[:3]) + "."
            
            log.info(f"Scanning LAN subnet {subnet}0/24 on port {port} (local_ip={local_ip})...")
            
            import asyncio as aio
            async def probe(ip_addr):
                try:
                    # Direct HTTP health check (skip TCP pre-check)
                    async with aiohttp.ClientSession() as session:
                        async with session.get(
                            f"http://{ip_addr}:{port}/api/health",
                            timeout=aiohttp.ClientTimeout(total=2)
                        ) as resp:
                            if resp.status == 200:
                                data = await resp.json()
                                node_name = data.get("node", ip_addr)
                                # Skip self
                                if node_name == self.node_name:
                                    return None
                                return {
                                    "name": node_name,
                                    "address": ip_addr,
                                    "port": port,
                                    "status": "discovered",
                                    "version": data.get("version", "?"),
                                    "peers": data.get("peers", {})
                                }
                except Exception:
                    pass
                return None
            
            tasks = [probe(f"{subnet}{i}") for i in range(1, 255)]
            results = await aio.wait_for(aio.gather(*tasks, return_exceptions=True), timeout=15)
            for r in results:
                if r and r is not None:
                    discovered.append(r)
            
            log.info(f"LAN discovery found {len(discovered)} nodes")
        except Exception as e:
            log.error(f"LAN discovery failed: {e}")
            
        return discovered

    async def check_health(self, peer: dict) -> dict:
        """Checks the health of a remote mesh."""
        url = f"http://{peer['address']}:{peer['port']}/api/health"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=2) as resp:
                    if resp.status == 200:
                        return {"status": "online", "last_seen": time.time()}
        except Exception:
            pass
        return {"status": "offline", "last_seen": time.time()}

    async def fetch_capabilities(self, peer: dict) -> List[str]:
        """Fetches capabilities from a remote mesh."""
        url = f"http://{peer['address']}:{peer['port']}/api/overview"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=2) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        return data.get("capabilities", [])
        except Exception as e:
            log.warning(f"Failed to fetch capabilities for {peer['name']}: {e}")
        return []

    async def poll_all_peers(self):
        """Periodic poll of all federated peers."""
        config = self.get_config()
        for p in config.get("peers", []):
            health = await self.check_health(p)
            p["status"] = health["status"]
            p["last_seen"] = health["last_seen"]
            if health["status"] == "online":
                p["capabilities"] = await self.fetch_capabilities(p)
        self.save_config(config)

    def get_status(self):
        config = self.get_config()
        peers = list(config.get("peers", []))
        return {
            "enabled": config.get("enabled", False),
            "peer_count": len(peers),
            "active_count": len([p for p in peers if p.get("status") == "online"]),
            "tunnel_count": len(self.bridge.tunnels),
            "peers": peers
        }

    async def get_status_with_mesh(self, pg_pool=None, node_name="nova"):
        """Get federation status merged with mesh-discovered nodes from PG."""
        config = self.get_config()
        config_peers = {p["name"]: p for p in config.get("peers", [])}
        
        # Fetch all mesh nodes from PG
        mesh_nodes = []
        if pg_pool:
            try:
                rows = await pg_pool.fetch(
                    """SELECT node_name, host, p2p_port, status, version, last_heartbeat,
                              http_available, p2p_available
                       FROM mesh.mesh_nodes ORDER BY node_name"""
                )
                for row in rows:
                    name = row["node_name"]
                    if name == node_name:
                        continue  # Skip self
                    row_dict = dict(row)
                    # Determine status from availability flags
                    is_online = row_dict.get("p2p_available") or row_dict.get("http_available")
                    status_val = "online" if is_online else (row_dict.get("status") or "unknown")
                    mesh_nodes.append({
                        "name": name,
                        "address": row_dict.get("host") or "",
                        "port": 8650,  # Dashboard port is always 8650
                        "trust": "trusted",
                        "ssh_tunnel": False,
                        "status": status_val,
                        "version": row_dict.get("version") or "?",
                        "last_seen": str(row_dict["last_heartbeat"]) if row_dict.get("last_heartbeat") else None,
                        "capabilities": [],
                        "source": "mesh"
                    })
            except Exception as e:
                log.warning(f"Failed to fetch mesh nodes for federation: {e}")
        
        # Merge: mesh nodes take priority but preserve static config extras
        merged = {}
        for name, p in config_peers.items():
            merged[name] = p
        for n in mesh_nodes:
            if n["name"] in merged:
                # Update address/status from mesh if more recent
                merged[n["name"]]["address"] = n["address"]
                merged[n["name"]]["status"] = n["status"]
                merged[n["name"]]["version"] = n.get("version", "?")
                if n.get("last_seen"):
                    merged[n["name"]]["last_seen"] = n["last_seen"]
                merged[n["name"]]["source"] = "mesh+config"
            else:
                merged[n["name"]] = n
        
        peers = list(merged.values())
        return {
            "enabled": config.get("enabled", True),
            "peer_count": len(peers),
            "active_count": len([p for p in peers if p.get("status") == "online"]),
            "tunnel_count": len(self.bridge.tunnels),
            "peers": peers
        }

# Singleton instance
manager = FederationManager()
