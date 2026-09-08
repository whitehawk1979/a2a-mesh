"""
Tailscale VPN beépítés a mesh P2P rétegébe.

Determinisztikus IP-választás: minden node a configjában megadott
`network.prefer` (lan|vpn|auto) alapján választ a static_nodes bejegyzések
közül. `auto` mód: Tailscale fut → VPN IP-t preferál (titkosított, LAN-független);
Tailscale nem fut → LAN IP marad.

Használat:
    from core.vpn import resolve_peer_addresses, get_preferred_address

    candidates = resolve_peer_addresses(node, "morzsa")
    # → [{'ip': '100.65.232.47', 'transport': 'vpn'}, {'ip': '192.168.1.30', ...}]
"""

import asyncio
import logging
import re
import socket
import subprocess
from typing import Dict, List, Optional

log = logging.getLogger("a2a_mesh.vpn")

VPN_PREFIX = "100."  # Tailscale CGNAT range
VPN_CHECK_INTERVAL_S = 120  # 2 percenként újra-ellenőrzés


def _tailscale_ip(timeout_s: float = 4.0) -> Optional[str]:
    """A node saját Tailscale IP-je `tailscale ip -4`-gyel, hibamentes fallback-kel."""
    try:
        r = subprocess.run(["tailscale", "ip", "-4"], capture_output=True, text=True, timeout=timeout_s)
        if r.returncode == 0:
            ip = r.stdout.strip().split("\n")[0].strip()
            if ip.startswith(VPN_PREFIX):
                return ip
    except FileNotFoundError:
        pass
    except Exception as e:
        log.debug(f"tailscale ip parancs hiba: {e}")
    return None


def _local_vpn_ip() -> Optional[str]:
    """Fallback: a node saját 100.x IP-je interface-szintű socket-kapcsolódással."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            s.connect(("100.100.100.100", 53))  # Tailscale DNS — nem küld csomagot
            ip = s.getsockname()[0]
            if ip.startswith(VPN_PREFIX):
                return ip
        finally:
            s.close()
    except Exception:
        pass
    return None


def is_vpn_available() -> bool:
    """Van-e a gépen működő Tailscale (parancs + interface)."""
    return (_tailscale_ip() is not None) or (_local_vpn_ip() is not None)


def get_preferred_address(network_cfg, candidates: List[Dict]) -> Optional[Dict]:
    """Determinisztikus cím-választás a `network.prefer` (lan|vpn|auto) szerint.

    candidates: static_nodes bejegyzések ehhez a peer-hez:
        [{'ip': ..., 'p2p_port': ...}, ...]
    """
    prefer = (getattr(network_cfg, "prefer", "auto") or "auto").lower()
    if not candidates:
        return None
    vpn_cands = [c for c in candidates if str(c.get("ip", "")).startswith(VPN_PREFIX)]
    lan_cands = [c for c in candidates if not str(c.get("ip", "")).startswith(VPN_PREFIX)]
    if prefer == "vpn":
        return vpn_cands[0] if vpn_cands else (lan_cands[0] if lan_cands else None)
    if prefer == "lan":
        return lan_cands[0] if lan_cands else (vpn_cands[0] if vpn_cands else None)
    # auto: VPN elérhető → VPN-előnyben (titkosítatlan LAN helyett titkosított VPN)
    if is_vpn_available():
        return vpn_cands[0] if vpn_cands else (lan_cands[0] if lan_cands else None)
    return lan_cands[0] if lan_cands else (vpn_cands[0] if vpn_cands else None)


def resolve_peer_addresses(node, peer_name: str) -> List[Dict]:
    """Egy peer minden ismert címe a static_nodes listából (LAN + VPN)."""
    static_nodes = []
    try:
        disc = getattr(node.config, "discovery", None)
        static_nodes = getattr(disc, "static_nodes", []) or []
    except Exception:
        pass
    cands = []
    for sn in static_nodes:
        try:
            if (sn.get("name") or "").lower() == (peer_name or "").lower():
                cands.append({
                    "ip": sn.get("ip", ""),
                    "p2p_port": int(sn.get("p2p_port", 8645)),
                    "transport": "vpn" if str(sn.get("ip", "")).startswith(VPN_PREFIX) else "lan",
                })
        except Exception:
            continue
    return cands


async def vpn_health_loop(node) -> None:
    """Háttér-loop: VPN-állapot figyelése, state változásán log + (jövőben) újracsatlakozás.

    Nem LLM-függő, nem dinamikus újracsatlakozás — csak figyel és logol, hogy a
    diagnosztika látható legyen. Az újracsatlakozás a meglévő self-heal loopokra hagyatkozik.
    """
    last_state = None
    while True:
        try:
            vpn_ip = _tailscale_ip() or _local_vpn_ip()
            state = {"vpn_available": vpn_ip is not None, "vpn_ip": vpn_ip}
            if state != last_state:
                if vpn_ip:
                    log.info(f"🔒 VPN aktív: Tailscale IP {vpn_ip}")
                else:
                    log.warning("⚠️ VPN nem elérhető — P2P LAN címekre esik vissza")
                last_state = health_state = state  # noqa: F841 — diagnosztikai cache
            await asyncio.sleep(VPN_CHECK_INTERVAL_S)
        except asyncio.CancelledError:
            return
        except Exception as e:
            log.debug(f"vpn_health_loop hiba: {e}")
            await asyncio.sleep(VPN_CHECK_INTERVAL_S)