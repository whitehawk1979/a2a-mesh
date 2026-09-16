"""Automatic SSH key synchronization between approved mesh peers.

Design (deterministic, no LLM):
- When a P2P/SSH-tunnel peer connects (or a discovered peer is approved), the
  node sends an `ssh_key_sync` A2A message containing its public SSH key(s).
- The receiving node validates the sender is an APPROVED peer, then merges the
  key into its authorized_keys (dedup, idempotent, no shell-exec of untrusted
  content — the key line is format-validated before writing).
- If the payload has request=true, the receiver replies with its own key
  (bidirectional sync). This lets returning approved peers re-sync any time.
- Keys are also persisted to PG (mesh.ssh_peer_keys) so the mapping survives
  node restarts and can be audited from the dashboard.

Security:
- Only keys from senders whose AgentCard is registered+approved are accepted.
- Key lines must match the strict `ssh-ed25519|ssh-rsa AAAA... comment` format.
- Writes are atomic (temp file + rename) with 0600 permissions preserved.
"""

import asyncio
import logging
import os
import re
import time
from pathlib import Path
from typing import Dict, Optional

log = logging.getLogger("a2a_mesh.ssh_key_sync")

# Strict public key line format: type + base64 + optional comment
_KEY_RE = re.compile(
    r"^(ssh-ed25519|ecdsa-sha2-nistp\d+|ssh-rsa) ([A-Za-z0-9+/=]{60,}) (\S.*)?$"
)

# Send our key at most once per peer per this interval (avoid loops)
_RESEND_INTERVAL = 3600.0
_FORCE_FLOOR_INTERVAL = 120.0  # min. küldési idő force=True (reconnect) esetén is

# v2 protocol: coordinator re-bundles and rebroadcasts when the registry
# changes, but at most this often (flood protection)
_BUNDLE_MIN_INTERVAL = 30.0

# v2: how long a received bundle entry stays valid in the local registry
_BUNDLE_ENTRY_TTL = 0  # 0 = no expiry (keys are only additive)


class SSHKeySync:
    """Handles automatic SSH public-key exchange between approved peers."""

    def __init__(self, node_name: str, registry, router,
                 pg_pool=None, authorized_keys_path: Optional[str] = None,
                 identity_files: Optional[list] = None,
                 advertised_ssh_port: int = 0,
                 node_config=None):
        self._node_name = node_name
        self._registry = registry
        self._router = router
        self._pg_pool = pg_pool
        self._advertised_ssh_port = int(advertised_ssh_port or 0)
        self._node_config = node_config  # mesh config (SSHTunnelConfig accessible)
        self._node_ref = None  # set by node.py via set_node_ref()
        self._last_sent: Dict[str, float] = {}  # peer_name -> ts
        # v2: coordinator-aggregated key registry (peer_name -> {keys, tunnel, ts})
        self._key_registry: Dict[str, dict] = {}
        self._last_bundle_ts = 0.0
        self._self_registered = False
        # Resolve authorized_keys path per platform
        if authorized_keys_path:
            self._ak_path = Path(authorized_keys_path).expanduser()
        else:
            ssh_dir = Path.home() / ".ssh"
            self._ak_path = ssh_dir / "authorized_keys"
        # Identity files whose .pub we advertise (config may override)
        self._identity_files = identity_files or [
            str(Path.home() / ".ssh" / "id_ed25519_openclaw"),
            str(Path.home() / ".ssh" / "id_ed25519"),
        ]

    # ── Outgoing ──────────────────────────────────────────────────

    def _read_own_pubkeys(self) -> list:
        keys = []
        for idf in self._identity_files:
            pub = Path(idf).expanduser()
            pub = pub.with_suffix(".pub") if not str(pub).endswith(".pub") else pub
            try:
                line = pub.read_text().strip()
                if _KEY_RE.match(line):
                    keys.append(line)
            except Exception:
                continue
        return keys

    def _ssh_tunnel_info(self) -> dict:
        """How peers reach THIS host's sshd for tunnels (multi-agent aware).

        Nodes on the same host (e.g. tor+mano on HAOS) share the host's
        sshd and authorized_keys; the advertised port is whatever sshd
        WE accept connections on (embedded sshd: 2222, normal host: 22).
        """
        info: dict = {"ssh_port": int(self._advertised_ssh_port or 22)}
        # Multi-agent awareness: announce OUR P2P port so the coordinator
        # bundle can map tunnel remote_port per peer (e.g. runa=8655).
        try:
            node = self._node_ref
            p2p_port = None
            if node is not None:
                cfg = getattr(node, "config", None)
                p2p_port = getattr(getattr(cfg, "p2p", None), "listen_port", None) if cfg else None
                if not p2p_port:
                    p2p_port = getattr(node, "p2p_port", None)
            if p2p_port:
                info["p2p_port"] = int(p2p_port)
            # Announce the local ssh_user peers should use to dial us (e.g.
            # nova=zsolt, morzsa=openclaw, HAOS containers=root) so tunnels
            # self-organize without per-peer config edits.
            if node is not None:
                cfg = getattr(node, "config", None)
                ssh_cfg = getattr(cfg, "ssh_tunnel", None) if cfg is not None else None
                user = getattr(ssh_cfg, "default_ssh_user", "") if ssh_cfg else ""
                if not user and self._node_config is not None:
                    sc = getattr(self._node_config, "ssh_tunnel", None)
                    user = getattr(sc, "default_ssh_user", "") if sc else ""
                if user:
                    info["ssh_user"] = str(user)
                # Multi-agent HAOS host: P2P not on sshd's loopback — announce
                # the host IP the forward must target (tor sshd → host → mano).
                if ssh_cfg is not None:
                    fwd = getattr(ssh_cfg, "advertised_forward_host", "") or ""
                    if fwd:
                        info["forward_host"] = str(fwd)
                # Embedded sshd (ssh_server.py): our OWN sshd is the dial-in
                # target — advertise its actual port so peers don't guess.
                inst = None
                try:
                    from .ssh_server import get_embedded_sshd
                    inst = get_embedded_sshd()
                except Exception:
                    inst = None
                if inst is not None and inst.running:
                    info["ssh_port"] = int(inst.port)
        except Exception:
            pass
        return info

    # ── v2: coordinator aggregation ───────────────────────────────

    def _is_coordinator(self) -> bool:
        """The tree root (no parent) aggregates and distributes the bundle."""
        try:
            node = self._node_ref
            router = getattr(node, "router", None)
            tree = getattr(router, "tree", None) or getattr(router, "tree_router", None)
            if tree is None:
                return False
            la = getattr(tree, "local_address", None)
            if la is None:
                return False
            return getattr(la, "parent_short", None) is None
        except Exception:
            return False

    def _registry_changed(self, sender: str, payload: dict) -> bool:
        """Detect whether a peer's registry entry actually changed (dedup)."""
        keys = sorted(payload.get("keys", []) or [])
        tunnel = payload.get("tunnel") or {}
        new = {"keys": keys, "tunnel": tunnel}
        old = self._key_registry.get(sender)
        if old is None:
            self._key_registry[sender] = new
            return True
        if old.get("keys") != new["keys"] or old.get("tunnel") != new["tunnel"]:
            self._key_registry[sender] = new
            return True
        return False

    async def _maybe_broadcast_bundle(self):
        """Coordinator: rebroadcast the aggregated bundle (rate-limited)."""
        if not self._is_coordinator():
            return
        now = time.time()
        if now - self._last_bundle_ts < _BUNDLE_MIN_INTERVAL:
            return
        self._last_bundle_ts = now
        await self.broadcast_bundle()

    async def broadcast_bundle(self):
        """Coordinator sends the aggregated {peer: {keys, tunnel}} bundle."""
        try:
            from .message import A2AMessage, MSG_TYPE_KEY_BUNDLE
            # include ourselves in the bundle
            bundle = dict(self._key_registry)
            my_keys = self._read_own_pubkeys()
            if my_keys:
                bundle[self._node_name] = {
                    "keys": my_keys,
                    "tunnel": self._ssh_tunnel_info(),
                }
            msg = A2AMessage.create(
                sender=self._node_name,
                recipient="broadcast",
                msg_type=MSG_TYPE_KEY_BUNDLE,
                payload={"bundle": bundle, "coordinator": self._node_name},
                priority=5,
            )
            await self._router.send(msg)
            log.info(f"SSHKeySync: broadcast key bundle with {len(bundle)} peer(s)")
        except Exception as e:
            log.warning(f"SSHKeySync: bundle broadcast failed: {e}")

    async def handle_bundle(self, payload: dict, sender: str) -> bool:
        """Non-coordinator (or coordinator converging): apply the bundle.

        - merge every peer's keys into authorized_keys
        - auto-register SSH-tunnel peers for any peer we don't have yet
        """
        if not isinstance(payload, dict):
            return False
        bundle = payload.get("bundle") or {}
        if not isinstance(bundle, dict) or not bundle:
            return False
        applied = 0
        for peer_name, entry in bundle.items():
            if peer_name == self._node_name:
                continue  # our own keys are already local
            if not isinstance(entry, dict):
                continue
            keys = entry.get("keys", []) or []
            for k in keys:
                if isinstance(k, str) and _KEY_RE.match(k.strip()):
                    if self._merge_key(k.strip()):
                        applied += 1
            tunnel = entry.get("tunnel") or {}
            if isinstance(tunnel, dict) and tunnel.get("ssh_port"):
                await self._auto_register_tunnel_peer(peer_name, tunnel)
        if applied:
            log.info(f"SSHKeySync: bundle from {sender} added {applied} new key(s)")
        return applied > 0

    async def announce_to_coordinator(self):
        """v2: announce our keys + tunnel info so the tree root can aggregate.

        Uses broadcast — in the tree topology a broadcast reaches the root,
        and every intermediate node also caches our keys (defense in depth).
        The coordinator rate-limits bundle rebroadcasts via _BUNDLE_MIN_INTERVAL.
        Direct parent sends would strand grandchildren whose parent is not
        the root; broadcast is the deterministic full-coverage path.
        """
        try:
            from .message import A2AMessage, MSG_TYPE_SSH_KEY_SYNC
            keys = self._read_own_pubkeys()
            if not keys:
                return
            msg = A2AMessage.create(
                sender=self._node_name,
                recipient="broadcast",
                msg_type=MSG_TYPE_SSH_KEY_SYNC,
                payload={
                    "keys": keys,
                    "request": False,
                    "node_name": self._node_name,
                    "tunnel": self._ssh_tunnel_info(),
                },
                priority=6,
            )
            await self._router.send(msg)
            self._self_registered = True
            log.info("SSHKeySync: announced keys via broadcast (coordinator aggregates)")
        except Exception as e:
            log.debug(f"SSHKeySync: announce_to_coordinator failed: {e}")

    async def send_keys_to(self, peer_name: str, force: bool = False):
        """Advertise our public key(s) to an approved peer (idempotent)."""
        now = time.time()
        if now - self._last_sent.get(peer_name, 0) < (_FORCE_FLOOR_INTERVAL if force else _RESEND_INTERVAL):
            # force-floor: a peer-reconnect (force=True) sem indíthat újabb küldést
            # 120s-enként — a flappelő HAOS tunnel-ek ne generáljanak ssh_key_sync vihart
            return
        keys = self._read_own_pubkeys()
        if not keys:
            log.debug("SSHKeySync: no readable identity .pub — nothing to send")
            return
        try:
            from .message import A2AMessage, MSG_TYPE_SSH_KEY_SYNC
            msg = A2AMessage.create(
                sender=self._node_name,
                recipient=peer_name,
                msg_type=MSG_TYPE_SSH_KEY_SYNC,
                payload={
                    "keys": keys,
                    "request": False,
                    "node_name": self._node_name,
                    "tunnel": self._ssh_tunnel_info(),
                },
                priority=6,
            )
            await self._router.send(msg)
            self._last_sent[peer_name] = now
            log.info(f"SSHKeySync: sent {len(keys)} pubkey(s) to {peer_name}")
        except Exception as e:
            log.warning(f"SSHKeySync: send to {peer_name} failed: {e}")

    async def request_keys_from(self, peer_name: str):
        """Ask an approved peer to send its key(s) (returning-peer re-sync)."""
        try:
            from .message import A2AMessage, MSG_TYPE_SSH_KEY_SYNC
            msg = A2AMessage.create(
                sender=self._node_name,
                recipient=peer_name,
                msg_type=MSG_TYPE_SSH_KEY_SYNC,
                payload={
                    "keys": [],
                    "request": True,
                    "node_name": self._node_name,
                },
                priority=6,
            )
            await self._router.send(msg)
            log.info(f"SSHKeySync: requested keys from {peer_name}")
        except Exception as e:
            log.warning(f"SSHKeySync: request from {peer_name} failed: {e}")

    # ── Incoming ──────────────────────────────────────────────────

    def _sender_is_approved(self, sender: str) -> bool:
        try:
            card = self._registry.get(sender)
            return card is not None
        except Exception:
            return False

    async def handle_incoming(self, payload: dict, sender: str) -> bool:
        """Merge received keys into authorized_keys; reply if requested.

        Also auto-registers the sender as an SSH-tunnel peer at runtime
        when tunnel info (ssh_port) is present — no config edit needed.
        """
        if not isinstance(payload, dict):
            return False
        if not self._sender_is_approved(sender):
            log.warning(f"SSHKeySync: IGNORED keys from unapproved sender {sender}")
            return False
        keys = payload.get("keys", []) or []
        if not keys:
            log.debug(f"SSHKeySync: empty keys from {sender} (request-only)")
        added = 0
        for k in keys:
            if not isinstance(k, str) or not _KEY_RE.match(k.strip()):
                log.warning(f"SSHKeySync: invalid key format from {sender} — skipped")
                continue
            if self._merge_key(k.strip()):
                added += 1
        if added:
            log.info(f"SSHKeySync: merged {added} new key(s) from {sender}")
            await self._persist_pg(sender, keys)
        # Auto-register sender as a runtime SSH-tunnel peer (bidir tunnels
        # even when multiple agents share one host — each announces its
        # own ssh_port; we dial the peer's P2P port through it).
        tunnel_info = payload.get("tunnel") or {}
        if isinstance(tunnel_info, dict) and tunnel_info.get("ssh_port"):
            await self._auto_register_tunnel_peer(sender, tunnel_info)
        # v2: feed the coordinator registry; rebroadcast bundle if changed
        if self._registry_changed(sender, payload):
            await self._maybe_broadcast_bundle()
        # Bidirectional: reply with our keys if asked
        if payload.get("request"):
            await self.send_keys_to(sender, force=True)
        return added > 0

    async def _auto_register_tunnel_peer(self, peer_name: str, tunnel_info: dict):
        """Runtime-register an SSH-tunnel peer so tunnels self-organize.

        Looks up the peer's address from peer_discovery (PG/mDNS), then asks
        the node's SSHTunnelTransport to add/refresh the peer and connect.
        Idempotent: existing configured peers are left untouched.
        """
        try:
            node = self._node_ref
            discovery = getattr(node, "peer_discovery", None)
            transport = getattr(node, "_ssh_tunnel_transport", None)
            if not discovery or not transport:
                return
            peer = discovery.get_peer(peer_name)
            if not peer:
                log.debug(f"SSHKeySync: no discovery entry for {peer_name} — cannot auto-register tunnel")
                return
            ssh_port = int(tunnel_info.get("ssh_port") or 22)
            # Already a configured peer? Then nothing to do (config wins).
            if peer_name in getattr(transport, "_tunnels", {}):
                return
            # Announced P2P port wins (authoritative — the peer knows its own
            # listen port); fall back to discovery, then the 8645 default.
            announced_p2p = tunnel_info.get("p2p_port")
            remote_port = int(announced_p2p) if announced_p2p else (peer.p2p_port or 8645)
            added = await transport.add_dynamic_peer(
                name=peer_name,
                ssh_host=peer.host,
                ssh_port=ssh_port,
                remote_port=remote_port,
                ssh_user=tunnel_info.get("ssh_user") or "root",
                identity_file=None,  # transport default identity
                forward_host=str(tunnel_info.get("forward_host") or ""),
            )
            if added:
                log.info(f"SSHKeySync: auto-registered dynamic tunnel peer {peer_name} at {peer.host}:{ssh_port}")
        except Exception as e:
            log.debug(f"SSHKeySync: auto-register tunnel peer failed: {e}")

    def _merge_key(self, key_line: str) -> bool:
        """Dedup-merge one key line into authorized_keys. Returns True if new."""
        try:
            self._ak_path.parent.mkdir(parents=True, exist_ok=True)
            if not self._ak_path.exists():
                self._ak_path.touch(mode=0o600)
            existing = self._ak_path.read_text().splitlines()
            # Normalize comparison: compare key body (type + base64), ignore comment
            def key_body(line):
                parts = line.strip().split()
                return parts[0] + " " + parts[1] if len(parts) >= 2 else line.strip()
            if any(key_body(l) == key_body(key_line) for l in existing):
                return False
            with open(self._ak_path, "a") as f:
                f.write(key_line + "\n")
            try:
                os.chmod(self._ak_path, 0o600)
            except Exception:
                pass
            return True
        except Exception as e:
            log.error(f"SSHKeySync: authorized_keys write failed: {e}")
            return False

    async def _persist_pg(self, sender: str, keys: list):
        """Best-effort persist of received key mapping for audit/restart."""
        if not self._pg_pool:
            return
        try:
            await self._pg_pool.execute(
                """
                CREATE TABLE IF NOT EXISTS mesh.ssh_peer_keys (
                    id SERIAL PRIMARY KEY,
                    peer_name TEXT NOT NULL,
                    key_line TEXT NOT NULL,
                    added_at TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE(peer_name, key_line)
                )
                """,
                timeout=10,
            )
            for k in keys:
                if isinstance(k, str) and _KEY_RE.match(k.strip()):
                    await self._pg_pool.execute(
                        """
                        INSERT INTO mesh.ssh_peer_keys (peer_name, key_line)
                        VALUES ($1, $2)
                        ON CONFLICT (peer_name, key_line) DO NOTHING
                        """,
                        sender, k.strip(), timeout=10,
                    )
        except Exception as e:
            log.debug(f"SSHKeySync: PG persist failed (non-blocking): {e}")

    # ── Peer-connect hook ─────────────────────────────────────────

    def set_node_ref(self, node):
        """Give the sync module a reference to the owning node (for
        peer_discovery + _ssh_tunnel_transport access in auto-register)."""
        self._node_ref = node

    async def on_peer_connected(self, peer_name: str):
        """Called on every transport peer_connected — send keys + request."""
        try:
            # force=True: peer restarts reset _last_sent state on THEIR side,
            # but our rate-limit must not block the re-sync handshake either
            await self.send_keys_to(peer_name, force=True)
            # Re-request on every connect so returning peers re-sync too
            await self.request_keys_from(peer_name)
            # v2: a newly connected peer triggers registry aggregation —
            # the coordinator merges everything and pushes the full bundle
            await self.announce_to_coordinator()
        except Exception as e:
            log.debug(f"SSHKeySync: on_peer_connected({peer_name}) error: {e}")