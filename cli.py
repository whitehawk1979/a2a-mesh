#!/usr/bin/env python3
"""A2A Mesh CLI — Command-line interface for mesh node management.

Usage:
    a2a-mesh start [--name NAME] [--port PORT] [--config FILE]
    a2a-mesh send --to RECIPIENT --type TYPE --payload JSON
    a2a-mesh broadcast --type TYPE --payload JSON
    a2a-mesh status
    a2a-mesh discover
    a2a-mesh keygen
    a2a-mesh test
"""

import asyncio
import json
import os
import sys
import time

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from a2a_mesh.core.config import MeshConfig
from a2a_mesh.core.message import A2AMessage, MSG_TYPE_DIRECTIVE
from a2a_mesh.core.encryption import MeshEncryption
from a2a_mesh.node import MeshNode


def cmd_elect(trigger: bool = False):
    """Show coordinator election status or trigger election."""
    from a2a_mesh.core.election import CoordinatorElection, ElectionConfig
    from a2a_mesh.core.config import MeshConfig

    config_file = os.path.expanduser("~/.hermes/mesh_config.yaml")
    config = MeshConfig.from_yaml(config_file) if os.path.exists(config_file) else MeshConfig()

    import psycopg2
    conn = psycopg2.connect(
        host=config.pg.host, port=config.pg.port,
        dbname=config.pg.dbname, user=config.pg.user,
        password=config.pg.password,
    )
    cur = conn.cursor()

    # Get all nodes from PG
    cur.execute("SELECT node_name, role, short_addr, parent_addr, depth FROM mesh.mesh_nodes ORDER BY short_addr")
    rows = cur.fetchall()

    # Find coordinator
    coord_row = None
    routers = []
    for name, role, addr, parent, depth in rows:
        if role == "coordinator":
            coord_row = (name, addr)
        elif role == "router":
            routers.append((name, addr))

    cur.close()
    conn.close()

    if not coord_row:
        print("❌ No coordinator found in mesh!")
        return

    # Create election engine
    election = CoordinatorElection(
        self_name=config.node_name,
        self_addr=0x0000,  # will be updated
        self_role=config.topology.node_role,
        config=ElectionConfig(
            heartbeat_interval=config.heartbeat.interval,
            suspect_threshold=config.heartbeat.warning_threshold,
            down_threshold=config.heartbeat.critical_threshold,
        ),
    )
    election.register_coordinator(coord_row[0], coord_row[1], is_original=True)

    # Update self addr
    for row_name, row_role, row_addr, row_parent, row_depth in rows:
        if row_name == config.node_name:
            election.self_addr = row_addr
            break

    if trigger:
        print("🏛️ Triggering coordinator election...")
        routers_sorted = sorted(routers, key=lambda r: r[1])
        if election.should_initiate_election(routers_sorted):
            claim = election.initiate_election()
            print(f"   Claim: {claim['node_name']} (0x{claim['short_addr']:04X})")
            print(f"   Reason: {claim['claim_reason']}")
            print(f"   Original coordinator: {claim['original_coordinator']}")
        else:
            print("   No election needed — coordinator is active or this node is not senior")
    else:
        status = election.get_status()
        print("Coordinator Election Status")
        print("=" * 50)
        coord = status["coordinator"]
        self_info = status["self"]
        print(f"  Coordinator:    {coord['node_name']} ({coord['short_addr']})")
        print(f"  State:          {coord['state']}")
        print(f"  Original:       {coord['is_original']}")
        print(f"  Last heartbeat: {coord['last_heartbeat_age']}")
        print()
        print(f"  Self:           {self_info['name']} ({self_info['addr']})")
        print(f"  Role:           {self_info['role']}")
        print(f"  Acting coord:   {self_info['is_acting_coordinator']}")
        print()
        print(f"  Routers (failover candidates):")
        for name, addr in sorted(routers, key=lambda r: r[1]):
            marker = " ← senior" if routers and addr == min(routers, key=lambda r: r[1])[1] else ""
            print(f"    0x{addr:04X}  {name}{marker}")


def cmd_leave(node_name: str = ""):
    """Leave mesh network gracefully — deregister from PG."""
    config_file = os.path.expanduser("~/.hermes/mesh_config.yaml")
    config = MeshConfig.from_yaml(config_file) if os.path.exists(config_file) else MeshConfig()
    name = node_name or config.node_name

    import psycopg2
    conn = psycopg2.connect(
        host=config.pg.host, port=config.pg.port,
        dbname=config.pg.dbname, user=config.pg.user,
        password=config.pg.password,
    )
    conn.autocommit = True
    cur = conn.cursor()

    # Check node exists
    cur.execute("SELECT node_name, role, short_addr FROM mesh.mesh_nodes WHERE node_name = %s", (name,))
    row = cur.fetchone()

    if not row:
        print(f"❌ Node '{name}' not found in mesh")
        cur.close()
        conn.close()
        return

    _, role, addr = row

    # Don't allow coordinator to leave without re-election
    if role == "coordinator":
        cur.execute("SELECT COUNT(*) FROM mesh.mesh_nodes WHERE role = 'router' AND status = 'active'")
        router_count = cur.fetchone()[0]
        if router_count == 0:
            print("🔴 Cannot leave — no routers to take over as coordinator!")
            print("   Add a router first, or force with --force")
            cur.close()
            conn.close()
            return
        print(f"⚠️  Coordinator leaving — election will be triggered")

    # Mark node as offline and set address to 0xFFFF (left mesh)
    cur.execute("""
        UPDATE mesh.mesh_nodes 
        SET status = 'offline', short_addr = 65535, last_heartbeat = NOW() 
        WHERE node_name = %s
    """, (name,))

    print(f"👋 Node '{name}' (0x{addr:04X}, {role}) left the mesh")
    print(f"   Status set to 'offline', address released")

    cur.close()
    conn.close()


def cmd_send_file(filepath: str, recipient: str, description: str = ""):
    """Send a file to a mesh node via file transfer."""
    import asyncio
    from a2a_mesh.file_transfer import FileTransfer

    config_file = os.path.expanduser("~/.hermes/mesh_config.yaml")
    config = MeshConfig.from_yaml(config_file) if os.path.exists(config_file) else MeshConfig()

    if not os.path.exists(filepath):
        print(f"❌ File not found: {filepath}")
        return

    file_size = os.path.getsize(filepath)
    ft = FileTransfer(config)
    result = asyncio.run(
        ft.send_file(filepath, recipient, description=description)
    )

    if result.success:
        print(f"✅ File sent via {result.method}")
        print(f"   Filename: {os.path.basename(filepath)}")
        print(f"   Size: {file_size:,} bytes")
        print(f"   SHA-256: {result.sha256[:16]}...")
        print(f"   Message ID: {result.message_id[:8]}")
        if result.remote_path:
            print(f"   MinIO path: {result.remote_path}")
    else:
        print(f"❌ File transfer failed: {result.error}")


def cmd_receive_file(message_id: str = "", output_dir: str = ""):
    """Receive a file from a mesh message."""
    import asyncio
    from a2a_mesh.file_transfer import FileTransfer

    config_file = os.path.expanduser("~/.hermes/mesh_config.yaml")
    config = MeshConfig.from_yaml(config_file) if os.path.exists(config_file) else MeshConfig()

    ft = FileTransfer(config)

    if message_id:
        # Fetch message from PG and receive file
        import psycopg2
        conn = psycopg2.connect(
            host=config.pg.host, port=config.pg.port,
            dbname=config.pg.dbname, user=config.pg.user,
            password=config.pg.password,
        )
        cur = conn.cursor()
        cur.execute("""
            SELECT id, sender, recipient, msg_type, payload
            FROM mesh.mesh_messages
            WHERE id = %s AND msg_type = 'file_share'
        """, (message_id,))
        row = cur.fetchone()
        cur.close()
        conn.close()

        if not row:
            print(f"❌ No file_share message found with ID: {message_id}")
            return

        msg = A2AMessage.create(
            sender=row[1],
            recipient=row[2],
            msg_type=row[3],
            payload=row[4] if isinstance(row[4], dict) else {},
        )

        saved_path = asyncio.run(
            ft.receive_file(msg, output_dir=output_dir or None)
        )
        if saved_path:
            print(f"✅ File received: {saved_path}")
        else:
            print(f"❌ File receive failed")
    else:
        # List recent file_share messages
        import psycopg2
        conn = psycopg2.connect(
            host=config.pg.host, port=config.pg.port,
            dbname=config.pg.dbname, user=config.pg.user,
            password=config.pg.password,
        )
        cur = conn.cursor()
        cur.execute("""
            SELECT id, sender, recipient, payload->>'filename' as filename,
                   payload->>'transfer_method' as method,
                   payload->>'file_size' as file_size,
                   created_at
            FROM mesh.mesh_messages
            WHERE msg_type = 'file_share'
            ORDER BY created_at DESC LIMIT 10
        """)
        rows = cur.fetchall()
        cur.close()
        conn.close()

        if not rows:
            print("No file share messages found.")
            return

        print("Recent file transfers:")
        print("-" * 70)
        for row in rows:
            msg_id, sender, recipient, filename, method, fsize, created = row
            size_str = f"{int(fsize):,} bytes" if fsize else "unknown"
            print(f"  {msg_id[:8]}  {sender} → {recipient}")
            print(f"    {filename} ({size_str}) via {method}")
            print(f"    {created}")
            print()


def cmd_list_files():
    """List shared files in MinIO."""
    from a2a_mesh.file_transfer import FileTransfer

    config_file = os.path.expanduser("~/.hermes/mesh_config.yaml")
    config = MeshConfig.from_yaml(config_file) if os.path.exists(config_file) else MeshConfig()

    ft = FileTransfer(config)
    files = ft.list_shared_files(prefix=config.node_name + "/")

    if not files:
        # Try without prefix
        files = ft.list_shared_files()

    if not files:
        print("No shared files found.")
        return

    print("Shared files in MinIO:")
    print("-" * 60)
    for f in files:
        print(f"  {f['path']}  ({f['size']})  {f['date']}")


def cmd_keygen():
    """Generate a new Ed25519 keypair for mesh signing."""
    private_hex, public_hex = MeshEncryption.generate_keypair()
    print(f"Private key (signing_key): {private_hex}")
    print(f"Public key (verify_key):   {public_hex}")
    print()
    print("Add to mesh_config.yaml:")
    print(f"  security:")
    print(f"    signing_key: {private_hex}")


def cmd_health(port: int = 8650, output_json: bool = False):
    """Check mesh node health via HTTP endpoint."""
    import urllib.request
    import json as json_mod

    try:
        url = f"http://localhost:{port}/health"
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json_mod.loads(resp.read().decode())

        if output_json:
            print(json_mod.dumps(data, indent=2))
        else:
            status = data.get("status", "unknown")
            node = data.get("node", "?")
            role = data.get("role", "?")
            addr = data.get("address", "?")
            uptime = data.get("uptime_seconds", 0)
            transports = data.get("transports", {})
            ack = data.get("ack", {})
            oq = data.get("offline_queue", {})

            emoji = "🟢" if status == "running" else "🔴"
            print(f"{emoji} Mesh Node: {node}")
            print(f"   Role: {role}")
            print(f"   Address: {addr}")
            print(f"   Uptime: {uptime:.0f}s")
            print(f"   Transports: PG={'✅' if transports.get('pg') else '❌'} "
                  f"P2P={'✅' if transports.get('p2p') else '❌'} "
                  f"HTTP={'✅' if transports.get('http') else '❌'} "
                  f"BLE={'✅' if transports.get('ble') else '❌'}")
            print(f"   ACK: {ack.get('pending', 0)} pending, {ack.get('acknowledged', 0)} acked, {ack.get('failed', 0)} failed")
            print(f"   Offline Queue: {oq.get('queued', 0)} queued, {oq.get('delivered', 0)} delivered")
            print(f"   Messages: {data.get('messages_sent', 0)} sent, {data.get('messages_received', 0)} received")
        return 0

    except urllib.error.URLError:
        print(f"🔴 Mesh node not responding on port {port}")
        print("   Is the daemon running? Start with: python3 -m a2a_mesh.node")
        return 1
    except Exception as e:
        print(f"🔴 Health check failed: {e}")
        return 1


def cmd_test():
    """Run self-test for mesh components."""
    print("🧪 A2A Mesh Self-Test\n")

    # Test 1: Message serialization
    print("1. Message serialization... ", end="")
    msg = A2AMessage.create(
        sender="test-sender",
        recipient="test-recipient",
        msg_type="test",
        payload={"hello": "world"},
    )
    json_str = msg.to_json()
    msg2 = A2AMessage.from_json(json_str)
    assert msg.id == msg2.id
    assert msg.sender == msg2.sender
    print("✅")

    # Test 2: Message bytes (msgpack)
    print("2. Message bytes (msgpack)... ", end="")
    data = msg.to_bytes()
    msg3 = A2AMessage.from_bytes(data)
    assert msg.id == msg3.id
    print("✅")

    # Test 3: Dedup cache
    print("3. Dedup cache... ", end="")
    from a2a_mesh.core.dedup import DedupCache
    cache = DedupCache(max_size=100, ttl_seconds=60)
    assert not cache.check_and_add("msg-1")  # First time: not duplicate
    assert cache.check_and_add("msg-1")       # Second time: duplicate
    assert not cache.check_and_add("msg-2")   # New msg: not duplicate
    print("✅")

    # Test 4: Encryption
    print("4. Encryption (Ed25519)... ", end="")
    try:
        enc = MeshEncryption()
        content = "test message content"
        sig = enc.sign_message(content)
        assert enc.verify_message(content, sig, enc.verify_key_hex)
        assert not enc.verify_message("wrong content", sig, enc.verify_key_hex)
        print("✅")
    except ImportError:
        print("⚠️  pynacl not installed")

    # Test 5: Router
    print("5. Router... ", end="")
    from a2a_mesh.core.router import MeshRouter
    router = MeshRouter("test-node")
    assert router.node_name == "test-node"
    stats = router.get_stats()
    assert "sent" in stats
    print("✅")

    # Test 6: Config
    print("6. Config... ", end="")
    config = MeshConfig()
    assert config.node_name == "nova"
    assert config.pg.host == "192.168.1.30"
    assert config.p2p.listen_port == 8645
    print("✅")

    # Test 7: Message routing
    print("7. Message routing... ", end="")
    broadcast = A2AMessage.create(
        sender="nova",
        recipient="broadcast",
        msg_type="heartbeat",
        payload={"status": "ok"},
    )
    assert broadcast.is_broadcast()
    assert not broadcast.is_expired()

    directed = A2AMessage.create(
        sender="nova",
        recipient="morzsa",
        msg_type="directive",
        payload={"action": "ping"},
    )
    assert not directed.is_broadcast()
    fwd = directed.decrement_ttl()
    assert fwd.ttl == 9
    assert fwd.hop_count == 1
    print("✅")

    # Test 8: Zigbee topology
    print("8. Zigbee topology... ", end="")
    from a2a_mesh.core.topology import NodeRole, MeshAddress, AddressManager
    from a2a_mesh.core.tree_router import TreeRouter

    mgr = AddressManager(max_children=20, max_routers=6, max_depth=5)
    coord = mgr.assign_address("nova", NodeRole.COORDINATOR)
    router1 = mgr.assign_address("morzsa", NodeRole.ROUTER, parent_short=0)
    ed1 = mgr.assign_address("worker-1", NodeRole.END_DEVICE, parent_short=1)

    assert coord.is_coordinator
    assert router1.is_router
    assert ed1.is_end_device
    assert mgr.compute_cskip(0) > 0
    assert mgr.compute_cskip(5) == 0
    print("✅")

    # Test 9: TreeRouter
    print("9. TreeRouter... ", end="")
    tree_router = TreeRouter(coord, mgr)
    local_msg = A2AMessage.create(sender="nova", recipient="nova", msg_type="test", payload={})
    hops = tree_router.route_message(local_msg, routing_mode="tree")
    assert hops == []  # Local delivery
    print("✅")

    # Test 10: Sleepy end device buffer
    print("10. Sleepy end device buffer... ", end="")
    router_node = TreeRouter(
        MeshAddress(short=1, extended="morzsa", role=NodeRole.ROUTER, depth=1, parent_short=0),
        mgr
    )
    buffered_msg = A2AMessage.create(sender="nova", recipient="worker-1", msg_type="task", payload={"x": 1})
    router_node.buffer_message(ed1.short, buffered_msg)
    retrieved = router_node.get_buffered_messages(ed1.short)
    assert len(retrieved) == 1
    print("✅")

    # Test 11: Coordinator Election & Failover
    print("11. Coordinator election... ", end="")
    from a2a_mesh.core.election import CoordinatorElection, ElectionConfig, CoordinatorState

    el_config = ElectionConfig(heartbeat_interval=300, suspect_threshold=600, down_threshold=900)
    el = CoordinatorElection("morzsa", 0x0001, "router", el_config)

    # Register coordinator (nova = 0x0000)
    el.register_coordinator("nova", 0x0000, is_original=True)
    assert el.coordinator.state == CoordinatorState.ACTIVE

    # Check health — coordinator just registered, should be active
    state = el.check_coordinator_health([])
    assert state == CoordinatorState.ACTIVE

    # Simulate coordinator down (set heartbeat to 20 min ago)
    el.coordinator.last_heartbeat = __import__("time").time() - 1200
    state = el.check_coordinator_health([("router1", 0x0001), ("router2", 0x0002)])
    assert state == CoordinatorState.DOWN

    # Morzsa (0x0001) is senior-most router — should initiate election
    assert el.should_initiate_election([("router2", 0x0002)]) == True
    claim = el.initiate_election()
    assert claim["node_name"] == "morzsa"
    assert claim["short_addr"] == 0x0001
    assert claim["claim_reason"] == "coordinator_down"
    assert el.is_acting_coordinator == True

    # Handle coordinator return
    result = el.handle_coordinator_return("nova", 0x0000)
    assert result["type"] == "coordinator_return"
    assert el.is_acting_coordinator == False
    assert el.coordinator.is_original == True

    # Test election claim handling
    el2 = CoordinatorElection("router2", 0x0002, "router", el_config)
    el2.register_coordinator("nova", 0x0000)
    # router2 should accept claim from morzsa (0x0001 < 0x0002 = senior)
    accepted = el2.handle_election_claim({"node_name": "morzsa", "short_addr": 0x0001})
    assert accepted == True
    assert el2.coordinator.node_name == "morzsa"
    assert el2.coordinator.is_original == False

    print("✅")

    # Test 12: File transfer classification
    print("12. File transfer classification... ", end="")
    from a2a_mesh.file_transfer import FileTransfer, PG_THRESHOLD, MINIO_THRESHOLD
    ft = FileTransfer(config)
    assert ft._classify_size(500) == "pg_base64", f"Expected pg_base64, got {ft._classify_size(500)}"
    assert ft._classify_size(PG_THRESHOLD - 1) == "pg_base64"
    assert ft._classify_size(PG_THRESHOLD + 1) == "minio_s3"
    assert ft._classify_size(MINIO_THRESHOLD - 1) == "minio_s3"
    assert ft._classify_size(MINIO_THRESHOLD + 1) == "scp"
    print("✅")

    # Test 13: File transfer SHA-256
    print("13. File SHA-256 hash... ", end="")
    import tempfile
    with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
        f.write("test file content for mesh transfer")
        tmp_path = f.name
    sha = ft._sha256_file(tmp_path)
    assert len(sha) == 64, f"SHA-256 length wrong: {len(sha)}"
    os.unlink(tmp_path)
    print("✅")

    print(f"\n✅ All tests passed! (13/13)")

    # Test 14: ACK tracking
    print("14. ACK manager... ", end="")
    from a2a_mesh.core.ack import AckManager, AckType, AckStatus, PRIORITY_TIMEOUTS
    ack = AckManager(node_name="test_nova")
    msg = A2AMessage.create(sender="nova", recipient="morzsa", msg_type="directive", payload={"test": True})
    ack.track(msg)
    stats = ack.get_stats()
    assert stats["pending"] == 1, f"Expected 1 pending, got {stats['pending']}"
    assert PRIORITY_TIMEOUTS[5] == 45, f"P5 timeout should be 45s, got {PRIORITY_TIMEOUTS[5]}"
    ack_msg = ack.create_ack(msg, AckType.DELIVERED)
    assert ack_msg.type == "ack", f"ACK type should be 'ack', got {ack_msg.type}"
    assert ack_msg.payload["ack_for"] == msg.id
    result = ack.process_ack(ack_msg)
    assert result is not None, "ACK should be processed"
    assert result.status == AckStatus.ACKNOWLEDGED
    stats = ack.get_stats()
    assert stats["pending"] == 0, f"Expected 0 pending after ACK, got {stats['pending']}"
    print("✅")

    # Test 15: Message compression
    print("15. Message compression... ", end="")
    import zlib
    big_payload = {"data": "x" * 10000}  # > 4KB threshold
    big_msg = A2AMessage.create(sender="nova", recipient="morzsa", msg_type="directive", payload=big_payload)
    compressed = big_msg.compress_payload()
    assert compressed.payload.get("__compressed__") == True, "Should be compressed"
    assert compressed.payload["original_size"] > compressed.payload["compressed_size"]
    decompressed = compressed.decompress_payload()
    assert decompressed.payload["data"] == "x" * 10000, "Decompressed data should match"
    # Small payload should not be compressed
    small_msg = A2AMessage.create(sender="nova", recipient="morzsa", msg_type="directive", payload={"x": 1})
    assert small_msg.compress_payload().payload.get("__compressed__") is None, "Small payload should not be compressed"
    print("✅")

    # Test 16: Message size validation
    print("16. Message size validation... ", end="")
    from a2a_mesh.core.message import MAX_MESSAGE_SIZE
    ok_msg = A2AMessage.create(sender="nova", recipient="morzsa", msg_type="directive", payload={"test": True})
    valid, size = ok_msg.validate_size()
    assert valid == True, f"Small message should be valid, size={size}"
    assert size < MAX_MESSAGE_SIZE
    print("✅")

    # Test 17: Node authentication
    print("17. Node authentication... ", end="")
    from a2a_mesh.core.auth import NodeAuthenticator, AuthConfig, JoinRequest, AuthMode
    auth_config = AuthConfig(mode="open")
    auth = NodeAuthenticator(auth_config)
    join = JoinRequest(
        node_name="test_node",
        node_role="router",
        public_key="test_key_abc123",
        timestamp=time.time(),
        nonce="random_nonce_123",
    )
    authorized, reason = auth.authenticate_join(join)
    assert authorized == True, f"Open mode should authorize: {reason}"
    assert auth.is_authenticated("test_node") == True
    # Whitelist mode
    wl_config = AuthConfig(mode="whitelist", whitelist={"morzsa", "nova"})
    wl_auth = NodeAuthenticator(wl_config)
    join2 = JoinRequest(node_name="morzsa", node_role="router", public_key="key2", timestamp=time.time(), nonce="n2")
    authorized2, reason2 = wl_auth.authenticate_join(join2)
    assert authorized2 == True, f"Whitelisted node should be authorized: {reason2}"
    join3 = JoinRequest(node_name="unknown", node_role="router", public_key="key3", timestamp=time.time(), nonce="n3")
    authorized3, _ = wl_auth.authenticate_join(join3)
    assert authorized3 == False, "Unknown node should be rejected in whitelist mode"
    print("✅")

    print(f"\n✅ All tests passed! (17/17)")


def cmd_status():
    """Show mesh node status with topology info."""
    config_file = os.path.expanduser("~/.hermes/mesh_config.yaml")
    if os.path.exists(config_file):
        config = MeshConfig.from_yaml(config_file)
    else:
        config = MeshConfig()

    topo = config.topology
    print("A2A Mesh Status")
    print("=" * 50)
    print(f"  Node name:    {config.node_name}")
    print(f"  Node role:     {topo.node_role.upper()}")
    print(f"  Routing mode: {topo.routing_mode}")
    print(f"  PG host:      {config.pg.host}:{config.pg.port}")
    print(f"  P2P port:     {config.p2p.listen_port}")
    print(f"  HTTP url:     {config.http.url}")
    print(f"  Discovery:    {'mDNS + static' if config.discovery.mdns_enabled else 'static only'}")
    print()
    print("Topology")
    print("-" * 50)
    print(f"  Max children: {topo.max_children} (Cm)")
    print(f"  Max routers:   {topo.max_routers} (Rm)")
    print(f"  Max depth:     {topo.max_depth} (Lm)")
    print(f"  Trust center:  {'ON' if topo.trust_center_enabled else 'OFF'}")
    print(f"  Sleepy ED:     {'ON' if topo.enable_sleepy_end_devices else 'OFF'}")
    print(f"  Route cache:   {'ON' if topo.enable_route_cache else 'OFF'} (TTL: {topo.route_cache_ttl}s)")
    print()

    # Show address table from PG if available
    try:
        import psycopg2
        conn = psycopg2.connect(
            host=config.pg.host, port=config.pg.port,
            dbname=config.pg.dbname, user=config.pg.user,
            password=config.pg.password,
        )
        cur = conn.cursor()
        # Check if mesh_nodes table exists
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = 'mesh' AND table_name = 'mesh_nodes'
            )
        """)
        if cur.fetchone()[0]:
            cur.execute("SELECT node_name, role, short_addr, parent_addr, joined_at FROM mesh.mesh_nodes ORDER BY short_addr")
            rows = cur.fetchall()
            if rows:
                print(f"Known nodes ({len(rows)}):")
                for row in rows:
                    name, role, short_addr, parent_addr, joined_at = row
                    parent_str = f"parent=0x{parent_addr:04X}" if parent_addr is not None else "root"
                    print(f"  0x{short_addr:04X}  {role:<12}  {name:<15}  {parent_str}")
            else:
                print("No nodes registered yet")
        else:
            print("Mesh tables not initialized yet — run 'a2a-mesh init' first")
        conn.close()
    except Exception as e:
        print(f"  PG connection: {e}")


def cmd_init():
    """Initialize mesh tables in PG database."""
    config_file = os.path.expanduser("~/.hermes/mesh_config.yaml")
    if os.path.exists(config_file):
        config = MeshConfig.from_yaml(config_file)
    else:
        config = MeshConfig()

    import psycopg2
    conn = psycopg2.connect(
        host=config.pg.host, port=config.pg.port,
        dbname=config.pg.dbname, user=config.pg.user,
        password=config.pg.password,
    )
    cur = conn.cursor()

    # Create mesh_nodes table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS mesh.mesh_nodes (
            node_name VARCHAR(64) PRIMARY KEY,
            role VARCHAR(20) NOT NULL DEFAULT 'end_device',
            short_addr INTEGER NOT NULL,
            extended_uuid VARCHAR(128) NOT NULL,
            parent_addr INTEGER,
            depth INTEGER NOT NULL DEFAULT 0,
            public_key TEXT,
            transport_info JSONB,
            status VARCHAR(20) NOT NULL DEFAULT 'active',
            joined_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            last_heartbeat TIMESTAMPTZ,
            UNIQUE(short_addr),
            UNIQUE(extended_uuid)
        )
    """)

    # Create mesh_messages table for message tracking
    cur.execute("""
        CREATE TABLE IF NOT EXISTS mesh.mesh_messages (
            id VARCHAR(48) PRIMARY KEY,
            sender VARCHAR(64) NOT NULL,
            recipient VARCHAR(64),
            msg_type VARCHAR(40) NOT NULL,
            priority INTEGER NOT NULL DEFAULT 5,
            payload JSONB,
            routing_mode VARCHAR(10) DEFAULT 'hybrid',
            src_addr INTEGER,
            dst_addr INTEGER,
            route_path INTEGER[],
            status VARCHAR(20) DEFAULT 'sent',
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            delivered_at TIMESTAMPTZ
        )
    """)

    # Create indexes
    cur.execute("CREATE INDEX IF NOT EXISTS idx_mesh_nodes_role ON mesh.mesh_nodes(role)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_mesh_nodes_status ON mesh.mesh_nodes(status)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_mesh_messages_sender ON mesh.mesh_messages(sender)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_mesh_messages_recipient ON mesh.mesh_messages(recipient)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_mesh_messages_created ON mesh.mesh_messages(created_at)")

    # Add NOTIFY trigger for mesh_channel
    cur.execute("""
        CREATE OR REPLACE FUNCTION mesh.notify_mesh_channel() RETURNS trigger AS $$
        BEGIN
            PERFORM pg_notify('mesh_channel', json_build_object(
                'id', NEW.id,
                'sender', NEW.sender,
                'recipient', NEW.recipient,
                'msg_type', NEW.msg_type,
                'priority', NEW.priority
            )::text);
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql
    """)

    cur.execute("""
        DROP TRIGGER IF EXISTS mesh_message_notify ON mesh.mesh_messages;
        CREATE TRIGGER mesh_message_notify
            AFTER INSERT ON mesh.mesh_messages
            FOR EACH ROW EXECUTE FUNCTION mesh.notify_mesh_channel()
    """)

    conn.commit()

    # Register coordinator in mesh.mesh_nodes
    from a2a_mesh.core.topology import NodeRole, AddressManager
    mgr = AddressManager(
        max_children=config.topology.max_children,
        max_routers=config.topology.max_routers,
        max_depth=config.topology.max_depth,
    )
    coord = mgr.assign_address(config.node_name, NodeRole.COORDINATOR)

    import uuid
    ext_uuid = str(uuid.uuid4())
    cur.execute("""
        INSERT INTO mesh.mesh_nodes (node_name, role, short_addr, extended_uuid, parent_addr, depth, status)
        VALUES (%s, %s, %s, %s, NULL, 0, 'active')
        ON CONFLICT (node_name) DO UPDATE SET
            role = EXCLUDED.role,
            short_addr = EXCLUDED.short_addr,
            status = 'active',
            last_heartbeat = NOW()
    """, (config.node_name, 'coordinator', coord.short, ext_uuid))
    conn.commit()
    cur.close()
    conn.close()

    print("✅ Mesh tables initialized:")
    print("   - mesh.mesh_nodes (node registry)")
    print("   - mesh.mesh_messages (message tracking)")
    print("   - NOTIFY trigger on mesh_channel")
    print(f"\n📍 Coordinator registered: {config.node_name} = 0x{coord.short:04X}")


def cmd_topology():
    """Show mesh topology tree."""
    config_file = os.path.expanduser("~/.hermes/mesh_config.yaml")
    if os.path.exists(config_file):
        config = MeshConfig.from_yaml(config_file)
    else:
        config = MeshConfig()

    import psycopg2
    try:
        conn = psycopg2.connect(
            host=config.pg.host, port=config.pg.port,
            dbname=config.pg.dbname, user=config.pg.user,
            password=config.pg.password,
        )
        cur = conn.cursor()
        cur.execute("SELECT node_name, role, short_addr, depth, parent_addr FROM mesh.mesh_nodes ORDER BY short_addr")
        rows = cur.fetchall()
        conn.close()
    except Exception as e:
        print(f"PG error: {e}")
        return

    if not rows:
        print("No nodes in topology — run 'a2a-mesh init' first")
        return

    # Build tree
    from a2a_mesh.core.topology import NodeRole
    nodes_by_parent = {}
    root = None
    for name, role, short_addr, depth, parent_addr in rows:
        parent_key = parent_addr if parent_addr is not None else None
        if parent_key not in nodes_by_parent:
            nodes_by_parent[parent_key] = []
        role_icon = {"coordinator": "⭐", "router": "📡", "end_device": "📱"}.get(role, "❓")
        nodes_by_parent[parent_key].append((short_addr, name, role, role_icon))
        if role == "coordinator":
            root = (short_addr, name, role, role_icon)

    if root:
        print("Mesh Topology")
        print("=" * 50)
        _print_tree(root, nodes_by_parent, indent=0)
    else:
        print("No coordinator found!")

def _print_tree(node, nodes_by_parent, indent=0):
    """Recursively print topology tree."""
    short_addr, name, role, icon = node
    prefix = "  " * indent + ("└─ " if indent > 0 else "")
    print(f"{prefix}{icon} {name} (0x{short_addr:04X}, {role})")
    children = nodes_by_parent.get(short_addr, [])
    for child in children:
        _print_tree(child, nodes_by_parent, indent + 1)


def cmd_join(role: str, name: str, parent: str = ""):
    """Join the mesh network with a specified role."""
    config_file = os.path.expanduser("~/.hermes/mesh_config.yaml")
    if os.path.exists(config_file):
        config = MeshConfig.from_yaml(config_file)
    else:
        config = MeshConfig()

    # Use provided name or default from config
    node_name = name if name else config.node_name

    # Update role in config
    config.topology.node_role = role

    import psycopg2
    from a2a_mesh.core.topology import NodeRole, AddressManager

    conn = psycopg2.connect(
        host=config.pg.host, port=config.pg.port,
        dbname=config.pg.dbname, user=config.pg.user,
        password=config.pg.password,
    )
    cur = conn.cursor()

    # Get current address table
    cur.execute("SELECT node_name, role, short_addr, extended_uuid FROM mesh.mesh_nodes")
    rows = cur.fetchall()

    mgr = AddressManager(
        max_children=config.topology.max_children,
        max_routers=config.topology.max_routers,
        max_depth=config.topology.max_depth,
    )

    # Rebuild address table
    for n, r, short, ext in rows:
        node_role = NodeRole(r)
        addr = mgr.assign_address(n, node_role, parent_short=0)

    # Determine parent
    parent_short = 0  # Default: coordinator
    if parent:
        cur.execute("SELECT short_addr FROM mesh.mesh_nodes WHERE node_name = %s", (parent,))
        row = cur.fetchone()
        if row:
            parent_short = row[0]

    # Assign address for this node
    node_role = NodeRole(role)
    addr = mgr.assign_address(node_name, node_role, parent_short=parent_short)

    # Register in PG
    import uuid
    ext_uuid = str(uuid.uuid4())
    cur.execute("""
        INSERT INTO mesh.mesh_nodes (node_name, role, short_addr, extended_uuid, parent_addr, depth, status)
        VALUES (%s, %s, %s, %s, %s, %s, 'active')
        ON CONFLICT (node_name) DO UPDATE SET
            role = EXCLUDED.role,
            short_addr = EXCLUDED.short_addr,
            parent_addr = EXCLUDED.parent_addr,
            depth = EXCLUDED.depth,
            status = 'active',
            last_heartbeat = NOW()
    """, (config.node_name, role, addr.short, ext_uuid, parent_short, addr.depth))

    conn.commit()
    cur.close()
    conn.close()

    icon = {"coordinator": "⭐", "router": "📡", "end_device": "📱"}.get(role, "❓")
    print(f"{icon} Joined mesh as {role.upper()}")
    print(f"   Address: 0x{addr.short:04X}")
    print(f"   Depth: {addr.depth}")
    print(f"   Parent: 0x{parent_short:04X}")


async def cmd_send(recipient: str, msg_type: str, payload_str: str, priority: int = 5):
    """Send a message via mesh."""
    config_path = os.path.expanduser("~/.hermes/mesh_config.yaml")
    if os.path.exists(config_path):
        config = MeshConfig.from_yaml(config_path)
    else:
        config = MeshConfig()
    node = MeshNode(config)

    try:
        if await node.start():
            payload = json.loads(payload_str) if payload_str else {}
            result = await node.send_direct(recipient, msg_type, payload, priority)
            print(f"Send result: {result}")
        else:
            print("Failed to start node")
    finally:
        await node.stop()


async def cmd_broadcast(msg_type: str, payload_str: str, priority: int = 5):
    """Broadcast a message via mesh."""
    config_path = os.path.expanduser("~/.hermes/mesh_config.yaml")
    if os.path.exists(config_path):
        config = MeshConfig.from_yaml(config_path)
    else:
        config = MeshConfig()
    node = MeshNode(config)

    try:
        if await node.start():
            payload = json.loads(payload_str) if payload_str else {}
            result = await node.broadcast(msg_type, payload, priority)
            print(f"Broadcast result: {result}")
        else:
            print("Failed to start node")
    finally:
        await node.stop()


async def cmd_run(name: str, port: int, config_path: str, tls: bool = False, tls_cert: str = "", tls_key: str = "", tls_ca: str = "", tls_verify: bool = False):
    """Run the mesh node interactively."""
    config_file = os.path.expanduser(config_path)
    if os.path.exists(config_file):
        config = MeshConfig.from_yaml(config_file)
    else:
        config = MeshConfig()

    config.node_name = name
    # --port is the dashboard/health port, P2P uses its own port (default 8645)
    config.health_port = port
    # Keep P2P on separate port unless explicitly configured
    if config.p2p.listen_port == 8645:
        # Default — keep it, don't override with dashboard port
        pass

    # Apply TLS CLI overrides
    if tls:
        config.p2p.tls_enabled = True
        if tls_cert:
            config.p2p.tls_cert = os.path.expanduser(tls_cert)
        if tls_key:
            config.p2p.tls_key = os.path.expanduser(tls_key)
        if tls_ca:
            config.p2p.tls_ca = os.path.expanduser(tls_ca)
        if tls_verify:
            config.p2p.tls_verify_peer = True

    node = MeshNode(config)

    # Auto-steer: classify incoming mesh messages by priority
    from core.auto_steer import AutoSteerProcessor
    steer = AutoSteerProcessor(name, config)

    def handle_message(msg: A2AMessage):
        level = steer.classify_message(msg)
        print(f"📨 {msg.sender} → {msg.recipient}: {msg.type} (P{msg.priority}) [{level}]")
        if msg.payload:
            print(f"   {json.dumps(msg.payload, indent=2)[:200]}")

    node.add_handler(handle_message)

    try:
        if await node.start():
            status = node.get_status()
            print(f"🟢 Mesh node '{name}' started")
            for tname, tstatus in status.get("transports", {}).items():
                avail = tstatus.get("available", False) if isinstance(tstatus, dict) else str(tstatus)
                print(f"   {tname}: {'✅' if avail else '❌'}")
            print("Press Ctrl+C to stop")
            while True:
                await asyncio.sleep(1)
        else:
            print("🔴 Failed to start mesh node")
            sys.exit(1)
    except KeyboardInterrupt:
        pass
    finally:
        await node.stop()


def main():
    """CLI entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="A2A Mesh CLI")
    subparsers = parser.add_subparsers(dest="command", help="Commands")

    # start
    start_parser = subparsers.add_parser("start", help="Start mesh node")
    start_parser.add_argument("--name", "-n", default=os.environ.get("A2A_NODE_NAME", "nova"))
    start_parser.add_argument("--port", "-p", type=int, default=8645)
    start_parser.add_argument("--config", "-c", default="~/.hermes/mesh_config.yaml")
    start_parser.add_argument("--tls", action="store_true", default=False, help="Enable TLS for P2P transport")
    start_parser.add_argument("--tls-cert", default="", help="Path to TLS certificate (PEM)")
    start_parser.add_argument("--tls-key", default="", help="Path to TLS private key (PEM)")
    start_parser.add_argument("--tls-ca", default="", help="Path to CA certificate for peer verification")
    start_parser.add_argument("--tls-verify", action="store_true", default=False, help="Verify peer TLS certificates")

    # send
    send_parser = subparsers.add_parser("send", help="Send a message")
    send_parser.add_argument("--to", "-t", required=True, help="Recipient")
    send_parser.add_argument("--type", "-T", default="directive", help="Message type")
    send_parser.add_argument("--payload", "-p", default="{}", help="JSON payload")
    send_parser.add_argument("--priority", "-P", type=int, default=5)

    # broadcast
    bc_parser = subparsers.add_parser("broadcast", help="Broadcast a message")
    bc_parser.add_argument("--type", "-T", default="broadcast", help="Message type")
    bc_parser.add_argument("--payload", "-p", default="{}", help="JSON payload")
    bc_parser.add_argument("--priority", "-P", type=int, default=5)

    # status
    subparsers.add_parser("status", help="Show mesh status and topology")

    # init
    subparsers.add_parser("init", help="Initialize mesh tables in PG")

    # topology
    subparsers.add_parser("topology", help="Show mesh topology tree")

    # join
    join_parser = subparsers.add_parser("join", help="Join mesh with role")
    join_parser.add_argument("--role", "-r", default="router",
                             choices=["coordinator", "router", "end_device"],
                             help="Node role")
    join_parser.add_argument("--name", "-n", default="", help="Node name (default: config node_name)")
    join_parser.add_argument("--parent", "-p", default="", help="Parent node name")

    # elect
    elect_parser = subparsers.add_parser("elect", help="Coordinator election status or trigger")
    elect_parser.add_argument("--trigger", "-t", action="store_true",
                             help="Trigger coordinator election")

    # leave
    leave_parser = subparsers.add_parser("leave", help="Leave mesh network gracefully")
    leave_parser.add_argument("--name", "-n", default="", help="Node name (default: config node_name)")

    # send-file
    sf_parser = subparsers.add_parser("send-file", help="Send a file to a mesh node")
    sf_parser.add_argument("filepath", help="Path to file to send")
    sf_parser.add_argument("--to", "-t", required=True, help="Recipient node name")
    sf_parser.add_argument("--desc", "-d", default="", help="File description")

    # receive-file
    rf_parser = subparsers.add_parser("receive-file", help="Receive a file from a mesh message")
    rf_parser.add_argument("--id", "-i", default="", help="Message ID to receive")
    rf_parser.add_argument("--output", "-o", default="", help="Output directory")

    # list-files
    subparsers.add_parser("list-files", help="List shared files in MinIO")

    # discover
    subparsers.add_parser("discover", help="Discover mesh nodes")

    # keygen
    subparsers.add_parser("keygen", help="Generate signing keypair")

    # health
    health_parser = subparsers.add_parser("health", help="Check mesh node health")
    health_parser.add_argument("--port", type=int, default=8650, help="Health endpoint port")
    health_parser.add_argument("--json", action="store_true", help="Output raw JSON")

    # test
    subparsers.add_parser("test", help="Run self-tests")

    args = parser.parse_args()

    if args.command == "start":
        asyncio.run(cmd_run(args.name, args.port, args.config,
                            tls=args.tls, tls_cert=args.tls_cert,
                            tls_key=args.tls_key, tls_ca=args.tls_ca,
                            tls_verify=args.tls_verify))
    elif args.command == "send":
        asyncio.run(cmd_send(args.to, args.type, args.payload, args.priority))
    elif args.command == "broadcast":
        asyncio.run(cmd_broadcast(args.type, args.payload, args.priority))
    elif args.command == "status":
        cmd_status()
    elif args.command == "init":
        cmd_init()
    elif args.command == "topology":
        cmd_topology()
    elif args.command == "join":
        cmd_join(args.role, args.name, args.parent)
    elif args.command == "elect":
        cmd_elect(trigger=args.trigger)
    elif args.command == "leave":
        cmd_leave(args.name)
    elif args.command == "send-file":
        cmd_send_file(args.filepath, args.to, args.desc)
    elif args.command == "receive-file":
        cmd_receive_file(args.id, args.output)
    elif args.command == "list-files":
        cmd_list_files()
    elif args.command == "keygen":
        cmd_keygen()
    elif args.command == "health":
        cmd_health(port=args.port, output_json=args.json)
    elif args.command == "test":
        cmd_test()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()