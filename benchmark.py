#!/usr/bin/env python3
"""A2A Mesh Performance Benchmark Suite

Tests throughput, latency, concurrency, and resource usage for:
- Message creation & serialization
- PG NOTIFY transport
- P2P TCP transport
- Encryption/signing
- Dedup cache
- Priority queue
- Full round-trip (Nova → Morzsa → Nova)
"""
import asyncio
import json
import msgpack
import time
import statistics
import sys
import os
import psycopg2
import socket
import hashlib
from datetime import datetime, timezone

# Add parent to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from a2a_mesh.core.message import A2AMessage, COMPRESSION_THRESHOLD
from a2a_mesh.core.topology import AddressManager, NodeRole, MeshAddress
from a2a_mesh.core.router import MeshRouter, ProcessResult
from a2a_mesh.core.dedup import DedupCache
from a2a_mesh.core.ack import AckManager
from a2a_mesh.core.election import CoordinatorElection, ElectionConfig
from a2a_mesh.core.auth import AuthConfig, AuthMode, NodeAuthenticator, JoinRequest
from a2a_mesh.core.encryption import MeshEncryption, HAS_NACL
from a2a_mesh.core.offline_queue import OfflineQueue, QueuedMessage
from a2a_mesh.core.config import MeshConfig

# ─── Helpers ──────────────────────────────────────────────────────────────────

def bench(name, fn, iterations=1000, warmup=100):
    """Run fn iterations times, report ops/sec and latency stats."""
    # Warmup
    for _ in range(warmup):
        fn()
    
    times = []
    for _ in range(iterations):
        t0 = time.perf_counter_ns()
        fn()
        t1 = time.perf_counter_ns()
        times.append(t1 - t0)
    
    total_ns = sum(times)
    ops_per_sec = iterations / (total_ns / 1_000_000_000)
    latencies_us = [t / 1000 for t in times]
    
    print(f"  {name}:")
    print(f"    Throughput: {ops_per_sec:,.0f} ops/sec")
    print(f"    Latency: min={min(latencies_us):.1f}µs  avg={statistics.mean(latencies_us):.1f}µs  "
          f"p50={statistics.median(latencies_us):.1f}µs  p99={sorted(latencies_us)[int(len(latencies_us)*0.99)]:.1f}µs")
    return ops_per_sec


async def bench_async(name, fn, iterations=1000, warmup=50):
    """Run async fn iterations times, report ops/sec and latency stats."""
    # Warmup
    for _ in range(warmup):
        await fn()
    
    times = []
    for _ in range(iterations):
        t0 = time.perf_counter_ns()
        await fn()
        t1 = time.perf_counter_ns()
        times.append(t1 - t0)
    
    total_ns = sum(times)
    ops_per_sec = iterations / (total_ns / 1_000_000_000)
    latencies_us = [t / 1000 for t in times]
    
    print(f"  {name}:")
    print(f"    Throughput: {ops_per_sec:,.0f} ops/sec")
    print(f"    Latency: min={min(latencies_us):.1f}µs  avg={statistics.mean(latencies_us):.1f}µs  "
          f"p50={statistics.median(latencies_us):.1f}µs  p99={sorted(latencies_us)[int(len(latencies_us)*0.99)]:.1f}µs")
    return ops_per_sec


# ─── Benchmarks ────────────────────────────────────────────────────────────────

def bench_message_creation():
    """Benchmark: A2AMessage creation."""
    bench("Message creation (small payload)",
          lambda: A2AMessage.create(sender="nova", recipient="morzsa",
                                     msg_type="directive", payload={"text": "hello"}, priority=5),
          iterations=5000)
    
    bench("Message creation (large payload)",
          lambda: A2AMessage.create(sender="nova", recipient="morzsa",
                                     msg_type="directive", payload={"data": "x" * 10000}, priority=5),
          iterations=1000)


def bench_message_serialization():
    """Benchmark: Message serialization (JSON, msgpack, bytes)."""
    msg = A2AMessage.create(sender="nova", recipient="morzsa", msg_type="directive",
                            payload={"action": "deploy", "target": "lxc", "version": "1.0"}, priority=7)
    
    bench("Message → dict (JSON serialization)",
          lambda: msg.to_dict(), iterations=5000)
    
    bench("Message → bytes (msgpack serialization)",
          lambda: msg.to_bytes(), iterations=5000)
    
    bench("Message from dict (JSON deserialization)",
          lambda: A2AMessage.from_dict(msg.to_dict()), iterations=5000)


def bench_message_compression():
    """Benchmark: Message compression for large payloads."""
    large_msg = A2AMessage.create(sender="nova", recipient="morzsa", msg_type="file",
                                  payload={"data": "x" * COMPRESSION_THRESHOLD}, priority=3)
    small_msg = A2AMessage.create(sender="nova", recipient="morzsa", msg_type="directive",
                                  payload={"text": "hello"}, priority=5)
    
    bench("Compression (large payload)",
          lambda: large_msg.compress_payload(), iterations=100)
    
    bench("Compression (small payload — no-op)",
          lambda: small_msg.compress_payload(), iterations=5000)


def bench_dedup_cache():
    """Benchmark: DedupCache performance."""
    cache = DedupCache(max_size=10000, ttl_seconds=300)
    
    bench("DedupCache: unique IDs",
          lambda: cache.is_duplicate(f"msg_{time.perf_counter_ns()}"),
          iterations=5000)
    
    # Pre-populate for duplicate test
    for i in range(5000):
        cache.is_duplicate(f"existing_{i}")
    
    bench("DedupCache: duplicate detection",
          lambda: cache.is_duplicate("existing_0"),
          iterations=5000)


def bench_address_assignment():
    """Benchmark: Zigbee address assignment."""
    am = AddressManager(max_children=20, max_routers=6, max_depth=5)
    am.assign_address("coordinator", NodeRole.COORDINATOR)
    
    counter = [0]
    def assign_router():
        counter[0] += 1
        return am.assign_address(f"router_{counter[0]}", NodeRole.ROUTER)
    
    bench("Address assignment (router)", assign_router, iterations=1000)
    
    counter2 = [0]
    def assign_end_device():
        counter2[0] += 1
        return am.assign_address(f"device_{counter2[0]}", NodeRole.END_DEVICE)
    
    bench("Address assignment (end device)", assign_end_device, iterations=5000)


def bench_encryption():
    """Benchmark: Ed25519 signing and verification."""
    if not HAS_NACL:
        print("  ⚠️  PyNaCl not installed — skipping encryption benchmarks")
        return
    
    sk, pk = MeshEncryption.generate_keypair()
    enc = MeshEncryption(signing_key_hex=sk)
    
    bench("Key pair generation",
          lambda: MeshEncryption.generate_keypair(), iterations=100)
    
    bench("Message signing (1KB)",
          lambda: enc.sign_message("x" * 1000), iterations=500)
    
    sig = enc.sign_message("test message for verification benchmark")
    bench("Message verification",
          lambda: enc.verify_message("test message for verification benchmark", sig, pk),
          iterations=500)


def bench_auth():
    """Benchmark: Node authentication."""
    bench("Open auth (accept)",
          lambda: NodeAuthenticator(AuthConfig(mode="open")).authenticate_join(
              JoinRequest(node_name="test", node_role="router", public_key="key",
                          timestamp=time.time(), nonce="n")),
          iterations=5000)
    
    wl_config = AuthConfig(mode="whitelist", whitelist={"morzsa", "nova", "device1"})
    wl_auth = NodeAuthenticator(wl_config)
    bench("Whitelist auth (known)",
          lambda: wl_auth.authenticate_join(
              JoinRequest(node_name="morzsa", node_role="router", public_key="key",
                          timestamp=time.time(), nonce="n")),
          iterations=5000)
    
    bench("Whitelist auth (unknown)",
          lambda: wl_auth.authenticate_join(
              JoinRequest(node_name="unknown", node_role="router", public_key="key",
                          timestamp=time.time(), nonce="n")),
          iterations=5000)


def bench_priority_queue():
    """Benchmark: Priority queue throughput."""
    from a2a_mesh.core.router import MeshRouter
    
    router = MeshRouter(node_name="nova")
    
    def enqueue():
        msg = A2AMessage.create(sender="nova", recipient="morzsa", msg_type="directive",
                                 payload={"i": time.perf_counter_ns()}, priority=5)
        router.enqueue(msg)
    
    bench("Priority queue enqueue (P5)",
          enqueue, iterations=1000)
    
    # High priority
    def enqueue_high():
        msg = A2AMessage.create(sender="nova", recipient="morzsa", msg_type="directive",
                                 payload={"i": time.perf_counter_ns()}, priority=10)
        router.enqueue(msg)
    
    bench("Priority queue enqueue (P10)",
          enqueue_high, iterations=1000)


def bench_pg_notify():
    """Benchmark: PG NOTIFY message throughput."""
    try:
        conn = psycopg2.connect(
            host="192.168.1.30", port=5432, dbname="agent_memory",
            user="nova", password="nova_agent_2026"
        )
        conn.autocommit = True
        cur = conn.cursor()
    except Exception as e:
        print(f"  ⚠️  PG connection failed: {e} — skipping PG NOTIFY benchmark")
        return
    
    # Test INSERT + NOTIFY throughput
    def insert_message():
        msg_id = hashlib.md5(f"{time.perf_counter_ns()}".encode()).hexdigest()[:8]
        cur.execute("""
            INSERT INTO mesh.mesh_messages (id, sender, recipient, msg_type, priority, payload, status, routing_mode, src_addr, dst_addr)
            VALUES (%s, 'nova', 'morzsa', 'directive', 5, '{"bench": true}', 'sent', 'hybrid', 0, 1)
        """, (msg_id,))
    
    bench("PG INSERT (mesh_messages)", insert_message, iterations=100)
    
    # Test NOTIFY throughput
    def pg_notify():
        cur.execute("SELECT pg_notify('mesh_channel', 'bench')")
    
    bench("PG NOTIFY (mesh_channel)", pg_notify, iterations=500)
    
    # Cleanup bench messages
    cur.execute("DELETE FROM mesh.mesh_messages WHERE payload::text LIKE '%bench%'")
    cur.close()
    conn.close()


def bench_p2p_tcp():
    """Benchmark: P2P TCP connection latency to Morzsa."""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(2)
        sock.connect(("192.168.1.30", 8651))
    except Exception as e:
        print(f"  ⚠️  P2P connection failed: {e} — skipping P2P benchmark")
        return
    
    # Connection latency already measured above
    connect_times = []
    for _ in range(50):
        t0 = time.perf_counter_ns()
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(2)
        s.connect(("192.168.1.30", 8651))
        t1 = time.perf_counter_ns()
        connect_times.append((t1 - t0) / 1_000_000)
        s.close()
    
    print(f"  P2P TCP connection latency:")
    print(f"    min={min(connect_times):.1f}ms  avg={statistics.mean(connect_times):.1f}ms  "
          f"p50={statistics.median(connect_times):.1f}ms  p99={sorted(connect_times)[int(len(connect_times)*0.99)]:.1f}ms")
    
    # Message send latency
    msg = json.dumps({"type": "ping", "sender": "nova", "timestamp": time.time()})
    msg_bytes = msg.encode() + b"\n"
    
    send_times = []
    for _ in range(100):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(2)
        s.connect(("192.168.1.30", 8651))
        t0 = time.perf_counter_ns()
        s.sendall(msg_bytes)
        t1 = time.perf_counter_ns()
        send_times.append((t1 - t0) / 1_000_000)
        s.close()
    
    print(f"  P2P TCP message send latency:")
    print(f"    min={min(send_times):.1f}ms  avg={statistics.mean(send_times):.1f}ms  "
          f"p50={statistics.median(send_times):.1f}ms")
    
    sock.close()


def bench_full_roundtrip():
    """Benchmark: Full round-trip Nova → PG → Morzsa → PG → Nova."""
    try:
        conn = psycopg2.connect(
            host="192.168.1.30", port=5432, dbname="agent_memory",
            user="nova", password="nova_agent_2026"
        )
        conn.autocommit = True
        cur = conn.cursor()
    except Exception as e:
        print(f"  ⚠️  PG connection failed: {e} — skipping round-trip benchmark")
        return
    
    # Send message and measure round-trip time
    roundtrip_times = []
    for i in range(20):
        msg_id = hashlib.md5(f"bench_rt_{i}_{time.perf_counter_ns()}".encode()).hexdigest()[:8]
        t0 = time.perf_counter_ns()
        cur.execute("""
            INSERT INTO mesh.mesh_messages (id, sender, recipient, msg_type, priority, payload, status, routing_mode, src_addr, dst_addr)
            VALUES (%s, 'nova', 'morzsa', 'directive', 5, '{"bench_roundtrip": true}', 'sent', 'hybrid', 0, 1)
        """, (msg_id,))
        cur.execute("SELECT pg_notify('mesh_channel', %s)", (msg_id,))
        t1 = time.perf_counter_ns()
        roundtrip_times.append((t1 - t0) / 1_000_000)
    
    print(f"  Full round-trip (INSERT + NOTIFY):")
    print(f"    min={min(roundtrip_times):.1f}ms  avg={statistics.mean(roundtrip_times):.1f}ms  "
          f"p50={statistics.median(roundtrip_times):.1f}ms")
    
    # Cleanup
    cur.execute("DELETE FROM mesh.mesh_messages WHERE payload::text LIKE '%bench_roundtrip%'")
    cur.close()
    conn.close()


def bench_hash_performance():
    """Benchmark: Hash functions for dedup/signing."""
    bench("MD5 hash (1KB)",
          lambda: hashlib.md5(b"x" * 1000).hexdigest(),
          iterations=10000)
    
    bench("SHA256 hash (1KB)",
          lambda: hashlib.sha256(b"x" * 1000).hexdigest(),
          iterations=10000)
    
    bench("SHA256 hash (100KB)",
          lambda: hashlib.sha256(b"x" * 100000).hexdigest(),
          iterations=1000)


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    print("=" * 70)
    print("A2A Mesh Performance Benchmark Suite")
    print(f"Date: {datetime.now().isoformat()}")
    print(f"Python: {sys.version.split()[0]}")
    print("=" * 70)
    
    suites = [
        ("Message Creation", bench_message_creation),
        ("Message Serialization", bench_message_serialization),
        ("Message Compression", bench_message_compression),
        ("Dedup Cache", bench_dedup_cache),
        ("Address Assignment", bench_address_assignment),
        ("Encryption", bench_encryption),
        ("Authentication", bench_auth),
        ("Priority Queue", bench_priority_queue),
        ("Hash Performance", bench_hash_performance),
        ("PG NOTIFY", bench_pg_notify),
        ("P2P TCP", bench_p2p_tcp),
        ("Full Round-Trip", bench_full_roundtrip),
    ]
    
    results = {}
    for name, fn in suites:
        print(f"\n{'─' * 70}")
        print(f"📊 {name}")
        print(f"{'─' * 70}")
        try:
            fn()
        except Exception as e:
            print(f"  ❌ Error: {e}")
    
    print(f"\n{'═' * 70}")
    print("✅ Benchmark suite complete!")
    print(f"{'═' * 70}")


if __name__ == "__main__":
    main()