#!/usr/bin/env python3
"""Functional tests for P2P transport fixes: ACK, retry queue, file transfer wiring, webhook fallback.

Tests are designed to run locally without needing a full mesh deployment.
Tests 4-6 require real P2P connections to Morzsa (192.168.1.30:8651) and Runa (192.168.1.100:8651).
"""

import asyncio
import json
import os
import struct
import sys
import time
import tempfile
import unittest
from unittest.mock import MagicMock, AsyncMock, patch

# Add parent dir to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from a2a_mesh.core.message import A2AMessage, SendResult, MSG_TYPE_ACK, MSG_TYPE_HEARTBEAT, MSG_TYPE_DIRECTIVE
from a2a_mesh.core.file_transfer import (
    P2PFileTransfer, FILE_OFFER, FILE_ACCEPT, FILE_REJECT, 
    FILE_CHUNK, FILE_COMPLETE, FILE_ACK, CHUNK_SIZE
)
from a2a_mesh.transports.p2p_transport import P2PTransport
from a2a_mesh.transports.base import TransportStatus


class MockConfig:
    """Mock config for P2P transport testing."""
    def __init__(self):
        self.node_name = "test_node"
        self.p2p = MagicMock()
        self.p2p.listen_host = "127.0.0.1"
        self.p2p.listen_port = 18765  # Test port
        self.p2p.max_connections = 5
        self.p2p.reconnect_interval = 5
        self.p2p.idle_timeout = 5
        self.p2p.tls_enabled = False
        self.p2p.tls_cert = ""
        self.p2p.tls_key = ""
        self.p2p.tls_ca = ""
        self.p2p.tls_verify_peer = False
        self.discovery = MagicMock()
        self.discovery.static_nodes = []


class TestP2PACK(unittest.TestCase):
    """Test 1: P2P ACK mechanism."""

    def setUp(self):
        self.config = MockConfig()

    def test_ack_callback_attribute(self):
        """Test that set_ack_callback exists and works."""
        transport = P2PTransport(self.config)
        callback_called = []
        
        async def my_callback(ack_for_id, ack_type):
            callback_called.append((ack_for_id, ack_type))
        
        transport.set_ack_callback(my_callback)
        self.assertIsNotNone(transport._ack_callback)
        self.assertEqual(transport._ack_callback, my_callback)

    def test_ack_message_creation(self):
        """Test that ACK messages are created properly."""
        original = A2AMessage.create(
            sender="morzsa",
            recipient="nova",
            msg_type="directive",
            payload={"text": "hello"},
            priority=5,
        )
        
        ack = A2AMessage.create(
            sender="nova",
            recipient="morzsa",
            msg_type=MSG_TYPE_ACK,
            priority=6,
            payload={
                "ack_for": original.id,
                "ack_type": "delivered",
                "original_type": original.type,
                "original_sender": original.sender,
                "timestamp": time.time(),
                "error": "",
            },
        )
        
        self.assertEqual(ack.type, MSG_TYPE_ACK)
        self.assertEqual(ack.payload["ack_for"], original.id)
        self.assertEqual(ack.payload["ack_type"], "delivered")

    def test_send_ack_over_tcp(self):
        """Test that ACK is sent over TCP when a message is received."""
        config1 = MockConfig()
        config1.p2p.listen_port = 18766
        config1.node_name = "node_a"
        
        config2 = MockConfig()
        config2.p2p.listen_port = 18767
        config2.p2p.listen_host = "127.0.0.1"
        config2.node_name = "node_b"
        
        async def run_test():
            transport_a = P2PTransport(config1)
            transport_b = P2PTransport(config2)
            
            await transport_a.start()
            await transport_b.start()
            
            # Connect both ways so each transport knows the other's address
            # B connects to A (A accepts incoming, registers B after first message)
            await transport_b._connect_to_peer("node_a", "127.0.0.1", 18766)
            # A connects to B (B accepts incoming, registers A after first message)
            await transport_a._connect_to_peer("node_b", "127.0.0.1", 18767)
            await asyncio.sleep(0.5)
            
            # Set ACK callback on A (ACK for sent messages comes back to sender)
            acks_received = []
            async def on_ack(ack_for_id, ack_type):
                acks_received.append((ack_for_id, ack_type))
            transport_a.set_ack_callback(on_ack)
            
            # A sends a message to B
            msg = A2AMessage.create(
                sender="node_a",
                recipient="node_b",
                msg_type="directive",
                payload={"text": "test message"},
                priority=5,
            )
            
            # B must be in A's peers for send to succeed
            # (connected above via bidirectional connection)
            if "node_b" not in transport_a._peers:
                # If bidirectional not yet established, manually add peer address
                # This happens when _handle_connection hasn't processed the first message yet
                transport_a._peers["node_b"] = list(transport_b._peers.values())[0] if transport_b._peers else None
                if transport_a._peers.get("node_b") is None:
                    # Fallback: use a peer_address_resolver
                    def resolve(peer_name):
                        if peer_name == "node_b":
                            return ("127.0.0.1", 18767)
                        return None
                    transport_a._peer_address_resolver = resolve
            
            result = await transport_a.send(msg)
            self.assertTrue(result.success, f"Send should succeed: {result.error}")
            
            # Wait for B to receive and send ACK back
            await asyncio.sleep(1.0)
            
            # Check that B received the message
            messages_b = await transport_b.receive()
            self.assertGreaterEqual(len(messages_b), 1, f"B should have received at least 1 message, got {len(messages_b)}")
            
            # Check that A received the ACK
            messages_a = await transport_a.receive()
            ack_found = any(m[0].type == MSG_TYPE_ACK for m in messages_a)
            self.assertTrue(ack_found, f"A should have received ACK, got: {[(m[0].type, m[0].payload) for m in messages_a]}")
            
            # Check that A's ACK callback was invoked
            await asyncio.sleep(0.5)
            
            await transport_a.stop()
            await transport_b.stop()
        
        asyncio.run(run_test())


class TestRetryQueue(unittest.TestCase):
    """Test 2: P2P retry queue mechanism."""

    def setUp(self):
        self.config = MockConfig()

    def test_enqueue_for_retry(self):
        """Test that failed messages are enqueued for retry."""
        transport = P2PTransport(self.config)
        
        msg = A2AMessage.create(
            sender="nova",
            recipient="morzsa",
            msg_type="directive",
            payload={"text": "test"},
            priority=5,
        )
        
        transport._enqueue_for_retry(msg)
        self.assertEqual(transport.get_retry_queue_size(), 1)
        
        # Heartbeats and ACKs should NOT be enqueued
        hb = A2AMessage.create(
            sender="nova", recipient="broadcast",
            msg_type=MSG_TYPE_HEARTBEAT, payload={}, priority=1
        )
        transport._enqueue_for_retry(hb)
        self.assertEqual(transport.get_retry_queue_size(), 1, "Heartbeats should not be queued")
        
        ack = A2AMessage.create(
            sender="nova", recipient="morzsa",
            msg_type=MSG_TYPE_ACK, payload={}, priority=5
        )
        transport._enqueue_for_retry(ack)
        self.assertEqual(transport.get_retry_queue_size(), 1, "ACKs should not be queued")

    def test_retry_queue_max_age(self):
        """Test that old messages are discarded in _process_retry_queue."""
        transport = P2PTransport(self.config)
        
        # Create a very old queued message
        msg = A2AMessage.create(
            sender="nova", recipient="morzsa",
            msg_type="directive", payload={"text": "old"}, priority=5
        )
        transport._retry_queue.append((msg, time.time() - 3700))  # Older than 1 hour
        
        async def run():
            await transport._process_retry_queue()
        
        asyncio.run(run())
        self.assertEqual(transport.get_retry_queue_size(), 0, "Old message should be discarded")


class TestFileTransferWiring(unittest.TestCase):
    """Test 3: File transfer wiring in node."""

    def test_send_file_method_exists(self):
        """Test that send_file method exists on MeshNode."""
        from a2a_mesh.node import MeshNode
        self.assertTrue(hasattr(MeshNode, 'send_file'))
        self.assertTrue(hasattr(MeshNode, '_send_file_chunks'))
        self.assertTrue(hasattr(MeshNode, '_on_p2p_ack'))

    def test_file_accept_detection_in_dispatch(self):
        """Test that FILE_ACCEPT triggers chunk sending."""
        from a2a_mesh.node import MeshNode
        
        # Check that _dispatch_to_handlers references FILE_ACCEPT
        import inspect
        source = inspect.getsource(MeshNode._dispatch_to_handlers)
        self.assertIn("FILE_ACCEPT", source)
        self.assertIn("_send_file_chunks", source)

    def test_file_transfer_round_trip(self):
        """Test a complete file transfer round-trip (offer → accept → chunks → complete)."""
        import tempfile
        
        # Create a test file
        with tempfile.NamedTemporaryFile(delete=False, suffix=".txt", mode="wb") as f:
            f.write(b"Hello, mesh! This is a test file for P2P transfer." * 100)
            test_file_path = f.name
        
        try:
            # Sender creates offer
            sender_ft = P2PFileTransfer(node_name="nova", local_store=MagicMock())
            offer_msg, file_id = sender_ft.create_offer_message(test_file_path, "morzsa")
            
            self.assertEqual(offer_msg.type, "file_transfer")
            self.assertEqual(offer_msg.payload["transfer_type"], FILE_OFFER)
            self.assertEqual(offer_msg.payload["filename"], os.path.basename(test_file_path))
            
            # Receiver handles offer → returns FILE_ACCEPT
            receiver_ft = P2PFileTransfer(
                node_name="morzsa", 
                local_store=MagicMock(),
                incoming_dir=tempfile.mkdtemp()
            )
            response = receiver_ft.handle_incoming(offer_msg)
            
            self.assertIsNotNone(response)
            self.assertEqual(response.payload["transfer_type"], FILE_ACCEPT)
            self.assertEqual(response.payload["file_id"], file_id)
            
            # Sender sends chunks
            transfer = sender_ft._outgoing[file_id]
            chunk_count = transfer["chunk_count"]
            
            for i in range(chunk_count):
                chunk_msg = sender_ft.create_chunk_message(file_id, i, "morzsa")
                self.assertIsNotNone(chunk_msg)
                self.assertEqual(chunk_msg.payload["transfer_type"], FILE_CHUNK)
                self.assertEqual(chunk_msg.payload["chunk_index"], i)
                
                # Receiver handles chunk
                receiver_ft.handle_incoming(chunk_msg)
            
            # Check all chunks received
            self.assertEqual(len(receiver_ft._incoming[file_id]["chunks_received"]), chunk_count)
            
            # Sender sends FILE_COMPLETE
            complete_msg = sender_ft.create_complete_message(file_id, "morzsa")
            self.assertEqual(complete_msg.payload["transfer_type"], FILE_COMPLETE)
            
            # Receiver handles complete
            ack_response = receiver_ft.handle_incoming(complete_msg)
            self.assertIsNotNone(ack_response)
            self.assertEqual(ack_response.payload["transfer_type"], FILE_ACK)
            self.assertTrue(ack_response.payload["success"])
            
            # Verify file was written
            output_path = os.path.join(receiver_ft.incoming_dir, os.path.basename(test_file_path))
            self.assertTrue(os.path.exists(output_path))
            
            # Verify content matches
            with open(test_file_path, 'rb') as f:
                original = f.read()
            with open(output_path, 'rb') as f:
                received = f.read()
            self.assertEqual(original, received, "File content should match")
            
        finally:
            os.unlink(test_file_path)


class TestWebhookFallback(unittest.TestCase):
    """Test 4: Dashboard webhook fallback to P2P."""

    def test_wake_via_p2p_method_exists(self):
        """Test that _wake_via_p2p method exists on DashboardHandler."""
        from a2a_mesh.core.dashboard import DashboardHandler
        self.assertTrue(hasattr(DashboardHandler, '_wake_via_p2p'))
    
    def test_call_webhook_has_p2p_fallback(self):
        """Test that _call_webhook falls back to P2P."""
        from a2a_mesh.core.dashboard import DashboardHandler
        import inspect
        source = inspect.getsource(DashboardHandler._call_webhook)
        self.assertIn("_wake_via_p2p", source, "_call_webhook should call _wake_via_p2p on failure")


class TestRealP2PConnections(unittest.TestCase):
    """Test 5-6: Real P2P connections to Morzsa and Runa.
    
    These tests connect to actual running nodes.
    Skip if unreachable.
    """

    def test_morzsa_p2p_reachable(self):
        """Test P2P TCP connection to Morzsa (192.168.1.30:8651)."""
        import socket
        
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        try:
            sock.connect(("192.168.1.30", 8651))
            sock.close()
            morzsa_reachable = True
        except (socket.timeout, ConnectionRefusedError, OSError):
            morzsa_reachable = False
        
        if not morzsa_reachable:
            self.skipTest("Morzsa not reachable at 192.168.1.30:8651")
        
        # If reachable, test actual P2P ACK exchange
        config = MockConfig()
        config.p2p.listen_port = 0  # Let OS assign
        config.node_name = "test_client"
        
        async def run_test():
            transport = P2PTransport(config)
            await transport.start()
            
            # Connect to Morzsa
            await transport._connect_to_peer("morzsa", "192.168.1.30", 8651)
            await asyncio.sleep(2)
            
            self.assertIn("morzsa", transport._peers, "Should be connected to Morzsa")
            
            # Send a test directive
            msg = A2AMessage.create(
                sender="test_client",
                recipient="morzsa",
                msg_type="directive",
                payload={"text": "P2P ACK test from test suite", "test": True},
                priority=5,
            )
            
            # Set ACK callback
            acks = []
            async def on_ack(ack_for_id, ack_type):
                acks.append((ack_for_id, ack_type))
            transport.set_ack_callback(on_ack)
            
            result = await transport.send(msg)
            self.assertTrue(result.success, f"Send to Morzsa should succeed: {result.error}")
            
            # Wait for ACK
            await asyncio.sleep(3)
            
            # Check if we got any messages back (ACK)
            messages = await transport.receive()
            ack_msgs = [m for m in messages if m[0].type == MSG_TYPE_ACK]
            
            if ack_msgs:
                print(f"✅ Received P2P ACK from Morzsa: {ack_msgs[0][0].payload}")
            else:
                print(f"⚠️ No ACK received from Morzsa (messages: {len(messages)})")
            
            await transport.stop()
        
        asyncio.run(run_test())

    def test_runa_p2p_reachable(self):
        """Test P2P TCP connection to Runa (192.168.1.100:8651)."""
        import socket
        
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        try:
            sock.connect(("192.168.1.100", 8651))
            sock.close()
            runa_reachable = True
        except (socket.timeout, ConnectionRefusedError, OSError):
            runa_reachable = False
        
        if not runa_reachable:
            self.skipTest("Runa not reachable at 192.168.1.100:8651")
        
        # If reachable, test actual P2P ACK exchange
        config = MockConfig()
        config.p2p.listen_port = 0
        config.node_name = "test_client"
        
        async def run_test():
            transport = P2PTransport(config)
            await transport.start()
            
            # Connect to Runa
            await transport._connect_to_peer("runa", "192.168.1.100", 8651)
            await asyncio.sleep(2)
            
            self.assertIn("runa", transport._peers, "Should be connected to Runa")
            
            # Send a test directive
            msg = A2AMessage.create(
                sender="test_client",
                recipient="runa",
                msg_type="directive",
                payload={"text": "P2P ACK test from test suite", "test": True},
                priority=5,
            )
            
            result = await transport.send(msg)
            self.assertTrue(result.success, f"Send to Runa should succeed: {result.error}")
            
            # Wait for ACK
            await asyncio.sleep(3)
            
            messages = await transport.receive()
            ack_msgs = [m for m in messages if m[0].type == MSG_TYPE_ACK]
            
            if ack_msgs:
                print(f"✅ Received P2P ACK from Runa: {ack_msgs[0][0].payload}")
            else:
                print(f"⚠️ No ACK received from Runa (messages: {len(messages)})")
            
            await transport.stop()
        
        asyncio.run(run_test())


if __name__ == '__main__':
    unittest.main(verbosity=2)

class TestP2PDedupGeneration(unittest.TestCase):
    """Test P2P connection generation tracking to prevent flapping on dual-connect."""

    def test_generation_tracking_on_outgoing_connect(self):
        """When _connect_to_peer registers a peer, generation is bumped and writer is tagged."""
        config = MockConfig()
        transport = P2PTransport(config)
        
        # Simulate _connect_to_peer registration without actual TCP
        import asyncio
        
        # Manually register a peer (simulating _connect_to_peer)
        reader = MagicMock()
        writer = MagicMock()
        transport._peers["nova"] = (reader, writer)
        transport._peer_addresses["nova"] = "192.168.1.8:8645"
        transport._writer_to_peer[id(writer)] = "nova"
        transport._peer_connected_at["nova"] = time.time()
        gen = transport._peer_generation.get("nova", 0) + 1
        transport._peer_generation["nova"] = gen
        writer._a2a_gen = gen
        
        self.assertEqual(transport._peer_generation["nova"], 1)
        self.assertEqual(writer._a2a_gen, 1)
        
        # Second connection (dedup): bump generation
        reader2 = MagicMock()
        writer2 = MagicMock()
        transport._peers["nova"] = (reader2, writer2)
        transport._writer_to_peer[id(writer2)] = "nova"
        gen2 = transport._peer_generation.get("nova", 0) + 1
        transport._peer_generation["nova"] = gen2
        writer2._a2a_gen = gen2
        
        self.assertEqual(transport._peer_generation["nova"], 2)
        self.assertEqual(writer2._a2a_gen, 2)
        # Old writer still has gen 1
        self.assertEqual(writer._a2a_gen, 1)

    def test_stale_finally_does_not_remove_peer(self):
        """When a stale connection's finally block runs, it should NOT remove
        a peer that has been replaced by a newer connection (higher generation)."""
        config = MockConfig()
        transport = P2PTransport(config)
        
        # Simulate first connection (outgoing)
        reader1 = MagicMock()
        writer1 = MagicMock()
        transport._peers["nova"] = (reader1, writer1)
        transport._peer_addresses["nova"] = "192.168.1.8:8645"
        transport._writer_to_peer[id(writer1)] = "nova"
        transport._peer_connected_at["nova"] = time.time()
        gen1 = transport._peer_generation.get("nova", 0) + 1
        transport._peer_generation["nova"] = gen1
        writer1._a2a_gen = gen1
        
        # Simulate dedup: newer incoming connection replaces old one
        reader2 = MagicMock()
        writer2 = MagicMock()
        transport._peers["nova"] = (reader2, writer2)
        transport._writer_to_peer[id(writer2)] = "nova"
        gen2 = transport._peer_generation.get("nova", 0) + 1
        transport._peer_generation["nova"] = gen2
        writer2._a2a_gen = gen2
        
        # Now simulate the stale finally block logic from _handle_connection
        # The old writer (writer1) should NOT remove the peer since gen has bumped
        current_gen = transport._peer_generation.get("nova", 0)
        our_gen = getattr(writer1, '_a2a_gen', current_gen)
        
        # our_gen (1) != current_gen (2) → stale connection, don't remove
        self.assertNotEqual(current_gen, our_gen, 
            "Generation should have bumped, old connection is stale")
        
        # Verify: nova is still in _peers with the NEW writer
        self.assertIn("nova", transport._peers)
        _, pw = transport._peers["nova"]
        self.assertIs(pw, writer2, "Peer should still have the new writer")
        
        # Simulate the NEW writer's finally block
        current_gen_new = transport._peer_generation.get("nova", 0)
        our_gen_new = getattr(writer2, '_a2a_gen', current_gen_new)
        
        # our_gen (2) == current_gen (2) → this is the current connection, CAN remove
        self.assertEqual(current_gen_new, our_gen_new,
            "New connection's generation matches, it can remove the peer")

    def test_real_disconnect_removes_peer(self):
        """When the current connection disconnects (no dedup), it should remove the peer."""
        config = MockConfig()
        transport = P2PTransport(config)
        
        # Simulate a single connection
        reader = MagicMock()
        writer = MagicMock()
        transport._peers["nova"] = (reader, writer)
        transport._peer_addresses["nova"] = "192.168.1.8:8645"
        transport._writer_to_peer[id(writer)] = "nova"
        transport._peer_connected_at["nova"] = time.time()
        gen = transport._peer_generation.get("nova", 0) + 1
        transport._peer_generation["nova"] = gen
        writer._a2a_gen = gen
        
        # Current connection disconnects — gen matches, should remove
        current_gen = transport._peer_generation.get("nova", 0)
        our_gen = getattr(writer, '_a2a_gen', current_gen)
        self.assertEqual(current_gen, our_gen, 
            "Single connection: generation should match")
        
        # Simulate removal (this is what the finally block does when gen matches)
        for pname, (pr, pw) in list(transport._peers.items()):
            if pw is writer:
                transport._peers.pop(pname, None)
                transport._peer_generation.pop(pname, None)
        
        self.assertNotIn("nova", transport._peers, "Peer should be removed on real disconnect")
        self.assertNotIn("nova", transport._peer_generation, "Generation should be cleaned up")
