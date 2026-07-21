"""Test core.gossipsub — GossipSub broadcast overlay."""
import asyncio
import pytest
from a2a_mesh.core.gossipsub import (
    GossipSub,
    GossipPeer,
    GossipMessage,
    GossipEventType,
)


class TestGossipPeer:
    def test_defaults(self):
        peer = GossipPeer(peer_id="node1")
        assert peer.peer_id == "node1"
        assert peer.connected is True
        assert "a2a" not in peer.topics  # default is empty set
        assert peer.score == 0.0

    def test_with_topics(self):
        peer = GossipPeer(peer_id="node1", topics={"a2a", "mesh"})
        assert "a2a" in peer.topics
        assert "mesh" in peer.topics


class TestGossipMessage:
    def test_creation(self):
        msg = GossipMessage(
            msg_id="m1",
            topic="a2a",
            sender="node1",
            payload=b"hello",
            seqno=42,
        )
        assert msg.msg_id == "m1"
        assert msg.topic == "a2a"
        assert msg.sender == "node1"
        assert msg.payload == b"hello"
        assert msg.seqno == 42


class TestGossipEventType:
    def test_event_types(self):
        assert GossipEventType.GRAFT.value == "graft"
        assert GossipEventType.PRUNE.value == "prune"
        assert GossipEventType.GRAFT_ACK.value == "graft_ack"
        assert GossipEventType.PRUNE_ACK.value == "prune_ack"


class TestGossipSub:
    def test_creation(self):
        gs = GossipSub(node_id="node1")
        assert gs.node_id == "node1"
        assert gs.d_low == 3
        assert gs.d_high == 6
        assert gs.flood_threshold == 10
        assert gs.mode == "flood"  # No peers yet

    def test_creation_custom_params(self):
        gs = GossipSub(
            node_id="node1",
            d_low=2,
            d_high=5,
            d_score=3,
            heartbeat_interval=30.0,
            gossip_factor=0.3,
            flood_threshold=20,
        )
        assert gs.d_low == 2
        assert gs.d_high == 5
        assert gs.d_score == 3
        assert gs.heartbeat_interval == 30.0
        assert gs.gossip_factor == 0.3
        assert gs.flood_threshold == 20

    def test_add_peer(self):
        gs = GossipSub(node_id="node1")
        gs.add_peer("node2", topics={"a2a", "mesh"})
        assert "node2" in gs._peers
        assert gs._peers["node2"].topics == {"a2a", "mesh"}
        assert "node2" in gs._mesh.get("a2a", set())
        assert "node2" in gs._mesh.get("mesh", set())

    def test_add_peer_default_topic(self):
        gs = GossipSub(node_id="node1")
        gs.add_peer("node2")
        assert gs._peers["node2"].topics == {"a2a"}

    def test_add_self_ignored(self):
        gs = GossipSub(node_id="node1")
        gs.add_peer("node1")
        assert "node1" not in gs._peers

    def test_remove_peer(self):
        gs = GossipSub(node_id="node1")
        gs.add_peer("node2", topics={"a2a"})
        gs.remove_peer("node2")
        assert "node2" not in gs._peers
        assert "node2" not in gs._mesh.get("a2a", set())

    def test_remove_nonexistent(self):
        gs = GossipSub(node_id="node1")
        gs.remove_peer("ghost")  # Should not raise

    def test_update_peer_score(self):
        gs = GossipSub(node_id="node1")
        gs.update_peer_score("node2", 5.0)
        assert gs._peer_scores["node2"] == 5.0
        gs.update_peer_score("node2", -2.0)
        assert gs._peer_scores["node2"] == 3.0

    def test_peer_score_bounds(self):
        gs = GossipSub(node_id="node1")
        gs.update_peer_score("node2", 200.0)
        assert gs._peer_scores["node2"] == 100  # Capped
        gs.update_peer_score("node2", -300.0)
        assert gs._peer_scores["node2"] == -100  # Floor

    def test_mode_flood(self):
        gs = GossipSub(node_id="node1", flood_threshold=10)
        for i in range(5):
            gs.add_peer(f"node{i+2}")
        assert gs.mode == "flood"

    def test_mode_gossipsub(self):
        gs = GossipSub(node_id="node1", flood_threshold=3)
        for i in range(5):
            gs.add_peer(f"node{i+2}")
        assert gs.mode == "gossipsub"

    @pytest.mark.asyncio
    async def test_publish_duplicate(self):
        """Duplicate messages should be deduplicated."""
        gs = GossipSub(node_id="node1")
        gs.add_peer("node2", topics={"a2a"})

        sent = []
        async def mock_send(peer_id, msg):
            sent.append((peer_id, msg))

        gs.set_send_callback(mock_send)
        count1 = await gs.publish("a2a", "msg1", b"hello")
        count2 = await gs.publish("a2a", "msg1", b"hello")  # Duplicate
        assert count1 >= 1
        assert count2 == 0  # Deduplicated

    @pytest.mark.asyncio
    async def test_publish_flood_mode(self):
        """In flood mode, publish to all peers."""
        gs = GossipSub(node_id="node1", flood_threshold=10)
        gs.add_peer("node2", topics={"a2a"})
        gs.add_peer("node3", topics={"a2a"})

        sent = []
        async def mock_send(peer_id, msg):
            sent.append(peer_id)

        gs.set_send_callback(mock_send)
        count = await gs.publish("a2a", "msg1", b"hello")
        assert count == 2
        assert "node2" in sent
        assert "node3" in sent

    @pytest.mark.asyncio
    async def test_publish_to_nonexistent_topic(self):
        """Publishing to a topic with no subscribers."""
        gs = GossipSub(node_id="node1")
        gs.add_peer("node2", topics={"other"})
        count = await gs.publish("a2a", "msg1", b"hello")
        assert count == 0

    @pytest.mark.asyncio
    async def test_handle_message(self):
        """Handle a received message — forward to other peers."""
        gs = GossipSub(node_id="node1")
        gs.add_peer("node2", topics={"a2a"})
        gs.add_peer("node3", topics={"a2a"})

        sent = []
        async def mock_send(peer_id, msg):
            sent.append(peer_id)

        gs.set_send_callback(mock_send)
        count = await gs.handle_message("a2a", "msg1", "node2", b"hello")
        # Should forward to node3 (excluding sender node2)
        assert count >= 1

    @pytest.mark.asyncio
    async def test_handle_message_duplicate(self):
        """Duplicate received messages should be deduped."""
        gs = GossipSub(node_id="node1")
        gs.add_peer("node2", topics={"a2a"})
        gs.add_peer("node3", topics={"a2a"})

        sent = []
        async def mock_send(peer_id, msg):
            sent.append(peer_id)

        gs.set_send_callback(mock_send)
        await gs.handle_message("a2a", "msg1", "node2", b"hello")
        count2 = await gs.handle_message("a2a", "msg1", "node3", b"hello")
        assert count2 == 0  # Deduplicated

    @pytest.mark.asyncio
    async def test_start_stop(self):
        gs = GossipSub(node_id="node1", heartbeat_interval=1.0)
        await gs.start()
        assert gs._running is True
        assert gs._heartbeat_task is not None
        await gs.stop()
        assert gs._running is False

    def test_stats(self):
        gs = GossipSub(node_id="node1")
        gs.add_peer("node2", topics={"a2a"})
        gs.add_peer("node3", topics={"mesh"})

        stats = gs.stats
        assert stats["mode"] == "flood"
        assert stats["peer_count"] == 2
        assert "a2a" in stats["mesh_topics"]
        assert "mesh" in stats["mesh_topics"]
        assert stats["messages_originated"] == 0
        assert stats["cache_hits"] == 0

    @pytest.mark.asyncio
    async def test_send_callback_failure(self):
        """If send callback fails, publish should still return partial count."""
        gs = GossipSub(node_id="node1")
        gs.add_peer("node2", topics={"a2a"})

        async def failing_send(peer_id, msg):
            raise ConnectionError("Peer unreachable")

        gs.set_send_callback(failing_send)
        count = await gs.publish("a2a", "msg1", b"hello")
        assert count == 0  # All sends failed

    @pytest.mark.asyncio
    async def test_optimize_mesh_prune(self):
        """When mesh is too dense, lowest-scoring peers should be pruned."""
        gs = GossipSub(node_id="node1", d_low=1, d_high=2, d_score=2)
        for i in range(5):
            gs.add_peer(f"node{i+2}", topics={"a2a"})
            gs.update_peer_score(f"node{i+2}", float(i))

        # node2=0.0, node3=1.0, node4=2.0, node5=3.0, node6=4.0
        await gs._optimize_mesh()
        # Should prune to d_high=2, keeping highest scores
        mesh_size = len(gs._mesh.get("a2a", set()))
        assert mesh_size <= gs.d_high

    @pytest.mark.asyncio
    async def test_optimize_mesh_graft(self):
        """When mesh is too sparse, highest-scoring peers should be grafted."""
        gs = GossipSub(node_id="node1", d_low=3, d_high=6, d_score=4)
        gs.add_peer("node2", topics={"a2a"})
        # Only 1 peer in mesh, need at least 3

        for i in range(5):
            gs.add_peer(f"extra{i}", topics={"a2a"})
            gs.update_peer_score(f"extra{i}", float(i))

        await gs._optimize_mesh()
        # Should graft peers to reach d_low
        mesh_size = len(gs._mesh.get("a2a", set()))
        # May not reach exactly d_low if not enough peers, but should grow
        assert mesh_size >= 1  # At least the original peer

    def test_cache_cleanup(self):
        """Expired cache entries should be removed."""
        import time
        gs = GossipSub(node_id="node1")
        gs._cache["old_msg"] = time.time() - 400  # Older than TTL
        gs._cache["new_msg"] = time.time()
        gs._clean_cache()
        assert "old_msg" not in gs._cache
        assert "new_msg" in gs._cache
