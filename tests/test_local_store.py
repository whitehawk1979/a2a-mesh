"""Test core.local_store — SQLite fallback for decentralized operation"""
import pytest
import tempfile
import os
import time

from a2a_mesh.core.local_store import LocalStore


@pytest.fixture
def store():
    """Create a temporary local store for testing."""
    db_path = tempfile.mktemp(suffix=".db")
    s = LocalStore(node_name="test_node", db_path=db_path)
    yield s
    s.close()
    if os.path.exists(db_path):
        os.unlink(db_path)


class TestOutboundQueue:
    def test_enqueue_outbound(self, store):
        msg_id = "msg-001"
        assert store.enqueue_outbound(
            msg_id=msg_id, sender="nova", recipient="morzsa",
            msg_type="directive", priority=7, payload='{"action":"test"}',
        )
        pending = store.get_pending_outbound()
        assert len(pending) == 1
        assert pending[0]["msg_id"] == msg_id
        assert pending[0]["priority"] == 7

    def test_mark_outbound_sent(self, store):
        store.enqueue_outbound("msg-002", "nova", "morzsa", "directive", 5, '{}')
        store.mark_outbound_sent("msg-002")
        pending = store.get_pending_outbound()
        assert len(pending) == 0

    def test_mark_outbound_pg_synced(self, store):
        store.enqueue_outbound("msg-003", "nova", "morzsa", "directive", 5, '{}')
        store.mark_outbound_pg_synced("msg-003")
        unsynced = store.get_unsynced_count()
        assert unsynced["unsynced_outbound"] == 0

    def test_increment_retry(self, store):
        store.enqueue_outbound("msg-004", "nova", "morzsa", "directive", 5, '{}')
        store.increment_retry("msg-004")
        store.increment_retry("msg-004")
        pending = store.get_pending_outbound()
        assert pending[0]["retry_count"] == 2

    def test_cleanup_outbound(self, store):
        store.enqueue_outbound("msg-005", "nova", "morzsa", "directive", 5, '{}')
        store.mark_outbound_pg_synced("msg-005")
        # Manually age the record
        store._conn.execute(
            "UPDATE outbound_queue SET created_at = ? WHERE msg_id = ?",
            (time.time() - 48 * 3600, "msg-005")
        )
        store._conn.commit()
        store.cleanup_outbound(max_age_hours=24)
        unsynced = store.get_unsynced_count()
        assert unsynced["unsynced_outbound"] == 0

    def test_priority_ordering(self, store):
        store.enqueue_outbound("msg-low", "nova", "morzsa", "directive", 1, '{}')
        store.enqueue_outbound("msg-high", "nova", "morzsa", "directive", 10, '{}')
        store.enqueue_outbound("msg-mid", "nova", "morzsa", "directive", 5, '{}')
        pending = store.get_pending_outbound()
        priorities = [p["priority"] for p in pending]
        assert priorities == [10, 5, 1]

    def test_duplicate_enqueue_ignored(self, store):
        store.enqueue_outbound("msg-dup", "nova", "morzsa", "directive", 5, '{}')
        result = store.enqueue_outbound("msg-dup", "nova", "morzsa", "directive", 5, '{}')
        assert result  # INSERT OR IGNORE returns True
        pending = store.get_pending_outbound()
        assert len(pending) == 1  # Only one entry


class TestInboundMessages:
    def test_store_inbound(self, store):
        assert store.store_inbound(
            msg_id="in-001", sender="morzsa", recipient="nova",
            msg_type="heartbeat", priority=3, payload='{"status":"ok"}',
            from_transport="p2p",
        )
        unprocessed = store.get_unprocessed_inbound()
        assert len(unprocessed) == 1
        assert unprocessed[0]["from_transport"] == "p2p"

    def test_mark_inbound_processed(self, store):
        store.store_inbound("in-002", "morzsa", "nova", "heartbeat", 3, '{}')
        store.mark_inbound_processed("in-002")
        unprocessed = store.get_unprocessed_inbound()
        assert len(unprocessed) == 0

    def test_inbound_priority_ordering(self, store):
        store.store_inbound("in-low", "morzsa", "nova", "heartbeat", 1, '{}')
        store.store_inbound("in-high", "morzsa", "nova", "heartbeat", 9, '{}')
        unprocessed = store.get_unprocessed_inbound()
        priorities = [u["priority"] for u in unprocessed]
        assert priorities == [9, 1]


class TestFileTransfers:
    def test_store_file_chunk(self, store):
        assert store.store_file_chunk(
            file_id="file-001", filename="test.txt", size=1024,
            sender="nova", recipient="morzsa", transfer_method="p2p",
            chunk_index=0, total_chunks=1,
        )
        pending = store.get_pending_file_chunks(recipient="morzsa")
        assert len(pending) == 1
        assert pending[0]["filename"] == "test.txt"

    def test_mark_file_complete(self, store):
        store.store_file_chunk("file-002", "data.bin", 2048, "nova", "morzsa", "p2p")
        store.mark_file_complete("file-002")
        pending = store.get_pending_file_chunks()
        assert len(pending) == 0

    def test_multiple_chunks(self, store):
        for i in range(3):
            store.store_file_chunk(
                file_id=f"file-chunk-{i}", filename="big.bin", size=3072,
                sender="nova", recipient="morzsa", transfer_method="p2p",
                chunk_index=i, total_chunks=3,
            )
        pending = store.get_pending_file_chunks(recipient="morzsa")
        assert len(pending) == 3


class TestSteerCache:
    def test_cache_steer(self, store):
        store.cache_steer(
            steer_id="steer-001", sender="morzsa", action="restart_service",
            params='{"service":"watcher"}', priority=8, status="pending",
        )
        active = store.get_active_steers()
        assert len(active) == 1
        assert active[0]["action"] == "restart_service"

    def test_update_steer_status(self, store):
        store.cache_steer("steer-002", "morzsa", "deploy", '{}', 7, "pending")
        store.update_steer_status("steer-002", "executing", result="deploying...")
        active = store.get_active_steers()
        assert active[0]["status"] == "executing"

    def test_completed_steers_not_active(self, store):
        store.cache_steer("steer-003", "morzsa", "deploy", '{}', 7, "pending")
        store.update_steer_status("steer-003", "completed", result="ok")
        active = store.get_active_steers()
        assert len(active) == 0


class TestPeerStatus:
    def test_update_peer_status(self, store):
        store.update_peer_status(
            node_name="morzsa", role="router", address="192.168.1.30:8651",
            pg_available=True, minio_available=True, p2p_available=True,
        )
        status = store.get_peer_status("morzsa")
        assert status is not None
        assert status["p2p_available"] == 1

    def test_get_available_peers(self, store):
        store.update_peer_status("morzsa", "router", "192.168.1.30:8651", True, True, True)
        store.update_peer_status("offline_node", "end_device", "10.0.0.1:8651", False, False, False)
        # Set last_seen to past for offline_node
        store._conn.execute(
            "UPDATE peer_status SET last_seen = ? WHERE node_name = ?",
            (time.time() - 600, "offline_node")
        )
        store._conn.commit()
        available = store.get_available_peers(require_p2p=True)
        assert len(available) == 1
        assert available[0]["node_name"] == "morzsa"


class TestStats:
    def test_get_stats(self, store):
        store.enqueue_outbound("s-001", "nova", "morzsa", "directive", 5, '{}')
        store.store_inbound("s-002", "morzsa", "nova", "heartbeat", 3, '{}')
        stats = store.get_stats()
        assert "outbound_pending" in stats
        assert "inbound_unprocessed" in stats
        assert "peers" in stats

    def test_get_unsynced_count(self, store):
        store.enqueue_outbound("u-001", "nova", "morzsa", "directive", 5, '{}')
        store.store_inbound("u-002", "morzsa", "nova", "heartbeat", 3, '{}')
        counts = store.get_unsynced_count()
        assert counts["unsynced_outbound"] == 1
        assert counts["unprocessed_inbound"] == 1