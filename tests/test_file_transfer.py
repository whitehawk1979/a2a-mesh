"""Test core.file_transfer — P2P direct file transfer"""
import pytest
import tempfile
import os

from a2a_mesh.core.local_store import LocalStore
from a2a_mesh.core.file_transfer import P2PFileTransfer, CHUNK_SIZE


@pytest.fixture
def store_and_transfer():
    db_path = tempfile.mktemp(suffix=".db")
    incoming_dir = tempfile.mkdtemp()
    store = LocalStore(node_name="test_nova", db_path=db_path)
    transfer = P2PFileTransfer(
        node_name="nova",
        local_store=store,
        incoming_dir=incoming_dir,
    )
    yield store, transfer
    store.close()
    os.unlink(db_path)
    import shutil
    shutil.rmtree(incoming_dir, ignore_errors=True)


class TestFileOffer:
    def test_create_offer(self, store_and_transfer):
        store, transfer = store_and_transfer
        # Create a test file
        test_file = tempfile.mktemp(suffix=".txt")
        with open(test_file, 'w') as f:
            f.write("Hello, P2P file transfer!")

        msg, file_id = transfer.create_offer_message(
            file_path=test_file, recipient="morzsa", priority=7
        )
        assert msg.type == "file_transfer"
        assert msg.recipient == "morzsa"
        assert msg.priority == 7
        payload = msg.payload if isinstance(msg.payload, dict) else {}
        assert payload["transfer_type"] == "file_offer"
        assert payload["filename"] == os.path.basename(test_file)
        assert payload["chunk_count"] == 1  # Small file = 1 chunk
        assert payload["chunk_size"] == CHUNK_SIZE

        os.unlink(test_file)

    def test_create_offer_large_file(self, store_and_transfer):
        store, transfer = store_and_transfer
        # Create a file larger than CHUNK_SIZE
        test_file = tempfile.mktemp(suffix=".bin")
        with open(test_file, 'wb') as f:
            f.write(os.urandom(CHUNK_SIZE * 3 + 100))  # 3+ chunks

        msg, file_id = transfer.create_offer_message(
            file_path=test_file, recipient="morzsa"
        )
        payload = msg.payload if isinstance(msg.payload, dict) else {}
        assert payload["chunk_count"] == 4  # 3 full + 1 partial
        os.unlink(test_file)

    def test_create_offer_nonexistent_file(self, store_and_transfer):
        store, transfer = store_and_transfer
        with pytest.raises(FileNotFoundError):
            transfer.create_offer_message(
                file_path="/nonexistent/file.txt", recipient="morzsa"
            )


class TestFileChunk:
    def test_create_chunk(self, store_and_transfer):
        store, transfer = store_and_transfer
        test_file = tempfile.mktemp(suffix=".txt")
        with open(test_file, 'w') as f:
            f.write("A" * 1024)

        _, file_id = transfer.create_offer_message(test_file, "morzsa")
        chunk_msg = transfer.create_chunk_message(file_id, 0, "morzsa")
        assert chunk_msg is not None
        payload = chunk_msg.payload if isinstance(chunk_msg.payload, dict) else {}
        assert payload["transfer_type"] == "file_chunk"
        assert payload["chunk_index"] == 0
        assert "data" in payload
        assert "chunk_hash" in payload

        os.unlink(test_file)

    def test_create_chunk_nonexistent_transfer(self, store_and_transfer):
        store, transfer = store_and_transfer
        result = transfer.create_chunk_message("fake-id", 0, "morzsa")
        assert result is None


class TestFileReceive:
    def test_handle_offer_accept(self, store_and_transfer):
        store, transfer = store_and_transfer
        from a2a_mesh.core.message import A2AMessage

        offer_msg = A2AMessage.create(
            sender="morzsa", recipient="nova",
            msg_type="file_transfer", priority=5,
            payload={
                "transfer_type": "file_offer",
                "file_id": "test-file-001",
                "filename": "document.pdf",
                "file_size": 1024,
                "file_hash": "abc123",
                "chunk_count": 1,
                "chunk_size": CHUNK_SIZE,
            }
        )

        response = transfer.handle_incoming(offer_msg)
        assert response is not None
        payload = response.payload if isinstance(response.payload, dict) else {}
        assert payload["transfer_type"] == "file_accept"
        assert payload["file_id"] == "test-file-001"
        assert payload["ready"] == True

    def test_handle_offer_insufficient_space(self, store_and_transfer):
        store, transfer = store_and_transfer
        from a2a_mesh.core.message import A2AMessage

        # Request an absurdly large file
        offer_msg = A2AMessage.create(
            sender="morzsa", recipient="nova",
            msg_type="file_transfer", priority=5,
            payload={
                "transfer_type": "file_offer",
                "file_id": "huge-file",
                "filename": "huge.bin",
                "file_size": 1000 * 1024 * 1024 * 1024 * 1024,  # 1 Petabyte
                "file_hash": "xyz789",
                "chunk_count": 999999,
                "chunk_size": CHUNK_SIZE,
            }
        )

        response = transfer.handle_incoming(offer_msg)
        assert response is not None
        payload = response.payload if isinstance(response.payload, dict) else {}
        assert payload["transfer_type"] == "file_reject"

    def test_handle_chunk_and_complete(self, store_and_transfer):
        store, transfer = store_and_transfer
        from a2a_mesh.core.message import A2AMessage
        import base64, hashlib

        # First, send an offer
        file_content = b"Hello, P2P direct transfer!"
        file_hash = hashlib.sha256(file_content).hexdigest()

        offer_msg = A2AMessage.create(
            sender="morzsa", recipient="nova",
            msg_type="file_transfer", priority=5,
            payload={
                "transfer_type": "file_offer",
                "file_id": "test-complete-001",
                "filename": "hello.txt",
                "file_size": len(file_content),
                "file_hash": file_hash,
                "chunk_count": 1,
                "chunk_size": CHUNK_SIZE,
            }
        )
        transfer.handle_incoming(offer_msg)

        # Send chunk
        chunk_hash = hashlib.sha256(file_content).hexdigest()[:16]
        chunk_msg = A2AMessage.create(
            sender="morzsa", recipient="nova",
            msg_type="file_transfer", priority=3,
            payload={
                "transfer_type": "file_chunk",
                "file_id": "test-complete-001",
                "chunk_index": 0,
                "chunk_hash": chunk_hash,
                "data": base64.b64encode(file_content).decode(),
            }
        )
        transfer.handle_incoming(chunk_msg)

        # Send complete
        complete_msg = A2AMessage.create(
            sender="morzsa", recipient="nova",
            msg_type="file_transfer", priority=5,
            payload={
                "transfer_type": "file_complete",
                "file_id": "test-complete-001",
                "file_hash": file_hash,
                "total_chunks": 1,
            }
        )
        ack_msg = transfer.handle_incoming(complete_msg)
        assert ack_msg is not None
        ack_payload = ack_msg.payload if isinstance(ack_msg.payload, dict) else {}
        assert ack_payload["transfer_type"] == "file_ack"
        assert ack_payload["success"] == True

        # Verify file was written
        output_path = os.path.join(transfer.incoming_dir, "hello.txt")
        assert os.path.exists(output_path)
        with open(output_path, 'rb') as f:
            assert f.read() == file_content


class TestTransferStats:
    def test_get_stats(self, store_and_transfer):
        store, transfer = store_and_transfer
        stats = transfer.get_transfer_stats()
        assert "incoming_transfers" in stats
        assert "outgoing_transfers" in stats
        assert "chunk_size" in stats
        assert stats["chunk_size"] == CHUNK_SIZE