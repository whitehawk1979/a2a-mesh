"""A2A Mesh P2P File Transfer — Direct file transfer over P2P channel.

When MinIO is unavailable, files can be transferred directly between peers
via the P2P TCP connection using chunked transfer protocol.

Protocol:
1. Sender sends FILE_OFFER message (metadata: filename, size, chunk_count)
2. Receiver sends FILE_ACCEPT or FILE_REJECT
3. Sender transmits FILE_CHUNK messages (64KB chunks)
4. After last chunk, sender sends FILE_COMPLETE
5. Receiver confirms with FILE_ACK

Messages use the standard A2A message envelope with special payload types.
"""
import asyncio
import base64
import hashlib
import logging
import os
import time
from typing import Optional, Dict, Tuple
from pathlib import Path

from ..core.message import A2AMessage
from ..core.local_store import LocalStore

log = logging.getLogger("a2a_mesh.file_transfer")

# File transfer payload types
FILE_OFFER = "file_offer"
FILE_ACCEPT = "file_accept"
FILE_REJECT = "file_reject"
FILE_CHUNK = "file_chunk"
FILE_COMPLETE = "file_complete"
FILE_ACK = "file_ack"

# Max chunk size: 512KB for P2P (large files), base64 encoded = ~683KB in message
# P2P message max is 10MB, so this leaves plenty of room
CHUNK_SIZE = 512 * 1024


class P2PFileTransfer:
    """Direct file transfer over P2P channel.

    Handles:
    - Breaking files into chunks
    - Encoding chunks into A2A messages
    - Reassembling chunks on the receiving side
    - Integrity verification (SHA-256)
    - Transfer state tracking via LocalStore
    """

    def __init__(self, node_name: str, local_store: LocalStore, 
                 incoming_dir: Optional[str] = None):
        self.node_name = node_name
        self.local_store = local_store
        self.incoming_dir = incoming_dir or os.path.expanduser(
            "~/.hermes/scripts/a2a_mesh/incoming_files"
        )
        os.makedirs(self.incoming_dir, exist_ok=True)

        # Active transfers: file_id → {chunks, received, checksum, etc.}
        self._incoming: Dict[str, Dict] = {}
        self._outgoing: Dict[str, Dict] = {}

    def create_offer_message(self, file_path: str, recipient: str,
                             priority: int = 5) -> Tuple[A2AMessage, str]:
        """Create a FILE_OFFER message for a file.

        Returns (message, file_id).
        """
        file_path = os.path.expanduser(file_path)
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")

        filename = os.path.basename(file_path)
        file_size = os.path.getsize(file_path)

        # Calculate SHA-256 of entire file
        sha256 = hashlib.sha256()
        with open(file_path, 'rb') as f:
            while chunk := f.read(CHUNK_SIZE):
                sha256.update(chunk)
        file_hash = sha256.hexdigest()

        # Calculate chunk count
        chunk_count = (file_size + CHUNK_SIZE - 1) // CHUNK_SIZE

        file_id = hashlib.md5(f"{filename}:{file_size}:{time.time()}".encode()).hexdigest()[:16]

        # Store outgoing transfer state
        self._outgoing[file_id] = {
            "file_path": file_path,
            "filename": filename,
            "file_size": file_size,
            "file_hash": file_hash,
            "chunk_count": chunk_count,
            "current_chunk": 0,
            "recipient": recipient,
            "started_at": time.time(),
        }

        # Also store in local_store for persistence
        self.local_store.store_file_chunk(
            file_id=file_id, filename=filename, size=file_size,
            sender=self.node_name, recipient=recipient,
            transfer_method="p2p", chunk_index=0, total_chunks=chunk_count,
        )

        msg = A2AMessage.create(
            sender=self.node_name,
            recipient=recipient,
            msg_type="file_transfer",
            priority=priority,
            payload={
                "transfer_type": FILE_OFFER,
                "file_id": file_id,
                "filename": filename,
                "file_size": file_size,
                "file_hash": file_hash,
                "chunk_count": chunk_count,
                "chunk_size": CHUNK_SIZE,
            }
        )

        log.info(f"FILE_OFFER: {filename} ({file_size}B, {chunk_count} chunks) → {recipient}")
        return msg, file_id

    def create_chunk_message(self, file_id: str, chunk_index: int,
                             recipient: str, priority: int = 3) -> Optional[A2AMessage]:
        """Create a FILE_CHUNK message for a specific chunk.

        Reads the chunk from disk and encodes it as base64 in the payload.
        """
        transfer = self._outgoing.get(file_id)
        if not transfer:
            log.error(f"No outgoing transfer found for {file_id}")
            return None

        file_path = transfer["file_path"]
        offset = chunk_index * CHUNK_SIZE

        try:
            with open(file_path, 'rb') as f:
                f.seek(offset)
                chunk_data = f.read(CHUNK_SIZE)
        except Exception as e:
            log.error(f"Failed to read chunk {chunk_index} of {file_id}: {e}")
            return None

        if not chunk_data:
            return None

        # Calculate chunk hash for integrity
        chunk_hash = hashlib.sha256(chunk_data).hexdigest()[:16]

        msg = A2AMessage.create(
            sender=self.node_name,
            recipient=recipient,
            msg_type="file_transfer",
            priority=priority,
            payload={
                "transfer_type": FILE_CHUNK,
                "file_id": file_id,
                "chunk_index": chunk_index,
                "chunk_hash": chunk_hash,
                "data": base64.b64encode(chunk_data).decode('ascii'),
            }
        )

        log.debug(f"FILE_CHUNK {chunk_index}/{transfer['chunk_count']} → {recipient}")
        return msg

    def create_complete_message(self, file_id: str, recipient: str) -> A2AMessage:
        """Create a FILE_COMPLETE message after all chunks are sent."""
        transfer = self._outgoing.get(file_id, {})
        msg = A2AMessage.create(
            sender=self.node_name,
            recipient=recipient,
            msg_type="file_transfer",
            priority=5,
            payload={
                "transfer_type": FILE_COMPLETE,
                "file_id": file_id,
                "file_hash": transfer.get("file_hash", ""),
                "total_chunks": transfer.get("chunk_count", 0),
            }
        )
        log.info(f"FILE_COMPLETE: {file_id} → {recipient}")
        return msg

    def handle_incoming(self, message: A2AMessage) -> Optional[A2AMessage]:
        """Handle an incoming file transfer message.

        Returns a response message if needed (FILE_ACCEPT/REJECT/ACK).
        """
        payload = message.payload if isinstance(message.payload, dict) else {}
        if isinstance(message.payload, str):
            try:
                payload = json.loads(message.payload)
            except Exception:
                payload = {}

        transfer_type = payload.get("transfer_type", "")
        file_id = payload.get("file_id", "")

        if transfer_type == FILE_OFFER:
            return self._handle_offer(message, payload)
        elif transfer_type == FILE_CHUNK:
            return self._handle_chunk(message, payload)
        elif transfer_type == FILE_COMPLETE:
            return self._handle_complete(message, payload)
        elif transfer_type == FILE_ACK:
            self._handle_ack(message, payload)
            return None

        return None

    def _handle_offer(self, message: A2AMessage, payload: Dict) -> A2AMessage:
        """Handle FILE_OFFER — accept or reject."""
        file_id = payload.get("file_id", "")
        filename = payload.get("filename", "unknown")
        file_size = payload.get("file_size", 0)
        file_hash = payload.get("file_hash", "")
        chunk_count = payload.get("chunk_count", 0)

        # Check disk space (need at least 2x file_size)
        stat = os.statvfs(self.incoming_dir)
        free_space = stat.f_bavail * stat.f_frsize
        if file_size > free_space * 0.5:
            log.warning(f"Rejecting file {filename}: insufficient disk space")
            return A2AMessage.create(
                sender=self.node_name,
                recipient=message.sender,
                msg_type="file_transfer",
                priority=message.priority,
                payload={
                    "transfer_type": FILE_REJECT,
                    "file_id": file_id,
                    "reason": "insufficient disk space",
                }
            )

        # Initialize incoming transfer state
        self._incoming[file_id] = {
            "filename": filename,
            "file_size": file_size,
            "file_hash": file_hash,
            "chunk_count": chunk_count,
            "chunks_received": set(),
            "chunks_data": {},
            "sender": message.sender,
            "started_at": time.time(),
        }

        log.info(f"FILE_ACCEPT: {filename} ({file_size}B) from {message.sender}")

        return A2AMessage.create(
            sender=self.node_name,
            recipient=message.sender,
            msg_type="file_transfer",
            priority=message.priority,
            payload={
                "transfer_type": FILE_ACCEPT,
                "file_id": file_id,
                "ready": True,
            }
        )

    def _handle_chunk(self, message: A2AMessage, payload: Dict) -> None:
        """Handle FILE_CHUNK — store chunk data."""
        import json  # lazy import for payload parsing

        file_id = payload.get("file_id", "")
        chunk_index = payload.get("chunk_index", 0)
        chunk_hash = payload.get("chunk_hash", "")
        data_b64 = payload.get("data", "")

        transfer = self._incoming.get(file_id)
        if not transfer:
            log.warning(f"Received chunk for unknown transfer {file_id}")
            return None

        # Decode chunk data
        try:
            chunk_data = base64.b64decode(data_b64)
        except Exception as e:
            log.error(f"Failed to decode chunk {chunk_index}: {e}")
            return None

        # Verify chunk hash
        actual_hash = hashlib.sha256(chunk_data).hexdigest()[:16]
        if chunk_hash and actual_hash != chunk_hash:
            log.error(f"Chunk {chunk_index} hash mismatch: expected {chunk_hash}, got {actual_hash}")
            return None

        # Store chunk
        transfer["chunks_data"][chunk_index] = chunk_data
        transfer["chunks_received"].add(chunk_index)

        progress = len(transfer["chunks_received"]) / transfer["chunk_count"] * 100
        log.debug(f"FILE_CHUNK {chunk_index}: {progress:.0f}% complete for {transfer['filename']}")

        return None  # Chunks are acknowledged by FILE_COMPLETE handler

    def _handle_complete(self, message: A2AMessage, payload: Dict) -> A2AMessage:
        """Handle FILE_COMPLETE — reassemble file and verify hash."""
        file_id = payload.get("file_id", "")
        expected_hash = payload.get("file_hash", "")

        transfer = self._incoming.get(file_id)
        if not transfer:
            log.warning(f"Received COMPLETE for unknown transfer {file_id}")
            return None

        # Check if all chunks received
        if len(transfer["chunks_received"]) != transfer["chunk_count"]:
            missing = set(range(transfer["chunk_count"])) - transfer["chunks_received"]
            log.warning(f"Missing {len(missing)} chunks for {transfer['filename']}: {missing}")

        # Reassemble file
        output_path = os.path.join(self.incoming_dir, transfer["filename"])
        sha256 = hashlib.sha256()

        try:
            with open(output_path, 'wb') as f:
                for i in range(transfer["chunk_count"]):
                    chunk_data = transfer["chunks_data"].get(i, b'')
                    f.write(chunk_data)
                    sha256.update(chunk_data)
        except Exception as e:
            log.error(f"Failed to write file {transfer['filename']}: {e}")
            return A2AMessage.create(
                sender=self.node_name,
                recipient=message.sender,
                msg_type="file_transfer",
                priority=3,
                payload={
                    "transfer_type": FILE_REJECT,
                    "file_id": file_id,
                    "reason": f"write failed: {e}",
                }
            )

        # Verify file hash
        actual_hash = sha256.hexdigest()
        if expected_hash and actual_hash != expected_hash:
            log.error(f"File hash mismatch: expected {expected_hash[:16]}, got {actual_hash[:16]}")
            os.unlink(output_path)
            return A2AMessage.create(
                sender=self.node_name,
                recipient=message.sender,
                msg_type="file_transfer",
                priority=3,
                payload={
                    "transfer_type": FILE_REJECT,
                    "file_id": file_id,
                    "reason": "hash mismatch",
                }
            )

        # Mark as complete in local store
        self.local_store.mark_file_complete(file_id)

        # Cleanup transfer state
        duration = time.time() - transfer["started_at"]
        log.info(f"FILE received: {transfer['filename']} ({transfer['file_size']}B) in {duration:.1f}s")

        # Cleanup incoming state
        del self._incoming[file_id]

        return A2AMessage.create(
            sender=self.node_name,
            recipient=message.sender,
            msg_type="file_transfer",
            priority=3,
            payload={
                "transfer_type": FILE_ACK,
                "file_id": file_id,
                "file_hash": actual_hash,
                "success": True,
                "output_path": output_path,
            }
        )

    def _handle_ack(self, message: A2AMessage, payload: Dict) -> None:
        """Handle FILE_ACK — sender confirms receiver got the file."""
        file_id = payload.get("file_id", "")
        success = payload.get("success", False)

        if file_id in self._outgoing:
            if success:
                log.info(f"FILE transfer confirmed: {file_id}")
                # Cleanup outgoing state
                del self._outgoing[file_id]
            else:
                log.warning(f"FILE transfer failed: {file_id}")

    def get_transfer_stats(self) -> Dict:
        """Get file transfer statistics."""
        return {
            "incoming_transfers": len(self._incoming),
            "outgoing_transfers": len(self._outgoing),
            "incoming_dir": self.incoming_dir,
            "chunk_size": CHUNK_SIZE,
        }


# Lazy import for json
import json