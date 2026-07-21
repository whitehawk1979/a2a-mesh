"""A2A Mesh File Transfer — Multi-tier file sharing between mesh nodes.

Tiers based on file size:
  <1MB  → PG base64 (embedded in message payload)
  1-50MB → MinIO S3 (upload + URL in message)
  >50MB → SCP/SMB (manual, out-of-band)

Usage:
    ft = FileTransfer(config)
    
    # Send a file
    result = await ft.send_file(
        local_path="/path/to/file.pdf",
        recipient="morzsa",
        msg_type="file_share",
    )
    
    # Receive a file
    saved_path = await ft.receive_file(message)
"""

import asyncio
import base64
import hashlib
import json
import logging
import os
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Tuple

from .core.message import A2AMessage
from .core.config import MeshConfig

log = logging.getLogger("a2a_mesh.file_transfer")

# Size thresholds (in bytes)
PG_THRESHOLD = 1 * 1024 * 1024       # 1 MB
MINIO_THRESHOLD = 50 * 1024 * 1024    # 50 MB

# MinIO bucket paths
MINIO_BUCKET = "agent-share"


@dataclass
class FileTransferResult:
    """Result of a file transfer operation."""
    success: bool
    method: str  # "pg_base64", "minio_s3", "scp", "failed"
    message_id: str = ""
    remote_path: str = ""
    file_size: int = 0
    sha256: str = ""
    error: str = ""


class FileTransfer:
    """Multi-tier file transfer for A2A mesh nodes."""

    def __init__(self, config: MeshConfig):
        self.config = config
        self.node_name = config.node_name
        self._mc_alias = "morzsa-s3"
        self._minio_endpoint = f"http://{config.pg.host}:9000"  # Same host as PG
        self._minio_access = "nova"
        self._minio_secret = "NovaAgent2026"
        self._download_dir = os.path.expanduser("~/.hermes/mesh_downloads")
        os.makedirs(self._download_dir, exist_ok=True)

    def _classify_size(self, size_bytes: int) -> str:
        """Determine transfer method based on file size."""
        if size_bytes < PG_THRESHOLD:
            return "pg_base64"
        elif size_bytes < MINIO_THRESHOLD:
            return "minio_s3"
        else:
            return "scp"

    def _sha256_file(self, path: str) -> str:
        """Calculate SHA-256 hash of a file."""
        h = hashlib.sha256()
        with open(path, "rb") as f:
            while chunk := f.read(8192):
                h.update(chunk)
        return h.hexdigest()

    # ─── SEND ────────────────────────────────────────────────────

    async def send_file(
        self,
        local_path: str,
        recipient: str,
        msg_type: str = "file_share",
        description: str = "",
        priority: int = 5,
        metadata: Optional[dict] = None,
    ) -> FileTransferResult:
        """Send a file to a mesh node via the appropriate transfer method."""
        path = Path(local_path)
        if not path.exists():
            return FileTransferResult(False, "failed", error=f"File not found: {local_path}")

        file_size = path.stat().st_size
        sha256 = self._sha256_file(str(path))
        method = self._classify_size(file_size)

        log.info(f"Sending file {path.name} ({file_size} bytes) via {method}")

        # Build payload
        payload = {
            "file_transfer": True,
            "filename": path.name,
            "file_size": file_size,
            "sha256": sha256,
            "transfer_method": method,
            "description": description,
            "sender_node": self.node_name,
        }
        if metadata:
            payload["metadata"] = metadata

        if method == "pg_base64":
            # Embed file directly in message
            with open(str(path), "rb") as f:
                file_data = base64.b64encode(f.read()).decode("ascii")
            payload["data_base64"] = file_data
            msg = A2AMessage.create(
                sender=self.node_name,
                recipient=recipient,
                msg_type=msg_type,
                payload=payload,
                priority=priority,
            )
            return FileTransferResult(
                success=True,
                method="pg_base64",
                message_id=msg.id,
                file_size=file_size,
                sha256=sha256,
            )

        elif method == "minio_s3":
            # Upload to MinIO, put URL in message
            remote_path = await self._upload_to_minio(str(path), recipient)
            if not remote_path:
                return FileTransferResult(False, "minio_s3", error="MinIO upload failed")

            payload["minio_path"] = remote_path
            payload["minio_bucket"] = MINIO_BUCKET
            msg = A2AMessage.create(
                sender=self.node_name,
                recipient=recipient,
                msg_type=msg_type,
                payload=payload,
                priority=priority,
            )
            return FileTransferResult(
                success=True,
                method="minio_s3",
                message_id=msg.id,
                remote_path=remote_path,
                file_size=file_size,
                sha256=sha256,
            )

        else:  # scp
            # Provide SCP instructions in message
            payload["scp_instructions"] = {
                "source": f"{self.node_name}:{str(path)}",
                "filename": path.name,
                "file_size": file_size,
                "sha256": sha256,
                "note": "File too large for automatic transfer. Use SCP/SMB to copy manually.",
            }
            msg = A2AMessage.create(
                sender=self.node_name,
                recipient=recipient,
                msg_type=msg_type,
                payload=payload,
                priority=priority,
            )
            return FileTransferResult(
                success=True,
                method="scp",
                message_id=msg.id,
                file_size=file_size,
                sha256=sha256,
            )

    # ─── RECEIVE ─────────────────────────────────────────────────

    async def receive_file(
        self,
        message: A2AMessage,
        output_dir: Optional[str] = None,
    ) -> Optional[str]:
        """Receive a file from a mesh message and save to disk.
        
        Returns the local file path on success, None on failure.
        """
        payload = message.payload
        if not payload.get("file_transfer"):
            log.error("Message is not a file transfer")
            return None

        method = payload.get("transfer_method", "unknown")
        filename = payload.get("filename", "unknown_file")
        expected_sha256 = payload.get("sha256", "")
        file_size = payload.get("file_size", 0)
        
        out_dir = output_dir or self._download_dir
        os.makedirs(out_dir, exist_ok=True)
        local_path = os.path.join(out_dir, filename)

        log.info(f"Receiving file {filename} ({file_size} bytes) via {method}")

        if method == "pg_base64":
            # Decode base64 data from payload
            data_b64 = payload.get("data_base64")
            if not data_b64:
                log.error("No base64 data in payload")
                return None

            try:
                file_data = base64.b64decode(data_b64)
                with open(local_path, "wb") as f:
                    f.write(file_data)
            except Exception as e:
                log.error(f"Base64 decode failed: {e}")
                return None

        elif method == "minio_s3":
            # Download from MinIO
            minio_path = payload.get("minio_path")
            bucket = payload.get("minio_bucket", MINIO_BUCKET)
            if not minio_path:
                log.error("No MinIO path in payload")
                return None

            success = await self._download_from_minio(minio_path, local_path, bucket)
            if not success:
                log.error("MinIO download failed")
                return None

        else:  # scp
            log.warning(f"File {filename} too large for automatic transfer. SCP required.")
            # Save instructions file
            scp_info = payload.get("scp_instructions", {})
            instructions_path = os.path.join(out_dir, f"{filename}.scp_instructions")
            with open(instructions_path, "w") as f:
                json.dump(scp_info, f, indent=2)
            log.info(f"SCP instructions saved to {instructions_path}")
            return instructions_path

        # Verify SHA-256
        if expected_sha256:
            actual_sha256 = self._sha256_file(local_path)
            if actual_sha256 != expected_sha256:
                log.error(f"SHA-256 mismatch! Expected {expected_sha256}, got {actual_sha256}")
                os.remove(local_path)
                return None
            log.info(f"SHA-256 verified ✅")

        log.info(f"File saved to {local_path} ({os.path.getsize(local_path)} bytes)")
        return local_path

    # ─── MINIO OPERATIONS ─────────────────────────────────────────

    async def _upload_to_minio(self, local_path: str, recipient: str) -> Optional[str]:
        """Upload file to MinIO S3. Returns the object path."""
        filename = os.path.basename(local_path)
        # Path: {sender}/{recipient}/{filename}
        remote_path = f"{self.node_name}/{recipient}/{filename}"

        try:
            cmd = [
                "mc", "cp", local_path,
                f"{self._mc_alias}/{MINIO_BUCKET}/{remote_path}"
            ]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
            if result.returncode == 0:
                log.info(f"Uploaded {filename} to MinIO: {remote_path}")
                return remote_path
            else:
                log.error(f"MinIO upload failed: {result.stderr}")
                return None
        except Exception as e:
            log.error(f"MinIO upload error: {e}")
            return None

    async def _download_from_minio(
        self, remote_path: str, local_path: str, bucket: str = MINIO_BUCKET
    ) -> bool:
        """Download file from MinIO S3."""
        try:
            cmd = [
                "mc", "cp",
                f"{self._mc_alias}/{bucket}/{remote_path}",
                local_path
            ]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
            if result.returncode == 0:
                log.info(f"Downloaded {remote_path} from MinIO")
                return True
            else:
                log.error(f"MinIO download failed: {result.stderr}")
                return False
        except Exception as e:
            log.error(f"MinIO download error: {e}")
            return False

    # ─── CLI HELPERS ──────────────────────────────────────────────

    def list_shared_files(self, prefix: str = "") -> list:
        """List files available in MinIO for this node."""
        try:
            cmd = ["mc", "ls", f"{self._mc_alias}/{MINIO_BUCKET}/{prefix}"]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
            if result.returncode == 0:
                files = []
                for line in result.stdout.strip().split("\n"):
                    if line.strip():
                        # mc ls format: [date] [size] [path]
                        parts = line.strip().split()
                        if len(parts) >= 3:
                            files.append({
                                "path": parts[-1],
                                "size": parts[-2],
                                "date": " ".join(parts[:3]),
                            })
                return files
            return []
        except Exception as e:
            log.error(f"MinIO list error: {e}")
            return []

    def get_download_url(self, remote_path: str, bucket: str = MINIO_BUCKET, expires: str = "7d") -> Optional[str]:
        """Generate a presigned download URL for a MinIO object."""
        try:
            cmd = ["mc", "share", "download", f"--expire", expires,
                   f"{self._mc_alias}/{bucket}/{remote_path}"]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
            if result.returncode == 0:
                # Extract URL from output
                for line in result.stdout.strip().split("\n"):
                    if line.startswith("http"):
                        return line.strip()
            return None
        except Exception as e:
            log.error(f"MinIO share URL error: {e}")
            return None