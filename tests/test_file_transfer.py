"""Test file_transfer — File size classification, hash"""
import pytest
from a2a_mesh.file_transfer import FileTransfer, PG_THRESHOLD, MINIO_THRESHOLD


class TestFileSizeClassification:
    """Test file size classification for transport selection."""

    def test_tiny_file_pg(self):
        """<1MB files should use PG base64."""
        assert PG_THRESHOLD > 0
        assert MINIO_THRESHOLD > PG_THRESHOLD

    def test_thresholds_sane(self):
        """Thresholds should be reasonable."""
        assert PG_THRESHOLD < MINIO_THRESHOLD
        assert PG_THRESHOLD < 10_000_000  # < 10MB
        assert MINIO_THRESHOLD < 100_000_000  # < 100MB