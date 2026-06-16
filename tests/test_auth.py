"""Test core.auth — Node authentication (open, whitelist, TOFU)"""
import pytest
import time
from a2a_mesh.core.auth import AuthMode, AuthConfig, NodeAuthenticator, JoinRequest


def _make_join(node_name="test_node", public_key="test_key"):
    return JoinRequest(
        node_name=node_name,
        node_role="router",
        public_key=public_key,
        timestamp=time.time(),
        nonce="test_nonce",
    )


class TestNodeAuth:
    """Test node authentication modes."""

    def test_open_mode_accepts_all(self):
        config = AuthConfig(mode="open")
        auth = NodeAuthenticator(config)
        result, reason = auth.authenticate_join(_make_join("unknown_node", "any_key"))
        assert result is True

    def test_whitelist_mode_accepts_known(self):
        config = AuthConfig(mode="whitelist", whitelist={"morzsa"})
        auth = NodeAuthenticator(config)
        result, reason = auth.authenticate_join(_make_join("morzsa", "key123"))
        assert result is True

    def test_whitelist_mode_rejects_unknown(self):
        config = AuthConfig(mode="whitelist", whitelist={"morzsa"})
        auth = NodeAuthenticator(config)
        result, reason = auth.authenticate_join(_make_join("unknown", "key"))
        assert result is False

    def test_tofu_mode_first_accept(self):
        """Trust On First Use — accept first connection."""
        config = AuthConfig(mode="tofu")
        auth = NodeAuthenticator(config)
        result, reason = auth.authenticate_join(_make_join("new_node", "new_key"))
        assert result is True

    def test_tofu_mode_subsequent_same_key(self):
        config = AuthConfig(mode="tofu")
        auth = NodeAuthenticator(config)
        auth.authenticate_join(_make_join("node1", "key1"))
        result, reason = auth.authenticate_join(_make_join("node1", "key1"))
        assert result is True

    def test_tofu_mode_subsequent_different_key(self):
        config = AuthConfig(mode="tofu")
        auth = NodeAuthenticator(config)
        auth.authenticate_join(_make_join("node1", "key1"))
        result, reason = auth.authenticate_join(_make_join("node1", "key2"))
        assert result is False

    def test_auth_mode_enum_values(self):
        assert AuthMode.OPEN.value == "open"
        assert AuthMode.WHITELIST.value == "whitelist"
        assert AuthMode.TRUST_ON_FIRST_USE.value == "tofu"