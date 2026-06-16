"""Test core.auth — Dashboard user authentication"""
import pytest
import os
import tempfile
import time
from a2a_mesh.core.auth import AuthManager, DashboardUser


@pytest.fixture
def auth(tmp_path):
    """Create a fresh AuthManager with temp DB."""
    db_path = str(tmp_path / "test_users.db")
    return AuthManager(db_path=db_path)


class TestAuthRegistration:
    """Test user registration."""

    def test_register_user(self, auth):
        user = auth.register_user("testuser", "Test User", "password123")
        assert user is not None
        assert user.username == "testuser"
        assert user.display_name == "Test User"
        assert user.role == "user"

    def test_register_owner(self, auth):
        user = auth.register_user("admin", "Admin", "admin123", role="owner")
        assert user is not None
        assert user.role == "owner"

    def test_register_duplicate_fails(self, auth):
        auth.register_user("testuser", "Test", "pass123")
        result = auth.register_user("testuser", "Another", "pass456")
        assert result is None

    def test_register_short_username_fails(self, auth):
        with pytest.raises(ValueError):
            auth.register_user("a", "A", "pass123")

    def test_register_short_password_fails(self, auth):
        with pytest.raises(ValueError):
            auth.register_user("testuser", "Test", "12345")

    def test_register_invalid_role_fails(self, auth):
        with pytest.raises(ValueError):
            auth.register_user("testuser", "Test", "password", role="superadmin")

    def test_default_owner_created(self, tmp_path):
        db_path = str(tmp_path / "fresh.db")
        auth = AuthManager(db_path=db_path)
        # Default owner 'zsolt' should exist
        result = auth.login("zsolt", "mesh2026")
        assert result is not None
        assert result["user"].role == "owner"


class TestAuthLogin:
    """Test user login."""

    def test_login_success(self, auth):
        auth.register_user("testuser", "Test User", "password123")
        result = auth.login("testuser", "password123")
        assert result is not None
        assert result["user"].username == "testuser"
        assert result["token"] is not None

    def test_login_case_insensitive(self, auth):
        auth.register_user("TestUser", "Test", "password123")
        result = auth.login("testuser", "password123")
        assert result is not None

    def test_login_wrong_password(self, auth):
        auth.register_user("testuser", "Test", "password123")
        result = auth.login("testuser", "wrongpass")
        assert result is None

    def test_login_nonexistent_user(self, auth):
        result = auth.login("nobody", "password123")
        assert result is None

    def test_login_returns_token(self, auth):
        auth.register_user("testuser", "Test", "password123")
        result = auth.login("testuser", "password123")
        assert ":" in result["token"]  # JWT-like format

    def test_login_updates_last_login(self, auth):
        auth.register_user("testuser", "Test", "password123")
        time.sleep(0.01)
        auth.login("testuser", "password123")
        user = auth.get_user(auth.login("testuser", "password123")["user"].user_id)
        assert user.last_login > 0


class TestAuthToken:
    """Test token verification."""

    def test_verify_valid_token(self, auth):
        auth.register_user("testuser", "Test", "password123")
        result = auth.login("testuser", "password123")
        user = auth.verify_token(result["token"])
        assert user is not None
        assert user.username == "testuser"

    def test_verify_invalid_token(self, auth):
        user = auth.verify_token("invalid_token")
        assert user is None

    def test_logout_invalidates_token(self, auth):
        auth.register_user("testuser", "Test", "password123")
        result = auth.login("testuser", "password123")
        token = result["token"]
        auth.logout(token)
        user = auth.verify_token(token)
        assert user is None


class TestAuthRateLimit:
    """Test login rate limiting."""

    def test_rate_limit_after_5_attempts(self, auth):
        auth.register_user("testuser", "Test", "password123")
        for i in range(5):
            auth.login("testuser", "wrongpass")

        with pytest.raises(ValueError, match="Too many"):
            auth.login("testuser", "wrongpass")


class TestAuthUserManagement:
    """Test user management."""

    def test_list_users(self, auth):
        auth.register_user("user1", "User One", "pass123")
        auth.register_user("user2", "User Two", "pass456")
        users = auth.list_users()
        # 2 registered + default owner (zsolt) from AuthManager init
        assert len(users) >= 2

    def test_update_user(self, auth):
        user = auth.register_user("testuser", "Test", "password123")
        auth.update_user(user.user_id, display_name="Updated Name")
        updated = auth.get_user(user.user_id)
        assert updated.display_name == "Updated Name"

    def test_change_password(self, auth):
        user = auth.register_user("testuser", "Test", "password123")
        auth.change_password(user.user_id, "newpassword")
        result = auth.login("testuser", "newpassword")
        assert result is not None

    def test_delete_user(self, auth):
        user = auth.register_user("testuser", "Test", "password123")
        auth.delete_user(user.user_id)
        deleted = auth.get_user(user.user_id)
        assert deleted.is_active is False

    def test_cleanup_expired_sessions(self, auth):
        auth.register_user("testuser", "Test", "password123")
        result = auth.login("testuser", "password123")
        auth.cleanup_sessions()
        # Token should still be valid (24h expiry)
        user = auth.verify_token(result["token"])
        assert user is not None