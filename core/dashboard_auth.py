"""A2A Mesh Dashboard — Auth mixin.

User authentication, registration, login, logout, and user management endpoints.
"""

import logging

log = logging.getLogger("a2a_mesh.dashboard.auth")


class DashboardAuthMixin:
    """Auth-related endpoints extracted from DashboardHandler."""

    async def _api_auth_register(self, request):
        """Register a new user. Only owners can create other users."""
        from aiohttp import web
        caller, err = self._require_auth(request)
        if err:
            return err

        if caller.role != "owner":
            return web.json_response({"error": "Only owners can register new users"}, status=403)

        try:
            data = await request.json()
        except Exception:
            return web.json_response({"error": "Invalid JSON"}, status=400)

        username = data.get("username", "").strip().lower()
        display_name = data.get("display_name", "").strip()
        password = data.get("password", "")
        role = data.get("role", "user")

        if not username or not password:
            return web.json_response({"error": "Username and password required"}, status=400)

        try:
            user = self.auth.register_user(username, display_name or username, password, role=role)
        except ValueError as e:
            return web.json_response({"error": str(e)}, status=400)

        if not user:
            return web.json_response({"error": "Username already taken"}, status=409)

        return web.json_response({
            "status": "registered",
            "user": user.to_dict(),
        })

    async def _api_auth_login(self, request):
        """Login and get a token."""
        from aiohttp import web
        try:
            data = await request.json()
        except Exception:
            return web.json_response({"error": "Invalid JSON"}, status=400)

        username = data.get("username", "").strip()
        password = data.get("password", "")

        if not username or not password:
            return web.json_response({"error": "Username and password required"}, status=400)

        try:
            result = self.auth.login(username, password)
        except ValueError as e:
            return web.json_response({"error": str(e)}, status=429)

        if not result:
            return web.json_response({"error": "Invalid username or password"}, status=401)

        response = web.json_response({
            "status": "ok",
            "token": result["token"],
            "user": result["user"].to_dict(),
        })
        response.set_cookie("a2a_token", result["token"], max_age=86400, httponly=True, samesite="Lax")
        return response

    async def _api_auth_logout(self, request):
        """Logout and invalidate token."""
        from aiohttp import web
        auth_header = request.headers.get("Authorization", "")
        token = auth_header[7:] if auth_header.startswith("Bearer ") else ""
        if not token:
            token = request.cookies.get("a2a_token", "")

        if token:
            self.auth.logout(token)

        response = web.json_response({"status": "ok"})
        response.del_cookie("a2a_token")
        return response

    async def _api_auth_me(self, request):
        """Return current user info."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        return web.json_response({"user": user.to_dict()})

    async def _api_users(self, request):
        """List all users (owner only)."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        if user.role != "owner":
            return web.json_response({"error": "Owner access required"}, status=403)

        users = self.auth.list_users()
        return web.json_response({
            "users": [u.to_dict() for u in users],
            "total": len(users),
        })

    async def _api_auth_users(self, request):
        """List all users for management UI (owner only). Returns extended info."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        if user.role != "owner":
            return web.json_response({"error": "Owner access required"}, status=403)

        users = self.auth.list_users()
        users_data = []
        for u in users:
            d = u.to_dict()
            d.pop("password_hash", None)
            users_data.append(d)
        return web.json_response({
            "users": users_data,
            "total": len(users_data),
        })

    async def _api_auth_delete_user(self, request):
        """Delete (deactivate) a user by username (owner only)."""
        from aiohttp import web
        caller, err = self._require_auth(request)
        if err:
            return err
        if caller.role != "owner":
            return web.json_response({"error": "Owner access required"}, status=403)

        username = request.match_info.get("username", "")
        if not username:
            return web.json_response({"error": "Username required"}, status=400)

        if username.lower() == caller.username.lower():
            return web.json_response({"error": "Cannot delete your own account"}, status=400)

        target = self.auth.get_user_by_username(username)
        if not target:
            return web.json_response({"error": f"User '{username}' not found"}, status=404)

        self.auth.delete_user(target.user_id)
        log.info(f"Owner '{caller.username}' deleted user '{username}'")

        return web.json_response({
            "status": "deleted",
            "username": username.lower(),
        })

    async def _api_auth_change_password(self, request):
        """Change a user's password (owner only)."""
        from aiohttp import web
        caller, err = self._require_auth(request)
        if err:
            return err
        if caller.role != "owner":
            return web.json_response({"error": "Owner access required"}, status=403)

        username = request.match_info.get("username", "")
        if not username:
            return web.json_response({"error": "Username required"}, status=400)

        try:
            data = await request.json()
        except Exception:
            return web.json_response({"error": "Invalid JSON"}, status=400)

        new_password = data.get("new_password", "")
        if not new_password:
            return web.json_response({"error": "new_password is required"}, status=400)
        if len(new_password) < 6:
            return web.json_response({"error": "Password must be at least 6 characters"}, status=400)

        target = self.auth.get_user_by_username(username)
        if not target:
            return web.json_response({"error": f"User '{username}' not found"}, status=404)

        try:
            self.auth.change_password(target.user_id, new_password)
            log.info(f"Owner '{caller.username}' changed password for user '{username}'")
        except ValueError as e:
            return web.json_response({"error": str(e)}, status=400)

        return web.json_response({
            "status": "password_changed",
            "username": username.lower(),
        })

    async def _api_auth_sync(self, request):
        """Trigger user sync from PG. POST endpoint for manual sync."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        if user.role != "owner":
            return web.json_response({"error": "Owner access required"}, status=403)

        try:
            self.auth._sync_from_pg()
            users = self.auth.list_users()
            return web.json_response({
                "status": "synced",
                "users_pulled": len(users),
            })
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)

    async def _api_auth_sync_pull(self, request):
        """Pull users from PG into local SQLite. GET endpoint for other nodes."""
        from aiohttp import web
        user, err = self._require_auth(request)
        if err:
            return err
        if user.role != "owner":
            return web.json_response({"error": "Owner access required"}, status=403)

        try:
            self.auth._sync_from_pg()
            users = self.auth.list_users()
            return web.json_response({
                "status": "synced",
                "users": [u.to_dict() for u in users],
                "total": len(users),
            })
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)