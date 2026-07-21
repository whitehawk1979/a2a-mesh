"""Test core.config — YAML config loading, env interpolation"""
import pytest
import os
import tempfile
from a2a_mesh.core.config import MeshConfig, PGConfig

# Env vars that MeshConfig/PGConfig read from — must be saved/restored in tests
_A2A_ENV_VARS = [
    "A2A_PG_HOST", "A2A_PG_PORT", "A2A_PG_DBNAME", "A2A_PG_USER", "A2A_PG_PASSWORD",
    "A2A_MESH_PG_DSN", "A2A_HTTP_URL", "A2A_HEALTH_URL",
    "WEBHOOK_PORT", "WEBHOOK_SECRET", "A2A_TELEGRAM_CHAT_ID",
]


def _isolate_env():
    """Remove A2A env vars so they don't pollute config defaults."""
    saved = {}
    for key in _A2A_ENV_VARS:
        if key in os.environ:
            saved[key] = os.environ.pop(key)
    return saved


def _restore_env(saved):
    """Restore previously saved env vars."""
    for key in list(os.environ.keys()):
        if key in _A2A_ENV_VARS and key not in saved:
            del os.environ[key]
    os.environ.update(saved)


class TestMeshConfig:
    """Test MeshConfig loading from YAML."""

    def test_default_config(self):
        saved = _isolate_env()
        try:
            config = MeshConfig()
            assert config.node_name is not None
            assert config.pg is not None
            assert config.p2p is not None
        finally:
            _restore_env(saved)

    def test_from_yaml_full(self):
        saved = _isolate_env()
        try:
            yaml_content = """
mesh:
  node_name: test_node
  transports:
    pg_notify:
      host: 192.168.1.30
      port: 5432
      dbname: test_db
      user: test_user
      password: test_pass
      channels:
        - mesh_channel
        - a2a_channel
    p2p:
      enabled: true
      listen_host: 0.0.0.0
      listen_port: 9000
    http:
      url: http://example.com:8199
      health_url: http://example.com:8198/health
      timeout: 10
      retries: 5
  topology:
    node_role: router
    max_children: 30
  webhook:
    url: http://example.com/webhook
    secret: my_secret
"""
            with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
                f.write(yaml_content)
                f.flush()
                config = MeshConfig.from_yaml(f.name)

            assert config.node_name == "test_node"
            assert config.pg.host == "192.168.1.30"
            assert config.pg.port == 5432
            assert config.pg.password == "test_pass"
            assert config.pg.user == "test_user"
            assert config.p2p.listen_port == 9000
            assert config.http.url == "http://example.com:8199"
            assert config.http.retries == 5
            os.unlink(f.name)
        finally:
            _restore_env(saved)

    def test_from_yaml_env_interpolation(self):
        saved = _isolate_env()
        try:
            yaml_content = """
mesh:
  node_name: ${NODE_NAME}
  transports:
    pg_notify:
      host: ${PG_HOST}
      port: 5432
      dbname: agent_memory
      user: ${PG_USER}
      password: ${PG_PASSWORD}
"""
            os.environ["NODE_NAME"] = "env_node"
            os.environ["PG_HOST"] = "10.0.0.1"
            os.environ["PG_USER"] = "env_user"
            os.environ["PG_PASSWORD"] = "env_pass"

            with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
                f.write(yaml_content)
                f.flush()
                config = MeshConfig.from_yaml(f.name)

            assert config.node_name == "env_node"
            assert config.pg.host == "10.0.0.1"
            assert config.pg.user == "env_user"
            assert config.pg.password == "env_pass"

            for key in ["NODE_NAME", "PG_HOST", "PG_USER", "PG_PASSWORD"]:
                os.environ.pop(key, None)
            os.unlink(f.name)
        finally:
            _restore_env(saved)

    def test_from_yaml_missing_file(self):
        saved = _isolate_env()
        try:
            config = MeshConfig.from_yaml("/nonexistent/path.yaml")
            assert config.node_name is not None
        finally:
            _restore_env(saved)

    def test_from_yaml_partial(self):
        saved = _isolate_env()
        try:
            yaml_content = """
mesh:
  node_name: morzsa
  transports:
    pg_notify:
      host: 192.168.1.30
"""
            with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
                f.write(yaml_content)
                f.flush()
                config = MeshConfig.from_yaml(f.name)

            assert config.node_name == "morzsa"
            assert config.pg.host == "192.168.1.30"
            assert config.pg.port == 5432
            assert config.p2p.listen_port == 8645
            os.unlink(f.name)
        finally:
            _restore_env(saved)

    def test_from_yaml_discovery_static_nodes_nested(self):
        """Test discovery.static.nodes (nested YAML format) is loaded."""
        saved = _isolate_env()
        try:
            yaml_content = """
mesh:
  node_name: test_nested
  discovery:
    static:
      nodes:
        - name: nova
          ip: 192.168.1.8
          p2p_port: 8645
        - name: runa
          ip: 192.168.1.30
          p2p_port: 8646
"""
            with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
                f.write(yaml_content)
                f.flush()
                config = MeshConfig.from_yaml(f.name)

            assert len(config.discovery.static_nodes) == 2
            assert config.discovery.static_nodes[0]['name'] == 'nova'
            assert config.discovery.static_nodes[1]['name'] == 'runa'
            os.unlink(f.name)
        finally:
            _restore_env(saved)

    def test_from_yaml_discovery_static_nodes_flat(self):
        """Test discovery.static_nodes (flat YAML format) is loaded — backward compat."""
        saved = _isolate_env()
        try:
            yaml_content = """
mesh:
  node_name: test_flat
  discovery:
    static_nodes:
      - name: nova
        ip: 192.168.1.8
        p2p_port: 8645
      - name: runa
        ip: 192.168.1.30
        p2p_port: 8646
"""
            with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
                f.write(yaml_content)
                f.flush()
                config = MeshConfig.from_yaml(f.name)

            assert len(config.discovery.static_nodes) == 2
            assert config.discovery.static_nodes[0]['name'] == 'nova'
            assert config.discovery.static_nodes[1]['name'] == 'runa'
            os.unlink(f.name)
        finally:
            _restore_env(saved)
