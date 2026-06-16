"""Test core.config — YAML config loading, env interpolation"""
import pytest
import os
import tempfile
from a2a_mesh.core.config import MeshConfig, PGConfig


class TestMeshConfig:
    """Test MeshConfig loading from YAML."""

    def test_default_config(self):
        config = MeshConfig()
        assert config.node_name is not None
        assert config.pg is not None
        assert config.p2p is not None

    def test_from_yaml_full(self):
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
        assert config.p2p.listen_port == 9000
        assert config.http.url == "http://example.com:8199"
        assert config.http.retries == 5
        os.unlink(f.name)

    def test_from_yaml_env_interpolation(self):
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

    def test_from_yaml_missing_file(self):
        config = MeshConfig.from_yaml("/nonexistent/path.yaml")
        assert config.node_name is not None

    def test_from_yaml_partial(self):
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