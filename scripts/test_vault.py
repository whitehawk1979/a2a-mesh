import sys
sys.path.insert(0, "/Users/zsolt/.hermes/scripts/a2a_mesh")

from core.vault import get_secret, set_secret, resolve_config_value

fails = 0

# 1) set + get roundtrip (OS keyring on macOS)
set_secret("MESH/TEST_VAULT_KEY", "test-secret-value-123")
v = get_secret("MESH/TEST_VAULT_KEY")
if v != "test-secret-value-123":
    print(f"FAIL roundtrip: {v!r}"); fails += 1
else:
    print("roundtrip (OS keyring): OK")

# 2) resolve_config_value with vault: reference
r = resolve_config_value("vault:MESH/TEST_VAULT_KEY")
if r != "test-secret-value-123":
    print(f"FAIL vault: reference: {r!r}"); fails += 1
else:
    print("vault: reference resolution: OK")

# 3) passthrough for plaintext
r = resolve_config_value("plaintext-password")
if r != "plaintext-password":
    print(f"FAIL passthrough: {r!r}"); fails += 1
else:
    print("plaintext passthrough: OK")

# 4) unresolved reference passes through (legacy behavior, logged upstream)
r = resolve_config_value("vault:MESH/DOES_NOT_EXIST")
if r == "vault:MESH/DOES_NOT_EXIST":
    print("unresolved reference passthrough: OK")
else:
    print(f"FAIL unresolved: {r!r}"); fails += 1

# 5) config loader integration: load the real mesh config with vault-resolved PG password
from core.config import MeshConfig
cfg = MeshConfig.from_yaml("/Users/zsolt/.hermes/scripts/a2a_mesh/mesh_config_nova.yaml")
if cfg.pg.password:
    print(f"config load with pg password: OK (resolved, len={len(cfg.pg.password)})")
else:
    print("FAIL config pg password empty"); fails += 1

print()
print("RESULT:", "ALL PASSED" if fails == 0 else f"{fails} FAILURES")