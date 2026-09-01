import sys, os, json, base64, tempfile, shutil
sys.path.insert(0, "/Users/zsolt/.hermes/scripts/a2a_mesh")

# Force the FILE-VAULT path in an isolated temp dir (simulates a Linux/HAOS
# node with no OS keyring backend): point A2A_VAULT_FILE + KEY at temp paths.
tmp = tempfile.mkdtemp(prefix="vault_test_")
os.environ["A2A_VAULT_FILE"] = os.path.join(tmp, "vault.json")
os.environ["A2A_VAULT_KEY"] = os.path.join(tmp, "vault.key")

# Force "no OS keyring": monkeypatch the keyring lib detector
import core.vault as V
V._keyring_lib = lambda: None

fails = 0

# 1) set + get roundtrip on the file vault
ok = V.set_secret("MESH/PG_PASSWORD", "file-vault-secret-123")
if not ok:
    print("FAIL: set_secret returned False"); fails += 1
v = V.get_secret("MESH/PG_PASSWORD")
if v != "file-vault-secret-123":
    print(f"FAIL roundtrip: {v!r}"); fails += 1
else:
    mode = json.load(open(os.environ["A2A_VAULT_FILE"]))["MESH/PG_PASSWORD"]["mode"]
    print(f"file vault roundtrip: OK (mode={mode})")

# 2) plaintext NOT on disk
raw = open(os.environ["A2A_VAULT_FILE"], "rb").read()
if b"file-vault-secret-123" in raw:
    print("FAIL: plaintext visible in vault file"); fails += 1
else:
    print("plaintext off-disk: OK")

# 3) key file permissions 0600
perms = os.stat(os.environ["A2A_VAULT_KEY"]).st_mode & 0o777
if perms == 0o600:
    print("key file 0600: OK")
else:
    print(f"FAIL key perms: {oct(perms)}"); fails += 1

# 4) tamper detection (HMAC)
store = json.load(open(os.environ["A2A_VAULT_FILE"]))
blob = store["MESH/PG_PASSWORD"]["blob"]
tampered = base64.b64decode(blob)
tampered = tampered[:-1] + bytes([tampered[-1] ^ 1])
store["MESH/PG_PASSWORD"]["blob"] = base64.b64encode(tampered).decode()
json.dump(store, open(os.environ["A2A_VAULT_FILE"], "w"))
v = V.get_secret("MESH/PG_PASSWORD")
if v is not None:
    print("FAIL: tampered blob accepted"); fails += 1
else:
    print("tamper detection: OK")

# 5) resolve_config_value through the file vault
V.set_secret("MESH/TEST_REF", "ref-value-42")
r = V.resolve_config_value("vault:MESH/TEST_REF")
if r != "ref-value-42":
    print(f"FAIL vault: ref: {r!r}"); fails += 1
else:
    print("vault: reference: OK")

# 6) env fallback
os.environ["A2A_VAULT_ENV_ONLY"] = "env-secret-7"
v = V.get_secret("MESH/ENV_ONLY")
if v != "env-secret-7":
    print(f"FAIL env fallback: {v!r}"); fails += 1
else:
    print("env fallback: OK")

# 7) backend info string
print("backends:", V.backend_info())

shutil.rmtree(tmp)
print()
print("RESULT:", "ALL PASSED" if fails == 0 else f"{fails} FAILURES")