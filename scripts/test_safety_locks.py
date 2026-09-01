import sys
sys.path.insert(0, "/Users/zsolt/.hermes/scripts/a2a_mesh")
from core.safety_locks import check_command, SafetyLocks

blocked = [
    "dd if=zerofile of=/dev/sda",           # raw write to device
    "mkfs.ext4 /dev/sdb1",
    "rm -rf /",
    "rm -rf ~/",
    "shutdown -h now",
    "systemctl reboot",
    "iptables -F",
    "git reset --hard origin/main",
    "DROP TABLE agent_memory",
    "TRUNCATE TABLE shared_delegations",
    "wipefs /dev/sda1",
]
allowed = [
    "ls -la /tmp",
    "grep -r pattern core/",
    "git pull origin main",
    "curl -s http://127.0.0.1:8650/health",
    "DROP TABLE IF EXISTS messages_fts_data",  # FTS exempt
    "rm -rf /tmp/build",  # not / or ~
    "systemctl --user restart a2a-mesh",
    "docker ps",
]
fails = 0
for c in blocked:
    r = check_command(c)
    if not r:
        print(f"FAIL (not blocked): {c}"); fails += 1
    else:
        print(f"  blocked: {c[:33]:35} -> {r}")
for c in allowed:
    r = check_command(c)
    if r:
        print(f"FAIL (false positive): {c} -> {r}"); fails += 1
    else:
        print(f"  allowed: {c[:45]}")
# allowlist escape
sl = SafetyLocks(allowlist=["systemctl reboot --approved-maintenance"])
r = sl.check("systemctl reboot --approved-maintenance")
assert r is None, "allowlist escape failed"
print()
print("RESULT:", "ALL PASSED" if fails == 0 else f"{fails} FAILURES")