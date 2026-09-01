import sys
sys.path.insert(0, "/Users/zsolt/.hermes/scripts/a2a_mesh")
from core.hindsight_sync import HindsightSync

fails = 0

# 1) Basic RRF: item in BOTH lists should outrank items in one list only
mesh = [
    {"rrf_key": "a", "memory_value": "shared-top-result"},
    {"rrf_key": "b", "memory_value": "mesh-only-1"},
    {"rrf_key": "c", "memory_value": "mesh-only-2"},
]
kw = [
    {"rrf_key": "z", "memory_value": "kw-only-1"},
    {"rrf_key": "a", "memory_value": "shared-top-result"},
    {"rrf_key": "y", "memory_value": "kw-only-2"},
]
fused = HindsightSync._rrf_fuse([("vector_mesh", mesh), ("keyword", kw)])
top = fused[0]
if top["rrf_key"] == "a" and set(top["rrf_sources"]) == {"vector_mesh", "keyword"}:
    print(f"shared item fused to top: OK (rrf={top['rrf_score']}, sources={top['rrf_sources']})")
else:
    print(f"FAIL: top is {top}"); fails += 1

# 2) Order preservation within a single list (no other lists)
single = HindsightSync._rrf_fuse([("only", [{"rrf_key": str(i)} for i in range(5)])])
if [x["rrf_key"] for x in single] == [str(i) for i in range(5)]:
    print("single-list order preserved: OK")
else:
    print("FAIL: single list order"); fails += 1

# 3) Empty input
if HindsightSync._rrf_fuse([]) == []:
    print("empty input: OK")
else:
    print("FAIL: empty input"); fails += 1

# 4) Manual score check: 'a' is rank1 in mesh (1/61) + rank2 in kw (1/62) = 0.03252
expected = round(1.0 / 61 + 1.0 / 62, 5)
if abs(top["rrf_score"] - expected) < 1e-5:
    print(f"score math: OK (expected {expected})")
else:
    print(f"FAIL: score math (top={top['rrf_score']}, expected={expected})"); fails += 1

print()
print("RESULT:", "ALL PASSED" if fails == 0 else f"{fails} FAILURES")
sys.exit(1 if fails else 0)