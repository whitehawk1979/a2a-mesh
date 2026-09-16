#!/usr/bin/env python3
"""Batch-embed mesh_memory entries using Brain server API.
Runs in background — embeds all entries without embedding."""

import json
import time
import urllib.request
import subprocess
import sys

BRAIN_URL = "http://127.0.0.1:3322"
SSH_CMD = [
    "ssh", "-i", "/Users/zsolt/.ssh/id_ed25519_openclaw",
    "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=no",
    "openclaw@192.168.1.30",
]
PG_PASSWORD = "nova_agent_2026"

def get_entries_without_embedding(limit=100):
    """Fetch mesh_memory entries without embedding from Morzsa PG."""
    sql = f"psql -h localhost -U nova -d agent_memory -t -A -c \"SELECT id, LEFT(memory_value, 2000) FROM mesh.mesh_memory WHERE embedding IS NULL ORDER BY id DESC LIMIT {limit}\""
    r = subprocess.run(
        SSH_CMD + [sql],
        capture_output=True, text=True, input=PG_PASSWORD + "\n",
        timeout=30
    )
    entries = []
    for line in r.stdout.strip().split('\n'):
        if '|' in line:
            parts = line.split('|', 1)
            try:
                entries.append({"id": int(parts[0]), "text": parts[1][:2000]})
            except:
                pass
    return entries

def embed_entry(entry_id, text):
    """Embed a single entry via Brain API."""
    data = json.dumps({"id": entry_id, "text": text[:2000]}).encode()
    req = urllib.request.Request(
        f"{BRAIN_URL}/mesh/memory/embed",
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST"
    )
    resp = urllib.request.urlopen(req, timeout=30)
    return json.loads(resp.read())

def main():
    batch_size = 100
    total_success = 0
    total_fail = 0
    round_num = 0
    
    while True:
        round_num += 1
        entries = get_entries_without_embedding(batch_size)
        if not entries:
            print(f"Round {round_num}: No more entries to embed. Done!")
            break
        
        print(f"Round {round_num}: Processing {len(entries)} entries...")
        for entry in entries:
            try:
                result = embed_entry(entry["id"], entry["text"])
                if result.get("success"):
                    total_success += 1
                else:
                    total_fail += 1
            except Exception as e:
                total_fail += 1
                if total_fail % 10 == 0:
                    print(f"  Warning: {total_fail} failures so far. Last error: {e}")
        
        print(f"  Progress: {total_success} ok, {total_fail} fail")
        time.sleep(0.5)
    
    print(f"\n=== Complete: {total_success} embedded, {total_fail} failed ===")

if __name__ == "__main__":
    main()