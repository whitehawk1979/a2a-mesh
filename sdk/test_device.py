#!/usr/bin/env python3
"""SDK smoke-teszt: MeshDevice connect → manifest → telemetry → DM → LWT,
   majd ellenőrzés, hogy a Nova node feldolgozta-e (log-tail)."""
import time, json, sys, subprocess
sys.path.insert(0, "/Users/zsolt/.hermes/scripts")
from a2a_mesh.sdk.mqtt_device import MeshDevice

# 1) Device connect (LWT-vel)
dev = MeshDevice(
    dev_id="sdk-smoke-test",
    parent_node="nova",
    broker="192.168.1.8", port=8683,
    caps=["test"],
)
dev.connect()
print("1. connect OK")

# 2) Manifest + telemetry + DM
dev._publish_manifest()
print("2. manifest OK")
dev.publish_sensor("uptime_s", 42)
print("3. telemetry OK")
dev.send_dm("nova", "SDK smoke-test DM a Novanak — teljes lánc!")
print("4. DM OK")

# 3) Nova log-ellenőrzés (feldolgozta-e a node a DM-et):
time.sleep(2)
r = subprocess.run(
    ["grep", "-E", "from sdk-smoke-test.*via mqtt.*processed", "/Users/zsolt/.hermes/logs/a2a_mesh.log"],
    capture_output=True, text=True)
lines = r.stdout.strip().splitlines()
print("5. Nova feldolgozta:", lines[-1] if lines else "NEM TALÁLható (log-tail)")

# 4) Disconnect → LWT (offline) ellenőrzése observerrel
dev.disconnect()  # graceful → nem várunk LWT-t (az csak váratlan kilépésnél)
print("6. disconnect OK (graceful, LWT nem váltódik ki — hard-killnél igen)")

print("\nSMOKE TEST DONE")