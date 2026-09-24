import argparse
import random
import time
import sys
import os

# Ensure sdk is importable if run from repo root
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from sdk.mqtt_device import MeshDevice

def main():
    parser = argparse.ArgumentParser(description="A2A Mesh Device SDK Test CLI")
    parser.add_argument("--dev", default="vsensor-1", help="Device ID")
    parser.add_argument("--kind", default="sensor", help="Device kind")
    parser.add_argument("--caps", default="temp,relay", help="Capabilities (comma separated)")
    parser.add_argument("--broker", default="127.0.0.1", help="MQTT Broker")
    parser.add_argument("--port", type=int, default=8683, help="MQTT Port")
    parser.add_argument("--parent", default="nova", help="Parent node ID")
    parser.add_argument("--mode", choices=["once", "loop"], default="loop", help="Test mode")
    args = parser.parse_args()

    caps = args.caps.split(",")
    # For this test, assume relay is writable state if present in caps
    writable = [c for c in caps if c == "relay"]

    dev = MeshDevice(
        dev_id=args.dev,
        kind=args.kind,
        parent_node=args.parent,
        broker=args.broker,
        port=args.port,
        caps=caps,
        writable_state=writable
    )

    # Custom logic for loop mode
    if args.mode == "loop":
        def on_cmd(key, payload):
            print(f"[*] Handling CMD [{key}] -> {payload}")
            dev.publish_state(f"cmd_ack:{key}", payload)
            if key == "relay":
                dev.publish_state("relay", payload)

        def on_dm(sender, payload):
            print(f"[*] Handling DM from {sender}: {payload}")

        dev.on_cmd = on_cmd
        dev.on_dm = on_dm

    print(f"Connecting {args.dev} to {args.broker}:{args.port}...")
    if not dev.connect():
        print("Failed to connect to broker.")
        sys.exit(1)

    try:
        if args.mode == "once":
            temp = 21.5 + random.random() * 2
            print(f"Publishing sensor temp: {temp:.2f}")
            dev.publish_sensor("temp", f"{temp:.2f}")
            
            print("Publishing state relay: off")
            dev.publish_state("relay", "off")
            
            msg = f"hello from {args.dev} — MQTT device SDK self-test"
            print(f"Sending DM to {args.parent}: {msg}")
            dev.send_dm(args.parent, msg)
            
            time.sleep(1) # Give time for publish
            dev.disconnect()
            print("Done.")
        else:
            print("Entering loop mode. Press Ctrl+C to exit.")
            while True:
                temp = 21.5 + random.random() * 2
                dev.publish_sensor("temp", f"{temp:.2f}")
                time.sleep(2)
    except KeyboardInterrupt:
        print("\nStopping...")
    finally:
        dev.disconnect()

if __name__ == "__main__":
    main()
