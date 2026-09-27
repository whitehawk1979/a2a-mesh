"""A2A Mesh MQTT Proxy — REST API to Mosquitto broker.

This module provides:
- /api/mqtt/topics  → topic tree with subscriber counts and last payloads
- /api/mqtt/publish → publish to any topic on the broker

Fetches retained messages directly from the broker (via a short-lived
paho client) so ALL topics are visible — not just transport-tracked
device presence. Falls back to transport.devices if broker query fails.
"""

import asyncio
import json
import logging
import threading
import time
from typing import Any, Dict, List, Optional

log = logging.getLogger("a2a_mesh.mqtt_proxy")


class MQTTProxy:
    """REST interface to the local Mosquitto broker."""

    def __init__(self, mqtt_transport):
        self._mqtt_tr = mqtt_transport

    # ── Broker query (retained scan) ──────────────────────────────

    def _query_broker(self, timeout: float = 3.0) -> Optional[Dict[str, Dict]]:
        """Connect to broker, subscribe a2a/#, collect all retained messages.

        Returns {topic: message_dict} or None on failure.
        """
        try:
            import paho.mqtt.client as mqtt
        except ImportError:
            log.debug("paho-mqtt not available for proxy query")
            return None

        # Transport exposes resolved host/port directly
        host = getattr(self._mqtt_tr, "_host", None) or "127.0.0.1"
        port = getattr(self._mqtt_tr, "_port", None) or 8683

        collected: Dict[str, Dict] = {}
        done = threading.Event()

        def on_connect(client, userdata, flags, rc, properties=None):
            try:
                client.subscribe("a2a/#", qos=0)
            except Exception:
                done.set()

        def on_message(client, userdata, msg):
            try:
                payload_raw = msg.payload.decode("utf-8", errors="replace")
                try:
                    payload = json.loads(payload_raw)
                except (json.JSONDecodeError, ValueError):
                    payload = payload_raw
                collected[msg.topic] = {
                    "topic": msg.topic,
                    "payload": payload,
                    "timestamp": time.time(),
                    "sender": msg.topic.split("/")[2] if len(msg.topic.split("/")) > 2 else "?",
                    "qos": msg.qos,
                    "retain": msg.retain,
                }
            except Exception as e:
                log.debug(f"proxy on_message error: {e}")

        def on_subscribe(client, userdata, mid, rc, properties=None):
            # After SUBACK give a short grace for PUBLISH delivery
            threading.Timer(0.8, done.set).start()

        try:
            try:
                client = mqtt.Client(
                    mqtt.CallbackAPIVersion.VERSION2,
                    client_id=f"a2a-mqtt-explorer-{int(time.time())}",
                )
            except (AttributeError, TypeError):
                client = mqtt.Client(client_id=f"a2a-mqtt-explorer-{int(time.time())}")
            client.on_connect = on_connect
            client.on_message = on_message
            try:
                client.on_subscribe = on_subscribe
            except AttributeError:
                pass
            client.connect(host, int(port), keepalive=10)
            client.loop_start()
            got_done = done.wait(timeout)
            # Fallback completion if no subscribe callback (v1 api)
            if not collected and not got_done:
                time.sleep(1.0)
            client.loop_stop()
            client.disconnect()
            return collected if collected else {}
        except Exception as e:
            log.warning(f"MQTT broker query failed ({host}:{port}): {e}")
            return None

    # ── Transport fallback (device presence only) ─────────────────

    def _from_transport(self) -> Dict[str, Dict]:
        """Build topic map from transport.devices (legacy path)."""
        collected: Dict[str, Dict] = {}
        if not self._mqtt_tr or not getattr(self._mqtt_tr, "devices", None):
            return collected
        for entity_id, entry in self._mqtt_tr.devices.items():
            kind = entry.get("kind", "nodes")
            ts = entry.get("ts", time.time())
            status = entry.get("status", "online")
            if kind == "nodes":
                topic = f"a2a/nodes/{entity_id}/status"
                collected[topic] = {
                    "topic": topic,
                    "payload": {"node_name": entity_id, "status": str(status),
                                "manifest": entry.get("manifest", {})},
                    "timestamp": ts, "sender": entity_id, "qos": 1, "retain": True,
                }
            elif kind == "devices":
                if entry.get("manifest"):
                    topic = f"a2a/devices/{entity_id}/manifest"
                    collected[topic] = {
                        "topic": topic, "payload": entry["manifest"],
                        "timestamp": ts, "sender": entity_id, "qos": 0, "retain": True,
                    }
                if status:
                    topic = f"a2a/devices/{entity_id}/status"
                    collected[topic] = {
                        "topic": topic,
                        "payload": {"status": str(status), "ts": ts},
                        "timestamp": ts, "sender": entity_id, "qos": 0, "retain": True,
                    }
        return collected

    # ── Public API ────────────────────────────────────────────────

    async def get_topic_tree(self) -> Dict[str, Any]:
        """Build hierarchical topic tree — retained scan first, transport fallback."""
        messages = await asyncio.to_thread(self._query_broker)
        if messages is None:
            # Broker unreachable — fall back to transport device presence
            messages = self._from_transport()

        structured: Dict[str, Dict] = {}
        for topic, msg in messages.items():
            structured[topic] = {"count": 1, "messages": [msg]}

        return {
            "topics": structured,
            "total_messages": len(structured),
            "subscriber_count": len(getattr(self._mqtt_tr, "_subscriptions", []) or [])
            if self._mqtt_tr else 0,
        }

    async def publish_to_broker(self, topic: str, payload: Any,
                                qos: int = 1, retain: bool = False) -> Dict[str, Any]:
        """Publish message to MQTT broker via paho client."""
        try:
            if not self._mqtt_tr or not self._mqtt_tr.is_available():
                return {"success": False, "error": "MQTT not available"}

            payload_str = json.dumps(payload, ensure_ascii=False, default=str)

            result = self._mqtt_tr._client.publish(
                topic, payload_str, qos=qos, retain=retain
            )

            log.info(f"Published: {topic} @ QoS{qos}, retain={retain}")

            return {
                "success": True,
                "topic": topic,
                "qos": qos,
                "retain": retain,
            }
        except Exception as e:
            log.error(f"Failed to publish {topic}: {e}", exc_info=True)
            return {"success": False, "error": str(e)}


def _match_pattern(pattern: str, topic: str) -> bool:
    """Match MQTT wildcard patterns against actual topics."""
    pat_parts = pattern.split("/")
    top_parts = topic.split("/")

    for i in range(len(pat_parts)):
        if i >= len(top_parts):
            return False
        if pat_parts[i] == "#":
            return True
        if pat_parts[i] != "+" and pat_parts[i] != top_parts[i]:
            return False

    return True
