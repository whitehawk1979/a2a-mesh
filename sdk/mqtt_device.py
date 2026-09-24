import json
import time
import logging
import uuid
from datetime import datetime, timezone
import paho.mqtt.client as mqtt

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("MeshDevice")

class MeshDevice:
    def __init__(self, dev_id, kind="sensor", parent_node="nova", broker="127.0.0.1", port=8683, 
                 caps=None, writable_state=None, version="0.1.0", keepalive=30):
        self.dev_id = dev_id
        self.kind = kind
        self.parent_node = parent_node
        self.broker = broker
        self.port = port
        self.caps = caps or []
        self.writable_state = writable_state or []
        self.version = version
        self.keepalive = keepalive

        # MQTT Client Setup
        try:
            self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"dev-{dev_id}")
        except AttributeError:
            # Fallback for paho-mqtt < 2.0
            self.client = mqtt.Client(client_id=f"dev-{dev_id}")

        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message
        self.client.on_disconnect = self._on_disconnect

        # Overridable handlers
        self.on_cmd = self._default_on_cmd
        self.on_dm = self._default_on_dm

        self._connected = False

    @property
    def is_connected(self):
        return self._connected

    def _default_on_cmd(self, key, payload):
        logger.info(f"CMD received [{key}]: {payload}")

    def _default_on_dm(self, sender, payload):
        logger.info(f"DM received from {sender}: {payload}")

    def _on_connect(self, client, userdata, flags, reason_code, properties=None):
        # Support both v1 and v2 signatures (v1 doesn't have properties)
        logger.info(f"Connected to broker {self.broker}:{self.port} (code: {reason_code})")
        self._connected = True
        
        # Subscriptions
        self.client.subscribe(f"a2a/devices/{self.dev_id}/cmd/+", qos=1)
        self.client.subscribe(f"a2a/chat/dm/{self.dev_id}", qos=1)
        
        # Publish Manifest and Status
        self._publish_manifest()
        self.publish_status("online")

    def _on_disconnect(self, client, userdata, disconnect_flags, rc, properties=None):
        logger.info("Disconnected from broker")
        self._connected = False

    def _on_message(self, client, userdata, msg):
        topic = msg.topic
        payload_str = msg.payload.decode("utf-8")

        if topic.startswith(f"a2a/devices/{self.dev_id}/cmd/"):
            key = topic.replace(f"a2a/devices/{self.dev_id}/cmd/", "")
            self.on_cmd(key, payload_str)
        elif topic == f"a2a/chat/dm/{self.dev_id}":
            try:
                data = json.loads(payload_str)
                self.on_dm(data.get("sender"), data.get("payload"))
            except json.JSONDecodeError:
                logger.error(f"Invalid DM JSON: {payload_str}")

    def _publish_manifest(self):
        manifest = {
            "dev_id": self.dev_id,
            "kind": self.kind,
            "parent_node": self.parent_node,
            "version": self.version,
            "caps": self.caps,
            "subs": [f"a2a/devices/{self.dev_id}/cmd/{k}" for k in self.writable_state] + [f"a2a/chat/dm/{self.dev_id}"],
            "pubs": [f"a2a/devices/{self.dev_id}/sensors/{c}" for c in self.caps] + 
                    [f"a2a/devices/{self.dev_id}/state/{k}" for k in self.writable_state],
            "writable_state": self.writable_state,
            "wakes": [],
            "ts": datetime.now(timezone.utc).isoformat()
        }
        self.client.publish(f"a2a/devices/{self.dev_id}/manifest", json.dumps(manifest), qos=1, retain=True)

    def publish_status(self, status):
        payload = {
            "dev_id": self.dev_id,
            "status": status,
            "ts": datetime.now(timezone.utc).isoformat()
        }
        self.client.publish(f"a2a/devices/{self.dev_id}/status", json.dumps(payload), qos=1, retain=True)

    def connect(self):
        try:
            # LWT: will_set(topic, payload, qos, retain)
            lwt_payload = json.dumps({
                "dev_id": self.dev_id,
                "status": "offline",
                "ts": datetime.now(timezone.utc).isoformat()
            })
            self.client.will_set(f"a2a/devices/{self.dev_id}/status", payload=lwt_payload, qos=1, retain=True)
            
            self.client.connect(self.broker, self.port, keepalive=self.keepalive)
            self.client.loop_start()
            
            # Wait for connection callback
            start_time = time.time()
            while not self._connected and (time.time() - start_time) < 5:
                time.sleep(0.1)
            
            return self._connected
        except Exception as e:
            logger.error(f"Connection failed: {e}")
            return False

    def disconnect(self):
        if self._connected:
            self.publish_status("offline")
            self.client.disconnect()
            self.client.loop_stop()
            self._connected = False

    def publish_sensor(self, key, value):
        topic = f"a2a/devices/{self.dev_id}/sensors/{key}"
        self.client.publish(topic, str(value), qos=0)

    def publish_state(self, key, value):
        topic = f"a2a/devices/{self.dev_id}/state/{key}"
        self.client.publish(topic, str(value), qos=1, retain=True)

    def send_dm(self, to, content):
        topic = f"a2a/chat/dm/{to}"
        envelope = {
            "id": uuid.uuid4().hex,
            "type": "a2a_message",
            "sender": self.dev_id,
            "recipient": to,
            "payload": content,
            "ts": datetime.now(timezone.utc).isoformat()
        }
        self.client.publish(topic, json.dumps(envelope), qos=1)
