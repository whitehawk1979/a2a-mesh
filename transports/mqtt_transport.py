"""A2A Mesh MQTT Transport — Lightweight pub/sub transport using paho-mqtt.

This transport allows mesh nodes to communicate via an MQTT broker,
providing a decoupled alternative to direct P2P TCP connections.
"""

import asyncio
import collections
import json
import logging
import time
import uuid
from typing import List, Optional, Dict, Any

import paho.mqtt.client as mqtt

from .base import TransportAdapter, TransportStatus
from ..core.message import A2AMessage, SendResult

log = logging.getLogger("a2a_mesh.transports")

class MQTTTransport(TransportAdapter):
    """MQTT implementation of the A2A Mesh TransportAdapter.
    
    Uses a central MQTT broker for message routing and presence tracking.
    Supports both v1 and v2 paho-mqtt callback signatures for compatibility.
    """
    
    @property
    def name(self) -> str:
        return "mqtt"

    def __init__(self, config):
        self.config = config
        # Read MQTT specific config
        self._mqtt_cfg = getattr(config, 'mqtt', None)
        
        # Default values
        self._enabled = False
        self._host = '127.0.0.1'
        self._port = 8683
        self._client_prefix = 'mesh'
        
        if self._mqtt_cfg:
            self._enabled = getattr(self._mqtt_cfg, 'enabled', False)
            self._host = getattr(self._mqtt_cfg, 'host', '127.0.0.1')
            self._port = getattr(self._mqtt_cfg, 'port', 8683)
            self._client_prefix = getattr(self._mqtt_cfg, 'client_prefix', 'mesh')
        
        # Node name: used for the DM topic and client ID
        # node.py analysis shows self.node_name = self.config.node_name
        self._node_name = getattr(config, 'node_name', 'node')
        
        # State
        self._client: Optional[mqtt.Client] = None
        self._connected = False
        self._last_error = ""
        self._latency = 0.0
        
        # Buffers
        self._rx = collections.deque(maxlen=5000)
        self.devices: Dict[str, Any] = {}
        self.telemetry: Dict[str, Dict[str, tuple]] = collections.defaultdict(dict)
        
        # Connection sync
        self._connect_event = asyncio.Event()

    async def start(self) -> bool:
        """Initialize MQTT client and connect to broker."""
        if not self._enabled:
            log.info(f"[{self.name}] Transport disabled in config")
            return False
        
        try:
            # Paho 2.x initialization
            try:
                self._client = mqtt.Client(
                    mqtt.CallbackAPIVersion.VERSION2, 
                    client_id=f"{self._client_prefix}-{self._node_name}-mqtt"
                )
            except (AttributeError, TypeError):
                # Fallback for paho < 2.0
                self._client = mqtt.Client(client_id=f"{self._client_prefix}-{self._node_name}-mqtt")

            # Setup callbacks
            self._client.on_connect = self._on_connect
            self._client.on_message = self._on_message
            self._client.on_disconnect = self._on_disconnect

            # LWT: a CONNECT csomagba kerül — ha a node váratlanul lezuhan,
            # a broker automatikusan "offline"-t tesz közzé (instant presence).
            try:
                self._client.will_set(f"a2a/nodes/{self._node_name}/status",
                                      "offline", qos=1, retain=True)
            except Exception as e:
                log.warning(f"[{self.name}] LWT will_set failed: {e}")

            # Connect (synchronous call, wrapped in thread or handled by loop_start)
            # Using loop_start() for background processing
            self._client.connect(self._host, self._port, keepalive=30)
            self._client.loop_start()
            
            # Wait for connection confirmation (up to 5s)
            try:
                await asyncio.wait_for(self._connect_event.wait(), timeout=5.0)
                log.info(f"[{self.name}] Connected to broker {self._host}:{self._port}")
                return True
            except asyncio.TimeoutError:
                log.warning(f"[{self.name}] Connection timeout after 5s")
                return False
                
        except Exception as e:
            self._last_error = str(e)
            log.error(f"[{self.name}] Start failed: {e}")
            return False

    def _on_connect(self, client, userdata, flags, rc, properties=None):
        """Handle connection event. Compatible with paho v1 and v2."""
        # v1: rc is the return code. v2: reason_code is passed.
        # Paho v2 on_connect has properties as last arg.
        
        # In v1, rc=0 is success. In v2, reason_code=0 is success.
        # We check the 'rc' parameter which is present in both (though named differently in some v2 docs)
        if rc == 0:
            self._connected = True
            self._connect_event.set()
            
            # Subscribe to necessary topics
            # QoS1 for reliable messaging
            self._client.subscribe(f"a2a/chat/dm/{self._node_name}", qos=1)
            self._client.subscribe("a2a/sys/broadcast", qos=1)
            self._client.subscribe("a2a/nodes/+/status", qos=1)

            # Saját presence publikálás (retained) — connect után.
            try:
                status_topic = f"a2a/nodes/{self._node_name}/status"
                self._client.publish(status_topic, "online", qos=1, retain=True)
                log.info(f"[{self.name}] Presence published: {status_topic} = online")
            except Exception as e:
                log.warning(f"[{self.name}] Presence publish failed: {e}")
            self._client.subscribe("a2a/devices/+/manifest", qos=1)
            self._client.subscribe("a2a/devices/+/status", qos=1)
            
            # QoS0 for high-frequency telemetry
            self._client.subscribe("a2a/devices/+/sensors/#", qos=0)
        else:
            self._connected = False
            self._last_error = f"Connection failed with rc={rc}"
            log.warning(f"[{self.name}] Connection failed with rc={rc}")

    def _on_disconnect(self, client, userdata, disconnect_flags=None, rc=None, properties=None):
        """Handle disconnection (paho v2 signature: flags, rc, properties)."""
        self._connected = False
        self._connect_event.clear()
        log.info(f"[{self.name}] Disconnected from broker (rc={rc})")

    def _on_message(self, client, userdata, msg):
        """Route incoming MQTT messages to appropriate buffers."""
        topic = msg.topic
        payload = msg.payload
        
        try:
            # 1. Messaging: DM and Broadcast
            if topic == f"a2a/chat/dm/{self._node_name}" or topic == "a2a/sys/broadcast":
                data = json.loads(payload)
                
                # Lean parsing into A2AMessage
                # Expected keys: id, type ('a2a_message'), sender, recipient, payload, ts
                msg_id = data.get('id', uuid.uuid4().hex)
                ts = data.get('ts', time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()))
                
                # Construct message matching a2a_mesh's internal structure
                # We use the logic implied by PGTransport's fallback
                message = A2AMessage.from_dict({
                    'id': msg_id,
                    'sender': data.get('sender', 'unknown'),
                    'recipient': data.get('recipient', 'broadcast'),
                    'type': data.get('type') or data.get('msg_type', 'a2a_message'),
                    'payload': data.get('payload', data),
                    'priority': data.get('priority', 5),
                    'ts': ts
                })
                self._rx.append(message)
                
            # 2. Presence: Manifests, Statuses
            elif any(topic.startswith(p) for p in ("a2a/nodes/", "a2a/devices/")):
                parts = topic.split('/')
                if len(parts) < 4: return
                
                kind = parts[2]     # 'nodes' or 'devices'
                entity_id = parts[3]
                attr = parts[4] if len(parts) > 4 else None
                
                if attr in ('manifest', 'status'):
                    # Presence lehet JSON (device-manifest) vagy plain string
                    # ("online"/"offline" — node presence, LWT). Mindkettőt elfogadjuk.
                    try:
                        val = json.loads(payload)
                    except (ValueError, UnicodeDecodeError):
                        val = payload.decode('utf-8', errors='replace') if isinstance(payload, (bytes, bytearray)) else str(payload)
                    try:
                        entry = self.devices.get(entity_id, {})
                        entry.update({
                            'kind': kind,
                            'ts': time.time(),
                        })
                        if attr == 'manifest':
                            entry['manifest'] = val
                        else:
                            entry['status'] = val
                        self.devices[entity_id] = entry
                    except Exception as e:
                        log.warning(f"[{self.name}] Presence cache update failed for {topic}: {e}")
                
                # 3. Telemetry: Sensors
                elif attr == 'sensors':
                    # topic: a2a/devices/{id}/sensors/{key}
                    if len(parts) >= 5:
                        sensor_key = parts[4]
                        try:
                            val = json.loads(payload)
                            # Bound telemetry to 1000 devices
                            if len(self.telemetry) < 1000 or entity_id in self.telemetry:
                                self.telemetry[entity_id][sensor_key] = (val, time.time())
                        except json.JSONDecodeError:
                            pass
                            
        except Exception as e:
            log.warning(f"[{self.name}] Error processing message on {topic}: {e}. Payload: {payload[:200]!r}")

    async def receive(self) -> List[A2AMessage]:
        """Drain the RX queue and return list of (message, transport_name) tuples."""
        messages = []
        while self._rx:
            messages.append((self._rx.popleft(), self.name))
        return messages

    async def send(self, message) -> SendResult:
        """Send a message via MQTT."""
        if not self._connected:
            return SendResult(success=False, error="mqtt not connected")
        
        try:
            # Topic resolution
            recipient = getattr(message, 'recipient', 'broadcast')
            msg_type = getattr(message, 'type', '')
            
            if recipient == 'broadcast' or msg_type == 'broadcast':
                topic = "a2a/sys/broadcast"
            else:
                topic = f"a2a/chat/dm/{recipient}"
            
            # Serialization (matching PG transport's style)
            # to_dict() is assumed to exist on A2AMessage based on prompt
            payload_dict = message.to_dict() if hasattr(message, 'to_dict') else {
                'id': getattr(message, 'id', uuid.uuid4().hex),
                'sender': getattr(message, 'sender', self._node_name),
                'recipient': recipient,
                'type': msg_type,
                'payload': getattr(message, 'payload', {}),
                'ts': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
            }
            
            payload_json = json.dumps(payload_dict)
            
            # Publish QoS1
            result = self._client.publish(topic, payload_json, qos=1)
            
            # Wait for publish confirmation if possible
            # Paho publish is async; we can check result.rc
            if result.rc == mqtt.MQTT_ERR_SUCCESS:
                return SendResult(success=True)
            else:
                return SendResult(success=False, error=f"MQTT publish error rc={result.rc}")
                
        except Exception as e:
            return SendResult(success=False, error=str(e))

    async def discover(self) -> List[Dict[str, Any]]:
        """Return currently known devices/nodes from the presence mirror."""
        # Match PGTransport.discover() format: list of dicts
        return [
            {'id': eid, **data} 
            for eid, data in self.devices.items()
        ]

    def is_available(self) -> bool:
        """Check if transport is connected."""
        return self._connected

    def get_status(self) -> TransportStatus:
        """Return current transport status."""
        return TransportStatus(
            available=self._connected,
            latency_ms=self._latency,
            error=self._last_error
        )

    async def stop(self) -> bool:
        """Disconnect and stop the background loop."""
        if self._client:
            self._client.loop_stop()
            self._client.disconnect()
        self._connected = False
        return True
