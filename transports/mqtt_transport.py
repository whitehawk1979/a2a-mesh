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
        
        # SSH key sync handler (set by node.py)
        self._ssh_key_handler = None

        # User-change notify handler (set by node.py; calls AuthManager._sync_from_pg)
        self._users_changed_handler = None
        
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
            
            # ── MQTT Wake topic — bármely node wake-elhet bármelyiket ──
            # Pattern: a2a/wake/{target} → DM wake, a2a/wake/+ wildcard
            try:
                self._client.subscribe("a2a/wake/+", qos=1)
                log.info(f"[{self.name}] Subscribed to MQTT wake topic: a2a/wake/+")
            except Exception as e:
                log.warning(f"[{self.name}] Wake topic subscribe failed: {e}")

            # Saját presence publikálás (retained) — connect után.
            try:
                status_topic = f"a2a/nodes/{self._node_name}/status"
                self._client.publish(status_topic, "online", qos=1, retain=True)
                log.info(f"[{self.name}] Presence published: {status_topic} = online")
            except Exception as e:
                log.warning(f"[{self.name}] Presence publish failed: {e}")
            # SSH key sync — minden node publikálja a publikus kulcsait
            try:
                self._client.subscribe("a2a/ssh_keys/+", qos=1)
                log.info(f"[{self.name}] Subscribed to SSH key sync: a2a/ssh_keys/+")
            except Exception as e:
                log.warning(f"[{self.name}] SSH key topic subscribe failed: {e}")

            # User sync — admin/owner modositas utan a tobbi node azonnal
            # lehuzzja a PG-friss user-listat (pendingus + jogosultsag valtozasok)
            try:
                self._client.subscribe("a2a/sys/users_changed", qos=1)
                log.info(f"[{self.name}] Subscribed to user change notifications: a2a/sys/users_changed")
            except Exception as e:
                log.warning(f"[{self.name}] users_changed subscribe failed: {e}")

            self._client.subscribe("a2a/devices/+/manifest", qos=1)
            self._client.subscribe("a2a/devices/+/status", qos=1)
            
            # QoS0 for high-frequency telemetry
            self._client.subscribe("a2a/devices/+/sensors/#", qos=0)
        else:
            self._connected = False
            self._last_error = f"Connection failed with rc={rc}"
            log.warning(f"[{self.name}] Connection failed with rc={rc}")

    def _on_disconnect(self, client, userdata, *args, **kwargs):
        """Handle disconnection (paho v1/v2 kompatibilis: *args, **kwargs)."""
        self._connected = False
        self._connect_event.clear()
        # VERSION1: (rc), VERSION2: (reasonCode, properties, callback_rc)
        rc = args[0] if args else kwargs.get('rc', kwargs.get('reasonCode', '?'))
        log.info(f"[{self.name}] Disconnected from broker (rc={rc})")

    def _on_message(self, client, userdata, msg):
        """Route incoming MQTT messages to appropriate handlers."""
        topic = msg.topic
        payload = msg.payload
        
        try:
            # ── MQTT Wake: a2a/wake/{target} → direkt POST /api/wake-agent-re ──
            parts = topic.split('/')
            if len(parts) >= 3 and parts[1] == 'wake':
                self._on_wake_message(topic, payload)
                return  # Wake nem megy az A2AMessage buffer-be
            
            # ── SSH key sync: a2a/ssh_keys/{sender} ──
            elif len(parts) >= 3 and parts[1] == 'ssh_keys':
                sender = parts[2]
                if self._ssh_key_handler:
                    try:
                        self._ssh_key_handler(sender, payload)
                    except Exception as e:
                        log.warning(f"[{self.name}] SSH key handler failed: {e}")
                return

            # ── User sync notify: a2a/sys/users_changed ──
            elif topic == "a2a/sys/users_changed":
                if self._users_changed_handler:
                    try:
                        self._users_changed_handler()
                    except Exception as e:
                        log.warning(f"[{self.name}] users_changed handler failed: {e}")
                return

            # Messaging: DM and Broadcast
            if topic == f"a2a/chat/dm/{self._node_name}" or topic == "a2a/sys/broadcast":
                data = json.loads(payload)
                
                # Lean parsing into A2AMessage
                msg_id = data.get('id', uuid.uuid4().hex)
                ts = data.get('ts', time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()))
                
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
            
            # Presence: Manifests, Statuses, Telemetry
            elif any(topic.startswith(p) for p in ("a2a/nodes/", "a2a/devices/")):
                kind = parts[1]
                entity_id = parts[2]
                attr = parts[3] if len(parts) > 3 else ''
                sub_key = parts[4] if len(parts) > 4 else None
                
                if attr in ('manifest', 'status'):
                    try:
                        val = json.loads(payload)
                    except (ValueError, UnicodeDecodeError):
                        val = payload.decode('utf-8', errors='replace') if isinstance(payload, (bytes, bytearray)) else str(payload)
                    try:
                        entry = self.devices.get(entity_id, {})
                        entry.update({'kind': kind, 'ts': time.time()})
                        if attr == 'manifest':
                            entry['manifest'] = val
                        else:
                            entry['status'] = val
                        self.devices[entity_id] = entry
                    except Exception as e:
                        log.warning(f"[{self.name}] Presence cache update failed for {topic}: {e}")
                
                elif attr == 'sensors' and sub_key:
                    try:
                        val = json.loads(payload)
                        if len(self.telemetry) < 1000 or entity_id in self.telemetry:
                            self.telemetry[entity_id][sub_key] = (val, time.time())
                    except (ValueError, UnicodeDecodeError):
                        pass
                        
        except Exception as e:
            log.warning(f"[{self.name}] Error processing message on {topic}: {e}. Payload: {payload[:200]!r}")
    
    def _on_wake_message(self, topic, raw_payload):
        """Handle incoming MQTT wake trigger — direct POST to localhost /api/wake-agent.
        
        Topic pattern: a2a/wake/{target} -> target = node_name that must process this.
        Payload: same dict as the dashboard_agents webhook payload.
        Uses aiohttp.ClientSession to POST directly to the node's own HTTP endpoint.
        """
        try:
            import sys
            sys.path.insert(0, '/Users/zsolt/.hermes/scripts/a2a_mesh')
            
            payload = json.loads(raw_payload)
        except (json.JSONDecodeError, TypeError):
            log.warning(f"[{self.name}] MQTT wake payload not valid JSON from topic {topic}")
            return
        
        # Extract target node name from topic: "a2a/wake/morzsa" -> "morzsa"
        parts = topic.split('/')
        if len(parts) < 3 or parts[1] != 'wake':
            return
        target_node = parts[2]
        
        # Only process if THIS node is the target (like /api/wake-agent local handler)
        if target_node != self._node_name:
            return
        
        sender = payload.get('sender', 'mqtt-wake')
        prompt = payload.get('prompt', '')[:2000]
        mesh_secret = payload.get('mesh_secret', '')
        
        if not prompt:
            log.info(f"[{self.name}] Empty prompt from MQTT wake on {topic}, skipping")
            return
        
        health_port = getattr(self.config, 'health_port', 8650) or 8650
        
        log.info(
            f"[{self.name}] TEE Wake received via MQTT topic={topic}, preview: {prompt[:60]}..."
        )
        
        # Direct POST to localhost /api/wake-agent — exactly the same flow as P2P/HTTP wake
        try:
            import asyncio
            async def _post():
                try:
                    import aiohttp as aiohttp_module
                    url = f"http://127.0.0.1:{health_port}/api/wake-agent"
                    async with aiohttp_module.ClientSession(timeout=aiohttp_module.ClientTimeout(total=120)) as sess:
                        async with sess.post(url, json=payload) as resp:
                            body = await resp.text()
                            log.info(f"[{self.name}] MQTT wake -> {target_node}: status={resp.status}, response_preview={body[:150]}")
                except Exception as e:
                    log.error(f"[{self.name}] MQTT wake POST failed for {target_node}: {e}")
            
            loop = asyncio.get_running_loop()
            loop.create_task(_post())
        except Exception as e:
            log.error(f"[{self.name}] Failed to POST MQTT wake for {target_node}: {e}")

    def set_ssh_key_handler(self, handler):
        """Register callback for incoming MQTT SSH key messages: handler(sender, payload_bytes)."""
        self._ssh_key_handler = handler

    def set_users_changed_handler(self, handler):
        """Register callback for user-change notifications: handler().

        Called when another node pushes changes to mesh_users (approve/reject/
        role change). The handler typically runs AuthManager._sync_from_pg()
        so local SQLite mirrors PG (the source of truth)."""
        self._users_changed_handler = handler

    def publish_users_changed(self, reason: str = ""):
        """Announce that mesh_users changed — other nodes should re-pull from PG."""
        if not self._connected or not self._client:
            return False
        try:
            payload = json.dumps({"node": self._node_name, "reason": reason, "ts": time.time()})
            self._client.publish("a2a/sys/users_changed", payload, qos=1, retain=False)
            log.info(f"[{self.name}] users_changed published ({reason})")
            return True
        except Exception as e:
            log.warning(f"[{self.name}] users_changed publish failed: {e}")
            return False

    def publish_ssh_keys(self, topic: str, payload: str, retain: bool = True):
        """Publish SSH public key(s) to MQTT topic (rate-limited by caller)."""
        if not self._connected or not self._client:
            return False
        try:
            self._client.publish(topic, payload, qos=1, retain=retain)
            return True
        except Exception as e:
            log.warning(f"[{self.name}] SSH key publish to {topic} failed: {e}")
            return False

    def publish_wake_to(self, target_node: str, payload: str):
        """Publish a wake message for a peer node via a2a/wake/{target_node}.

        The target node's _on_wake_message handler receives this and POSTs
        locally to its own /api/wake-agent endpoint.  This works independently
        of P2P/HTTP reachability — as long as the target is connected to the
        broker, the wake gets through.
        """
        if not self._connected or not self._client:
            return False
        try:
            self._client.publish(f"a2a/wake/{target_node}", payload, qos=1, retain=False)
            log.info(f"[{self.name}] Wake published for '{target_node}' via MQTT")
            return True
        except Exception as e:
            log.warning(f"[{self.name}] Wake publish to {target_node} failed: {e}")
            return False

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
            
            payload_json = json.dumps(payload_dict, default=str)
            
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
