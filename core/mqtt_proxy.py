"""A2A Mesh MQTT Proxy — REST API to Mosquitto broker.

This module provides:
- /api/mqtt/topics  → topic tree with subscriber counts and last payloads
- /api/mqtt/publish → publish to any topic on the broker

Uses paho-mqtt's Client object directly (not via aiohttp).
The broker must be reachable from the mesh node host.
"""

import json
import logging
import time
from typing import Any, Dict, List, Optional

log = logging.getLogger("a2a_mesh.mqtt_proxy")


class MQTTProxy:
    """REST interface to the local Mosquitto broker."""
    
    def __init__(self, mqtt_transport):
        self._mqtt_tr = mqtt_transport
    
    async def get_topic_tree(self) -> Dict[str, Any]:
        """Build hierarchical topic tree from all devices/nodes/status topics."""
        result = {}
        
        if not self._mqtt_tr or not self._mqtt_tr.devices:
            return {"topics": {}, "total_messages": 0}
        
        # Collect all device entries grouped by topic prefix
        messages = []
        
        for entity_id, entry in self._mqtt_tr.devices.items():
            kind = entry.get('kind', 'nodes')
            
            # Node statuses: a2a/nodes/{name}/status
            if kind == 'nodes':
                status = entry.get('status', 'online')
                topic = f"a2a/nodes/{entity_id}/status"
                
                # Count as message
                msg_ts = entry.get('ts', time.time())
                payload = {
                    'node_name': entity_id,
                    'status': str(status),
                    'manifest': entry.get('manifest', {})
                }
                messages.append({
                    'topic': topic,
                    'payload': payload,
                    'timestamp': msg_ts,
                    'sender': entity_id,
                    'qos': 1,
                    'retain': True
                })
            
            # Device manifests/statuses
            elif kind == 'devices':
                manifest = entry.get('manifest')
                status = entry.get('status')
                ts = entry.get('ts', time.time())
                
                if manifest:
                    topic = f"a2a/devices/{entity_id}/manifest"
                    messages.append({
                        'topic': topic,
                        'payload': manifest,
                        'timestamp': ts,
                        'sender': entity_id,
                        'qos': 0,
                        'retain': True
                    })
                
                if status:
                    topic = f"a2a/devices/{entity_id}/status"
                    messages.append({
                        'topic': topic,
                        'payload': {'status': str(status), 'ts': ts},
                        'timestamp': ts,
                        'sender': entity_id,
                        'qos': 0,
                        'retain': False
                    })
            
            # Telemetry data
            telemetry = entry.get('telemetry', {})
            for sensor_key, (val, ts) in telemetry.items():
                topic = f"a2a/devices/{entity_id}/sensors/{sensor_key}"
                messages.append({
                    'topic': topic,
                    'payload': val,
                    'timestamp': ts,
                    'sender': entity_id,
                    'qos': 0,
                    'retain': False
                })
        
        # Also collect presence from active subscriptions
        # The MQTT transport should have subscribed topics
        sub_topics = getattr(self._mqtt_tr, '_subscriptions', [])
        for sub in sub_topics:
            try:
                # Parse subscription topic pattern
                topic_pattern = sub if isinstance(sub, str) else sub.get('topic', '')
                if not topic_pattern: continue
                
                msg_count = 1
                last_msg = None
                
                # Try to find matching message in collected messages
                for m in messages:
                    if _match_pattern(topic_pattern, m['topic']):
                        last_msg = m
                        break
                
                if not last_msg:
                    last_msg = {
                        'topic': topic_pattern,
                        'payload': {'subscription': topic_pattern},
                        'timestamp': time.time(),
                        'sender': '?',
                        'qos': 0,
                        'retain': False
                    }
                
                result[topic_pattern] = {
                    'count': msg_count,
                    'messages': [last_msg]
                }
            except Exception:
                pass
        
        # Build structured result
        structured = {}
        total = len(messages)
        
        for m in messages:
            topic = m['topic']
            if topic not in structured:
                structured[topic] = {'count': 0, 'messages': []}
            structured[topic]['count'] += 1
            structured[topic]['messages'].append(m)
        
        return {
            'topics': structured,
            'total_messages': total,
            'subscriber_count': len(sub_topics) if sub_topics else 0
        }
    
    async def publish_to_broker(self, topic: str, payload: Any, 
                                qos: int = 1, retain: bool = False) -> Dict[str, Any]:
        """Publish message to MQTT broker via paho client."""
        try:
            if not self._mqtt_tr or not self._mqtt_tr.is_available():
                return {'success': False, 'error': 'MQTT not available'}
            
            # Serialize payload
            payload_str = json.dumps(payload, ensure_ascii=False, default=str)
            
            # Use paho client directly
            result = self._mqtt_tr._client.publish(
                topic, payload_str, qos=qos, retain=retain
            )
            
            log.info(f"Published: {topic} @ QoS{qos}, retain={retain}")
            
            return {
                'success': True,
                'topic': topic,
                'qos': qos,
                'retain': retain
            }
        except Exception as e:
            log.error(f"Failed to publish {topic}: {e}", exc_info=True)
            return {'success': False, 'error': str(e)}


def _match_pattern(pattern: str, topic: str) -> bool:
    """Match MQTT wildcard patterns against actual topics."""
    pat_parts = pattern.split('/')
    top_parts = topic.split('/')
    
    for i in range(len(pat_parts)):
        if i >= len(top_parts):
            return False
        if pat_parts[i] == '#':
            return True
        if pat_parts[i] != '+' and pat_parts[i] != top_parts[i]:
            return False
    
    return True
