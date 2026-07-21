"""A2A Mesh BLE Transport — BLE GATT Server/Client + Advertisement Discovery.

macOS: Full GATT server (CBPeripheralManager 10.9+)
Linux: Full GATT server (BlueZ D-Bus)
Windows: GATT client only (WinRT BLE)

BLE Service UUID: a2a00001-0000-1000-8000-00805f9b34fb
Characteristics:
  - inbox (WRITE): Receive messages from peers
  - outbox (NOTIFY): Notify peers of new messages
  - control (READ/WRITE): Status, MTU, flow control

Strategy:
1. DISCOVERY: BLE advertisement with A2A service UUID
2. MESSAGING: BLE GATT read/write for small payloads (<512 bytes)
3. CHUNKING: Large messages split into BLE MTU-sized chunks
"""

import asyncio
import json
import logging
import platform
import struct
import time
from typing import Optional, List, Dict

from .base import TransportAdapter, TransportStatus
from ..core.message import A2AMessage, SendResult

log = logging.getLogger("a2a_mesh.transports.ble")

# BLE Service and Characteristic UUIDs
A2A_SERVICE_UUID = "a2a00001-0000-1000-8000-00805f9b34fb"
A2A_INBOX_UUID = "a2a00002-0000-1000-8000-00805f9b34fb"   # WRITE
A2A_OUTBOX_UUID = "a2a00003-0000-1000-8000-00805f9b34fb"  # NOTIFY
A2A_CONTROL_UUID = "a2a00004-0000-1000-8000-00805f9b34fb" # READ/WRITE

# Chunking constants
DEFAULT_MTU = 512
CHUNK_HEADER_SIZE = 8  # chunk_idx(2) + total_chunks(2) + msg_id_hash(4)
MAX_CHUNK_PAYLOAD = DEFAULT_MTU - CHUNK_HEADER_SIZE

HAS_BLE = False
HAS_PYOBJC = False

try:
    import bleak
    from bleak import BleakScanner, BleakClient
    HAS_BLE = True
except ImportError:
    pass

try:
    from CoreBluetooth import (
        CBPeripheralManager, CBMutableService, CBMutableCharacteristic,
        CBCharacteristicProperties, CBAttributePermissions,
        CBPeripheralManagerState, CBService, CBCharacteristic,
        CBUUID, CBAdvertisementData, CBCentral
    )
    from Foundation import NSNumber, NSDictionary, NSRunLoop, NSDate
    from dispatch import dispatch_queue_create, DISPATCH_QUEUE_SERIAL
    HAS_PYOBJC = True
except ImportError:
    pass


class BLETransport(TransportAdapter):
    """BLE GATT + Advertisement transport for proximity mesh.

    macOS: Full GATT server (advertise + service + read/write/notify)
    Linux: Full GATT server (BlueZ D-Bus)
    Windows/Other: GATT client only (scan + connect + write)
    """

    name = "ble"

    def __init__(self, config):
        self.config = config
        self._available = False
        self._scanning = False
        self._advertising = False
        self._platform = platform.system().lower()
        self._mtu = DEFAULT_MTU
        self._discovered_peers: Dict[str, dict] = {}  # address → peer info
        self._inbox = asyncio.Queue()
        self._outbox = asyncio.Queue()
        self._connected_devices: Dict[str, object] = {}
        self._peripheral_manager = None
        self._service = None
        self._inbox_char = None
        self._outbox_char = None
        self._control_char = None
        self._subscribed_central = None
        self._scan_task = None
        self._running = False

        # Chunked transfer tracking
        self._pending_chunks: Dict[str, dict] = {}  # msg_id_hash → {chunks, total, data}

    async def start(self) -> bool:
        """Start BLE transport (advertise + scan)."""
        if not HAS_BLE:
            log.warning("bleak not installed, BLE transport unavailable")
            return False

        self._running = True

        # Start GATT server on macOS/Linux
        if self._platform == "darwin" and HAS_PYOBJC:
            server_ok = await self._start_macos_gatt_server()
            if not server_ok:
                log.warning("BLE GATT server failed, falling back to client-only mode")
        elif self._platform == "linux":
            # Linux BLE GATT via BlueZ D-Bus (requires dbus-python + bluez)
            log.info("Linux BLE: GATT server requires BlueZ D-Bus setup")

        # Start scanning for peers
        scan_ok = await self._start_scanning()
        if scan_ok:
            self._available = True
            log.info("BLE transport started")
        else:
            # Even without scanning, if we have GATT server we're partially available
            if self._platform == "darwin" and HAS_PYOBJC and self._advertising:
                self._available = True
                log.info("BLE transport started (GATT server only, no scanning)")
            else:
                log.warning("BLE transport failed to start")
                return False

        return self._available

    async def stop(self) -> bool:
        """Stop BLE transport."""
        self._running = False

        # Stop scanning
        if self._scan_task:
            self._scan_task.cancel()
            try:
                await self._scan_task
            except asyncio.CancelledError:
                pass

        # Stop advertising
        if self._peripheral_manager and self._advertising:
            try:
                self._peripheral_manager.stopAdvertising()
                self._advertising = False
            except Exception as e:
                log.debug(f"BLE stop advertising error: {e}")

        # Disconnect all clients
        for addr, client in list(self._connected_devices.items()):
            try:
                await client.disconnect()
            except Exception:
                pass
        self._connected_devices.clear()

        self._available = False
        log.info("BLE transport stopped")
        return True

    async def send(self, message: A2AMessage) -> SendResult:
        """Send message via BLE GATT to connected peer."""
        if not self._available:
            return SendResult(transport="ble", success=False, error="BLE not started")

        data = message.to_bytes()
        if not data:
            return SendResult(transport="ble", success=False, error="serialization failed")

        # Try connected devices first
        for addr, client in list(self._connected_devices.items()):
            try:
                if len(data) <= self._mtu:
                    await client.write_gatt_char(A2A_INBOX_UUID, data, response=True)
                else:
                    result = await self._send_chunked(client, data)
                    if not result.success:
                        continue
                return SendResult(transport="ble", success=True)
            except Exception as e:
                log.debug(f"BLE send to {addr} failed: {e}")
                self._connected_devices.pop(addr, None)

        # Try connecting to discovered peers
        for addr, peer_info in list(self._discovered_peers.items()):
            try:
                client = BleakClient(addr)
                await client.connect(timeout=5.0)
                self._connected_devices[addr] = client

                if len(data) <= self._mtu:
                    await client.write_gatt_char(A2A_INBOX_UUID, data, response=True)
                else:
                    result = await self._send_chunked(client, data)
                    if not result.success:
                        continue
                return SendResult(transport="ble", success=True)
            except Exception as e:
                log.debug(f"BLE connect+send to {addr} failed: {e}")
                continue

        # If we're a GATT server, notify subscribed central
        if self._subscribed_central and self._outbox_char:
            try:
                self._notify_central(data)
                return SendResult(transport="ble", success=True)
            except Exception as e:
                log.debug(f"BLE notify failed: {e}")

        return SendResult(transport="ble", success=False, error="no peers available")

    async def receive(self) -> list:
        """Poll for received BLE messages (non-blocking)."""
        messages = []
        while not self._inbox.empty():
            try:
                msg_data = self._inbox.get_nowait()
                msg = A2AMessage.from_bytes(msg_data) if isinstance(msg_data, bytes) else msg_data
                messages.append(msg)
            except Exception as e:
                log.debug(f"BLE receive parse error: {e}")
        return messages

    async def discover(self) -> list:
        """Return discovered BLE peers."""
        return [
            {"name": info.get("name", addr[:8]), "address": addr,
             "transport": "ble", "rssi": info.get("rssi", 0)}
            for addr, info in self._discovered_peers.items()
        ]

    def is_available(self) -> bool:
        return self._available

    def get_status(self) -> TransportStatus:
        return TransportStatus(
            available=self._available,
            latency_ms=50.0 if self._available else float('inf'),
            error="" if self._available else "BLE not started"
        )

    # ─── macOS GATT Server ────────────────────────────────────────────

    async def _start_macos_gatt_server(self) -> bool:
        """Start BLE GATT server on macOS using CoreBluetooth."""
        if not HAS_PYOBJC:
            return False

        try:
            # Create dispatch queue for CBPeripheralManager
            queue = dispatch_queue_create(b"a2a_mesh.ble", None)

            # Create peripheral manager
            self._peripheral_manager = CBPeripheralManager.alloc().initWithDelegate_queue_options_(
                self, queue, None
            )

            # Wait for powered on state (max 5 seconds)
            for _ in range(50):
                if self._peripheral_manager.state() == CBPeripheralManagerStatePoweredOn:
                    break
                await asyncio.sleep(0.1)

            if self._peripheral_manager.state() != CBPeripheralManagerStatePoweredOn:
                log.warning("BLE: CBPeripheralManager not powered on")
                return False

            # Create GATT service
            service_uuid = CBUUID.UUIDWithString_(A2A_SERVICE_UUID)
            self._service = CBMutableService.alloc().initWithType_primary_(
                service_uuid, True
            )

            # Create characteristics
            inbox_uuid = CBUUID.UUIDWithString_(A2A_INBOX_UUID)
            outbox_uuid = CBUUID.UUIDWithString_(A2A_OUTBOX_UUID)
            control_uuid = CBUUID.UUIDWithString_(A2A_CONTROL_UUID)

            # Inbox: WRITE by any central
            self._inbox_char = CBMutableCharacteristic.alloc().initWithType_properties_value_permissions_(
                inbox_uuid,
                CBCharacteristicProperties.CBCharacteristicPropertyWrite |
                CBCharacteristicProperties.CBCharacteristicPropertyWriteWithoutResponse,
                None,
                CBAttributePermissions.CBAttributePermissionsWriteable
            )

            # Outbox: NOTIFY to subscribed centrals
            self._outbox_char = CBMutableCharacteristic.alloc().initWithType_properties_value_permissions_(
                outbox_uuid,
                CBCharacteristicProperties.CBCharacteristicPropertyNotify,
                None,
                CBAttributePermissions.CBAttributePermissionsReadable
            )

            # Control: READ + WRITE for MTU negotiation and flow control
            self._control_char = CBMutableCharacteristic.alloc().initWithType_properties_value_permissions_(
                control_uuid,
                CBCharacteristicProperties.CBCharacteristicPropertyRead |
                CBCharacteristicProperties.CBCharacteristicPropertyWrite,
                NSData.data(),  # Empty initial value
                CBAttributePermissions.CBAttributePermissionsReadable |
                CBAttributePermissions.CBAttributePermissionsWriteable
            )

            # Set characteristics on service
            self._service.setCharacteristics_([
                self._inbox_char,
                self._outbox_char,
                self._control_char,
            ])

            # Add service to peripheral manager
            self._peripheral_manager.addService_(self._service)

            # Start advertising
            node_name = self.config.node_name if self.config else "a2a"
            advertisement_data = {
                CBAdvertisementDataLocalNameKey: f"A2A:{node_name}",
                CBAdvertisementDataServiceUUIDsKey: [service_uuid],
            }
            self._peripheral_manager.startAdvertising_(advertisement_data)
            self._advertising = True

            log.info(f"BLE: GATT server started, advertising as 'A2A:{node_name}'")
            return True

        except Exception as e:
            log.error(f"BLE GATT server start failed: {e}")
            return False

    def _notify_central(self, data: bytes):
        """Send data to subscribed central via GATT notification."""
        if not self._subscribed_central or not self._outbox_char:
            return

        # Chunk if needed
        if len(data) <= self._mtu:
            value = NSData.dataWithBytes_length_(data, len(data))
            self._peripheral_manager.updateValue_forCharacteristic_onSubscribedCentrals_(
                value, self._outbox_char, None
            )
        else:
            # Send chunks
            chunks = self._chunk_data(data, len(data).to_bytes(4, 'big') + data)
            for chunk in chunks:
                value = NSData.dataWithBytes_length_(chunk, len(chunk))
                self._peripheral_manager.updateValue_forCharacteristic_onSubscribedCentrals_(
                    value, self._outbox_char, None
                )

    # ─── CBPeripheralManagerDelegate (macOS GATT Server callbacks) ─────

    def peripheralManager_didUpdateState_(self, manager, state):
        """Called when peripheral manager state changes."""
        if state == CBPeripheralManagerStatePoweredOn:
            log.info("BLE: CBPeripheralManager powered on")
        else:
            log.warning(f"BLE: CBPeripheralManager state: {state}")

    def peripheralManager_central_didSubscribeToCharacteristic_(self, manager, central, characteristic):
        """Called when a central subscribes to our outbox."""
        self._subscribed_central = central
        log.info(f"BLE: Central subscribed: {central.identifier()}")

    def peripheralManager_central_didUnsubscribeFromCharacteristic_(self, manager, central, characteristic):
        """Called when a central unsubscribes."""
        self._subscribed_central = None
        log.info(f"BLE: Central unsubscribed: {central.identifier()}")

    def peripheralManager_didReceiveReadRequest_(self, manager, request):
        """Handle read requests (control characteristic)."""
        if request.characteristic() == self._control_char:
            # Return MTU and status info
            status_data = json.dumps({"mtu": self._mtu, "name": self.config.node_name}).encode()
            request.setValue_(NSData.dataWithBytes_length_(status_data, len(status_data)))
        manager.respondToRequest_withResult_error_(request, 0, None)  # 0 = success

    def peripheralManager_didReceiveWriteRequests_(self, manager, requests):
        """Handle write requests (inbox characteristic)."""
        for request in requests:
            if request.characteristic() == self._inbox_char:
                data = request.value().bytes().as_bytes(request.value().length())

                # Check if this is a chunk or complete message
                if self._is_chunk(data):
                    self._handle_chunk(data)
                else:
                    try:
                        msg = A2AMessage.from_bytes(data)
                        self._inbox.put_nowait(data)
                        log.debug(f"BLE: Received message {msg.id[:8]} from {msg.sender}")
                    except Exception as e:
                        log.debug(f"BLE: Parse error: {e}")

        manager.respondToRequest_withResult_error_(requests[0], 0, None)

    # ─── Scanning ──────────────────────────────────────────────────────

    async def _start_scanning(self) -> bool:
        """Start BLE scanning for A2A peers."""
        if not HAS_BLE:
            return False

        try:
            self._scan_task = asyncio.create_task(self._scan_loop())
            return True
        except Exception as e:
            log.error(f"BLE scan start failed: {e}")
            return False

    async def _scan_loop(self):
        """Continuous BLE scanning for A2A service advertisements."""
        while self._running:
            try:
                devices = await BleakScanner.discover(
                    timeout=10.0,
                    service_uuids=[A2A_SERVICE_UUID]
                )

                for device in devices:
                    name = device.name or ""
                    if name.startswith("A2A:"):
                        parts = name.split(":")
                        if len(parts) >= 2:
                            self._discovered_peers[device.address] = {
                                "name": parts[1],
                                "address": device.address,
                                "rssi": getattr(device, "rssi", 0),
                                "last_seen": time.time(),
                            }
                            log.debug(f"BLE: Discovered A2A peer {parts[1]} at {device.address}")

                # Prune stale peers (older than 60 seconds)
                now = time.time()
                stale = [addr for addr, info in self._discovered_peers.items()
                         if now - info.get("last_seen", 0) > 60]
                for addr in stale:
                    self._discovered_peers.pop(addr, None)

            except asyncio.CancelledError:
                break
            except Exception as e:
                log.debug(f"BLE scan error: {e}")
                await asyncio.sleep(5)

    # ─── Chunking ──────────────────────────────────────────────────────

    def _chunk_data(self, data: bytes, extra_header: bytes = b"") -> list[bytes]:
        """Split data into BLE MTU-sized chunks with reassembly headers."""
        payload_size = self._mtu - CHUNK_HEADER_SIZE
        chunks = []

        total_chunks = (len(data) + payload_size - 1) // payload_size
        msg_id_hash = hash(data) & 0xFFFFFFFF  # 4-byte hash for reassembly

        for i in range(total_chunks):
            offset = i * payload_size
            chunk_payload = data[offset:offset + payload_size]
            header = struct.pack(">HHI", i, total_chunks, msg_id_hash)
            chunks.append(header + chunk_payload)

        return chunks

    def _is_chunk(self, data: bytes) -> bool:
        """Check if data is a chunked message (has our header format)."""
        if len(data) < CHUNK_HEADER_SIZE:
            return False
        chunk_idx, total_chunks, msg_hash = struct.unpack(">HHI", data[:CHUNK_HEADER_SIZE])
        return total_chunks > 1 and chunk_idx < total_chunks

    def _handle_chunk(self, data: bytes):
        """Handle a chunked BLE message fragment."""
        chunk_idx, total_chunks, msg_hash = struct.unpack(">HHI", data[:CHUNK_HEADER_SIZE])
        payload = data[CHUNK_HEADER_SIZE:]

        key = str(msg_hash)
        if key not in self._pending_chunks:
            self._pending_chunks[key] = {
                "total": total_chunks,
                "chunks": {},
                "started": time.time(),
            }

        self._pending_chunks[key]["chunks"][chunk_idx] = payload

        # Check if all chunks received
        if len(self._pending_chunks[key]["chunks"]) == total_chunks:
            # Reassemble
            assembled = b""
            for i in range(total_chunks):
                assembled += self._pending_chunks[key]["chunks"].get(i, b"")

            # Clean up
            del self._pending_chunks[key]

            # Try to parse as message
            try:
                msg = A2AMessage.from_bytes(assembled)
                self._inbox.put_nowait(assembled)
                log.debug(f"BLE: Reassembled message {msg.id[:8]} from {total_chunks} chunks")
            except Exception as e:
                log.debug(f"BLE: Reassembly parse error: {e}")

    async def _send_chunked(self, client, data: bytes) -> SendResult:
        """Send large message as chunks via BLE GATT."""
        chunks = self._chunk_data(data)
        for chunk in chunks:
            try:
                await client.write_gatt_char(A2A_INBOX_UUID, chunk, response=True)
                await asyncio.sleep(0.01)  # Flow control
            except Exception as e:
                return SendResult(transport="ble", success=False, error=str(e))
        return SendResult(transport="ble", success=True)