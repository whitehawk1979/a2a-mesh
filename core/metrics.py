"""Prometheus-format metrics collector for A2A Mesh.

Provides:
- Counter: monotonically increasing value (messages sent, received, errors)
- Gauge: point-in-time value (queue depth, peer count, memory RSS)
- Histogram: distribution of values (latency, delegation response time)

All metrics are exposed via /metrics endpoint in Prometheus text format.
"""

import time
import threading
import resource
import logging
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from collections import defaultdict

log = logging.getLogger("a2a_mesh.metrics")

# ── Metric types ──

@dataclass
class Counter:
    """Monotonically increasing counter."""
    name: str
    help_text: str
    value: float = 0.0
    labels: Optional[Dict[str, str]] = None

    def inc(self, amount: float = 1.0):
        self.value += amount

    def render(self) -> str:
        label_str = ""
        if self.labels:
            parts = [f'{k}="{v}"' for k, v in self.labels.items()]
            label_str = "{" + ",".join(parts) + "}"
        return f"# HELP {self.name} {self.help_text}\n# TYPE {self.name} counter\n{self.name}{label_str} {self.value}"


@dataclass
class Gauge:
    """Point-in-time value that can go up or down."""
    name: str
    help_text: str
    value: float = 0.0
    labels: Optional[Dict[str, str]] = None

    def set(self, value: float):
        self.value = value

    def inc(self, amount: float = 1.0):
        self.value += amount

    def dec(self, amount: float = 1.0):
        self.value -= amount

    def render(self) -> str:
        label_str = ""
        if self.labels:
            parts = [f'{k}="{v}"' for k, v in self.labels.items()]
            label_str = "{" + ",".join(parts) + "}"
        return f"# HELP {self.name} {self.help_text}\n# TYPE {self.name} gauge\n{self.name}{label_str} {self.value}"


@dataclass
class HistogramBucket:
    """Single bucket in a histogram."""
    upper_bound: float
    count: float = 0.0


class Histogram:
    """Distribution tracking with configurable buckets.

    Tracks count, sum, and bucket counts in Prometheus histogram format.
    """

    def __init__(self, name: str, help_text: str, buckets: Optional[List[float]] = None,
                 labels: Optional[Dict[str, str]] = None):
        self.name = name
        self.help_text = help_text
        self.labels = labels or {}
        # Default Prometheus-style buckets (similar to client_golang defaults)
        if buckets is None:
            buckets = [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]
        # Add +Inf bucket (always required)
        self.buckets: List[HistogramBucket] = [
            HistogramBucket(upper_bound=b) for b in sorted(buckets)
        ]
        self.buckets.append(HistogramBucket(upper_bound=float("inf")))
        self.count = 0
        self.sum = 0.0
        self._lock = threading.Lock()

    def observe(self, value: float):
        """Record a value in the histogram."""
        with self._lock:
            self.count += 1
            self.sum += value
            for bucket in self.buckets:
                if value <= bucket.upper_bound:
                    bucket.count += 1

    def render(self) -> str:
        """Render in Prometheus text format."""
        label_str = ""
        if self.labels:
            parts = [f'{k}="{v}"' for k, v in self.labels.items()]
            label_str = "{" + ",".join(parts) + "}"

        lines = [f"# HELP {self.name} {self.help_text}",
                 f"# TYPE {self.name} histogram"]

        with self._lock:
            cumulative = 0.0
            for bucket in self.buckets:
                cumulative += bucket.count
                le_label = f'le="{bucket.upper_bound}"'
                if self.labels:
                    all_labels = ",".join([f'{k}="{v}"' for k, v in self.labels.items()] + [le_label])
                    lines.append(f"{self.name}{{{all_labels}}} {cumulative}")
                else:
                    lines.append(f"{self.name}{{{le_label}}} {cumulative}")

            lines.append(f"{self.name}_count{label_str} {self.count}")
            lines.append(f"{self.name}_sum{label_str} {self.sum:.6f}")

        return "\n".join(lines)


# ── Metrics Registry ──

class MetricsRegistry:
    """Central registry for all mesh metrics.

    Collects metrics from various subsystems (router, transport, delegation, etc.)
    and renders them in Prometheus text format for the /metrics endpoint.
    """

    def __init__(self, node_name: str = "unknown"):
        self.node_name = node_name
        self._counters: Dict[str, Counter] = {}
        self._gauges: Dict[str, Gauge] = {}
        self._histograms: Dict[str, Histogram] = {}
        self._lock = threading.Lock()
        self._start_time = time.time()

        # ── Core mesh metrics ──
        self._register_core_metrics()

    def _key(self, name: str, labels: Optional[Dict[str, str]] = None) -> str:
        """Create a unique key for a metric + its labels."""
        if labels:
            sorted_labels = tuple(sorted(labels.items()))
            return f"{name}|{sorted_labels}"
        return name

    def _register_core_metrics(self):
        """Register the standard A2A mesh metrics."""
        # Transport counters
        self.counter("a2a_mesh_messages_sent_total", "Total messages sent via mesh")
        self.counter("a2a_mesh_messages_received_total", "Total messages received via mesh")
        self.counter("a2a_mesh_messages_forwarded_total", "Total messages forwarded to other peers")
        self.counter("a2a_mesh_messages_duplicated_total", "Total duplicate messages filtered")
        self.counter("a2a_mesh_messages_errors_total", "Total message processing errors")
        self.counter("a2a_mesh_transport_sent_total", "Messages sent per transport", labels={"transport": "pg"})
        self.counter("a2a_mesh_transport_sent_total", "Messages sent per transport", labels={"transport": "p2p"})
        self.counter("a2a_mesh_transport_sent_total", "Messages sent per transport", labels={"transport": "http"})
        self.counter("a2a_mesh_transport_received_total", "Messages received per transport", labels={"transport": "pg"})
        self.counter("a2a_mesh_transport_received_total", "Messages received per transport", labels={"transport": "p2p"})
        self.counter("a2a_mesh_transport_received_total", "Messages received per transport", labels={"transport": "http"})
        self.gauge("a2a_mesh_transport_available", "Transport availability (1=available, 0=not)", labels={"transport": "pg"})
        self.gauge("a2a_mesh_transport_available", "Transport availability (1=available, 0=not)", labels={"transport": "p2p"})
        self.gauge("a2a_mesh_transport_available", "Transport availability (1=available, 0=not)", labels={"transport": "http"})
        self.gauge("a2a_mesh_transport_available", "Transport availability (1=available, 0=not)", labels={"transport": "ble"})

        # Queue gauges
        self.gauge("a2a_mesh_queue_depth", "Current queue depth", labels={"queue": "inbound"})
        self.gauge("a2a_mesh_queue_depth", "Current queue depth", labels={"queue": "outbound"})
        self.gauge("a2a_mesh_queue_depth", "Current queue depth", labels={"queue": "offline"})
        self.gauge("a2a_mesh_p2p_incoming_queue_depth", "P2P incoming message queue depth")

        # Peer gauges
        self.gauge("a2a_mesh_peer_count", "Number of connected P2P peers")

        # Delegation counters
        self.counter("a2a_mesh_delegations_created_total", "Total tasks delegated out")
        self.counter("a2a_mesh_delegations_completed_total", "Total tasks completed successfully")
        self.counter("a2a_mesh_delegations_failed_total", "Total tasks that failed")
        self.counter("a2a_mesh_delegations_claimed_total", "Total available tasks claimed by this node")
        self.gauge("a2a_mesh_delegations_active", "Number of currently running tasks")
        self.gauge("a2a_mesh_delegations_available", "Number of available (unclaimed) tasks")

        # Delegation SLA histogram
        self.histogram("a2a_mesh_delegation_response_time_seconds",
                       "Time from task creation to completion (seconds)",
                       buckets=[5, 15, 30, 60, 120, 300, 600, 1800, 3600])

        # Peer latency histogram — per-peer labels will use same ms buckets
        self._peer_latency_buckets = [1, 5, 10, 25, 50, 100, 250, 500, 1000, 5000]
        self.histogram("a2a_mesh_peer_latency_ms",
                       "P2P peer round-trip latency in milliseconds",
                       buckets=self._peer_latency_buckets)

        # System gauges
        self.gauge("a2a_mesh_memory_rss_mb", "Process RSS memory in MB")
        self.gauge("a2a_mesh_uptime_seconds", "Process uptime in seconds")
        self.gauge("a2a_mesh_cpu_percent", "Process CPU usage percentage")
        self.gauge("a2a_mesh_system_cpu_percent", "System CPU usage percentage")
        self.gauge("a2a_mesh_system_memory_percent", "System memory usage percentage")
        self.gauge("a2a_mesh_system_disk_percent", "Disk usage percentage on root partition")

        # Mesh availability (computed from peer count)
        self.gauge("a2a_mesh_availability_ratio", "Mesh availability ratio (connected peers / expected peers)")

        # Circuit breaker gauge
        self.gauge("a2a_mesh_circuit_breaker_open", "Circuit breaker status per peer (1=open, 0=closed)",
                    labels={"peer": ""})
        # We'll dynamically create these per peer — placeholder only

    # ── Registration ──

    def counter(self, name: str, help_text: str, labels: Optional[Dict[str, str]] = None) -> Counter:
        key = self._key(name, labels)
        with self._lock:
            if key not in self._counters:
                self._counters[key] = Counter(name=name, help_text=help_text, labels=labels)
            return self._counters[key]

    def gauge(self, name: str, help_text: str, labels: Optional[Dict[str, str]] = None) -> Gauge:
        key = self._key(name, labels)
        with self._lock:
            if key not in self._gauges:
                self._gauges[key] = Gauge(name=name, help_text=help_text, labels=labels)
            return self._gauges[key]

    def histogram(self, name: str, help_text: str, buckets: Optional[List[float]] = None,
                   labels: Optional[Dict[str, str]] = None) -> Histogram:
        key = self._key(name, labels)
        with self._lock:
            if key not in self._histograms:
                self._histograms[key] = Histogram(name=name, help_text=help_text,
                                                    buckets=buckets, labels=labels)
            return self._histograms[key]

    # ── Convenience methods ──

    def inc_counter(self, name: str, amount: float = 1.0, labels: Optional[Dict[str, str]] = None):
        c = self.counter(name, "", labels=labels)
        c.inc(amount)

    def set_gauge(self, name: str, value: float, labels: Optional[Dict[str, str]] = None):
        g = self.gauge(name, "", labels=labels)
        g.set(value)

    def observe_histogram(self, name: str, value: float, labels: Optional[Dict[str, str]] = None):
        h = self.histogram(name, "", labels=labels)
        h.observe(value)

    # ── Collect from node subsystems ──

    def collect_from_node(self, node):
        """Gather metrics from the MeshNode and all its subsystems.

        This is called on every /metrics scrape to get fresh values.
        """
        # ── Router stats ──
        stats = node.router.get_stats()
        self.set_gauge("a2a_mesh_messages_sent_total", stats.get("sent", 0))
        self.set_gauge("a2a_mesh_messages_received_total", stats.get("received", 0))
        # Also use as counters for cumulative values
        self.inc_counter("a2a_mesh_messages_forwarded_total", 0)
        self.inc_counter("a2a_mesh_messages_duplicated_total", 0)
        self.inc_counter("a2a_mesh_messages_errors_total", 0)

        # Update forwarded/duplicated/errors gauges (they're really counters but we set absolute)
        key_fwd = self._key("a2a_mesh_messages_forwarded_total")
        if key_fwd in self._counters:
            self._counters[key_fwd].value = stats.get("forwarded", 0)
        key_dup = self._key("a2a_mesh_messages_duplicated_total")
        if key_dup in self._counters:
            self._counters[key_dup].value = stats.get("duplicates", 0)
        key_err = self._key("a2a_mesh_messages_errors_total")
        if key_err in self._counters:
            self._counters[key_err].value = stats.get("errors", 0)

        # ── Queue depths ──
        inbound_stats = stats.get("inbound_queue", {})
        outbound_stats = stats.get("outbound_queue", {})
        self.set_gauge("a2a_mesh_queue_depth", inbound_stats.get("current_size", 0), labels={"queue": "inbound"})
        self.set_gauge("a2a_mesh_queue_depth", outbound_stats.get("current_size", 0), labels={"queue": "outbound"})

        # ── P2P transport ──
        p2p = node._p2p_transport
        p2p_peers = list(p2p._peers.keys()) if hasattr(p2p, '_peers') else []
        self.set_gauge("a2a_mesh_peer_count", len(p2p_peers))
        if hasattr(p2p, '_incoming_queue'):
            self.set_gauge("a2a_mesh_p2p_incoming_queue_depth", p2p._incoming_queue.qsize())

        # Peer latency (per-peer histogram with ms buckets)
        if hasattr(p2p, '_peer_latency'):
            for peer_name, lat in p2p._peer_latency.items():
                if lat > 0:
                    h = self.histogram("a2a_mesh_peer_latency_ms",
                                       "P2P peer round-trip latency in milliseconds",
                                       buckets=self._peer_latency_buckets,
                                       labels={"peer": peer_name})
                    h.observe(lat)

        # ── Transport availability (gauges, since these are current state) ──
        # We track transport availability as gauge (1=available, 0=not)
        for transport_name, transport in [("pg", node._pg_transport),
                                           ("p2p", node._p2p_transport),
                                           ("http", node._http_transport),
                                           ("ble", node._ble_transport)]:
            avail = 1 if (transport and transport.is_available()) else 0
            self.set_gauge("a2a_mesh_transport_available", avail, labels={"transport": transport_name})

        # ── Delegation ──
        delegation = node.delegation
        if delegation:
            active = len(delegation._active_tasks)
            self.set_gauge("a2a_mesh_delegations_active", active)

            # Circuit breaker status
            for peer, cb in delegation._circuit_breakers.items():
                cb_gauge = self.gauge("a2a_mesh_circuit_breaker_open",
                                       "Circuit breaker status per peer (1=open, 0=closed)",
                                       labels={"peer": peer})
                cb_gauge.set(1 if cb.get("open", False) else 0)

        # ── System ──
        try:
            rss_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0  # KB → MB
            self.set_gauge("a2a_mesh_memory_rss_mb", round(rss_mb, 1))
        except Exception:
            pass

        uptime = time.time() - node._start_time if node._start_time else 0
        self.set_gauge("a2a_mesh_uptime_seconds", round(uptime, 1))

        # CPU + system metrics via psutil (if available)
        try:
            import psutil
            proc = psutil.Process()
            self.set_gauge("a2a_mesh_cpu_percent", round(proc.cpu_percent(interval=0.1), 1))
            self.set_gauge("a2a_mesh_system_cpu_percent", round(psutil.cpu_percent(interval=0.1), 1))
            self.set_gauge("a2a_mesh_system_memory_percent", round(psutil.virtual_memory().percent, 1))
            self.set_gauge("a2a_mesh_system_disk_percent", round(psutil.disk_usage('/').percent, 1))
        except ImportError:
            pass  # psutil not available — skip system metrics

    # ── Render ──

    def render_prometheus(self) -> str:
        """Render all metrics in Prometheus text exposition format."""
        lines = []
        with self._lock:
            # Counters
            for key in sorted(self._counters.keys()):
                lines.append(self._counters[key].render())
            # Gauges
            for key in sorted(self._gauges.keys()):
                lines.append(self._gauges[key].render())
            # Histograms
            for key in sorted(self._histograms.keys()):
                lines.append(self._histograms[key].render())

        return "\n".join(lines) + "\n"


# ── Global singleton ──
_registry: Optional[MetricsRegistry] = None


def get_metrics(node_name: str = "unknown") -> MetricsRegistry:
    """Get or create the global metrics registry."""
    global _registry
    if _registry is None:
        _registry = MetricsRegistry(node_name)
    return _registry


def reset_metrics():
    """Reset the global metrics registry (for testing)."""
    global _registry
    _registry = None
