"""A2A Mesh Exception Hierarchy — Structured errors for precise handling.

Inspired by sushaan-k/a2a-mesh exceptions.py, adapted for our P2P mesh.
"""


class MeshError(Exception):
    """Base exception for all A2A Mesh errors."""
    def __init__(self, message: str, **context):
        super().__init__(message)
        self.context = context

    def __repr__(self):
        ctx = ", ".join(f"{k}={v}" for k, v in self.context.items())
        return f"{self.__class__.__name__}({super().__repr__()}{', ' + ctx if ctx else ''})"


# ─── Agent Errors ────────────────────────────────────────────────

class AgentError(MeshError):
    """Base for agent-related errors."""
    pass


class AgentNotFoundError(AgentError):
    """Agent not found in the registry."""
    pass


class AgentAlreadyRegisteredError(AgentError):
    """Agent already exists in the registry."""
    pass


class AgentUnhealthyError(AgentError):
    """Agent health score below minimum threshold."""
    pass


class AgentPendingApprovalError(AgentError):
    """Agent registration pending admin approval."""
    pass


class NoCapableAgentError(AgentError):
    """No agent found with the required capabilities."""
    pass


# ─── Routing Errors ───────────────────────────────────────────────

class RoutingError(MeshError):
    """Base for routing-related errors."""
    pass


class QueueFullError(RoutingError):
    """All capable agents are at capacity (queue depth exceeded)."""
    pass


class FallbackExhaustedError(RoutingError):
    """Both primary and fallback routing strategies failed."""
    pass


class StickySessionError(RoutingError):
    """Sticky session target agent is no longer available."""
    pass


# ─── Workflow Errors ──────────────────────────────────────────────

class WorkflowError(MeshError):
    """Base for workflow-related errors."""
    pass


class CyclicDependencyError(WorkflowError):
    """Workflow DAG contains a cycle."""
    pass


class TaskExecutionError(WorkflowError):
    """Task execution failed."""
    pass


class ConsensusNotReachedError(WorkflowError):
    """Consensus mode requirements not met (e.g., not enough responses)."""
    pass


class BudgetExceededError(WorkflowError):
    """Workflow or task cost exceeded the budget."""
    pass


class TaskTimeoutError(WorkflowError):
    """Task execution timed out."""
    pass


# ─── Auth Errors ──────────────────────────────────────────────────

class AuthError(MeshError):
    """Base for authentication/authorization errors."""
    pass


class TokenExpiredError(AuthError):
    """Authentication token has expired."""
    pass


class InsufficientScopeError(AuthError):
    """Token does not have the required scope for this operation."""
    pass


class TokenRevokedError(AuthError):
    """Token has been revoked."""
    pass


class AgentApprovalRequiredError(AuthError):
    """Operation requires admin approval (e.g., new agent registration)."""
    pass


# ─── Protocol Errors ──────────────────────────────────────────────

class ProtocolError(MeshError):
    """Base for protocol-related errors."""
    pass


class MessageFormatError(ProtocolError):
    """Message format is invalid."""
    pass


class SignatureVerificationError(ProtocolError):
    """Ed25519 signature verification failed."""
    pass


class EncryptionError(ProtocolError):
    """NaCl encryption/decryption error."""
    pass


# ─── Rate Limiting Errors ─────────────────────────────────────────

class RateLimitError(MeshError):
    """Rate limit exceeded."""
    def __init__(self, message: str = "Rate limit exceeded", retry_after: float = 0, **context):
        super().__init__(message, **context)
        self.retry_after = retry_after


# ─── Health Check Errors ──────────────────────────────────────────

class HealthCheckError(MeshError):
    """Base for health check errors."""
    pass


class HealthCheckTimeoutError(HealthCheckError):
    """Health check timed out."""
    pass


class HealthCheckFailedError(HealthCheckError):
    """Health check failed (unhealthy response)."""
    pass


# ─── Connection Errors ─────────────────────────────────────────────

class ConnectionError(MeshError):
    """Base for P2P connection errors."""
    pass


class PeerUnavailableError(ConnectionError):
    """Peer is not reachable via P2P."""
    pass


class PeerRejectedError(ConnectionError):
    """Peer rejected the connection."""
    pass


class TransportError(MeshError):
    """Base for transport errors."""
    pass


class PGTransportError(TransportError):
    """PostgreSQL transport error."""
    pass


class P2PTransportError(TransportError):
    """P2P transport error."""
    pass


# ─── Configuration Errors ──────────────────────────────────────────

class ConfigurationError(MeshError):
    """Configuration is invalid or incomplete."""
    pass