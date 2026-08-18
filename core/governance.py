"""
Governance/Egress Gates — delegation output validation.

Inspired by Marveen's agent-scaffold.ts PreToolUse hooks:
  - Egress filtering: block sensitive data from leaving the mesh
  - File access control: restrict which paths agents can read/write
  - Command validation: block dangerous commands in delegation results
  - PII detection: redact personal info from delegation outputs

A2A Mesh adaptation:
  - Applied to delegation results BEFORE returning to the leader
  - Configurable per-agent security profile (applier vs default)
  - Logs all blocked items for audit trail
  - API endpoint for rule management
"""

import re
import logging
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Tuple
from enum import Enum

log = logging.getLogger("governance")


class GateResult(Enum):
    PASS = "pass"        # Output is clean
    REDACT = "redact"    # Sensitive data found, redacted
    BLOCK = "block"      # Dangerous content, blocked entirely


@dataclass
class GateRule:
    """A single governance rule."""
    name: str
    pattern: str                    # Regex pattern to match
    action: GateResult              # What to do when matched
    replacement: str = "[REDACTED]"  # For REDACT actions
    description: str = ""
    enabled: bool = True


@dataclass
class GateCheckResult:
    """Result of a governance check."""
    action: GateResult
    cleaned_output: str = ""
    blocked_items: List[str] = field(default_factory=list)
    matched_rules: List[str] = field(default_factory=list)


# ── Default rules (Marveen-inspired egress/PII filtering) ──

DEFAULT_RULES: List[GateRule] = [
    # PII: email addresses
    GateRule(
        name="pii_email",
        pattern=r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
        action=GateResult.REDACT,
        replacement="[EMAIL REDACTED]",
        description="Redact email addresses from delegation outputs",
    ),
    # PII: phone numbers (international format)
    GateRule(
        name="pii_phone",
        pattern=r'\b\+?\d{1,3}[-.\s]?\(?\d{1,4}\)?[-.\s]?\d{3,4}[-.\s]?\d{3,4}\b',
        action=GateResult.REDACT,
        replacement="[PHONE REDACTED]",
        description="Redact phone numbers",
    ),
    # PII: credit card numbers
    GateRule(
        name="pii_credit_card",
        pattern=r'\b(?:\d[ -]*?){13,16}\b',
        action=GateResult.REDACT,
        replacement="[CARD REDACTED]",
        description="Redact credit card numbers",
    ),
    # Secret: API keys (common formats)
    GateRule(
        name="secret_api_key",
        pattern=r'(?:sk-|pk-|api[_-]?key|secret[_-]?key|access[_-]?token)[=:]\s*[\w\-]{20,}',
        action=GateResult.REDACT,
        replacement="[API KEY REDACTED]",
        description="Redact API keys and tokens",
    ),
    # Secret: Bearer tokens
    GateRule(
        name="secret_bearer",
        pattern=r'Bearer\s+[\w\-\.]{20,}',
        action=GateResult.REDACT,
        replacement="[BEARER TOKEN REDACTED]",
        description="Redact Bearer tokens",
    ),
    # Secret: passwords in config
    GateRule(
        name="secret_password",
        pattern=r'(?:password|passwd|pwd)\s*[=:]\s*\S+',
        action=GateResult.REDACT,
        replacement="[PASSWORD REDACTED]",
        description="Redact passwords from config output",
    ),
    # Dangerous: rm -rf /
    GateRule(
        name="danger_rm_rf",
        pattern=r'rm\s+-rf\s+/(?:\s|$)',
        action=GateResult.BLOCK,
        description="Block rm -rf / commands",
    ),
    # Dangerous: dd to disk
    GateRule(
        name="danger_dd",
        pattern=r'dd\s+.*of=/dev/(?:sd|nvme|hd)',
        action=GateResult.BLOCK,
        description="Block dd to physical disks",
    ),
    # Dangerous: mkfs
    GateRule(
        name="danger_mkfs",
        pattern=r'mkfs\.\w+\s+/dev/',
        action=GateResult.BLOCK,
        description="Block filesystem formatting",
    ),
    # Internal IP leakage (optional — disable for internal mesh comms)
    GateRule(
        name="internal_ip",
        pattern=r'\b192\.168\.\d+\.\d+\b',
        action=GateResult.REDACT,
        replacement="[IP REDACTED]",
        description="Redact internal IPs from external outputs",
        enabled=False,  # Disabled for internal mesh (we ARE internal)
    ),
]


class GovernanceGates:
    """Governance gate manager — applies rules to delegation outputs."""

    def __init__(self, rules: List[GateRule] = None):
        self.rules = rules or DEFAULT_RULES
        self._blocked_log: List[Dict] = []

    def add_rule(self, rule: GateRule):
        """Add a custom rule."""
        self.rules.append(rule)
        log.info(f"Governance: added rule '{rule.name}'")

    def remove_rule(self, name: str) -> bool:
        """Remove a rule by name."""
        before = len(self.rules)
        self.rules = [r for r in self.rules if r.name != name]
        return len(self.rules) < before

    def check(self, output: str, agent_name: str = "unknown",
              security_profile: str = "default") -> GateCheckResult:
        """Check delegation output against all enabled rules.
        
        Args:
            output: The delegation result text
            agent_name: Which agent produced this output
            security_profile: "applier" (leader, strict) or "default" (member, lenient)
        
        Returns:
            GateCheckResult with cleaned output and blocked items
        """
        cleaned = output
        blocked_items = []
        matched_rules = []

        for rule in self.rules:
            if not rule.enabled:
                continue

            # Applier profile: skip internal_ip rule (leaders need full info)
            if security_profile == "applier" and rule.name == "internal_ip":
                continue

            matches = re.findall(rule.pattern, cleaned)
            if matches:
                matched_rules.append(rule.name)

                if rule.action == GateResult.BLOCK:
                    blocked_items.append(f"{rule.name}: {matches[:3]}")
                    log.warning(
                        f"Governance BLOCK [{rule.name}] by {agent_name}: "
                        f"matched {len(matches)}x — {str(matches[:2])[:100]}"
                    )
                    return GateCheckResult(
                        action=GateResult.BLOCK,
                        cleaned_output="",
                        blocked_items=blocked_items,
                        matched_rules=matched_rules,
                    )
                elif rule.action == GateResult.REDACT:
                    cleaned = re.sub(rule.pattern, rule.replacement, cleaned)
                    log.info(
                        f"Governance REDACT [{rule.name}] by {agent_name}: "
                        f"redacted {len(matches)} occurrence(s)"
                    )

        # Log for audit trail
        if matched_rules:
            self._blocked_log.append({
                "agent": agent_name,
                "rules": matched_rules,
                "blocked_items": blocked_items,
                "security_profile": security_profile,
            })

        return GateCheckResult(
            action=GateResult.PASS if not blocked_items else GateResult.BLOCK,
            cleaned_output=cleaned,
            blocked_items=blocked_items,
            matched_rules=matched_rules,
        )

    def get_audit_log(self, limit: int = 100) -> List[Dict]:
        """Get audit log of all governance actions."""
        return self._blocked_log[-limit:]

    def get_rules(self) -> List[Dict]:
        """Get all rules as dicts for API."""
        return [
            {
                "name": r.name,
                "action": r.action.value,
                "description": r.description,
                "enabled": r.enabled,
                "pattern": r.pattern[:80] + "..." if len(r.pattern) > 80 else r.pattern,
            }
            for r in self.rules
        ]


# Global instance
_gates: Optional[GovernanceGates] = None


def get_gates() -> GovernanceGates:
    """Get or create the global governance gates instance."""
    global _gates
    if _gates is None:
        _gates = GovernanceGates()
    return _gates