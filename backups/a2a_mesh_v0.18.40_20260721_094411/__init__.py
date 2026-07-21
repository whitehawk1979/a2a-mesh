"""A2A Mesh — Transport-agnostic mesh communication for agents.

Provides P2P mesh networking between agents on any platform.
Primary: PG NOTIFY, Secondary: TCP P2P, Tertiary: HTTP/MCP
Discovery: mDNS (zeroconf) + static config
Encryption: Ed25519 signing + NaCl encryption
"""