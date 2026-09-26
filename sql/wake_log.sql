-- Wake 2.0: wake-log tábla (dedup, coalescing, watchdog, retry tracking)
CREATE TABLE IF NOT EXISTS mesh.wake_log (
    wake_id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    target_agent   TEXT NOT NULL,            -- agent neve (nova/tor/morzsa/runa/mano) vagy mcp:<name>
    target_host    TEXT,                     -- host ahol az agent fut (pl 192.168.1.30)
    wake_type      TEXT NOT NULL DEFAULT 'agent',   -- agent | mcp_dm | broadcast
    prompt_hash    TEXT,                     -- prompt tartalom hash (dedup kulcs)
    message_id     TEXT,                     -- eredeti chat message uuid (nullable)
    sender         TEXT,                     -- ki kezdeményezte
    status         TEXT NOT NULL DEFAULT 'sent',   -- sent | delivered | coalesced | failed | timeout
    attempts       INT NOT NULL DEFAULT 1,          -- küldési próbálkozások
    max_attempts   INT NOT NULL DEFAULT 3,
    sent_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    delivered_at   TIMESTAMPTZ,                     -- fogadó node visszaigazolása
    first_ack_at   TIMESTAMPTZ,                     -- első ack
    last_error     TEXT,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_wake_log_target_recent ON mesh.wake_log (target_agent, sent_at DESC);
CREATE INDEX IF NOT EXISTS idx_wake_log_status_timeout ON mesh.wake_log (status, sent_at) WHERE status = 'sent';

-- Watchdog-számok: wake-ok, amelyek 5 percen belül nem kaptak delivered_at-ot
-- (a node-ok a fogadáskor UPDATE ... SET status='delivered', delivered_at=now())
COMMENT ON TABLE mesh.wake_log IS 'Wake 2.0 — minden wake kérés loggolva: dedup, coalescing, delivered-ack, watchdog retry';