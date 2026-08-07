-- A2A Mesh v0.29.0 — PostgreSQL Schema Init
-- Run: psql -h <host> -U <user> -d <database> -f schema_init.sql
-- Or: install.sh --pg-init will run this automatically

CREATE SCHEMA IF NOT EXISTS mesh;

-- ─── Node Registry ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS mesh.mesh_nodes (
    node_name       TEXT PRIMARY KEY,
    host            TEXT,
    p2p_port        INT DEFAULT 8645,
    health_port     INT DEFAULT 8650,
    status          TEXT DEFAULT 'active',
    last_heartbeat  TIMESTAMPTZ,
    version         TEXT DEFAULT '0.29.0',
    capabilities     TEXT[] DEFAULT '{}',
    skills          TEXT[] DEFAULT '{}',
    pg_available    BOOLEAN DEFAULT false,
    p2p_available   BOOLEAN DEFAULT false,
    http_available  BOOLEAN DEFAULT false,
    provider_status JSONB DEFAULT '{}'
);

-- ─── Messages ───────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS mesh.mesh_messages (
    id           TEXT PRIMARY KEY,
    sender       TEXT NOT NULL,
    recipient     TEXT,
    msg_type     TEXT NOT NULL,
    payload      JSONB,
    priority     INT DEFAULT 5,
    status       TEXT DEFAULT 'sent',
    created_at   TIMESTAMPTZ DEFAULT NOW(),
    delivered_at  TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_mesh_messages_recipient ON mesh.mesh_messages (recipient, status);
CREATE INDEX IF NOT EXISTS idx_mesh_messages_created ON mesh.mesh_messages (created_at DESC);

-- ─── Delegation Tasks ──────────────────────────────────────────
CREATE TABLE IF NOT EXISTS mesh.mesh_tasks (
    task_id        TEXT PRIMARY KEY,
    sender         TEXT NOT NULL,
    assigned_agent TEXT,
    subject        TEXT,
    description    TEXT,
    status         TEXT DEFAULT 'pending',
    result         TEXT,
    priority       INT DEFAULT 5,
    created_at     TIMESTAMPTZ DEFAULT NOW(),
    started_at     TIMESTAMPTZ,
    completed_at   TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_mesh_tasks_status ON mesh.mesh_tasks (status, assigned_agent);
CREATE INDEX IF NOT EXISTS idx_mesh_tasks_created ON mesh.mesh_tasks (created_at DESC);

-- ─── Health History (v0.29.0) ──────────────────────────────────
CREATE TABLE IF NOT EXISTS mesh.mesh_health_history (
    history_id           BIGSERIAL PRIMARY KEY,
    node_name            TEXT NOT NULL,
    health_score         FLOAT DEFAULT 1.0,
    avg_latency_ms       FLOAT DEFAULT 0,
    total_requests       INT DEFAULT 0,
    total_failures       INT DEFAULT 0,
    total_successes      INT DEFAULT 0,
    consecutive_failures INT DEFAULT 0,
    provider_primary     TEXT DEFAULT '',
    provider_fallback    TEXT DEFAULT '',
    recorded_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_health_history_node_time ON mesh.mesh_health_history (node_name, recorded_at DESC);

-- ─── Config Suggestions (v0.29.0) ──────────────────────────────
CREATE TABLE IF NOT EXISTS mesh.mesh_suggestions (
    suggestion_id    TEXT PRIMARY KEY,
    node             TEXT NOT NULL,
    category         TEXT,
    priority         TEXT DEFAULT 'medium',
    title            TEXT,
    description      TEXT,
    current_value    TEXT,
    suggested_value  TEXT,
    rationale        TEXT,
    affected_nodes   TEXT[] DEFAULT '{}',
    status           TEXT DEFAULT 'pending',
    implemented_at   TIMESTAMPTZ,
    created_at       TIMESTAMPTZ DEFAULT NOW(),
    updated_at       TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_suggestions_status ON mesh.mesh_suggestions (status, created_at DESC);

-- ─── Mesh Events (audit log) ───────────────────────────────────
CREATE TABLE IF NOT EXISTS mesh.mesh_events (
    event_id   BIGSERIAL PRIMARY KEY,
    node_name  TEXT,
    event_type TEXT,
    details    JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_events_node_time ON mesh.mesh_events (node_name, created_at DESC);

-- ─── Permissions ───────────────────────────────────────────────
GRANT USAGE ON SCHEMA mesh TO PUBLIC;
GRANT ALL ON ALL TABLES IN SCHEMA mesh TO PUBLIC;
GRANT ALL ON ALL SEQUENCES IN SCHEMA mesh TO PUBLIC;