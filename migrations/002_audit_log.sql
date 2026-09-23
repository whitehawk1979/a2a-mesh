-- A2A Mesh — audit trail (idempotent migration)
-- Idea: minden kritikus esemény nyoma marad: delegation accept/reject,
-- skill install, config change, auth event, stb.
-- Run: psql "postgresql://nova:***@192.168.1.30:5432/agent_memory" -f migrations/002_audit_log.sql
-- Safe to re-run: every statement is IF NOT EXISTS / idempotent.

CREATE TABLE IF NOT EXISTS mesh.audit_log (
    log_id      BIGSERIAL PRIMARY KEY,
    ts          TIMESTAMPTZ NOT NULL DEFAULT now(),
    node        TEXT NOT NULL,              -- runa / morzsa / nova
    agent       TEXT,                       -- Rúna / Morzsa / Nova / <subagent>
    event_type  TEXT NOT NULL,              -- delegation_accept, delegation_reject, skill_install, config_change, auth_event...
    entity_type TEXT,                       -- delegation, skill, config, idea, message...
    entity_id   TEXT,                       -- task_id / skill name / config key
    severity    TEXT NOT NULL DEFAULT 'info',   -- info | warning | critical
    details     JSONB NOT NULL DEFAULT '{}'::jsonb  -- payload: before/after, reason, ack info...
);

-- Query patterns: recent events (dashboard), event filter, entity timeline
CREATE INDEX IF NOT EXISTS idx_audit_log_ts    ON mesh.audit_log (ts DESC);
CREATE INDEX IF NOT EXISTS idx_audit_log_event ON mesh.audit_log (event_type, ts DESC);
CREATE INDEX IF NOT EXISTS idx_audit_log_entity ON mesh.audit_log (entity_type, entity_id, ts DESC);

-- Grants (schema_init GRANT only covers tables existing at init time)
GRANT ALL ON mesh.audit_log TO PUBLIC;
GRANT USAGE, SELECT ON SEQUENCE mesh.audit_log_log_id_seq TO PUBLIC;

-- Retention hint: a monthly cleanup job can use:
--   DELETE FROM mesh.audit_log WHERE ts < now() - interval '180 days';