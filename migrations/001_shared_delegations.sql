-- A2A Mesh — shared_delegations schema sync (idempotent migration)
-- Source of truth for the mesh PG schema additions that the code depends on.
-- Run: psql "postgresql://nova:<pw>@192.168.1.30:5432/agent_memory" -f migrations/001_shared_delegations.sql
-- Safe to re-run: every statement is IF NOT EXISTS / idempotent.

-- updated_at: progress-tracking column used by stuck-delegation detection (P0 fix 2026-09-01)
ALTER TABLE shared_delegations ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NULL;
CREATE INDEX IF NOT EXISTS idx_shared_delegations_stuck
    ON shared_delegations (status, updated_at)
    WHERE status IN ('accepted', 'running');

-- task_type: handler dispatch (research/monitoring/code_review/generic) — 2026-09-01
-- (column already existed; guarded for fresh installs)
ALTER TABLE shared_delegations ADD COLUMN IF NOT EXISTS task_type TEXT DEFAULT 'generic';

-- kanban_card_id: delegation → kanban card linkage — pre-existing, guarded
ALTER TABLE shared_delegations ADD COLUMN IF NOT EXISTS kanban_card_id TEXT DEFAULT NULL;

-- history/audit: completed task archive — pre-existing, guarded
ALTER TABLE shared_delegations ADD COLUMN IF NOT EXISTS result_file TEXT DEFAULT NULL;