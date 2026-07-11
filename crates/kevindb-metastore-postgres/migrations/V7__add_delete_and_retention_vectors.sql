ALTER TABLE run_heads
    ADD COLUMN IF NOT EXISTS deleted_at_unix_nano BIGINT;

ALTER TABLE run_heads
    ADD COLUMN IF NOT EXISTS deletion_reason TEXT;

CREATE TABLE IF NOT EXISTS run_deletions (
    project_name TEXT NOT NULL,
    trace_id TEXT NOT NULL,
    span_id TEXT NOT NULL,
    deleted_at_unix_nano BIGINT NOT NULL,
    reason TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(project_name, trace_id, span_id)
);

CREATE INDEX IF NOT EXISTS ix_run_deletions_project_trace
    ON run_deletions(project_name, trace_id);

CREATE TABLE IF NOT EXISTS trace_segment_delete_vectors (
    trace_segment_id BIGINT NOT NULL REFERENCES trace_segments(id) ON DELETE CASCADE,
    project_name TEXT NOT NULL,
    trace_id TEXT NOT NULL,
    span_id TEXT NOT NULL,
    deleted_at_unix_nano BIGINT NOT NULL,
    reason TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(trace_segment_id, project_name, trace_id, span_id)
);

CREATE INDEX IF NOT EXISTS ix_trace_segment_delete_vectors_run
    ON trace_segment_delete_vectors(project_name, trace_id, span_id);

CREATE TABLE IF NOT EXISTS project_retention_policies (
    project_name TEXT PRIMARY KEY REFERENCES projects(name) ON DELETE CASCADE,
    ttl_unix_nanos BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
