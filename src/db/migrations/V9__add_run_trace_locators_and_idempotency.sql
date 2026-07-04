ALTER TABLE run_heads
    ADD COLUMN IF NOT EXISTS generated_run_id TEXT NOT NULL DEFAULT '';

ALTER TABLE run_events
    ADD COLUMN IF NOT EXISTS idempotency_key TEXT;

ALTER TABLE run_events
    ALTER COLUMN idempotency_key SET NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS ux_run_events_project_idempotency
    ON run_events(project_name, idempotency_key);

CREATE INDEX IF NOT EXISTS ix_run_heads_generated_run_id
    ON run_heads(generated_run_id)
    WHERE generated_run_id <> '';

CREATE TABLE IF NOT EXISTS run_locators (
    project_name TEXT NOT NULL,
    run_id TEXT NOT NULL DEFAULT '',
    generated_run_id TEXT NOT NULL DEFAULT '',
    trace_id TEXT NOT NULL,
    span_id TEXT NOT NULL,
    trace_segment_id BIGINT NOT NULL REFERENCES trace_segments(id) ON DELETE CASCADE,
    row_index BIGINT NOT NULL,
    event_type TEXT NOT NULL,
    event_time_unix_nano BIGINT NOT NULL,
    run_event_id BIGINT REFERENCES run_events(id),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(project_name, trace_id, span_id)
);

CREATE INDEX IF NOT EXISTS ix_run_locators_run_id
    ON run_locators(run_id)
    WHERE run_id <> '';

CREATE INDEX IF NOT EXISTS ix_run_locators_generated_run_id
    ON run_locators(generated_run_id)
    WHERE generated_run_id <> '';

CREATE TABLE IF NOT EXISTS trace_locators (
    project_name TEXT NOT NULL,
    trace_id TEXT NOT NULL,
    span_id TEXT NOT NULL,
    trace_segment_id BIGINT NOT NULL REFERENCES trace_segments(id) ON DELETE CASCADE,
    row_index BIGINT NOT NULL,
    event_type TEXT NOT NULL,
    event_time_unix_nano BIGINT NOT NULL,
    run_event_id BIGINT REFERENCES run_events(id),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(project_name, trace_id, span_id)
);

CREATE INDEX IF NOT EXISTS ix_trace_locators_trace
    ON trace_locators(project_name, trace_id, trace_segment_id);
