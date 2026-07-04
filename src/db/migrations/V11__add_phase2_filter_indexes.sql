ALTER TABLE run_heads
    ADD COLUMN IF NOT EXISTS root_run_id TEXT NOT NULL DEFAULT '';

ALTER TABLE run_heads
    ADD COLUMN IF NOT EXISTS root_span_id TEXT NOT NULL DEFAULT '';

ALTER TABLE run_heads
    ADD COLUMN IF NOT EXISTS latency_nanos BIGINT NOT NULL DEFAULT 0;

ALTER TABLE run_heads
    ADD COLUMN IF NOT EXISTS prompt_tokens BIGINT;

ALTER TABLE run_heads
    ADD COLUMN IF NOT EXISTS completion_tokens BIGINT;

ALTER TABLE run_heads
    ADD COLUMN IF NOT EXISTS total_tokens BIGINT;

ALTER TABLE run_heads
    ADD COLUMN IF NOT EXISTS total_cost DOUBLE PRECISION;

ALTER TABLE run_heads
    ADD COLUMN IF NOT EXISTS model_name TEXT;

ALTER TABLE run_heads
    ADD COLUMN IF NOT EXISTS provider_name TEXT;

UPDATE run_heads
SET
    root_run_id = CASE
        WHEN root_run_id <> '' THEN root_run_id
        WHEN run_id <> '' THEN run_id
        ELSE generated_run_id
    END,
    root_span_id = CASE
        WHEN root_span_id <> '' THEN root_span_id
        ELSE span_id
    END,
    latency_nanos = CASE
        WHEN end_time_unix_nano > start_time_unix_nano
        THEN end_time_unix_nano - start_time_unix_nano
        ELSE 0
    END;

CREATE INDEX IF NOT EXISTS ix_run_heads_project_run_type_start
    ON run_heads(project_name, run_type, start_time_unix_nano);

CREATE INDEX IF NOT EXISTS ix_run_heads_project_status_start
    ON run_heads(project_name, status, start_time_unix_nano);

CREATE INDEX IF NOT EXISTS ix_run_heads_project_latency
    ON run_heads(project_name, latency_nanos);

CREATE INDEX IF NOT EXISTS ix_run_heads_project_root
    ON run_heads(project_name, root_run_id, root_span_id);

CREATE INDEX IF NOT EXISTS ix_run_heads_project_model
    ON run_heads(project_name, model_name)
    WHERE model_name IS NOT NULL;

CREATE INDEX IF NOT EXISTS ix_run_heads_project_provider
    ON run_heads(project_name, provider_name)
    WHERE provider_name IS NOT NULL;

CREATE TABLE IF NOT EXISTS run_tags (
    project_name TEXT NOT NULL,
    trace_id TEXT NOT NULL,
    span_id TEXT NOT NULL,
    tag TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(project_name, trace_id, span_id, tag)
);

CREATE INDEX IF NOT EXISTS ix_run_tags_project_tag
    ON run_tags(project_name, tag, trace_id, span_id);

CREATE TABLE IF NOT EXISTS run_metadata (
    project_name TEXT NOT NULL,
    trace_id TEXT NOT NULL,
    span_id TEXT NOT NULL,
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(project_name, trace_id, span_id, key, value)
);

CREATE INDEX IF NOT EXISTS ix_run_metadata_project_key_value
    ON run_metadata(project_name, key, value, trace_id, span_id);

CREATE INDEX IF NOT EXISTS ix_run_metadata_project_key
    ON run_metadata(project_name, key, trace_id, span_id);

CREATE TABLE IF NOT EXISTS project_filter_stats (
    project_name TEXT NOT NULL,
    stat_name TEXT NOT NULL,
    distinct_count BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(project_name, stat_name)
);

ALTER TABLE feedback
    ADD COLUMN IF NOT EXISTS score_number DOUBLE PRECISION;

ALTER TABLE feedback
    ADD COLUMN IF NOT EXISTS value_text TEXT;

CREATE INDEX IF NOT EXISTS ix_feedback_trace_created
    ON feedback(trace_id, created_at_unix_nano)
    WHERE trace_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS ix_feedback_project_key_created
    ON feedback(project_name, key, created_at_unix_nano)
    WHERE project_name IS NOT NULL;

CREATE INDEX IF NOT EXISTS ix_feedback_key_score
    ON feedback(key, score_number)
    WHERE score_number IS NOT NULL;

CREATE INDEX IF NOT EXISTS ix_feedback_key_value
    ON feedback(key, value_text)
    WHERE value_text IS NOT NULL;
