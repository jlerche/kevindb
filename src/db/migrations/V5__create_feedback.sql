CREATE TABLE IF NOT EXISTS feedback (
    id TEXT PRIMARY KEY,
    run_id TEXT,
    trace_id TEXT,
    project_name TEXT,
    key TEXT NOT NULL,
    score_json TEXT,
    value_json TEXT,
    correction_json TEXT,
    comment TEXT,
    feedback_source_json TEXT,
    extra_json TEXT,
    created_at_unix_nano BIGINT NOT NULL,
    modified_at_unix_nano BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_feedback_run_created
    ON feedback(run_id, created_at_unix_nano);

CREATE INDEX IF NOT EXISTS ix_feedback_key_created
    ON feedback(key, created_at_unix_nano);
