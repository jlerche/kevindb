ALTER TABLE run_heads
    ADD COLUMN IF NOT EXISTS prompt_cost DOUBLE PRECISION;

ALTER TABLE run_heads
    ADD COLUMN IF NOT EXISTS completion_cost DOUBLE PRECISION;

ALTER TABLE run_heads
    ADD COLUMN IF NOT EXISTS first_token_latency_nanos BIGINT;

ALTER TABLE run_heads
    ADD COLUMN IF NOT EXISTS evaluator_score DOUBLE PRECISION;

ALTER TABLE thread_traces
    ADD COLUMN IF NOT EXISTS prompt_cost DOUBLE PRECISION;

ALTER TABLE thread_traces
    ADD COLUMN IF NOT EXISTS completion_cost DOUBLE PRECISION;

ALTER TABLE threads
    ADD COLUMN IF NOT EXISTS prompt_cost DOUBLE PRECISION;

ALTER TABLE threads
    ADD COLUMN IF NOT EXISTS completion_cost DOUBLE PRECISION;

CREATE INDEX IF NOT EXISTS ix_run_heads_project_start_type_model
    ON run_heads(project_name, start_time_unix_nano, run_type, model_name);

CREATE INDEX IF NOT EXISTS ix_run_heads_project_first_token_latency
    ON run_heads(project_name, first_token_latency_nanos)
    WHERE first_token_latency_nanos IS NOT NULL;

CREATE INDEX IF NOT EXISTS ix_run_heads_project_evaluator_score
    ON run_heads(project_name, evaluator_score)
    WHERE evaluator_score IS NOT NULL;

CREATE TABLE IF NOT EXISTS run_metric_rollups (
    project_name TEXT NOT NULL REFERENCES projects(name) ON DELETE CASCADE,
    bucket_size_unix_nanos BIGINT NOT NULL,
    time_bucket_start_unix_nano BIGINT NOT NULL,
    rollup_kind TEXT NOT NULL,
    group_value TEXT NOT NULL,
    run_count BIGINT NOT NULL,
    error_count BIGINT NOT NULL,
    latency_min_nanos BIGINT,
    latency_max_nanos BIGINT,
    latency_avg_nanos DOUBLE PRECISION,
    latency_p50_nanos DOUBLE PRECISION,
    latency_p95_nanos DOUBLE PRECISION,
    latency_p99_nanos DOUBLE PRECISION,
    prompt_tokens_sum BIGINT,
    prompt_tokens_avg DOUBLE PRECISION,
    completion_tokens_sum BIGINT,
    completion_tokens_avg DOUBLE PRECISION,
    total_tokens_sum BIGINT,
    total_tokens_avg DOUBLE PRECISION,
    prompt_cost_sum DOUBLE PRECISION,
    prompt_cost_avg DOUBLE PRECISION,
    completion_cost_sum DOUBLE PRECISION,
    completion_cost_avg DOUBLE PRECISION,
    total_cost_sum DOUBLE PRECISION,
    total_cost_avg DOUBLE PRECISION,
    evaluator_score_avg DOUBLE PRECISION,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(
        project_name,
        bucket_size_unix_nanos,
        time_bucket_start_unix_nano,
        rollup_kind,
        group_value
    )
);

CREATE INDEX IF NOT EXISTS ix_run_metric_rollups_project_kind_time
    ON run_metric_rollups(project_name, rollup_kind, time_bucket_start_unix_nano);

CREATE TABLE IF NOT EXISTS feedback_metric_rollups (
    project_name TEXT NOT NULL REFERENCES projects(name) ON DELETE CASCADE,
    bucket_size_unix_nanos BIGINT NOT NULL,
    time_bucket_start_unix_nano BIGINT NOT NULL,
    feedback_key TEXT NOT NULL,
    feedback_count BIGINT NOT NULL,
    score_count BIGINT NOT NULL,
    score_min DOUBLE PRECISION,
    score_max DOUBLE PRECISION,
    score_avg DOUBLE PRECISION,
    score_p50 DOUBLE PRECISION,
    score_p95 DOUBLE PRECISION,
    score_p99 DOUBLE PRECISION,
    score_distribution_json TEXT NOT NULL DEFAULT '{}',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(
        project_name,
        bucket_size_unix_nanos,
        time_bucket_start_unix_nano,
        feedback_key
    )
);

CREATE INDEX IF NOT EXISTS ix_feedback_metric_rollups_project_key_time
    ON feedback_metric_rollups(project_name, feedback_key, time_bucket_start_unix_nano);
