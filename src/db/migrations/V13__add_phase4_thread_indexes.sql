CREATE TABLE IF NOT EXISTS run_previews (
    project_name TEXT NOT NULL,
    trace_id TEXT NOT NULL,
    span_id TEXT NOT NULL,
    inputs_preview TEXT,
    outputs_preview TEXT,
    error_preview TEXT,
    first_token_time_unix_nano BIGINT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(project_name, trace_id, span_id)
);

CREATE TABLE IF NOT EXISTS threads (
    project_name TEXT NOT NULL REFERENCES projects(name) ON DELETE CASCADE,
    thread_id TEXT NOT NULL,
    count BIGINT NOT NULL DEFAULT 0,
    first_trace_id TEXT,
    last_trace_id TEXT,
    min_start_time_unix_nano BIGINT,
    max_start_time_unix_nano BIGINT,
    first_inputs TEXT,
    last_outputs TEXT,
    last_error TEXT,
    prompt_tokens BIGINT,
    completion_tokens BIGINT,
    total_tokens BIGINT,
    total_cost DOUBLE PRECISION,
    latency_p50 DOUBLE PRECISION,
    latency_p99 DOUBLE PRECISION,
    num_errored_turns BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(project_name, thread_id)
);

CREATE INDEX IF NOT EXISTS ix_threads_project_recent
    ON threads(project_name, max_start_time_unix_nano DESC, thread_id);

CREATE TABLE IF NOT EXISTS thread_traces (
    project_name TEXT NOT NULL REFERENCES projects(name) ON DELETE CASCADE,
    thread_id TEXT NOT NULL,
    trace_id TEXT NOT NULL,
    root_run_id TEXT NOT NULL DEFAULT '',
    root_span_id TEXT NOT NULL DEFAULT '',
    name TEXT,
    start_time_unix_nano BIGINT NOT NULL,
    end_time_unix_nano BIGINT NOT NULL DEFAULT 0,
    latency_nanos BIGINT NOT NULL DEFAULT 0,
    first_token_time_unix_nano BIGINT,
    inputs_preview TEXT,
    outputs_preview TEXT,
    error_preview TEXT,
    prompt_tokens BIGINT,
    completion_tokens BIGINT,
    total_tokens BIGINT,
    total_cost DOUBLE PRECISION,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(project_name, thread_id, trace_id),
    FOREIGN KEY(project_name, thread_id)
        REFERENCES threads(project_name, thread_id) ON DELETE CASCADE
        DEFERRABLE INITIALLY DEFERRED
);

CREATE INDEX IF NOT EXISTS ix_thread_traces_thread_start
    ON thread_traces(project_name, thread_id, start_time_unix_nano, trace_id);

CREATE INDEX IF NOT EXISTS ix_thread_traces_project_trace
    ON thread_traces(project_name, trace_id);

CREATE TABLE IF NOT EXISTS thread_messages (
    project_name TEXT NOT NULL REFERENCES projects(name) ON DELETE CASCADE,
    thread_id TEXT NOT NULL,
    trace_id TEXT NOT NULL,
    span_id TEXT NOT NULL,
    run_id TEXT NOT NULL DEFAULT '',
    role TEXT NOT NULL,
    preview TEXT NOT NULL,
    turn_order BIGINT NOT NULL,
    trace_segment_id BIGINT,
    row_index BIGINT,
    start_time_unix_nano BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(project_name, thread_id, trace_id, span_id, role)
);

CREATE INDEX IF NOT EXISTS ix_thread_messages_thread_order
    ON thread_messages(project_name, thread_id, turn_order, trace_id, span_id, role);

WITH run_thread_ids AS (
    SELECT
        heads.project_name,
        COALESCE(thread_meta.value, session_meta.value) AS thread_id,
        heads.trace_id,
        heads.span_id,
        heads.run_id,
        heads.generated_run_id,
        heads.root_run_id,
        heads.root_span_id,
        heads.name,
        heads.status,
        heads.is_root,
        heads.start_time_unix_nano,
        heads.end_time_unix_nano,
        heads.latency_nanos,
        heads.prompt_tokens,
        heads.completion_tokens,
        heads.total_tokens,
        heads.total_cost
    FROM run_heads heads
    LEFT JOIN run_metadata thread_meta
        ON thread_meta.project_name = heads.project_name
        AND thread_meta.trace_id = heads.trace_id
        AND thread_meta.span_id = heads.span_id
        AND thread_meta.key = 'thread_id'
    LEFT JOIN run_metadata session_meta
        ON session_meta.project_name = heads.project_name
        AND session_meta.trace_id = heads.trace_id
        AND session_meta.span_id = heads.span_id
        AND session_meta.key = 'session_id'
    WHERE COALESCE(thread_meta.value, session_meta.value) IS NOT NULL
        AND heads.deleted_at_unix_nano IS NULL
),
root_rows AS (
    SELECT DISTINCT ON (project_name, thread_id, trace_id)
        project_name,
        thread_id,
        trace_id,
        CASE
            WHEN run_id <> '' THEN run_id
            WHEN generated_run_id <> '' THEN generated_run_id
            WHEN root_run_id <> '' THEN root_run_id
            ELSE span_id
        END AS root_run_id,
        CASE
            WHEN root_span_id <> '' THEN root_span_id
            ELSE span_id
        END AS root_span_id,
        name
    FROM run_thread_ids
    ORDER BY project_name, thread_id, trace_id, is_root DESC, start_time_unix_nano ASC, span_id ASC
),
trace_groups AS (
    SELECT
        project_name,
        thread_id,
        trace_id,
        MIN(start_time_unix_nano) AS start_time_unix_nano,
        MAX(end_time_unix_nano) AS end_time_unix_nano,
        GREATEST(MAX(end_time_unix_nano) - MIN(start_time_unix_nano), 0) AS latency_nanos,
        SUM(prompt_tokens) AS prompt_tokens,
        SUM(completion_tokens) AS completion_tokens,
        SUM(total_tokens) AS total_tokens,
        SUM(total_cost) AS total_cost,
        MAX(CASE WHEN status = 'error' THEN 'error' ELSE NULL END) AS error_preview
    FROM run_thread_ids
    GROUP BY project_name, thread_id, trace_id
),
thread_keys AS (
    SELECT DISTINCT project_name, thread_id FROM trace_groups
)
INSERT INTO threads(project_name, thread_id)
SELECT project_name, thread_id FROM thread_keys
ON CONFLICT (project_name, thread_id) DO NOTHING;

WITH run_thread_ids AS (
    SELECT
        heads.project_name,
        COALESCE(thread_meta.value, session_meta.value) AS thread_id,
        heads.trace_id,
        heads.span_id,
        heads.run_id,
        heads.generated_run_id,
        heads.root_run_id,
        heads.root_span_id,
        heads.name,
        heads.status,
        heads.is_root,
        heads.start_time_unix_nano,
        heads.end_time_unix_nano,
        heads.latency_nanos,
        heads.prompt_tokens,
        heads.completion_tokens,
        heads.total_tokens,
        heads.total_cost
    FROM run_heads heads
    LEFT JOIN run_metadata thread_meta
        ON thread_meta.project_name = heads.project_name
        AND thread_meta.trace_id = heads.trace_id
        AND thread_meta.span_id = heads.span_id
        AND thread_meta.key = 'thread_id'
    LEFT JOIN run_metadata session_meta
        ON session_meta.project_name = heads.project_name
        AND session_meta.trace_id = heads.trace_id
        AND session_meta.span_id = heads.span_id
        AND session_meta.key = 'session_id'
    WHERE COALESCE(thread_meta.value, session_meta.value) IS NOT NULL
        AND heads.deleted_at_unix_nano IS NULL
),
root_rows AS (
    SELECT DISTINCT ON (project_name, thread_id, trace_id)
        project_name,
        thread_id,
        trace_id,
        CASE
            WHEN run_id <> '' THEN run_id
            WHEN generated_run_id <> '' THEN generated_run_id
            WHEN root_run_id <> '' THEN root_run_id
            ELSE span_id
        END AS root_run_id,
        CASE
            WHEN root_span_id <> '' THEN root_span_id
            ELSE span_id
        END AS root_span_id,
        name
    FROM run_thread_ids
    ORDER BY project_name, thread_id, trace_id, is_root DESC, start_time_unix_nano ASC, span_id ASC
),
trace_groups AS (
    SELECT
        project_name,
        thread_id,
        trace_id,
        MIN(start_time_unix_nano) AS start_time_unix_nano,
        MAX(end_time_unix_nano) AS end_time_unix_nano,
        GREATEST(MAX(end_time_unix_nano) - MIN(start_time_unix_nano), 0) AS latency_nanos,
        SUM(prompt_tokens) AS prompt_tokens,
        SUM(completion_tokens) AS completion_tokens,
        SUM(total_tokens) AS total_tokens,
        SUM(total_cost) AS total_cost,
        MAX(CASE WHEN status = 'error' THEN 'error' ELSE NULL END) AS error_preview
    FROM run_thread_ids
    GROUP BY project_name, thread_id, trace_id
)
INSERT INTO thread_traces(
    project_name, thread_id, trace_id, root_run_id, root_span_id, name,
    start_time_unix_nano, end_time_unix_nano, latency_nanos,
    error_preview, prompt_tokens, completion_tokens, total_tokens, total_cost
)
SELECT
    trace_groups.project_name,
    trace_groups.thread_id,
    trace_groups.trace_id,
    root_rows.root_run_id,
    root_rows.root_span_id,
    root_rows.name,
    trace_groups.start_time_unix_nano,
    trace_groups.end_time_unix_nano,
    trace_groups.latency_nanos,
    trace_groups.error_preview,
    trace_groups.prompt_tokens,
    trace_groups.completion_tokens,
    trace_groups.total_tokens,
    trace_groups.total_cost
FROM trace_groups
INNER JOIN root_rows
    ON root_rows.project_name = trace_groups.project_name
    AND root_rows.thread_id = trace_groups.thread_id
    AND root_rows.trace_id = trace_groups.trace_id
ON CONFLICT (project_name, thread_id, trace_id) DO NOTHING;

WITH ranked_first AS (
    SELECT DISTINCT ON (project_name, thread_id)
        project_name, thread_id, trace_id, start_time_unix_nano
    FROM thread_traces
    ORDER BY project_name, thread_id, start_time_unix_nano ASC, trace_id ASC
),
ranked_last AS (
    SELECT DISTINCT ON (project_name, thread_id)
        project_name, thread_id, trace_id, start_time_unix_nano
    FROM thread_traces
    ORDER BY project_name, thread_id, start_time_unix_nano DESC, trace_id DESC
),
summaries AS (
    SELECT
        traces.project_name,
        traces.thread_id,
        COUNT(*) AS count,
        MIN(traces.start_time_unix_nano) AS min_start_time_unix_nano,
        MAX(traces.start_time_unix_nano) AS max_start_time_unix_nano,
        SUM(traces.prompt_tokens) AS prompt_tokens,
        SUM(traces.completion_tokens) AS completion_tokens,
        SUM(traces.total_tokens) AS total_tokens,
        SUM(traces.total_cost) AS total_cost,
        SUM(CASE WHEN traces.error_preview IS NULL THEN 0 ELSE 1 END) AS num_errored_turns
    FROM thread_traces traces
    GROUP BY traces.project_name, traces.thread_id
)
UPDATE threads
SET
    count = summaries.count,
    first_trace_id = ranked_first.trace_id,
    last_trace_id = ranked_last.trace_id,
    min_start_time_unix_nano = summaries.min_start_time_unix_nano,
    max_start_time_unix_nano = summaries.max_start_time_unix_nano,
    prompt_tokens = summaries.prompt_tokens,
    completion_tokens = summaries.completion_tokens,
    total_tokens = summaries.total_tokens,
    total_cost = summaries.total_cost,
    num_errored_turns = summaries.num_errored_turns,
    updated_at = CURRENT_TIMESTAMP
FROM summaries
INNER JOIN ranked_first
    ON ranked_first.project_name = summaries.project_name
    AND ranked_first.thread_id = summaries.thread_id
INNER JOIN ranked_last
    ON ranked_last.project_name = summaries.project_name
    AND ranked_last.thread_id = summaries.thread_id
WHERE threads.project_name = summaries.project_name
    AND threads.thread_id = summaries.thread_id;
