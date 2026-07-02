CREATE TABLE IF NOT EXISTS projects (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS trace_segments (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    project_name TEXT NOT NULL REFERENCES projects(name) ON DELETE CASCADE,
    uri TEXT NOT NULL UNIQUE,
    etag TEXT NOT NULL,
    total_bytes BIGINT NOT NULL,
    span_count BIGINT NOT NULL,
    min_start_time_unix_nano BIGINT,
    max_end_time_unix_nano BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    compacted_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS ix_trace_segments_project_created
    ON trace_segments(project_name, created_at);

CREATE TABLE IF NOT EXISTS trace_segment_spans (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    trace_segment_id BIGINT NOT NULL REFERENCES trace_segments(id) ON DELETE CASCADE,
    project_name TEXT NOT NULL,
    trace_id TEXT NOT NULL,
    span_id TEXT NOT NULL,
    parent_span_id TEXT,
    name TEXT NOT NULL,
    start_time_unix_nano BIGINT NOT NULL,
    end_time_unix_nano BIGINT NOT NULL,
    status_code INTEGER NOT NULL,
    row_index BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(project_name, trace_id, span_id, trace_segment_id)
);

CREATE INDEX IF NOT EXISTS ix_trace_segment_spans_trace
    ON trace_segment_spans(project_name, trace_id);

CREATE INDEX IF NOT EXISTS ix_trace_segment_spans_span
    ON trace_segment_spans(project_name, span_id);

CREATE TABLE IF NOT EXISTS run_heads (
    project_name TEXT NOT NULL,
    trace_id TEXT NOT NULL,
    span_id TEXT NOT NULL,
    parent_span_id TEXT,
    name TEXT NOT NULL,
    start_time_unix_nano BIGINT NOT NULL,
    end_time_unix_nano BIGINT NOT NULL,
    status_code INTEGER NOT NULL,
    last_trace_segment_id BIGINT NOT NULL REFERENCES trace_segments(id),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(project_name, trace_id, span_id)
);

CREATE INDEX IF NOT EXISTS ix_run_heads_project_updated
    ON run_heads(project_name, updated_at);

CREATE INDEX IF NOT EXISTS ix_run_heads_trace
    ON run_heads(project_name, trace_id);
