ALTER TABLE trace_segments
    ADD COLUMN IF NOT EXISTS time_bucket_start_unix_nano BIGINT NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS ix_trace_segments_project_time_bucket
    ON trace_segments(project_name, time_bucket_start_unix_nano, created_at);

ALTER TABLE trace_segment_spans
    ADD COLUMN IF NOT EXISTS event_type TEXT NOT NULL DEFAULT 'end';

ALTER TABLE trace_segment_spans
    ADD COLUMN IF NOT EXISTS event_time_unix_nano BIGINT NOT NULL DEFAULT 0;

UPDATE trace_segment_spans
SET event_time_unix_nano = CASE
    WHEN end_time_unix_nano > 0 THEN end_time_unix_nano
    ELSE start_time_unix_nano
END
WHERE event_time_unix_nano = 0;

ALTER TABLE run_heads
    ADD COLUMN IF NOT EXISTS last_event_type TEXT NOT NULL DEFAULT 'end';

ALTER TABLE run_heads
    ADD COLUMN IF NOT EXISTS last_event_time_unix_nano BIGINT NOT NULL DEFAULT 0;

UPDATE run_heads
SET last_event_time_unix_nano = CASE
    WHEN end_time_unix_nano > 0 THEN end_time_unix_nano
    ELSE start_time_unix_nano
END
WHERE last_event_time_unix_nano = 0;

CREATE TABLE IF NOT EXISTS run_events (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    trace_segment_id BIGINT NOT NULL REFERENCES trace_segments(id) ON DELETE CASCADE,
    project_name TEXT NOT NULL,
    run_id TEXT NOT NULL,
    trace_id TEXT NOT NULL,
    span_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    event_time_unix_nano BIGINT NOT NULL,
    row_index BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS ix_run_events_project_trace_time
    ON run_events(project_name, trace_id, event_time_unix_nano);

CREATE INDEX IF NOT EXISTS ix_run_events_run_id_time
    ON run_events(run_id, event_time_unix_nano);
