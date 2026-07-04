ALTER TABLE trace_segments
    ADD COLUMN IF NOT EXISTS schema_version BIGINT NOT NULL DEFAULT 2;

ALTER TABLE run_heads
    ADD COLUMN IF NOT EXISTS last_row_index BIGINT NOT NULL DEFAULT 0;

ALTER TABLE run_heads
    ADD COLUMN IF NOT EXISTS last_run_event_id BIGINT REFERENCES run_events(id);

WITH latest_events AS (
    SELECT DISTINCT ON (project_name, trace_id, span_id)
        id,
        project_name,
        trace_id,
        span_id,
        row_index
    FROM run_events
    ORDER BY
        project_name,
        trace_id,
        span_id,
        event_time_unix_nano DESC,
        id DESC
)
UPDATE run_heads
SET
    last_row_index = latest_events.row_index,
    last_run_event_id = latest_events.id
FROM latest_events
WHERE run_heads.project_name = latest_events.project_name
    AND run_heads.trace_id = latest_events.trace_id
    AND run_heads.span_id = latest_events.span_id
    AND run_heads.last_run_event_id IS NULL;

CREATE INDEX IF NOT EXISTS ix_run_heads_run_locator
    ON run_heads(run_id, last_trace_segment_id, last_row_index)
    WHERE run_id <> '';

CREATE INDEX IF NOT EXISTS ix_run_heads_trace_locator
    ON run_heads(project_name, trace_id, last_trace_segment_id, last_row_index);

CREATE INDEX IF NOT EXISTS ix_run_events_lineage
    ON run_events(project_name, trace_id, span_id, event_time_unix_nano, id);
