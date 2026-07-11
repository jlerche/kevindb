ALTER TABLE trace_segment_spans
    ADD COLUMN IF NOT EXISTS run_id TEXT;

ALTER TABLE trace_segment_spans
    ADD COLUMN IF NOT EXISTS parent_run_id TEXT;

ALTER TABLE run_heads
    ADD COLUMN IF NOT EXISTS run_id TEXT;

ALTER TABLE run_heads
    ADD COLUMN IF NOT EXISTS parent_run_id TEXT;

UPDATE trace_segment_spans
SET run_id = 'migrated:' || project_name || ':' || trace_id || ':' || span_id
WHERE run_id IS NULL;

UPDATE run_heads
SET run_id = 'migrated:' || project_name || ':' || trace_id || ':' || span_id
WHERE run_id IS NULL;

ALTER TABLE trace_segment_spans ALTER COLUMN run_id SET NOT NULL;
ALTER TABLE run_heads ALTER COLUMN run_id SET NOT NULL;

CREATE INDEX IF NOT EXISTS ix_trace_segment_spans_run_id
    ON trace_segment_spans(run_id);

CREATE INDEX IF NOT EXISTS ix_run_heads_run_id
    ON run_heads(run_id);
