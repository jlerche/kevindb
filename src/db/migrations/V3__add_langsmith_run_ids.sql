ALTER TABLE trace_segment_spans
    ADD COLUMN IF NOT EXISTS run_id TEXT NOT NULL DEFAULT '';

ALTER TABLE trace_segment_spans
    ADD COLUMN IF NOT EXISTS parent_run_id TEXT;

ALTER TABLE run_heads
    ADD COLUMN IF NOT EXISTS run_id TEXT NOT NULL DEFAULT '';

ALTER TABLE run_heads
    ADD COLUMN IF NOT EXISTS parent_run_id TEXT;

CREATE INDEX IF NOT EXISTS ix_trace_segment_spans_run_id
    ON trace_segment_spans(run_id)
    WHERE run_id <> '';

CREATE INDEX IF NOT EXISTS ix_run_heads_run_id
    ON run_heads(run_id)
    WHERE run_id <> '';
