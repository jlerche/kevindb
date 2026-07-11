ALTER TABLE trace_segment_spans
    ADD COLUMN IF NOT EXISTS run_type TEXT NOT NULL DEFAULT 'span';

ALTER TABLE trace_segment_spans
    ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'success';

ALTER TABLE trace_segment_spans
    ADD COLUMN IF NOT EXISTS is_root BOOLEAN NOT NULL DEFAULT false;

ALTER TABLE run_heads
    ADD COLUMN IF NOT EXISTS run_type TEXT NOT NULL DEFAULT 'span';

ALTER TABLE run_heads
    ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'success';

ALTER TABLE run_heads
    ADD COLUMN IF NOT EXISTS is_root BOOLEAN NOT NULL DEFAULT false;

UPDATE trace_segment_spans
SET is_root = parent_span_id IS NULL;

UPDATE run_heads
SET is_root = parent_span_id IS NULL;

CREATE INDEX IF NOT EXISTS ix_run_heads_project_trace_start
    ON run_heads(project_name, trace_id, start_time_unix_nano);

CREATE INDEX IF NOT EXISTS ix_run_heads_project_root_start
    ON run_heads(project_name, is_root, start_time_unix_nano);
