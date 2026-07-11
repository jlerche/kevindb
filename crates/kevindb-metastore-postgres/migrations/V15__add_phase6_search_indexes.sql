ALTER TABLE trace_segments
    ADD COLUMN IF NOT EXISTS search_index_uri TEXT;

ALTER TABLE trace_segments
    ADD COLUMN IF NOT EXISTS search_index_bytes BIGINT NOT NULL DEFAULT 0;

ALTER TABLE trace_segments
    ADD COLUMN IF NOT EXISTS search_index_schema_version BIGINT NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS ix_trace_segments_search_index
    ON trace_segments(project_name, search_index_uri)
    WHERE search_index_uri IS NOT NULL;
