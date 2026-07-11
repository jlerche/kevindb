ALTER TABLE trace_segments
    ADD COLUMN IF NOT EXISTS compacted_at_unix_nano BIGINT;

ALTER TABLE trace_segments
    ADD COLUMN IF NOT EXISTS object_deleted_at_unix_nano BIGINT;
