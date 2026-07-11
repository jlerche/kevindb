CREATE INDEX IF NOT EXISTS ix_run_heads_project_last_segment
    ON run_heads(project_name, last_trace_segment_id);

CREATE INDEX IF NOT EXISTS ix_trace_segments_project_time_range
    ON trace_segments(project_name, min_start_time_unix_nano, max_end_time_unix_nano);
