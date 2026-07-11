ALTER TABLE trace_segment_spans
    DROP CONSTRAINT IF EXISTS trace_segment_spans_project_name_trace_id_span_id_trace_segment_id_key;

CREATE UNIQUE INDEX IF NOT EXISTS ux_trace_segment_spans_segment_row
    ON trace_segment_spans(project_name, trace_id, span_id, trace_segment_id, row_index);
