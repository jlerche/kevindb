CREATE TABLE projects (
    name TEXT PRIMARY KEY,
    id TEXT NOT NULL UNIQUE
);

CREATE TABLE trace_segments (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    project_name TEXT NOT NULL REFERENCES projects(name) ON DELETE CASCADE,
    uri TEXT NOT NULL UNIQUE,
    total_bytes BIGINT NOT NULL,
    span_count BIGINT NOT NULL,
    min_start_time_unix_nano BIGINT NOT NULL,
    time_bucket_start_unix_nano BIGINT NOT NULL,
    schema_version BIGINT NOT NULL,
    search_index_uri TEXT NOT NULL,
    search_index_bytes BIGINT NOT NULL,
    search_index_schema_version BIGINT NOT NULL,
    compacted_at_unix_nano BIGINT,
    object_deleted_at_unix_nano BIGINT
);

CREATE INDEX ix_trace_segments_active_bucket
    ON trace_segments(project_name, time_bucket_start_unix_nano, id)
    WHERE compacted_at_unix_nano IS NULL;

CREATE INDEX ix_trace_segments_compacted_cleanup
    ON trace_segments(compacted_at_unix_nano, id)
    WHERE compacted_at_unix_nano IS NOT NULL
        AND object_deleted_at_unix_nano IS NULL;

CREATE TABLE trace_segment_spans (
    trace_segment_id BIGINT NOT NULL REFERENCES trace_segments(id) ON DELETE CASCADE,
    project_name TEXT NOT NULL REFERENCES projects(name) ON DELETE CASCADE,
    trace_id TEXT NOT NULL,
    span_id TEXT NOT NULL,
    row_index BIGINT NOT NULL,
    PRIMARY KEY(trace_segment_id, row_index)
);

CREATE INDEX ix_trace_segment_spans_run
    ON trace_segment_spans(project_name, trace_id, span_id, trace_segment_id);

CREATE TABLE run_events (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    trace_segment_id BIGINT NOT NULL REFERENCES trace_segments(id) ON DELETE CASCADE,
    project_name TEXT NOT NULL REFERENCES projects(name) ON DELETE CASCADE,
    run_id TEXT NOT NULL,
    trace_id TEXT NOT NULL,
    span_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    event_time_unix_nano BIGINT NOT NULL,
    row_index BIGINT NOT NULL,
    idempotency_key TEXT NOT NULL,
    UNIQUE(project_name, idempotency_key)
);

CREATE INDEX ix_run_events_project_trace_time
    ON run_events(project_name, trace_id, event_time_unix_nano);

CREATE INDEX ix_run_events_run_id_time
    ON run_events(run_id, event_time_unix_nano);

CREATE INDEX ix_run_events_lineage
    ON run_events(project_name, trace_id, span_id, event_time_unix_nano, id);

CREATE TABLE run_heads (
    project_name TEXT NOT NULL REFERENCES projects(name) ON DELETE CASCADE,
    run_id TEXT NOT NULL UNIQUE,
    trace_id TEXT NOT NULL,
    span_id TEXT NOT NULL,
    parent_run_id TEXT,
    parent_span_id TEXT,
    name TEXT NOT NULL,
    run_type TEXT NOT NULL,
    start_time_unix_nano BIGINT NOT NULL,
    end_time_unix_nano BIGINT NOT NULL,
    status TEXT NOT NULL,
    is_root BOOLEAN NOT NULL,
    root_run_id TEXT NOT NULL,
    root_span_id TEXT NOT NULL,
    latency_nanos BIGINT NOT NULL,
    prompt_tokens BIGINT,
    completion_tokens BIGINT,
    total_tokens BIGINT,
    prompt_cost DOUBLE PRECISION,
    completion_cost DOUBLE PRECISION,
    total_cost DOUBLE PRECISION,
    first_token_latency_nanos BIGINT,
    evaluator_score DOUBLE PRECISION,
    model_name TEXT,
    provider_name TEXT,
    last_trace_segment_id BIGINT NOT NULL REFERENCES trace_segments(id),
    last_row_index BIGINT NOT NULL,
    last_event_type TEXT NOT NULL,
    last_event_time_unix_nano BIGINT NOT NULL,
    last_run_event_id BIGINT NOT NULL REFERENCES run_events(id),
    deleted_at_unix_nano BIGINT,
    PRIMARY KEY(project_name, trace_id, span_id)
);

CREATE INDEX ix_run_heads_project_trace_start
    ON run_heads(project_name, trace_id, start_time_unix_nano);

CREATE INDEX ix_run_heads_project_root_start
    ON run_heads(project_name, is_root, start_time_unix_nano);

CREATE INDEX ix_run_heads_project_last_segment
    ON run_heads(project_name, last_trace_segment_id);

CREATE INDEX ix_run_heads_project_run_type_start
    ON run_heads(project_name, run_type, start_time_unix_nano);

CREATE INDEX ix_run_heads_project_status_start
    ON run_heads(project_name, status, start_time_unix_nano);

CREATE INDEX ix_run_heads_project_latency
    ON run_heads(project_name, latency_nanos);

CREATE INDEX ix_run_heads_project_root
    ON run_heads(project_name, root_run_id, root_span_id);

CREATE INDEX ix_run_heads_project_model
    ON run_heads(project_name, model_name)
    WHERE model_name IS NOT NULL;

CREATE INDEX ix_run_heads_project_provider
    ON run_heads(project_name, provider_name)
    WHERE provider_name IS NOT NULL;

CREATE INDEX ix_run_heads_project_start_type_model
    ON run_heads(project_name, start_time_unix_nano, run_type, model_name);

CREATE INDEX ix_run_heads_project_first_token_latency
    ON run_heads(project_name, first_token_latency_nanos)
    WHERE first_token_latency_nanos IS NOT NULL;

CREATE INDEX ix_run_heads_project_evaluator_score
    ON run_heads(project_name, evaluator_score)
    WHERE evaluator_score IS NOT NULL;

CREATE TABLE run_locators (
    project_name TEXT NOT NULL REFERENCES projects(name) ON DELETE CASCADE,
    run_id TEXT NOT NULL UNIQUE,
    trace_id TEXT NOT NULL,
    span_id TEXT NOT NULL,
    trace_segment_id BIGINT NOT NULL REFERENCES trace_segments(id) ON DELETE CASCADE,
    row_index BIGINT NOT NULL,
    event_type TEXT NOT NULL,
    event_time_unix_nano BIGINT NOT NULL,
    run_event_id BIGINT NOT NULL REFERENCES run_events(id),
    PRIMARY KEY(project_name, trace_id, span_id),
    FOREIGN KEY(project_name, trace_id, span_id)
        REFERENCES run_heads(project_name, trace_id, span_id) ON DELETE CASCADE
);

CREATE TABLE trace_locators (
    project_name TEXT NOT NULL REFERENCES projects(name) ON DELETE CASCADE,
    trace_id TEXT NOT NULL,
    span_id TEXT NOT NULL,
    trace_segment_id BIGINT NOT NULL REFERENCES trace_segments(id) ON DELETE CASCADE,
    row_index BIGINT NOT NULL,
    event_type TEXT NOT NULL,
    event_time_unix_nano BIGINT NOT NULL,
    run_event_id BIGINT NOT NULL REFERENCES run_events(id),
    PRIMARY KEY(project_name, trace_id, span_id),
    FOREIGN KEY(project_name, trace_id, span_id)
        REFERENCES run_heads(project_name, trace_id, span_id) ON DELETE CASCADE
);

CREATE INDEX ix_trace_locators_trace
    ON trace_locators(project_name, trace_id, trace_segment_id);

CREATE TABLE feedback (
    id TEXT PRIMARY KEY,
    run_id TEXT,
    trace_id TEXT,
    project_name TEXT REFERENCES projects(name) ON DELETE CASCADE,
    key TEXT NOT NULL,
    score_json TEXT,
    value_json TEXT,
    score_number DOUBLE PRECISION,
    value_text TEXT,
    correction_json TEXT,
    comment TEXT,
    feedback_source_json TEXT,
    extra_json TEXT,
    created_at_unix_nano BIGINT NOT NULL,
    modified_at_unix_nano BIGINT NOT NULL
);

CREATE INDEX ix_feedback_run_created
    ON feedback(run_id, created_at_unix_nano);

CREATE INDEX ix_feedback_key_created
    ON feedback(key, created_at_unix_nano);

CREATE INDEX ix_feedback_trace_created
    ON feedback(trace_id, created_at_unix_nano)
    WHERE trace_id IS NOT NULL;

CREATE INDEX ix_feedback_project_key_created
    ON feedback(project_name, key, created_at_unix_nano)
    WHERE project_name IS NOT NULL;

CREATE INDEX ix_feedback_key_score
    ON feedback(key, score_number)
    WHERE score_number IS NOT NULL;

CREATE INDEX ix_feedback_key_value
    ON feedback(key, value_text)
    WHERE value_text IS NOT NULL;

CREATE TABLE trace_segment_delete_vectors (
    trace_segment_id BIGINT NOT NULL REFERENCES trace_segments(id) ON DELETE CASCADE,
    project_name TEXT NOT NULL REFERENCES projects(name) ON DELETE CASCADE,
    trace_id TEXT NOT NULL,
    span_id TEXT NOT NULL,
    deleted_at_unix_nano BIGINT NOT NULL,
    reason TEXT,
    PRIMARY KEY(trace_segment_id, project_name, trace_id, span_id)
);

CREATE INDEX ix_trace_segment_delete_vectors_run
    ON trace_segment_delete_vectors(project_name, trace_id, span_id);

CREATE TABLE project_retention_policies (
    project_name TEXT PRIMARY KEY REFERENCES projects(name) ON DELETE CASCADE,
    ttl_unix_nanos BIGINT NOT NULL,
    last_enforced_at_unix_nano BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE run_tags (
    project_name TEXT NOT NULL,
    trace_id TEXT NOT NULL,
    span_id TEXT NOT NULL,
    tag TEXT NOT NULL,
    PRIMARY KEY(project_name, trace_id, span_id, tag),
    FOREIGN KEY(project_name, trace_id, span_id)
        REFERENCES run_heads(project_name, trace_id, span_id) ON DELETE CASCADE
);

CREATE INDEX ix_run_tags_project_tag
    ON run_tags(project_name, tag, trace_id, span_id);

CREATE TABLE run_metadata (
    project_name TEXT NOT NULL,
    trace_id TEXT NOT NULL,
    span_id TEXT NOT NULL,
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    PRIMARY KEY(project_name, trace_id, span_id, key, value),
    FOREIGN KEY(project_name, trace_id, span_id)
        REFERENCES run_heads(project_name, trace_id, span_id) ON DELETE CASCADE
);

CREATE INDEX ix_run_metadata_project_key_value
    ON run_metadata(project_name, key, value, trace_id, span_id);

CREATE INDEX ix_run_metadata_project_key
    ON run_metadata(project_name, key, trace_id, span_id);

CREATE TABLE run_tree_nodes (
    project_name TEXT NOT NULL,
    trace_id TEXT NOT NULL,
    span_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    parent_span_id TEXT,
    root_span_id TEXT NOT NULL,
    root_run_id TEXT NOT NULL,
    depth BIGINT NOT NULL,
    sibling_order BIGINT NOT NULL,
    subtree_start BIGINT NOT NULL,
    subtree_end BIGINT NOT NULL,
    descendant_count BIGINT NOT NULL,
    unresolved_parent BOOLEAN NOT NULL,
    cycle_detected BOOLEAN NOT NULL,
    PRIMARY KEY(project_name, trace_id, span_id),
    FOREIGN KEY(project_name, trace_id, span_id)
        REFERENCES run_heads(project_name, trace_id, span_id) ON DELETE CASCADE
);

CREATE INDEX ix_run_tree_nodes_trace_interval
    ON run_tree_nodes(project_name, trace_id, subtree_start, subtree_end);

CREATE INDEX ix_run_tree_nodes_trace_root
    ON run_tree_nodes(project_name, trace_id, root_span_id, depth);

CREATE INDEX ix_run_tree_nodes_parent
    ON run_tree_nodes(project_name, trace_id, parent_span_id, sibling_order)
    WHERE parent_span_id IS NOT NULL;

CREATE INDEX ix_run_tree_nodes_cycle_guard
    ON run_tree_nodes(project_name, trace_id, cycle_detected)
    WHERE cycle_detected = true;

CREATE TABLE run_tree_edges (
    project_name TEXT NOT NULL REFERENCES projects(name) ON DELETE CASCADE,
    trace_id TEXT NOT NULL,
    parent_span_id TEXT NOT NULL,
    child_span_id TEXT NOT NULL,
    sibling_order BIGINT NOT NULL,
    depth BIGINT NOT NULL,
    PRIMARY KEY(project_name, trace_id, child_span_id)
);

CREATE INDEX ix_run_tree_edges_parent
    ON run_tree_edges(project_name, trace_id, parent_span_id, sibling_order);

CREATE TABLE run_previews (
    project_name TEXT NOT NULL,
    trace_id TEXT NOT NULL,
    span_id TEXT NOT NULL,
    inputs_preview TEXT,
    outputs_preview TEXT,
    error_preview TEXT,
    first_token_time_unix_nano BIGINT,
    PRIMARY KEY(project_name, trace_id, span_id),
    FOREIGN KEY(project_name, trace_id, span_id)
        REFERENCES run_heads(project_name, trace_id, span_id) ON DELETE CASCADE
);

CREATE TABLE threads (
    project_name TEXT NOT NULL REFERENCES projects(name) ON DELETE CASCADE,
    thread_id TEXT NOT NULL,
    count BIGINT NOT NULL DEFAULT 0,
    first_trace_id TEXT,
    last_trace_id TEXT,
    min_start_time_unix_nano BIGINT,
    max_start_time_unix_nano BIGINT,
    first_inputs TEXT,
    last_outputs TEXT,
    last_error TEXT,
    prompt_tokens BIGINT,
    completion_tokens BIGINT,
    total_tokens BIGINT,
    prompt_cost DOUBLE PRECISION,
    completion_cost DOUBLE PRECISION,
    total_cost DOUBLE PRECISION,
    latency_p50 DOUBLE PRECISION,
    latency_p99 DOUBLE PRECISION,
    num_errored_turns BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY(project_name, thread_id)
);

CREATE INDEX ix_threads_project_recent
    ON threads(project_name, max_start_time_unix_nano DESC, thread_id);

CREATE TABLE thread_traces (
    project_name TEXT NOT NULL,
    thread_id TEXT NOT NULL,
    trace_id TEXT NOT NULL,
    root_run_id TEXT NOT NULL,
    root_span_id TEXT NOT NULL,
    name TEXT,
    start_time_unix_nano BIGINT NOT NULL,
    end_time_unix_nano BIGINT NOT NULL,
    latency_nanos BIGINT NOT NULL,
    first_token_time_unix_nano BIGINT,
    inputs_preview TEXT,
    outputs_preview TEXT,
    error_preview TEXT,
    prompt_tokens BIGINT,
    completion_tokens BIGINT,
    total_tokens BIGINT,
    prompt_cost DOUBLE PRECISION,
    completion_cost DOUBLE PRECISION,
    total_cost DOUBLE PRECISION,
    PRIMARY KEY(project_name, thread_id, trace_id),
    FOREIGN KEY(project_name, thread_id)
        REFERENCES threads(project_name, thread_id) ON DELETE CASCADE
        DEFERRABLE INITIALLY DEFERRED
);

CREATE INDEX ix_thread_traces_thread_start
    ON thread_traces(project_name, thread_id, start_time_unix_nano, trace_id);

CREATE INDEX ix_thread_traces_project_trace
    ON thread_traces(project_name, trace_id);

CREATE TABLE thread_messages (
    project_name TEXT NOT NULL,
    thread_id TEXT NOT NULL,
    trace_id TEXT NOT NULL,
    span_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    role TEXT NOT NULL,
    preview TEXT NOT NULL,
    turn_order BIGINT NOT NULL,
    trace_segment_id BIGINT NOT NULL REFERENCES trace_segments(id),
    row_index BIGINT NOT NULL,
    start_time_unix_nano BIGINT NOT NULL,
    PRIMARY KEY(project_name, thread_id, trace_id, span_id, role),
    FOREIGN KEY(project_name, thread_id, trace_id)
        REFERENCES thread_traces(project_name, thread_id, trace_id) ON DELETE CASCADE
        DEFERRABLE INITIALLY DEFERRED
);

CREATE INDEX ix_thread_messages_thread_order
    ON thread_messages(project_name, thread_id, turn_order, trace_id, span_id, role);

CREATE TABLE run_metric_rollups (
    project_name TEXT NOT NULL REFERENCES projects(name) ON DELETE CASCADE,
    bucket_size_unix_nanos BIGINT NOT NULL,
    time_bucket_start_unix_nano BIGINT NOT NULL,
    rollup_kind TEXT NOT NULL,
    group_value TEXT NOT NULL,
    run_count BIGINT NOT NULL,
    error_count BIGINT NOT NULL,
    latency_min_nanos BIGINT,
    latency_max_nanos BIGINT,
    latency_avg_nanos DOUBLE PRECISION,
    latency_p50_nanos DOUBLE PRECISION,
    latency_p95_nanos DOUBLE PRECISION,
    latency_p99_nanos DOUBLE PRECISION,
    prompt_tokens_sum BIGINT,
    prompt_tokens_avg DOUBLE PRECISION,
    completion_tokens_sum BIGINT,
    completion_tokens_avg DOUBLE PRECISION,
    total_tokens_sum BIGINT,
    total_tokens_avg DOUBLE PRECISION,
    prompt_cost_sum DOUBLE PRECISION,
    prompt_cost_avg DOUBLE PRECISION,
    completion_cost_sum DOUBLE PRECISION,
    completion_cost_avg DOUBLE PRECISION,
    total_cost_sum DOUBLE PRECISION,
    total_cost_avg DOUBLE PRECISION,
    evaluator_score_avg DOUBLE PRECISION,
    PRIMARY KEY(
        project_name,
        bucket_size_unix_nanos,
        time_bucket_start_unix_nano,
        rollup_kind,
        group_value
    )
);

CREATE INDEX ix_run_metric_rollups_project_kind_time
    ON run_metric_rollups(project_name, rollup_kind, time_bucket_start_unix_nano);

CREATE TABLE feedback_metric_rollups (
    project_name TEXT NOT NULL REFERENCES projects(name) ON DELETE CASCADE,
    bucket_size_unix_nanos BIGINT NOT NULL,
    time_bucket_start_unix_nano BIGINT NOT NULL,
    feedback_key TEXT NOT NULL,
    feedback_count BIGINT NOT NULL,
    score_count BIGINT NOT NULL,
    score_min DOUBLE PRECISION,
    score_max DOUBLE PRECISION,
    score_avg DOUBLE PRECISION,
    score_p50 DOUBLE PRECISION,
    score_p95 DOUBLE PRECISION,
    score_p99 DOUBLE PRECISION,
    score_distribution_json TEXT NOT NULL,
    PRIMARY KEY(
        project_name,
        bucket_size_unix_nanos,
        time_bucket_start_unix_nano,
        feedback_key
    )
);

CREATE INDEX ix_feedback_metric_rollups_project_key_time
    ON feedback_metric_rollups(project_name, feedback_key, time_bucket_start_unix_nano);

CREATE TABLE project_routes (
    project_name TEXT PRIMARY KEY REFERENCES projects(name) ON DELETE CASCADE,
    node_id TEXT NOT NULL,
    last_segment_uri TEXT NOT NULL
);

CREATE TABLE compaction_leases (
    project_name TEXT PRIMARY KEY REFERENCES projects(name) ON DELETE CASCADE,
    holder_id TEXT NOT NULL,
    lease_expires_at_unix_nano BIGINT NOT NULL
);

CREATE INDEX ix_compaction_leases_expiry
    ON compaction_leases(lease_expires_at_unix_nano);
