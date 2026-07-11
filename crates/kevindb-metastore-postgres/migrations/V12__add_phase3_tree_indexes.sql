CREATE TABLE IF NOT EXISTS run_tree_nodes (
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
    unresolved_parent BOOLEAN NOT NULL DEFAULT false,
    cycle_detected BOOLEAN NOT NULL DEFAULT false,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(project_name, trace_id, span_id)
);

CREATE INDEX IF NOT EXISTS ix_run_tree_nodes_trace_interval
    ON run_tree_nodes(project_name, trace_id, subtree_start, subtree_end);

CREATE INDEX IF NOT EXISTS ix_run_tree_nodes_trace_root
    ON run_tree_nodes(project_name, trace_id, root_span_id, depth);

CREATE INDEX IF NOT EXISTS ix_run_tree_nodes_parent
    ON run_tree_nodes(project_name, trace_id, parent_span_id, sibling_order)
    WHERE parent_span_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS ix_run_tree_nodes_cycle_guard
    ON run_tree_nodes(project_name, trace_id, cycle_detected)
    WHERE cycle_detected = true;

CREATE TABLE IF NOT EXISTS run_tree_edges (
    project_name TEXT NOT NULL,
    trace_id TEXT NOT NULL,
    parent_span_id TEXT NOT NULL,
    child_span_id TEXT NOT NULL,
    sibling_order BIGINT NOT NULL,
    depth BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(project_name, trace_id, child_span_id)
);

CREATE INDEX IF NOT EXISTS ix_run_tree_edges_parent
    ON run_tree_edges(project_name, trace_id, parent_span_id, sibling_order);
