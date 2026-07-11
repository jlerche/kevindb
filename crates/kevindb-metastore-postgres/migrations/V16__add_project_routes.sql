CREATE TABLE IF NOT EXISTS project_routes (
    project_name TEXT PRIMARY KEY REFERENCES projects(name) ON DELETE CASCADE,
    node_id TEXT NOT NULL,
    last_segment_uri TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS ix_project_routes_node_updated
    ON project_routes(node_id, updated_at DESC);
