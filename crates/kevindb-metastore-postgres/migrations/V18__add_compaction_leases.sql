CREATE TABLE IF NOT EXISTS compaction_leases (
    project_name TEXT PRIMARY KEY REFERENCES projects(name) ON DELETE CASCADE,
    holder_id TEXT NOT NULL,
    lease_expires_at_unix_nano BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS ix_compaction_leases_expiry
    ON compaction_leases(lease_expires_at_unix_nano);
