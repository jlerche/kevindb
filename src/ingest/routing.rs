use anyhow::{Context, Result};

pub(super) async fn record_project_route(
    tx: &tokio_postgres::Transaction<'_>,
    project_name: &str,
    node_id: &str,
    last_segment_uri: &str,
) -> Result<()> {
    tx.execute(
        "INSERT INTO project_routes(project_name, node_id, last_segment_uri, updated_at)
        VALUES ($1, $2, $3, CURRENT_TIMESTAMP)
        ON CONFLICT (project_name)
        DO UPDATE SET
            node_id = EXCLUDED.node_id,
            last_segment_uri = EXCLUDED.last_segment_uri,
            updated_at = CURRENT_TIMESTAMP",
        &[&project_name, &node_id, &last_segment_uri],
    )
    .await
    .context("upsert project route")?;
    Ok(())
}
