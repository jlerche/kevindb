use anyhow::{Context, Result};
use tokio_postgres::NoTls;

use super::QueryEngine;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectRoute {
    pub project_name: String,
    pub node_id: String,
    pub last_segment_uri: String,
}

impl QueryEngine {
    pub async fn load_project_route(&self, project_name: &str) -> Result<Option<ProjectRoute>> {
        let (client, connection) = tokio_postgres::connect(&self.postgres_url, NoTls)
            .await
            .context("connect postgres for project route lookup")?;
        tokio::spawn(async move {
            if let Err(err) = connection.await {
                tracing::warn!(error = %err, "postgres project route connection failed");
            }
        });

        let route = client
            .query_opt(
                "SELECT project_name, node_id, last_segment_uri
                FROM project_routes
                WHERE project_name = $1",
                &[&project_name],
            )
            .await
            .context("load project route")?
            .map(project_route_from_row);
        Ok(route)
    }
}

fn project_route_from_row(row: tokio_postgres::Row) -> ProjectRoute {
    ProjectRoute {
        project_name: row.get(0),
        node_id: row.get(1),
        last_segment_uri: row.get(2),
    }
}
