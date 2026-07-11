use std::time::Instant;

use anyhow::{Context, Result};
use tokio_postgres::NoTls;

use super::{
    QueryEngine, RunQueryDiagnostics, RunSummary, TraceTree, TraceTreeQueryResult,
    trace_tree_from_runs,
};

impl QueryEngine {
    pub async fn load_trace_tree(&self, project_name: &str, trace_id: &str) -> Result<TraceTree> {
        Ok(self
            .load_trace_tree_with_diagnostics(project_name, trace_id)
            .await?
            .trace_tree)
    }

    pub async fn load_trace_tree_with_diagnostics(
        &self,
        project_name: &str,
        trace_id: &str,
    ) -> Result<TraceTreeQueryResult> {
        let postgres_started = Instant::now();
        let runs = load_trace_tree_runs(&self.postgres_url, project_name, trace_id).await?;
        let postgres_query_time = postgres_started.elapsed();
        let rows_returned = runs.len();

        Ok(TraceTreeQueryResult {
            trace_tree: trace_tree_from_runs(project_name, trace_id, runs),
            diagnostics: RunQueryDiagnostics {
                candidate_runs: rows_returned,
                rows_returned,
                postgres_query_time,
                ..RunQueryDiagnostics::default()
            },
        })
    }
}

async fn load_trace_tree_runs(
    postgres_url: &str,
    project_name: &str,
    trace_id: &str,
) -> Result<Vec<RunSummary>> {
    let (client, connection) = tokio_postgres::connect(postgres_url, NoTls)
        .await
        .context("connect postgres for trace tree lookup")?;
    tokio::spawn(async move {
        if let Err(err) = connection.await {
            tracing::warn!(error = %err, "postgres trace tree lookup connection failed");
        }
    });

    let rows = client
        .query(
            "SELECT
                heads.project_name,
                heads.run_id,
                heads.trace_id,
                heads.span_id,
                CASE
                    WHEN tree.parent_span_id IS NULL
                        OR parent_heads.span_id IS NULL
                        OR parent_heads.deleted_at_unix_nano IS NOT NULL
                    THEN NULL
                    ELSE heads.parent_run_id
                END AS parent_run_id,
                CASE
                    WHEN tree.parent_span_id IS NULL
                        OR parent_heads.span_id IS NULL
                        OR parent_heads.deleted_at_unix_nano IS NOT NULL
                    THEN NULL
                    ELSE tree.parent_span_id
                END AS parent_span_id,
                heads.name,
                heads.run_type,
                heads.status,
                heads.start_time_unix_nano,
                heads.end_time_unix_nano,
                tree.parent_span_id IS NULL AS is_root
            FROM run_tree_nodes tree
            INNER JOIN run_heads heads
                ON heads.project_name = tree.project_name
                AND heads.trace_id = tree.trace_id
                AND heads.span_id = tree.span_id
            LEFT JOIN run_heads parent_heads
                ON parent_heads.project_name = tree.project_name
                AND parent_heads.trace_id = tree.trace_id
                AND parent_heads.span_id = tree.parent_span_id
            WHERE tree.project_name = $1
                AND tree.trace_id = $2
                AND heads.deleted_at_unix_nano IS NULL
            ORDER BY tree.subtree_start ASC, heads.start_time_unix_nano ASC, heads.span_id ASC",
            &[&project_name, &trace_id],
        )
        .await
        .context("load trace tree metadata rows")?;

    Ok(rows
        .into_iter()
        .map(|row| RunSummary {
            project_name: row.get(0),
            run_id: row.get(1),
            trace_id: row.get(2),
            span_id: row.get(3),
            parent_run_id: row.get::<_, Option<String>>(4),
            parent_span_id: row.get::<_, Option<String>>(5),
            name: row.get(6),
            run_type: row.get(7),
            status: row.get(8),
            start_time_unix_nano: row.get(9),
            end_time_unix_nano: row.get(10),
            is_root: row.get(11),
            attributes_json: "{}".to_owned(),
        })
        .collect())
}
