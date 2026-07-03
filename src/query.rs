use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use arrow_array::{Array, BooleanArray, Int64Array, RecordBatch};
use arrow_array::{StringArray, StringViewArray, cast::AsArray};
use datafusion::common::GetExt;
use datafusion::datasource::provider::DefaultTableFactory;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::SessionContext;
use object_store::ObjectStore;
use tokio_postgres::NoTls;
use url::Url;
use uuid::Uuid;
use vortex_datafusion::VortexFormatFactory;

use crate::otlp::RunEventKind;

mod random_access;
mod tree;
pub use random_access::{RunEventSummary, RunLoadResult};
pub(crate) use tree::trace_tree_from_runs;
pub use tree::{RunNode, TraceTree};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunSummary {
    pub project_name: String,
    pub run_id: Option<String>,
    pub trace_id: String,
    pub span_id: String,
    pub parent_run_id: Option<String>,
    pub parent_span_id: Option<String>,
    pub name: String,
    pub run_type: String,
    pub status: String,
    pub start_time_unix_nano: i64,
    pub end_time_unix_nano: i64,
    pub is_root: bool,
    pub attributes_json: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunQuery {
    pub project_names: Vec<String>,
    pub trace_id: Option<String>,
    pub parent_run_id: Option<String>,
    pub parent_span_id: Option<String>,
    pub run_type: Option<String>,
    pub is_root: Option<bool>,
    pub error: Option<bool>,
    pub start_time_min_unix_nano: Option<i64>,
    pub start_time_max_unix_nano: Option<i64>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    pub retention_cutoff_unix_nano: Option<i64>,
    pub include_deleted: bool,
}

impl RunQuery {
    pub fn new(project_name: impl Into<String>) -> Self {
        Self {
            project_names: vec![project_name.into()],
            trace_id: None,
            parent_run_id: None,
            parent_span_id: None,
            run_type: None,
            is_root: None,
            error: None,
            start_time_min_unix_nano: None,
            start_time_max_unix_nano: None,
            limit: None,
            offset: None,
            retention_cutoff_unix_nano: None,
            include_deleted: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunQueryResult {
    pub runs: Vec<RunSummary>,
    pub diagnostics: RunQueryDiagnostics,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceTreeQueryResult {
    pub trace_tree: TraceTree,
    pub diagnostics: RunQueryDiagnostics,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RunQueryDiagnostics {
    pub candidate_segments: usize,
    pub vortex_files_opened: usize,
    pub rows_returned: usize,
    pub postgres_query_time: Duration,
    pub datafusion_planning_time: Duration,
    pub datafusion_execution_time: Duration,
}

pub struct QueryEngine {
    postgres_url: String,
    object_store: Arc<dyn ObjectStore>,
}

impl QueryEngine {
    pub fn new(postgres_url: impl Into<String>, object_store: Arc<dyn ObjectStore>) -> Self {
        Self {
            postgres_url: postgres_url.into(),
            object_store,
        }
    }

    pub async fn list_runs_in_trace(
        &self,
        project_name: &str,
        trace_id: &str,
    ) -> Result<Vec<RunSummary>> {
        self.load_trace(project_name, trace_id).await
    }

    pub async fn list_runs(&self, query: RunQuery) -> Result<Vec<RunSummary>> {
        let segment_uris = load_run_query_segment_uris(&self.postgres_url, &query).await?;
        let deleted_runs = if query.include_deleted {
            HashSet::new()
        } else {
            load_deleted_run_keys(&self.postgres_url, &query).await?
        };
        let runs = query_trace_segments_with_datafusion(
            Arc::clone(&self.object_store),
            segment_uris,
            &query,
        )
        .await?;
        Ok(runs
            .into_iter()
            .filter(|run| !deleted_runs.contains(&RunKey::from(run)))
            .filter(|run| run_matches_retention_filter(run, &query))
            .collect())
    }

    pub async fn list_runs_with_diagnostics(&self, query: RunQuery) -> Result<RunQueryResult> {
        let postgres_started = Instant::now();
        let segment_uris = load_run_query_segment_uris(&self.postgres_url, &query).await?;
        let deleted_runs = if query.include_deleted {
            HashSet::new()
        } else {
            load_deleted_run_keys(&self.postgres_url, &query).await?
        };
        let postgres_query_time = postgres_started.elapsed();
        let candidate_segments = segment_uris.len();

        let (runs, datafusion_timing) = query_trace_segments_with_datafusion_timed(
            Arc::clone(&self.object_store),
            segment_uris,
            &query,
        )
        .await?;
        let runs = runs
            .into_iter()
            .filter(|run| !deleted_runs.contains(&RunKey::from(run)))
            .filter(|run| run_matches_retention_filter(run, &query))
            .collect::<Vec<_>>();

        Ok(RunQueryResult {
            diagnostics: RunQueryDiagnostics {
                candidate_segments,
                vortex_files_opened: candidate_segments,
                rows_returned: runs.len(),
                postgres_query_time,
                datafusion_planning_time: datafusion_timing.planning_time,
                datafusion_execution_time: datafusion_timing.execution_time,
            },
            runs,
        })
    }

    pub async fn load_trace_tree(&self, project_name: &str, trace_id: &str) -> Result<TraceTree> {
        let runs = self.load_trace(project_name, trace_id).await?;
        Ok(trace_tree_from_runs(project_name, trace_id, runs))
    }

    pub async fn load_trace_tree_with_diagnostics(
        &self,
        project_name: &str,
        trace_id: &str,
    ) -> Result<TraceTreeQueryResult> {
        let result = self
            .load_trace_with_diagnostics(project_name, trace_id)
            .await?;
        Ok(TraceTreeQueryResult {
            trace_tree: trace_tree_from_runs(project_name, trace_id, result.runs),
            diagnostics: result.diagnostics,
        })
    }

    pub async fn delete_run(
        &self,
        project_name: &str,
        trace_id: &str,
        span_id: &str,
        reason: Option<&str>,
    ) -> Result<bool> {
        let deleted_at_unix_nano = current_time_unix_nano()?;
        let (mut client, connection) = tokio_postgres::connect(&self.postgres_url, NoTls)
            .await
            .context("connect postgres for run delete")?;
        tokio::spawn(async move {
            if let Err(err) = connection.await {
                tracing::warn!(error = %err, "postgres run delete connection failed");
            }
        });

        let tx = client.transaction().await.context("begin run delete")?;
        let deleted = delete_run_in_tx(
            &tx,
            project_name,
            trace_id,
            span_id,
            deleted_at_unix_nano,
            reason,
        )
        .await?;
        tx.commit().await.context("commit run delete")?;
        Ok(deleted)
    }

    pub async fn expire_project_runs_before(
        &self,
        project_name: &str,
        cutoff_unix_nano: i64,
    ) -> Result<usize> {
        let (mut client, connection) = tokio_postgres::connect(&self.postgres_url, NoTls)
            .await
            .context("connect postgres for retention expiration")?;
        tokio::spawn(async move {
            if let Err(err) = connection.await {
                tracing::warn!(error = %err, "postgres retention expiration connection failed");
            }
        });

        let rows = client
            .query(
                "SELECT trace_id, span_id
                FROM run_heads
                WHERE project_name = $1
                    AND start_time_unix_nano < $2
                    AND deleted_at_unix_nano IS NULL
                ORDER BY trace_id, span_id",
                &[&project_name, &cutoff_unix_nano],
            )
            .await
            .context("load retention expiration candidates")?;

        let tx = client
            .transaction()
            .await
            .context("begin retention expiration")?;
        let deleted_at_unix_nano = current_time_unix_nano()?;
        let mut deleted = 0;
        for row in rows {
            let trace_id: String = row.get(0);
            let span_id: String = row.get(1);
            if delete_run_in_tx(
                &tx,
                project_name,
                &trace_id,
                &span_id,
                deleted_at_unix_nano,
                Some("retention_expired"),
            )
            .await?
            {
                deleted += 1;
            }
        }
        tx.commit().await.context("commit retention expiration")?;
        Ok(deleted)
    }
}

pub fn generated_run_id(project_name: &str, trace_id: &str, span_id: &str) -> String {
    Uuid::new_v5(
        &Uuid::NAMESPACE_URL,
        format!("kevindb:run:{project_name}:{trace_id}:{span_id}").as_bytes(),
    )
    .to_string()
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct RunKey {
    project_name: String,
    trace_id: String,
    span_id: String,
}

impl From<&RunSummary> for RunKey {
    fn from(run: &RunSummary) -> Self {
        Self {
            project_name: run.project_name.clone(),
            trace_id: run.trace_id.clone(),
            span_id: run.span_id.clone(),
        }
    }
}

async fn load_run_query_segment_uris(postgres_url: &str, query: &RunQuery) -> Result<Vec<String>> {
    if query.project_names.is_empty() {
        return Ok(Vec::new());
    }

    let (client, connection) = tokio_postgres::connect(postgres_url, NoTls)
        .await
        .context("connect postgres for query metadata")?;
    tokio::spawn(async move {
        if let Err(err) = connection.await {
            tracing::warn!(error = %err, "postgres query metadata connection failed");
        }
    });

    let rows = client
        .query(run_candidate_segments_sql(query).as_str(), &[])
        .await
        .context("load run query segment uris")?;
    let mut uris = rows.into_iter().map(|row| row.get(0)).collect::<Vec<_>>();
    uris.sort();
    uris.dedup();
    Ok(uris)
}

fn run_candidate_segments_sql(query: &RunQuery) -> String {
    let where_sql = run_head_where_sql(query);
    let candidate_limit = query
        .limit
        .map(|limit| limit.saturating_add(query.offset.unwrap_or(0)))
        .filter(|limit| *limit > 0)
        .map(|limit| format!(" LIMIT {limit}"))
        .unwrap_or_default();

    format!(
        "SELECT DISTINCT candidate.uri
        FROM (
            SELECT trace_segments.uri, run_heads.start_time_unix_nano, run_heads.span_id
            FROM run_heads
            INNER JOIN trace_segments
                ON trace_segments.id = run_heads.last_trace_segment_id
            {joins}
            WHERE {where_sql}
            ORDER BY run_heads.start_time_unix_nano ASC, run_heads.span_id ASC{candidate_limit}
        ) AS candidate
        ORDER BY candidate.uri",
        joins = run_head_join_sql(query),
    )
}

fn run_head_join_sql(query: &RunQuery) -> String {
    let mut joins = Vec::new();
    if !query.include_deleted {
        joins.push(
            "LEFT JOIN run_deletions
                ON run_deletions.project_name = run_heads.project_name
                AND run_deletions.trace_id = run_heads.trace_id
                AND run_deletions.span_id = run_heads.span_id"
                .to_owned(),
        );
    }

    if joins.is_empty() {
        String::new()
    } else {
        format!("\n            {}", joins.join("\n            "))
    }
}

fn run_head_where_sql(query: &RunQuery) -> String {
    let mut predicates = run_query_predicates(query, "run_heads");
    predicates.push("trace_segments.compacted_at IS NULL".to_owned());

    if !query.include_deleted {
        predicates.push("run_heads.deleted_at_unix_nano IS NULL".to_owned());
        predicates.push("run_deletions.span_id IS NULL".to_owned());
    }

    if let Some(cutoff) = query.retention_cutoff_unix_nano {
        predicates.push(format!("run_heads.start_time_unix_nano >= {cutoff}"));
    }

    predicates.join(" AND ")
}

async fn load_deleted_run_keys(postgres_url: &str, query: &RunQuery) -> Result<HashSet<RunKey>> {
    if query.project_names.is_empty() {
        return Ok(HashSet::new());
    }

    let (client, connection) = tokio_postgres::connect(postgres_url, NoTls)
        .await
        .context("connect postgres for deleted run lookup")?;
    tokio::spawn(async move {
        if let Err(err) = connection.await {
            tracing::warn!(error = %err, "postgres deleted run lookup connection failed");
        }
    });

    let mut predicates = vec![format!(
        "project_name IN ({})",
        query
            .project_names
            .iter()
            .map(|project_name| sql_string_literal(project_name))
            .collect::<Vec<_>>()
            .join(", ")
    )];
    if let Some(trace_id) = &query.trace_id {
        predicates.push(format!("trace_id = {}", sql_string_literal(trace_id)));
    }

    let rows = client
        .query(
            format!(
                "SELECT project_name, trace_id, span_id
                FROM run_deletions
                WHERE {}",
                predicates.join(" AND ")
            )
            .as_str(),
            &[],
        )
        .await
        .context("load deleted runs")?;

    Ok(rows
        .into_iter()
        .map(|row| RunKey {
            project_name: row.get(0),
            trace_id: row.get(1),
            span_id: row.get(2),
        })
        .collect())
}

async fn delete_run_in_tx(
    tx: &tokio_postgres::Transaction<'_>,
    project_name: &str,
    trace_id: &str,
    span_id: &str,
    deleted_at_unix_nano: i64,
    reason: Option<&str>,
) -> Result<bool> {
    let deleted_row = tx
        .query_opt(
            "UPDATE run_heads
            SET deleted_at_unix_nano = $4,
                deletion_reason = $5,
                updated_at = CURRENT_TIMESTAMP
            WHERE project_name = $1
                AND trace_id = $2
                AND span_id = $3
                AND deleted_at_unix_nano IS NULL
            RETURNING run_id, generated_run_id, last_trace_segment_id, last_row_index",
            &[
                &project_name,
                &trace_id,
                &span_id,
                &deleted_at_unix_nano,
                &reason,
            ],
        )
        .await
        .context("mark run head deleted")?;

    let Some(deleted_row) = deleted_row else {
        return Ok(false);
    };
    let run_id: String = deleted_row.get(0);
    let _generated_run_id: String = deleted_row.get(1);
    let trace_segment_id: i64 = deleted_row.get(2);
    let row_index: i64 = deleted_row.get(3);

    let event_type = RunEventKind::Tombstone.as_str();
    let idempotency_key =
        format!("tombstone:{project_name}:{trace_id}:{span_id}:{deleted_at_unix_nano}");
    let event_row = tx
        .query_one(
            "INSERT INTO run_events(
                trace_segment_id, project_name, run_id, trace_id, span_id,
                event_type, event_time_unix_nano, row_index, idempotency_key
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (project_name, idempotency_key)
            DO UPDATE SET idempotency_key = EXCLUDED.idempotency_key
            RETURNING id",
            &[
                &trace_segment_id,
                &project_name,
                &run_id,
                &trace_id,
                &span_id,
                &event_type,
                &deleted_at_unix_nano,
                &row_index,
                &idempotency_key,
            ],
        )
        .await
        .context("insert tombstone run event")?;
    let run_event_id: i64 = event_row.get(0);

    tx.execute(
        "INSERT INTO run_deletions(
            project_name, trace_id, span_id, deleted_at_unix_nano, reason
        )
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (project_name, trace_id, span_id)
        DO UPDATE SET
            deleted_at_unix_nano = EXCLUDED.deleted_at_unix_nano,
            reason = EXCLUDED.reason",
        &[
            &project_name,
            &trace_id,
            &span_id,
            &deleted_at_unix_nano,
            &reason,
        ],
    )
    .await
    .context("upsert run deletion")?;

    tx.execute(
        "INSERT INTO trace_segment_delete_vectors(
            trace_segment_id, project_name, trace_id, span_id, deleted_at_unix_nano, reason
        )
        SELECT DISTINCT trace_segment_id, project_name, trace_id, span_id, $4::BIGINT, $5::TEXT
        FROM trace_segment_spans
        WHERE project_name = $1 AND trace_id = $2 AND span_id = $3
        ON CONFLICT (trace_segment_id, project_name, trace_id, span_id)
        DO UPDATE SET
            deleted_at_unix_nano = EXCLUDED.deleted_at_unix_nano,
            reason = EXCLUDED.reason",
        &[
            &project_name,
            &trace_id,
            &span_id,
            &deleted_at_unix_nano,
            &reason,
        ],
    )
    .await
    .context("write trace segment delete vectors")?;

    tx.execute(
        "UPDATE run_heads
        SET last_event_type = $4,
            last_event_time_unix_nano = $5,
            last_run_event_id = $6,
            updated_at = CURRENT_TIMESTAMP
        WHERE project_name = $1
            AND trace_id = $2
            AND span_id = $3",
        &[
            &project_name,
            &trace_id,
            &span_id,
            &event_type,
            &deleted_at_unix_nano,
            &run_event_id,
        ],
    )
    .await
    .context("advance deleted run head tombstone")?;

    tx.execute(
        "UPDATE run_locators
        SET event_type = $4,
            event_time_unix_nano = $5,
            run_event_id = $6,
            updated_at = CURRENT_TIMESTAMP
        WHERE project_name = $1
            AND trace_id = $2
            AND span_id = $3",
        &[
            &project_name,
            &trace_id,
            &span_id,
            &event_type,
            &deleted_at_unix_nano,
            &run_event_id,
        ],
    )
    .await
    .context("advance deleted run locator tombstone")?;

    tx.execute(
        "UPDATE trace_locators
        SET event_type = $4,
            event_time_unix_nano = $5,
            run_event_id = $6,
            updated_at = CURRENT_TIMESTAMP
        WHERE project_name = $1
            AND trace_id = $2
            AND span_id = $3",
        &[
            &project_name,
            &trace_id,
            &span_id,
            &event_type,
            &deleted_at_unix_nano,
            &run_event_id,
        ],
    )
    .await
    .context("advance deleted trace locator tombstone")?;

    Ok(true)
}

async fn query_trace_segments_with_datafusion(
    object_store: Arc<dyn ObjectStore>,
    segment_uris: Vec<String>,
    query: &RunQuery,
) -> Result<Vec<RunSummary>> {
    if segment_uris.is_empty() {
        return Ok(Vec::new());
    }

    let context = vortex_session_context(object_store)?;
    let sql = run_head_datafusion_sql(&segment_uris, query);
    let dataframe = context
        .sql(&sql)
        .await
        .context("plan DataFusion run head query")?;
    let batches = dataframe
        .collect()
        .await
        .context("execute DataFusion run head query")?;

    run_summaries_from_batches(&batches)
}

#[derive(Debug, Clone, Copy, Default)]
struct DataFusionQueryTiming {
    planning_time: Duration,
    execution_time: Duration,
}

async fn query_trace_segments_with_datafusion_timed(
    object_store: Arc<dyn ObjectStore>,
    segment_uris: Vec<String>,
    query: &RunQuery,
) -> Result<(Vec<RunSummary>, DataFusionQueryTiming)> {
    if segment_uris.is_empty() {
        return Ok((Vec::new(), DataFusionQueryTiming::default()));
    }

    let context = vortex_session_context(object_store)?;
    let sql = run_head_datafusion_sql(&segment_uris, query);

    let planning_started = Instant::now();
    let dataframe = context
        .sql(&sql)
        .await
        .context("plan DataFusion run head query")?;
    let planning_time = planning_started.elapsed();

    let execution_started = Instant::now();
    let batches = dataframe
        .collect()
        .await
        .context("execute DataFusion run head query")?;
    let execution_time = execution_started.elapsed();

    Ok((
        run_summaries_from_batches(&batches)?,
        DataFusionQueryTiming {
            planning_time,
            execution_time,
        },
    ))
}

fn run_head_datafusion_sql(segment_uris: &[String], query: &RunQuery) -> String {
    let source_sql = segment_uris
        .iter()
        .map(|uri| segment_source_sql(uri))
        .collect::<Vec<_>>()
        .join(" UNION ALL ");
    let where_sql = run_query_where_sql(query);
    let limit_sql = query
        .limit
        .map(|limit| format!(" LIMIT {limit}"))
        .unwrap_or_default();
    let offset_sql = query
        .offset
        .filter(|offset| *offset > 0)
        .map(|offset| format!(" OFFSET {offset}"))
        .unwrap_or_default();

    format!(
        "SELECT
            project_name, run_id, trace_id, span_id, parent_run_id, parent_span_id,
            name, run_type, status,
            start_time_unix_nano, end_time_unix_nano, is_root, attributes_json
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY project_name, trace_id, span_id
                    ORDER BY event_time_unix_nano DESC, end_time_unix_nano DESC, start_time_unix_nano DESC
                ) AS run_version
            FROM (
                SELECT
                    project_name,
                    NULLIF(run_id, '') AS run_id,
                    trace_id,
                    span_id,
                    parent_run_id,
                    parent_span_id,
                    name,
                    run_type,
                    CASE
                        WHEN end_time_unix_nano = 0 THEN 'pending'
                        WHEN status_code = 2 THEN 'error'
                        ELSE 'success'
                    END AS status,
                    start_time_unix_nano,
                    end_time_unix_nano,
                    event_time_unix_nano,
                    parent_span_id IS NULL AS is_root,
                    attributes_json
                FROM ({source_sql}) AS segment_spans
            ) AS versioned_runs
        ) AS runs
        WHERE run_version = 1 AND {where_sql}
        ORDER BY start_time_unix_nano ASC, span_id ASC{limit_sql}{offset_sql}",
    )
}

fn segment_source_sql(uri: &str) -> String {
    format!(
        "SELECT
            project_name,
            run_id,
            trace_id,
            span_id,
            parent_run_id,
            parent_span_id,
            name,
            run_type,
            start_time_unix_nano,
            end_time_unix_nano,
            status_code,
            event_type,
            event_time_unix_nano,
            attributes_json
        FROM {}",
        sql_object_store_path(uri)
    )
}

fn run_query_where_sql(query: &RunQuery) -> String {
    let predicates = run_query_predicates(query, "");
    if predicates.is_empty() {
        "true".to_owned()
    } else {
        predicates.join(" AND ")
    }
}

fn run_query_predicates(query: &RunQuery, table_alias: &str) -> Vec<String> {
    let column = |name: &str| {
        if table_alias.is_empty() {
            name.to_owned()
        } else {
            format!("{table_alias}.{name}")
        }
    };
    let mut predicates = Vec::new();
    if !query.project_names.is_empty() {
        predicates.push(format!(
            "{} IN ({})",
            column("project_name"),
            query
                .project_names
                .iter()
                .map(|project_name| sql_string_literal(project_name))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    if let Some(trace_id) = &query.trace_id {
        predicates.push(format!(
            "{} = {}",
            column("trace_id"),
            sql_string_literal(trace_id)
        ));
    }
    if let Some(parent_run_id) = &query.parent_run_id {
        predicates.push(format!(
            "{} = {}",
            column("parent_run_id"),
            sql_string_literal(parent_run_id)
        ));
    }
    if let Some(parent_span_id) = &query.parent_span_id {
        predicates.push(format!(
            "{} = {}",
            column("parent_span_id"),
            sql_string_literal(parent_span_id)
        ));
    }
    if let Some(run_type) = &query.run_type {
        predicates.push(format!(
            "{} = {}",
            column("run_type"),
            sql_string_literal(run_type)
        ));
    }
    if let Some(is_root) = query.is_root {
        predicates.push(format!(
            "{} = {}",
            column("is_root"),
            if is_root { "true" } else { "false" }
        ));
    }
    if let Some(error) = query.error {
        if error {
            predicates.push(format!("{} = 'error'", column("status")));
        } else {
            predicates.push(format!("{} <> 'error'", column("status")));
        }
    }
    if let Some(start_time_min_unix_nano) = query.start_time_min_unix_nano {
        predicates.push(format!(
            "{} >= {start_time_min_unix_nano}",
            column("start_time_unix_nano")
        ));
    }
    if let Some(start_time_max_unix_nano) = query.start_time_max_unix_nano {
        predicates.push(format!(
            "{} <= {start_time_max_unix_nano}",
            column("start_time_unix_nano")
        ));
    }

    predicates
}

fn vortex_session_context(object_store: Arc<dyn ObjectStore>) -> Result<SessionContext> {
    let factory = Arc::new(VortexFormatFactory::new());
    let object_store_url = Url::parse("file://").context("parse file object store url")?;
    let mut state = SessionStateBuilder::new()
        .with_default_features()
        .with_table_factory(
            factory.get_ext().to_uppercase(),
            Arc::new(DefaultTableFactory::new()),
        )
        .with_object_store(&object_store_url, object_store);

    if let Some(file_formats) = state.file_formats() {
        file_formats.push(factory as _);
    }

    Ok(SessionContext::new_with_state(state.build()).enable_url_table())
}

fn run_summaries_from_batches(batches: &[RecordBatch]) -> Result<Vec<RunSummary>> {
    let mut runs = Vec::new();
    for batch in batches {
        let project_names = string_column(batch, 0, "project_name")?;
        let run_ids = string_column(batch, 1, "run_id")?;
        let trace_ids = string_column(batch, 2, "trace_id")?;
        let span_ids = string_column(batch, 3, "span_id")?;
        let parent_run_ids = string_column(batch, 4, "parent_run_id")?;
        let parent_span_ids = string_column(batch, 5, "parent_span_id")?;
        let names = string_column(batch, 6, "name")?;
        let run_types = string_column(batch, 7, "run_type")?;
        let statuses = string_column(batch, 8, "status")?;
        let start_times = int64_column(batch, 9, "start_time_unix_nano")?;
        let end_times = int64_column(batch, 10, "end_time_unix_nano")?;
        let roots = bool_column(batch, 11, "is_root")?;
        let attributes_json_values = string_column(batch, 12, "attributes_json")?;

        for row in 0..batch.num_rows() {
            runs.push(RunSummary {
                project_name: project_names.value(row).to_owned(),
                run_id: optional_string_value(&run_ids, row),
                trace_id: trace_ids.value(row).to_owned(),
                span_id: span_ids.value(row).to_owned(),
                parent_run_id: optional_string_value(&parent_run_ids, row),
                parent_span_id: optional_string_value(&parent_span_ids, row),
                name: names.value(row).to_owned(),
                run_type: run_types.value(row).to_owned(),
                status: statuses.value(row).to_owned(),
                start_time_unix_nano: start_times.value(row),
                end_time_unix_nano: end_times.value(row),
                is_root: roots.value(row),
                attributes_json: attributes_json_values.value(row).to_owned(),
            });
        }
    }

    Ok(runs)
}

fn run_matches_retention_filter(run: &RunSummary, query: &RunQuery) -> bool {
    if let Some(cutoff) = query.retention_cutoff_unix_nano
        && run.start_time_unix_nano < cutoff
    {
        return false;
    }

    true
}

fn optional_string_value(column: &StringColumn<'_>, row: usize) -> Option<String> {
    if column.is_null(row) {
        None
    } else {
        Some(column.value(row).to_owned())
    }
}

enum StringColumn<'a> {
    Utf8(&'a StringArray),
    Utf8View(&'a StringViewArray),
}

impl StringColumn<'_> {
    fn is_null(&self, row: usize) -> bool {
        match self {
            Self::Utf8(column) => column.is_null(row),
            Self::Utf8View(column) => column.is_null(row),
        }
    }

    fn value(&self, row: usize) -> &str {
        match self {
            Self::Utf8(column) => column.value(row),
            Self::Utf8View(column) => column.value(row),
        }
    }
}

fn string_column<'a>(batch: &'a RecordBatch, index: usize, name: &str) -> Result<StringColumn<'a>> {
    let column = batch.column(index);
    if let Some(column) = column.as_string_opt::<i32>() {
        return Ok(StringColumn::Utf8(column));
    }
    if let Some(column) = column.as_string_view_opt() {
        return Ok(StringColumn::Utf8View(column));
    }

    Err(anyhow::anyhow!("column {name} is not Utf8 or Utf8View"))
}

fn int64_column<'a>(batch: &'a RecordBatch, index: usize, name: &str) -> Result<&'a Int64Array> {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<Int64Array>()
        .with_context(|| format!("column {name} is not Int64"))
}

fn bool_column<'a>(batch: &'a RecordBatch, index: usize, name: &str) -> Result<&'a BooleanArray> {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .with_context(|| format!("column {name} is not Boolean"))
}

fn sql_string_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn sql_object_store_path(uri: &str) -> String {
    let path = if uri.starts_with('/') {
        uri.to_owned()
    } else {
        format!("/{uri}")
    };
    sql_string_literal(&path)
}

fn current_time_unix_nano() -> Result<i64> {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .context("system clock is before unix epoch")?
        .as_nanos();
    i64::try_from(nanos).context("current time does not fit in i64 nanos")
}

#[cfg(test)]
mod tests;
