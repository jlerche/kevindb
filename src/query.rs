use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
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
const MAX_DATAFUSION_SEGMENTS_PER_BATCH: usize = 8;

pub mod filter;
mod object_store_stats;
mod planner;
mod random_access;
mod rows;
mod threads;
mod tree;
mod tree_access;
mod tree_filter;
use filter::FilterExpr;
use object_store_stats::{
    MeasuringObjectStore, ObjectStoreReadLimits, ObjectStoreReadSnapshot, datafusion_batch_query,
    enforce_runtime_object_store_limits, page_datafusion_runs,
};
pub(crate) use planner::{
    RunKey, SegmentCandidateRow, SegmentSource, estimate_vortex_object_store_requests,
    load_deleted_run_keys, load_run_query_plan, run_matches_retention_filter, run_query_where_sql,
    sql_string_literal,
};
pub use random_access::{RunEventSummary, RunLoadResult, RunProjection, TraceLoadResult};
pub(crate) use rows::run_summaries_from_batches;
pub use threads::{
    ThreadListPage, ThreadListQuery, ThreadMessageSummary, ThreadSummary, ThreadTracePage,
    ThreadTraceQuery, ThreadTraceSummary,
};
pub(crate) use tree::trace_tree_from_runs;
pub use tree::{RunNode, TraceTree};
pub use tree_filter::{TreeFilterExpr, TreeFilterMode, TreeFilterScope};

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

#[derive(Debug, Clone, PartialEq)]
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
    pub filter: Option<FilterExpr>,
    pub trace_filter: Option<FilterExpr>,
    pub tree_filter: Option<TreeFilterExpr>,
    pub include_payload: bool,
    pub newest_first: bool,
    pub limits: RunQueryLimits,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RunQueryLimits {
    pub max_candidate_segments: Option<usize>,
    pub max_candidate_runs: Option<usize>,
    pub max_estimated_object_store_requests: Option<usize>,
    pub max_candidate_bytes: Option<i64>,
    pub max_wall_time: Option<Duration>,
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
            filter: None,
            trace_filter: None,
            tree_filter: None,
            include_payload: true,
            newest_first: false,
            limits: RunQueryLimits::default(),
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
    pub candidate_runs: usize,
    pub candidate_bytes: i64,
    pub estimated_object_store_requests: usize,
    pub actual_object_store_requests: u64,
    pub actual_object_store_bytes_read: u64,
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
        if let Some(max_wall_time) = query.limits.max_wall_time {
            let query = query_without_wall_time_limit(query);
            return tokio::time::timeout(max_wall_time, self.list_runs_inner(query))
                .await
                .context("query exceeded max wall clock")?;
        }
        self.list_runs_inner(query).await
    }

    async fn list_runs_inner(&self, query: RunQuery) -> Result<Vec<RunSummary>> {
        let plan = load_run_query_plan(&self.postgres_url, &query).await?;
        let deleted_runs = if query.include_deleted {
            std::collections::HashSet::new()
        } else {
            load_deleted_run_keys(&self.postgres_url, &query).await?
        };
        let candidate_run_keys = plan.candidate_run_keys.clone();
        let runs = query_with_optional_timeout(
            Arc::clone(&self.object_store),
            plan.segments,
            &query,
            query.include_payload,
            Some(&candidate_run_keys),
        )
        .await?;
        Ok(runs
            .into_iter()
            .filter(|run| candidate_run_keys.contains(&RunKey::from(run)))
            .filter(|run| !deleted_runs.contains(&RunKey::from(run)))
            .filter(|run| run_matches_retention_filter(run, &query))
            .collect())
    }

    pub async fn list_runs_with_diagnostics(&self, query: RunQuery) -> Result<RunQueryResult> {
        if let Some(max_wall_time) = query.limits.max_wall_time {
            let query = query_without_wall_time_limit(query);
            return tokio::time::timeout(
                max_wall_time,
                self.list_runs_with_diagnostics_inner(query),
            )
            .await
            .context("query exceeded max wall clock")?;
        }
        self.list_runs_with_diagnostics_inner(query).await
    }

    async fn list_runs_with_diagnostics_inner(&self, query: RunQuery) -> Result<RunQueryResult> {
        let postgres_started = Instant::now();
        let plan = load_run_query_plan(&self.postgres_url, &query).await?;
        let deleted_runs = if query.include_deleted {
            std::collections::HashSet::new()
        } else {
            load_deleted_run_keys(&self.postgres_url, &query).await?
        };
        let postgres_query_time = postgres_started.elapsed();
        let candidate_segments = plan.segments.len();
        let candidate_runs = plan.candidate_runs;
        let candidate_bytes = plan.candidate_bytes;
        let estimated_object_store_requests = plan.estimated_object_store_requests;

        let candidate_run_keys = plan.candidate_run_keys.clone();
        let (runs, datafusion_timing, object_store_reads) = query_timed_with_optional_timeout(
            Arc::clone(&self.object_store),
            plan.segments,
            &query,
            query.include_payload,
            Some(&candidate_run_keys),
        )
        .await?;
        let runs = runs
            .into_iter()
            .filter(|run| candidate_run_keys.contains(&RunKey::from(run)))
            .filter(|run| !deleted_runs.contains(&RunKey::from(run)))
            .filter(|run| run_matches_retention_filter(run, &query))
            .collect::<Vec<_>>();

        Ok(RunQueryResult {
            diagnostics: RunQueryDiagnostics {
                candidate_segments,
                candidate_runs,
                candidate_bytes,
                estimated_object_store_requests,
                actual_object_store_requests: object_store_reads.request_count(),
                actual_object_store_bytes_read: object_store_reads.bytes_read,
                vortex_files_opened: candidate_segments,
                rows_returned: runs.len(),
                postgres_query_time,
                datafusion_planning_time: datafusion_timing.planning_time,
                datafusion_execution_time: datafusion_timing.execution_time,
            },
            runs,
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

fn query_without_wall_time_limit(mut query: RunQuery) -> RunQuery {
    query.limits.max_wall_time = None;
    query
}

pub fn generated_run_id(project_name: &str, trace_id: &str, span_id: &str) -> String {
    Uuid::new_v5(
        &Uuid::NAMESPACE_URL,
        format!("kevindb:run:{project_name}:{trace_id}:{span_id}").as_bytes(),
    )
    .to_string()
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

#[cfg(test)]
async fn query_trace_segments_with_datafusion(
    object_store: Arc<dyn ObjectStore>,
    segment_uris: Vec<String>,
    query: &RunQuery,
) -> Result<Vec<RunSummary>> {
    let segments = segment_uris
        .into_iter()
        .map(current_segment_source)
        .collect::<Vec<_>>();
    query_segment_sources_with_datafusion_projected(object_store, segments, query, true, None).await
}

async fn query_with_optional_timeout(
    object_store: Arc<dyn ObjectStore>,
    segments: Vec<SegmentSource>,
    query: &RunQuery,
    include_payload: bool,
    candidate_run_keys: Option<&std::collections::HashSet<RunKey>>,
) -> Result<Vec<RunSummary>> {
    let future = query_segment_sources_with_datafusion_projected(
        object_store,
        segments,
        query,
        include_payload,
        candidate_run_keys,
    );
    if let Some(max_wall_time) = query.limits.max_wall_time {
        tokio::time::timeout(max_wall_time, future)
            .await
            .context("query exceeded max wall clock")?
    } else {
        future.await
    }
}

async fn query_segment_sources_with_datafusion_projected(
    object_store: Arc<dyn ObjectStore>,
    segments: Vec<SegmentSource>,
    query: &RunQuery,
    include_payload: bool,
    candidate_run_keys: Option<&std::collections::HashSet<RunKey>>,
) -> Result<Vec<RunSummary>> {
    let (runs, _, _) = query_segment_sources_with_datafusion_timed(
        object_store,
        segments,
        query,
        include_payload,
        candidate_run_keys,
    )
    .await?;
    Ok(runs)
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct DataFusionQueryTiming {
    planning_time: Duration,
    execution_time: Duration,
}

pub(crate) async fn query_segment_sources_with_datafusion_timed(
    object_store: Arc<dyn ObjectStore>,
    segments: Vec<SegmentSource>,
    query: &RunQuery,
    include_payload: bool,
    candidate_run_keys: Option<&std::collections::HashSet<RunKey>>,
) -> Result<(
    Vec<RunSummary>,
    DataFusionQueryTiming,
    ObjectStoreReadSnapshot,
)> {
    if segments.is_empty() {
        return Ok((
            Vec::new(),
            DataFusionQueryTiming::default(),
            ObjectStoreReadSnapshot::default(),
        ));
    }

    let measured_store = Arc::new(MeasuringObjectStore::with_limits(
        object_store,
        ObjectStoreReadLimits::from_query(query),
    ));
    let context = vortex_session_context(measured_store.clone())?;
    let datafusion_query = datafusion_batch_query(query);
    let mut runs = Vec::new();
    let mut planning_time = Duration::ZERO;
    let mut execution_time = Duration::ZERO;

    for batch in segments.chunks(MAX_DATAFUSION_SEGMENTS_PER_BATCH) {
        let sql = run_head_datafusion_sql(
            batch,
            &datafusion_query,
            include_payload,
            candidate_run_keys,
        );

        let planning_started = Instant::now();
        let dataframe = context
            .sql(&sql)
            .await
            .context("plan DataFusion run head query")?;
        planning_time += planning_started.elapsed();

        let execution_started = Instant::now();
        let batches = dataframe
            .collect()
            .await
            .context("execute DataFusion run head query")?;
        execution_time += execution_started.elapsed();
        runs.extend(run_summaries_from_batches(&batches)?);
    }

    let object_store_reads = measured_store.snapshot();
    enforce_runtime_object_store_limits(query, object_store_reads)?;

    Ok((
        page_datafusion_runs(runs, query),
        DataFusionQueryTiming {
            planning_time,
            execution_time,
        },
        object_store_reads,
    ))
}

async fn query_timed_with_optional_timeout(
    object_store: Arc<dyn ObjectStore>,
    segments: Vec<SegmentSource>,
    query: &RunQuery,
    include_payload: bool,
    candidate_run_keys: Option<&std::collections::HashSet<RunKey>>,
) -> Result<(
    Vec<RunSummary>,
    DataFusionQueryTiming,
    ObjectStoreReadSnapshot,
)> {
    let future = query_segment_sources_with_datafusion_timed(
        object_store,
        segments,
        query,
        include_payload,
        candidate_run_keys,
    );
    if let Some(max_wall_time) = query.limits.max_wall_time {
        tokio::time::timeout(max_wall_time, future)
            .await
            .context("query exceeded max wall clock")?
    } else {
        future.await
    }
}

fn run_head_datafusion_sql(
    segments: &[SegmentSource],
    query: &RunQuery,
    include_payload: bool,
    candidate_run_keys: Option<&std::collections::HashSet<RunKey>>,
) -> String {
    let source_candidate_keys = segments
        .iter()
        .any(|segment| segment.candidate_rows.is_empty())
        .then_some(candidate_run_keys)
        .flatten();
    let source_where_sql = run_source_pushdown_where_sql(query, source_candidate_keys);
    let source_sql = segments
        .iter()
        .map(|segment| segment_source_sql(segment, include_payload, &source_where_sql))
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
                    ORDER BY event_time_unix_nano DESC, end_time_unix_nano DESC, start_time_unix_nano DESC, row_index DESC
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
                    row_index,
                    parent_span_id IS NULL AS is_root,
                    attributes_json
                FROM ({source_sql}) AS segment_spans
            ) AS versioned_runs
        ) AS runs
        WHERE run_version = 1 AND {where_sql}
        ORDER BY start_time_unix_nano {order_direction}, span_id ASC{limit_sql}{offset_sql}",
        order_direction = if query.newest_first { "DESC" } else { "ASC" },
    )
}

#[cfg(test)]
fn current_segment_source(uri: String) -> SegmentSource {
    SegmentSource {
        uri,
        total_bytes: 0,
        candidate_rows: Vec::new(),
    }
}

fn segment_source_sql(
    segment: &SegmentSource,
    include_payload: bool,
    source_where_sql: &str,
) -> String {
    let attributes_json_sql = attributes_json_sql(include_payload);
    let source_where_sql = segment_source_where_sql(segment, source_where_sql);

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
            row_index,
            {attributes_json_sql}
        FROM {}
        WHERE {source_where_sql}",
        sql_object_store_path(&segment.uri),
    )
}

fn segment_source_where_sql(segment: &SegmentSource, source_where_sql: &str) -> String {
    let Some(candidate_rows_sql) = segment_candidate_rows_where_sql(segment) else {
        return source_where_sql.to_owned();
    };

    format!("({source_where_sql}) AND ({candidate_rows_sql})")
}

fn run_source_pushdown_where_sql(
    query: &RunQuery,
    candidate_run_keys: Option<&std::collections::HashSet<RunKey>>,
) -> String {
    let mut predicates = Vec::new();
    if !query.project_names.is_empty() {
        predicates.push(format!(
            "project_name IN ({})",
            query
                .project_names
                .iter()
                .map(|project_name| sql_string_literal(project_name))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    if let Some(trace_id) = &query.trace_id {
        predicates.push(format!("trace_id = {}", sql_string_literal(trace_id)));
    }
    if let Some(parent_run_id) = &query.parent_run_id {
        predicates.push(format!(
            "parent_run_id = {}",
            sql_string_literal(parent_run_id)
        ));
    }
    if let Some(parent_span_id) = &query.parent_span_id {
        predicates.push(format!(
            "parent_span_id = {}",
            sql_string_literal(parent_span_id)
        ));
    }
    if let Some(run_type) = &query.run_type {
        predicates.push(format!("run_type = {}", sql_string_literal(run_type)));
    }
    if let Some(start_time_min_unix_nano) = query.start_time_min_unix_nano {
        predicates.push(format!(
            "start_time_unix_nano >= {start_time_min_unix_nano}"
        ));
    }
    if let Some(start_time_max_unix_nano) = query.start_time_max_unix_nano {
        predicates.push(format!(
            "start_time_unix_nano <= {start_time_max_unix_nano}"
        ));
    }
    if let Some(candidate_predicate) = candidate_run_source_pushdown_sql(candidate_run_keys) {
        predicates.push(candidate_predicate);
    }

    if predicates.is_empty() {
        "true".to_owned()
    } else {
        predicates.join(" AND ")
    }
}

fn candidate_run_source_pushdown_sql(
    candidate_run_keys: Option<&std::collections::HashSet<RunKey>>,
) -> Option<String> {
    let candidate_run_keys = candidate_run_keys?;
    if candidate_run_keys.is_empty() {
        return None;
    }

    let mut spans_by_trace =
        std::collections::BTreeMap::<(&str, &str), std::collections::BTreeSet<&str>>::new();
    for key in candidate_run_keys {
        spans_by_trace
            .entry((key.project_name.as_str(), key.trace_id.as_str()))
            .or_default()
            .insert(key.span_id.as_str());
    }

    let predicates = spans_by_trace
        .into_iter()
        .map(|((project_name, trace_id), span_ids)| {
            let span_ids = span_ids
                .into_iter()
                .map(sql_string_literal)
                .collect::<Vec<_>>()
                .join(", ");
            format!(
                "(project_name = {} AND trace_id = {} AND span_id IN ({}))",
                sql_string_literal(project_name),
                sql_string_literal(trace_id),
                span_ids
            )
        })
        .collect::<Vec<_>>();

    Some(format!("({})", predicates.join(" OR ")))
}

fn segment_candidate_rows_where_sql(segment: &SegmentSource) -> Option<String> {
    if segment.candidate_rows.is_empty() {
        return None;
    }

    let row_indexes = segment
        .candidate_rows
        .iter()
        .map(|row| row.row_index)
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .map(|row_index| row_index.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    Some(format!("row_index IN ({row_indexes})"))
}

pub(crate) fn attributes_json_sql(include_payload: bool) -> &'static str {
    if include_payload {
        "attributes_json"
    } else {
        "'{}' AS attributes_json"
    }
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
