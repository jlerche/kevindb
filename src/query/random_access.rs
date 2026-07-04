use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result};
use datafusion::prelude::SessionContext;
use object_store::path::Path;
use object_store::{Error as ObjectStoreError, ObjectStore, ObjectStoreExt};
use tokio_postgres::{NoTls, Row};

use super::object_store_stats::{MeasuringObjectStore, ObjectStoreReadSnapshot};
use super::{
    DataFusionQueryTiming, QueryEngine, RunKey, RunQuery, RunQueryDiagnostics, RunQueryResult,
    RunSummary, SegmentCandidateRow, SegmentSource, attributes_json_sql,
    estimate_vortex_object_store_requests, load_deleted_run_keys,
    query_segment_sources_with_datafusion_timed, run_matches_retention_filter,
    run_summaries_from_batches, sql_object_store_path, sql_string_literal, vortex_session_context,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunLoadResult {
    pub run: Option<RunSummary>,
    pub events: Vec<RunEventSummary>,
    pub diagnostics: RunQueryDiagnostics,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceLoadResult {
    pub runs: Vec<RunSummary>,
    pub events: Vec<RunEventSummary>,
    pub diagnostics: RunQueryDiagnostics,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunProjection {
    Summary,
    FullPayload,
    Events,
}

impl RunProjection {
    fn include_payload(self) -> bool {
        matches!(self, Self::FullPayload)
    }

    fn include_events(self) -> bool {
        matches!(self, Self::Events)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunEventSummary {
    pub project_name: String,
    pub run_id: Option<String>,
    pub generated_run_id: String,
    pub trace_id: String,
    pub span_id: String,
    pub event_type: String,
    pub event_time_unix_nano: i64,
    pub segment_uri: String,
    pub row_index: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RunLocator {
    segment_uri: String,
    project_name: String,
    stored_run_id: String,
    generated_run_id: String,
    trace_id: String,
    span_id: String,
    row_index: i64,
    total_bytes: i64,
}

impl QueryEngine {
    pub async fn load_run_by_id(&self, run_id: &str) -> Result<Option<RunSummary>> {
        Ok(self.load_run_by_id_with_diagnostics(run_id).await?.run)
    }

    pub async fn load_run_by_id_with_diagnostics(&self, run_id: &str) -> Result<RunLoadResult> {
        self.load_run_by_id_with_projection(run_id, RunProjection::FullPayload)
            .await
    }

    pub async fn load_run_by_id_with_projection(
        &self,
        run_id: &str,
        projection: RunProjection,
    ) -> Result<RunLoadResult> {
        if run_id.is_empty() {
            return Ok(RunLoadResult {
                run: None,
                events: Vec::new(),
                diagnostics: RunQueryDiagnostics::default(),
            });
        }

        let postgres_started = Instant::now();
        let Some(locator) = load_run_locator(&self.postgres_url, run_id, false).await? else {
            return Ok(RunLoadResult {
                run: None,
                events: Vec::new(),
                diagnostics: RunQueryDiagnostics {
                    postgres_query_time: postgres_started.elapsed(),
                    ..RunQueryDiagnostics::default()
                },
            });
        };
        let events = if projection.include_events() {
            load_run_events(&self.postgres_url, &locator).await?
        } else {
            Vec::new()
        };
        let postgres_query_time = postgres_started.elapsed();

        let (run, datafusion_timing, object_store_reads) = query_run_locator_segment(
            Arc::clone(&self.object_store),
            &locator,
            projection.include_payload(),
        )
        .await?;
        Ok(RunLoadResult {
            diagnostics: RunQueryDiagnostics {
                candidate_segments: 1,
                candidate_runs: usize::from(run.is_some()),
                candidate_bytes: locator.total_bytes,
                estimated_object_store_requests: estimate_vortex_object_store_requests(1),
                actual_object_store_requests: object_store_reads.request_count(),
                actual_object_store_bytes_read: object_store_reads.bytes_read,
                vortex_files_opened: 1,
                rows_returned: usize::from(run.is_some()),
                postgres_query_time,
                datafusion_planning_time: datafusion_timing.planning_time,
                datafusion_execution_time: datafusion_timing.execution_time,
            },
            run,
            events,
        })
    }

    pub async fn load_run_events_by_id(&self, run_id: &str) -> Result<Vec<RunEventSummary>> {
        if run_id.is_empty() {
            return Ok(Vec::new());
        }
        let Some(locator) = load_run_locator(&self.postgres_url, run_id, true).await? else {
            return Ok(Vec::new());
        };
        load_run_events(&self.postgres_url, &locator).await
    }

    pub async fn replay_run_by_id(&self, run_id: &str) -> Result<Option<RunSummary>> {
        if run_id.is_empty() {
            return Ok(None);
        }
        let Some(locator) = load_run_locator(&self.postgres_url, run_id, true).await? else {
            return Ok(None);
        };
        let Some(replayed_locator) = replay_event_locator(&self.postgres_url, &locator).await?
        else {
            return Ok(None);
        };
        let (run, _, _) =
            query_run_locator_segment(Arc::clone(&self.object_store), &replayed_locator, true)
                .await?;
        Ok(run)
    }

    pub async fn load_trace(&self, project_name: &str, trace_id: &str) -> Result<Vec<RunSummary>> {
        Ok(self
            .load_trace_with_diagnostics(project_name, trace_id)
            .await?
            .runs)
    }

    pub async fn load_trace_with_diagnostics(
        &self,
        project_name: &str,
        trace_id: &str,
    ) -> Result<RunQueryResult> {
        let result = self
            .load_trace_with_projection(project_name, trace_id, RunProjection::FullPayload)
            .await?;
        Ok(RunQueryResult {
            runs: result.runs,
            diagnostics: result.diagnostics,
        })
    }

    pub async fn load_trace_with_projection(
        &self,
        project_name: &str,
        trace_id: &str,
        projection: RunProjection,
    ) -> Result<TraceLoadResult> {
        let query = RunQuery {
            project_names: vec![project_name.to_owned()],
            trace_id: Some(trace_id.to_owned()),
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
            include_payload: projection.include_payload(),
            newest_first: false,
            limits: Default::default(),
        };

        let postgres_started = Instant::now();
        let segments = load_trace_segment_sources(&self.postgres_url, project_name, trace_id)
            .await
            .context("load trace locator segment uris")?;
        let deleted_runs = load_deleted_run_keys(&self.postgres_url, &query).await?;
        let candidate_segments = segments.len();
        let candidate_bytes = segments
            .iter()
            .map(|segment| segment.total_bytes)
            .sum::<i64>();
        let events = if projection.include_events() {
            load_trace_events(&self.postgres_url, project_name, trace_id).await?
        } else {
            Vec::new()
        };
        let postgres_query_time = postgres_started.elapsed();

        let (runs, datafusion_timing, object_store_reads) =
            query_segment_sources_with_datafusion_timed(
                Arc::clone(&self.object_store),
                segments,
                &query,
                projection.include_payload(),
                None,
            )
            .await?;
        let runs = runs
            .into_iter()
            .filter(|run| !deleted_runs.contains(&RunKey::from(run)))
            .filter(|run| run_matches_retention_filter(run, &query))
            .collect::<Vec<_>>();

        Ok(TraceLoadResult {
            diagnostics: RunQueryDiagnostics {
                candidate_segments,
                candidate_runs: runs.len(),
                candidate_bytes,
                estimated_object_store_requests: estimate_vortex_object_store_requests(
                    candidate_segments,
                ),
                actual_object_store_requests: object_store_reads.request_count(),
                actual_object_store_bytes_read: object_store_reads.bytes_read,
                vortex_files_opened: candidate_segments,
                rows_returned: runs.len(),
                postgres_query_time,
                datafusion_planning_time: datafusion_timing.planning_time,
                datafusion_execution_time: datafusion_timing.execution_time,
            },
            runs,
            events,
        })
    }
}

async fn load_run_locator(
    postgres_url: &str,
    run_id: &str,
    include_deleted: bool,
) -> Result<Option<RunLocator>> {
    let (client, connection) = tokio_postgres::connect(postgres_url, NoTls)
        .await
        .context("connect postgres for run locator lookup")?;
    tokio::spawn(async move {
        if let Err(err) = connection.await {
            tracing::warn!(error = %err, "postgres run locator lookup connection failed");
        }
    });

    let deletion_sql = if include_deleted {
        ""
    } else {
        "AND run_heads.deleted_at_unix_nano IS NULL
                AND run_deletions.span_id IS NULL"
    };
    let row = client
        .query_opt(
            format!(
                "SELECT
                    trace_segments.uri,
                    run_locators.project_name,
                    run_locators.run_id,
                    run_locators.generated_run_id,
                    run_locators.trace_id,
                    run_locators.span_id,
                    run_locators.row_index,
                    trace_segments.total_bytes
                FROM run_locators
                INNER JOIN trace_segments
                    ON trace_segments.id = run_locators.trace_segment_id
                INNER JOIN run_heads
                    ON run_heads.project_name = run_locators.project_name
                    AND run_heads.trace_id = run_locators.trace_id
                    AND run_heads.span_id = run_locators.span_id
                LEFT JOIN run_deletions
                    ON run_deletions.project_name = run_locators.project_name
                    AND run_deletions.trace_id = run_locators.trace_id
                    AND run_deletions.span_id = run_locators.span_id
                WHERE (run_locators.run_id = $1 OR run_locators.generated_run_id = $1)
                    AND trace_segments.compacted_at IS NULL
                    {deletion_sql}
                ORDER BY
                    run_locators.event_time_unix_nano DESC,
                    run_locators.run_event_id DESC NULLS LAST
                LIMIT 1",
            )
            .as_str(),
            &[&run_id],
        )
        .await
        .context("load run locator")?;

    Ok(row.map(run_locator_from_row))
}

fn run_locator_from_row(row: Row) -> RunLocator {
    RunLocator {
        segment_uri: row.get(0),
        project_name: row.get(1),
        stored_run_id: row.get(2),
        generated_run_id: row.get(3),
        trace_id: row.get(4),
        span_id: row.get(5),
        row_index: row.get(6),
        total_bytes: row.get(7),
    }
}

async fn query_run_locator_segment(
    object_store: Arc<dyn ObjectStore>,
    locator: &RunLocator,
    include_payload: bool,
) -> Result<(
    Option<RunSummary>,
    DataFusionQueryTiming,
    ObjectStoreReadSnapshot,
)> {
    let measured_store = Arc::new(MeasuringObjectStore::new(Arc::clone(&object_store)));
    let context = vortex_session_context(measured_store.clone())?;
    let sql = run_locator_datafusion_sql(locator, include_payload);
    let result = collect_run_locator_query(context, &sql).await;
    let object_store_reads = measured_store.snapshot();

    match result {
        Ok((mut runs, timing)) => Ok((runs.pop(), timing, object_store_reads)),
        Err(err) => {
            if matches!(
                object_store
                    .head(&Path::from(locator.segment_uri.as_str()))
                    .await,
                Err(ObjectStoreError::NotFound { .. })
            ) {
                return Err(err).with_context(|| {
                    format!("run locator object is missing: {}", locator.segment_uri)
                });
            }
            Err(err).with_context(|| format!("read run locator segment {}", locator.segment_uri))
        }
    }
}

async fn collect_run_locator_query(
    context: SessionContext,
    sql: &str,
) -> Result<(Vec<RunSummary>, DataFusionQueryTiming)> {
    let planning_started = Instant::now();
    let dataframe = context
        .sql(sql)
        .await
        .context("plan DataFusion run locator query")?;
    let planning_time = planning_started.elapsed();

    let execution_started = Instant::now();
    let batches = dataframe
        .collect()
        .await
        .context("execute DataFusion run locator query")?;
    let execution_time = execution_started.elapsed();

    Ok((
        run_summaries_from_batches(&batches)?,
        DataFusionQueryTiming {
            planning_time,
            execution_time,
        },
    ))
}

fn run_locator_datafusion_sql(locator: &RunLocator, include_payload: bool) -> String {
    let row_index_predicate = format!(" AND row_index = {}", locator.row_index);
    let run_predicate = if locator.stored_run_id.is_empty() {
        format!(
            "run_id = '' AND span_id = {}",
            sql_string_literal(&locator.span_id)
        )
    } else {
        format!("run_id = {}", sql_string_literal(&locator.stored_run_id))
    };
    let source_sql = format!(
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
            FROM {path}",
        attributes_json_sql = attributes_json_sql(include_payload),
        path = sql_object_store_path(&locator.segment_uri)
    );

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
                WHERE
                    project_name = {project_name}
                    AND trace_id = {trace_id}
                    AND {run_predicate}{row_index_predicate}
            ) AS versioned_runs
        ) AS runs
        WHERE run_version = 1
        LIMIT 1",
        project_name = sql_string_literal(&locator.project_name),
        trace_id = sql_string_literal(&locator.trace_id),
    )
}

async fn load_trace_segment_sources(
    postgres_url: &str,
    project_name: &str,
    trace_id: &str,
) -> Result<Vec<SegmentSource>> {
    let (client, connection) = tokio_postgres::connect(postgres_url, NoTls)
        .await
        .context("connect postgres for trace locator lookup")?;
    tokio::spawn(async move {
        if let Err(err) = connection.await {
            tracing::warn!(error = %err, "postgres trace locator lookup connection failed");
        }
    });

    let rows = client
        .query(
            "SELECT
                trace_segments.uri,
                trace_segments.total_bytes,
                trace_segments.schema_version,
                trace_locators.project_name,
                trace_locators.trace_id,
                trace_locators.span_id,
                trace_locators.row_index
            FROM trace_locators
            INNER JOIN trace_segments
                ON trace_segments.id = trace_locators.trace_segment_id
            LEFT JOIN run_deletions
                ON run_deletions.project_name = trace_locators.project_name
                AND run_deletions.trace_id = trace_locators.trace_id
                AND run_deletions.span_id = trace_locators.span_id
            WHERE trace_locators.project_name = $1
                AND trace_locators.trace_id = $2
                AND trace_segments.compacted_at IS NULL
                AND run_deletions.span_id IS NULL
            ORDER BY trace_segments.uri, trace_locators.span_id, trace_locators.row_index",
            &[&project_name, &trace_id],
        )
        .await
        .context("load trace locator segment uris")?;

    let mut segments_by_uri = std::collections::BTreeMap::<String, SegmentSource>::new();
    for row in rows {
        let uri: String = row.get(0);
        let total_bytes: i64 = row.get(1);
        let schema_version: i64 = row.get(2);
        segments_by_uri
            .entry(uri.clone())
            .or_insert_with(|| SegmentSource {
                uri,
                total_bytes,
                schema_version,
                candidate_rows: Vec::new(),
            })
            .candidate_rows
            .push(SegmentCandidateRow {
                project_name: row.get(3),
                trace_id: row.get(4),
                span_id: row.get(5),
                row_index: row.get(6),
            });
    }

    let mut segments = segments_by_uri.into_values().collect::<Vec<_>>();
    for segment in &mut segments {
        segment.candidate_rows.sort();
        segment.candidate_rows.dedup();
    }
    segments.sort_by(|left, right| left.uri.cmp(&right.uri));
    Ok(segments)
}

async fn load_run_events(postgres_url: &str, locator: &RunLocator) -> Result<Vec<RunEventSummary>> {
    let (client, connection) = tokio_postgres::connect(postgres_url, NoTls)
        .await
        .context("connect postgres for run event listing")?;
    tokio::spawn(async move {
        if let Err(err) = connection.await {
            tracing::warn!(error = %err, "postgres run event listing connection failed");
        }
    });

    let rows = client
        .query(
            "SELECT
                run_events.project_name,
                run_events.run_id,
                run_locators.generated_run_id,
                run_events.trace_id,
                run_events.span_id,
                run_events.event_type,
                run_events.event_time_unix_nano,
                trace_segments.uri,
                run_events.row_index
            FROM run_events
            INNER JOIN trace_segments
                ON trace_segments.id = run_events.trace_segment_id
            INNER JOIN run_locators
                ON run_locators.project_name = run_events.project_name
                AND run_locators.trace_id = run_events.trace_id
                AND run_locators.span_id = run_events.span_id
            WHERE run_events.project_name = $1
                AND run_events.trace_id = $2
                AND run_events.span_id = $3
            ORDER BY run_events.event_time_unix_nano ASC, run_events.id ASC",
            &[&locator.project_name, &locator.trace_id, &locator.span_id],
        )
        .await
        .context("load run events")?;

    Ok(rows
        .into_iter()
        .map(|row| {
            let run_id = row.get::<_, String>(1);
            RunEventSummary {
                project_name: row.get(0),
                run_id: (!run_id.is_empty()).then_some(run_id),
                generated_run_id: row.get(2),
                trace_id: row.get(3),
                span_id: row.get(4),
                event_type: row.get(5),
                event_time_unix_nano: row.get(6),
                segment_uri: row.get(7),
                row_index: row.get(8),
            }
        })
        .collect())
}

async fn load_trace_events(
    postgres_url: &str,
    project_name: &str,
    trace_id: &str,
) -> Result<Vec<RunEventSummary>> {
    let (client, connection) = tokio_postgres::connect(postgres_url, NoTls)
        .await
        .context("connect postgres for trace event listing")?;
    tokio::spawn(async move {
        if let Err(err) = connection.await {
            tracing::warn!(error = %err, "postgres trace event listing connection failed");
        }
    });

    let rows = client
        .query(
            "SELECT
                run_events.project_name,
                run_events.run_id,
                run_locators.generated_run_id,
                run_events.trace_id,
                run_events.span_id,
                run_events.event_type,
                run_events.event_time_unix_nano,
                trace_segments.uri,
                run_events.row_index
            FROM run_events
            INNER JOIN trace_segments
                ON trace_segments.id = run_events.trace_segment_id
            INNER JOIN run_locators
                ON run_locators.project_name = run_events.project_name
                AND run_locators.trace_id = run_events.trace_id
                AND run_locators.span_id = run_events.span_id
            WHERE run_events.project_name = $1
                AND run_events.trace_id = $2
            ORDER BY
                run_events.event_time_unix_nano ASC,
                run_events.id ASC",
            &[&project_name, &trace_id],
        )
        .await
        .context("load trace events")?;

    Ok(rows.into_iter().map(run_event_summary_from_row).collect())
}

fn run_event_summary_from_row(row: Row) -> RunEventSummary {
    let run_id = row.get::<_, String>(1);
    RunEventSummary {
        project_name: row.get(0),
        run_id: (!run_id.is_empty()).then_some(run_id),
        generated_run_id: row.get(2),
        trace_id: row.get(3),
        span_id: row.get(4),
        event_type: row.get(5),
        event_time_unix_nano: row.get(6),
        segment_uri: row.get(7),
        row_index: row.get(8),
    }
}

async fn replay_event_locator(
    postgres_url: &str,
    locator: &RunLocator,
) -> Result<Option<RunLocator>> {
    let (client, connection) = tokio_postgres::connect(postgres_url, NoTls)
        .await
        .context("connect postgres for run replay")?;
    tokio::spawn(async move {
        if let Err(err) = connection.await {
            tracing::warn!(error = %err, "postgres run replay connection failed");
        }
    });

    let rows = client
        .query(
            "SELECT
                trace_segments.uri,
                run_locators.project_name,
                run_locators.run_id,
                run_locators.generated_run_id,
                run_locators.trace_id,
                run_locators.span_id,
                run_events.row_index,
                trace_segments.total_bytes,
                run_events.event_type
            FROM run_events
            INNER JOIN trace_segments
                ON trace_segments.id = run_events.trace_segment_id
            INNER JOIN run_locators
                ON run_locators.project_name = run_events.project_name
                AND run_locators.trace_id = run_events.trace_id
                AND run_locators.span_id = run_events.span_id
            WHERE run_events.project_name = $1
                AND run_events.trace_id = $2
                AND run_events.span_id = $3
            ORDER BY run_events.event_time_unix_nano ASC, run_events.id ASC",
            &[&locator.project_name, &locator.trace_id, &locator.span_id],
        )
        .await
        .context("load run events for replay")?;

    let mut replayed = None;
    for row in rows {
        let event_type: String = row.get(8);
        if event_type == "tombstone" {
            replayed = None;
            continue;
        }

        replayed = Some(RunLocator {
            segment_uri: row.get(0),
            project_name: row.get(1),
            stored_run_id: row.get(2),
            generated_run_id: row.get(3),
            trace_id: row.get(4),
            span_id: row.get(5),
            row_index: row.get(6),
            total_bytes: row.get(7),
        });
    }

    Ok(replayed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_v2_run_locator_sql_filters_by_row_index() {
        let sql = run_locator_datafusion_sql(
            &RunLocator {
                segment_uri: "projects/demo/trace-segments/test.vortex".to_owned(),
                project_name: "demo".to_owned(),
                stored_run_id: "1111111111111111".to_owned(),
                generated_run_id: "generated".to_owned(),
                trace_id: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
                span_id: "1111111111111111".to_owned(),
                row_index: 7,
                total_bytes: 123,
            },
            true,
        );

        assert!(sql.contains("row_index = 7"));
        assert!(sql.contains("AND run_id = '1111111111111111'"));
    }

    #[test]
    fn generated_run_locator_sql_filters_by_empty_run_id_and_span_id() {
        let sql = run_locator_datafusion_sql(
            &RunLocator {
                segment_uri: "projects/demo/trace-segments/test.vortex".to_owned(),
                project_name: "demo".to_owned(),
                stored_run_id: String::new(),
                generated_run_id: "generated".to_owned(),
                trace_id: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
                span_id: "1111111111111111".to_owned(),
                row_index: 7,
                total_bytes: 123,
            },
            true,
        );

        assert!(sql.contains("run_id = '' AND span_id = '1111111111111111'"));
    }
}
