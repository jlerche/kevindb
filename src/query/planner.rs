use std::collections::{BTreeMap, HashSet};

use anyhow::{Context, Result, anyhow};
use tokio_postgres::NoTls;

use super::{RunQuery, RunSummary};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RunQueryPlan {
    pub segments: Vec<SegmentSource>,
    pub candidate_run_keys: HashSet<RunKey>,
    pub candidate_runs: usize,
    pub candidate_bytes: i64,
    pub estimated_object_store_requests: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SegmentSource {
    pub(crate) uri: String,
    pub(crate) total_bytes: i64,
    pub(crate) candidate_rows: Vec<SegmentCandidateRow>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct SegmentCandidateRow {
    pub(crate) project_name: String,
    pub(crate) trace_id: String,
    pub(crate) span_id: String,
    pub(crate) row_index: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct RunKey {
    pub(crate) project_name: String,
    pub(crate) trace_id: String,
    pub(crate) span_id: String,
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

pub(crate) async fn load_run_query_plan(
    postgres_url: &str,
    query: &RunQuery,
) -> Result<RunQueryPlan> {
    if query.project_names.is_empty() {
        return Ok(RunQueryPlan {
            segments: Vec::new(),
            candidate_run_keys: HashSet::new(),
            candidate_runs: 0,
            candidate_bytes: 0,
            estimated_object_store_requests: 0,
        });
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
        .query(run_candidate_runs_sql(query)?.as_str(), &[])
        .await
        .context("load run query candidates")?;
    let mut candidate_run_keys = HashSet::new();
    let mut segments_by_uri = BTreeMap::<String, SegmentSource>::new();

    for row in rows {
        let uri: String = row.get(0);
        let total_bytes: i64 = row.get(1);
        let project_name: String = row.get(2);
        let trace_id: String = row.get(3);
        let span_id: String = row.get(4);
        let row_index: i64 = row.get(5);
        candidate_run_keys.insert(RunKey {
            project_name: project_name.clone(),
            trace_id: trace_id.clone(),
            span_id: span_id.clone(),
        });
        segments_by_uri
            .entry(uri.clone())
            .or_insert_with(|| SegmentSource {
                uri,
                total_bytes,
                candidate_rows: Vec::new(),
            })
            .candidate_rows
            .push(SegmentCandidateRow {
                project_name,
                trace_id,
                span_id,
                row_index,
            });
    }

    let candidate_runs = candidate_run_keys.len();
    let candidate_bytes = segments_by_uri
        .values()
        .map(|segment| segment.total_bytes)
        .sum::<i64>();
    let estimated_object_store_requests =
        estimate_vortex_object_store_requests(segments_by_uri.len());
    enforce_limits(
        query,
        segments_by_uri.len(),
        candidate_runs,
        estimated_object_store_requests,
        candidate_bytes,
    )?;

    let mut segments = segments_by_uri.into_values().collect::<Vec<_>>();
    for segment in &mut segments {
        segment.candidate_rows.sort();
        segment.candidate_rows.dedup();
    }
    segments.sort_by(|left, right| left.uri.cmp(&right.uri));
    Ok(RunQueryPlan {
        segments,
        candidate_run_keys,
        candidate_runs,
        candidate_bytes,
        estimated_object_store_requests,
    })
}

fn run_candidate_runs_sql(query: &RunQuery) -> Result<String> {
    let where_sql = run_head_where_sql(query)?;
    let candidate_limit = query
        .limit
        .map(|limit| limit.saturating_add(query.offset.unwrap_or(0)))
        .filter(|limit| *limit > 0)
        .map(|limit| format!(" LIMIT {limit}"))
        .unwrap_or_default();
    let order_direction = if query.newest_first { "DESC" } else { "ASC" };

    Ok(format!(
        "SELECT
            candidate.uri,
            candidate.total_bytes,
            candidate.project_name,
            candidate.trace_id,
            candidate.span_id,
            candidate.last_row_index
            FROM (
                SELECT
                    trace_segments.uri,
                    trace_segments.total_bytes,
                    run_heads.project_name,
                run_heads.trace_id,
                run_heads.span_id,
                run_heads.start_time_unix_nano,
                run_heads.last_row_index
            FROM run_heads
            INNER JOIN trace_segments
                ON trace_segments.id = run_heads.last_trace_segment_id
            {joins}
            WHERE {where_sql}
            ORDER BY
                run_heads.start_time_unix_nano {order_direction},
                run_heads.span_id ASC,
                run_heads.last_row_index ASC{candidate_limit}
        ) AS candidate
        ORDER BY
            candidate.start_time_unix_nano {order_direction},
            candidate.span_id ASC,
            candidate.last_row_index ASC",
        joins = run_head_join_sql(query),
    ))
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

fn run_head_where_sql(query: &RunQuery) -> Result<String> {
    let mut predicates = run_query_predicates(query, "run_heads");
    predicates.push("trace_segments.compacted_at IS NULL".to_owned());

    if !query.include_deleted {
        predicates.push("run_heads.deleted_at_unix_nano IS NULL".to_owned());
        predicates.push("run_deletions.span_id IS NULL".to_owned());
    }

    if let Some(cutoff) = query.retention_cutoff_unix_nano {
        predicates.push(format!("run_heads.start_time_unix_nano >= {cutoff}"));
    }

    if let Some(filter) = &query.filter {
        predicates.push(
            filter
                .compile_run_head_filter("run_heads")
                .map_err(|err| anyhow!(err))?
                .predicate_sql,
        );
    }
    if let Some(trace_filter) = &query.trace_filter {
        let root_predicate = trace_filter
            .compile_run_head_filter("root_filter")
            .map_err(|err| anyhow!(err))?
            .predicate_sql;
        predicates.push(format!(
            "EXISTS (
                SELECT 1 FROM run_heads root_filter
                WHERE root_filter.project_name = run_heads.project_name
                    AND root_filter.trace_id = run_heads.trace_id
                    AND root_filter.is_root = true
                    AND {root_predicate}
            )"
        ));
    }

    Ok(predicates.join(" AND "))
}

fn enforce_limits(
    query: &RunQuery,
    candidate_segments: usize,
    candidate_runs: usize,
    estimated_object_store_requests: usize,
    candidate_bytes: i64,
) -> Result<()> {
    if let Some(limit) = query.limits.max_candidate_segments
        && candidate_segments > limit
    {
        return Err(anyhow!(
            "query rejected: candidate segments {candidate_segments} exceed limit {limit}"
        ));
    }
    if let Some(limit) = query.limits.max_candidate_runs
        && candidate_runs > limit
    {
        return Err(anyhow!(
            "query rejected: candidate runs {candidate_runs} exceed limit {limit}"
        ));
    }
    if let Some(limit) = query.limits.max_estimated_object_store_requests
        && estimated_object_store_requests > limit
    {
        return Err(anyhow!(
            "query rejected: estimated object-store requests {estimated_object_store_requests} exceed limit {limit}"
        ));
    }
    if let Some(limit) = query.limits.max_candidate_bytes
        && candidate_bytes > limit
    {
        return Err(anyhow!(
            "query rejected: candidate bytes {candidate_bytes} exceed limit {limit}"
        ));
    }
    Ok(())
}

pub(crate) fn estimate_vortex_object_store_requests(candidate_segments: usize) -> usize {
    // Local Vortex scans currently issue roughly 30-45 object-store requests per
    // opened file. Use a conservative pre-read estimate so request limits are real
    // guardrails instead of a segment-count proxy.
    const ESTIMATED_REQUESTS_PER_VORTEX_FILE: usize = 48;
    candidate_segments.saturating_mul(ESTIMATED_REQUESTS_PER_VORTEX_FILE)
}

pub(crate) async fn load_deleted_run_keys(
    postgres_url: &str,
    query: &RunQuery,
) -> Result<HashSet<RunKey>> {
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

pub(crate) fn run_query_where_sql(query: &RunQuery) -> String {
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

pub(crate) fn run_matches_retention_filter(run: &RunSummary, query: &RunQuery) -> bool {
    if let Some(cutoff) = query.retention_cutoff_unix_nano
        && run.start_time_unix_nano < cutoff
    {
        return false;
    }

    true
}

pub(crate) fn sql_string_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}
