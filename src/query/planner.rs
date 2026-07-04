use std::collections::HashSet;

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
    pub(crate) schema_version: i64,
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
    let mut segment_bytes = std::collections::BTreeMap::<(String, i64), i64>::new();

    for row in rows {
        let uri: String = row.get(0);
        let schema_version: i64 = row.get(1);
        let total_bytes: i64 = row.get(2);
        candidate_run_keys.insert(RunKey {
            project_name: row.get(3),
            trace_id: row.get(4),
            span_id: row.get(5),
        });
        segment_bytes
            .entry((uri, schema_version))
            .or_insert(total_bytes);
    }

    let candidate_runs = candidate_run_keys.len();
    let candidate_bytes = segment_bytes.values().copied().sum::<i64>();
    let estimated_object_store_requests = segment_bytes.len();
    enforce_limits(
        query,
        segment_bytes.len(),
        estimated_object_store_requests,
        candidate_bytes,
    )?;

    let mut segments = segment_bytes
        .into_keys()
        .map(|(uri, schema_version)| SegmentSource {
            uri,
            schema_version,
        })
        .collect::<Vec<_>>();
    segments.sort_by(|left, right| {
        left.uri
            .cmp(&right.uri)
            .then(left.schema_version.cmp(&right.schema_version))
    });
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
            candidate.schema_version,
            candidate.total_bytes,
            candidate.project_name,
            candidate.trace_id,
            candidate.span_id
        FROM (
            SELECT
                trace_segments.uri,
                trace_segments.schema_version,
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
