use std::time::Instant;

use anyhow::{Context, Result, anyhow};
use tokio_postgres::{NoTls, Row};

use super::filter::FilterExpr;
use super::{QueryEngine, RunQueryDiagnostics, sql_string_literal};

const DEFAULT_THREAD_PAGE_SIZE: usize = 20;
const MAX_THREAD_PAGE_SIZE: usize = 100;

#[derive(Debug, Clone, PartialEq)]
pub struct ThreadTraceQuery {
    pub project_name: String,
    pub thread_id: String,
    pub filter: Option<FilterExpr>,
    pub page_size: usize,
    pub cursor: Option<String>,
}

impl ThreadTraceQuery {
    pub fn new(project_name: impl Into<String>, thread_id: impl Into<String>) -> Self {
        Self {
            project_name: project_name.into(),
            thread_id: thread_id.into(),
            filter: None,
            page_size: DEFAULT_THREAD_PAGE_SIZE,
            cursor: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ThreadListQuery {
    pub project_name: String,
    pub filter: Option<FilterExpr>,
    pub min_start_time_unix_nano: Option<i64>,
    pub max_start_time_unix_nano: Option<i64>,
    pub page_size: usize,
    pub cursor: Option<String>,
}

impl ThreadListQuery {
    pub fn new(project_name: impl Into<String>) -> Self {
        Self {
            project_name: project_name.into(),
            filter: None,
            min_start_time_unix_nano: None,
            max_start_time_unix_nano: None,
            page_size: DEFAULT_THREAD_PAGE_SIZE,
            cursor: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ThreadTracePage {
    pub items: Vec<ThreadTraceSummary>,
    pub next_cursor: Option<String>,
    pub diagnostics: RunQueryDiagnostics,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ThreadListPage {
    pub items: Vec<ThreadSummary>,
    pub next_cursor: Option<String>,
    pub diagnostics: RunQueryDiagnostics,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ThreadTraceSummary {
    pub thread_id: String,
    pub trace_id: String,
    pub root_run_id: String,
    pub root_span_id: String,
    pub name: Option<String>,
    pub start_time_unix_nano: i64,
    pub end_time_unix_nano: i64,
    pub latency_nanos: i64,
    pub first_token_time_unix_nano: Option<i64>,
    pub inputs_preview: Option<String>,
    pub outputs_preview: Option<String>,
    pub error_preview: Option<String>,
    pub prompt_tokens: Option<i64>,
    pub completion_tokens: Option<i64>,
    pub total_tokens: Option<i64>,
    pub prompt_cost: Option<f64>,
    pub completion_cost: Option<f64>,
    pub total_cost: Option<f64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ThreadSummary {
    pub thread_id: String,
    pub trace_id: Option<String>,
    pub first_trace_id: Option<String>,
    pub last_trace_id: Option<String>,
    pub count: i64,
    pub min_start_time_unix_nano: Option<i64>,
    pub max_start_time_unix_nano: Option<i64>,
    pub first_inputs: Option<String>,
    pub last_outputs: Option<String>,
    pub last_error: Option<String>,
    pub prompt_tokens: Option<i64>,
    pub completion_tokens: Option<i64>,
    pub total_tokens: Option<i64>,
    pub total_cost: Option<f64>,
    pub latency_p50: Option<f64>,
    pub latency_p99: Option<f64>,
    pub num_errored_turns: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThreadMessageSummary {
    pub thread_id: String,
    pub trace_id: String,
    pub span_id: String,
    pub run_id: String,
    pub role: String,
    pub preview: String,
    pub turn_order: i64,
    pub trace_segment_id: Option<i64>,
    pub row_index: Option<i64>,
    pub start_time_unix_nano: i64,
}

impl QueryEngine {
    pub async fn list_thread_traces(&self, query: ThreadTraceQuery) -> Result<ThreadTracePage> {
        let postgres_started = Instant::now();
        let limit = normalized_page_size(query.page_size);
        let rows = query_thread_trace_rows(&self.postgres_url, &query, limit + 1).await?;
        let postgres_query_time = postgres_started.elapsed();
        let (items, next_cursor) =
            page_thread_trace_rows(rows.into_iter().map(thread_trace_from_row), limit);
        let rows_returned = items.len();

        Ok(ThreadTracePage {
            items,
            next_cursor,
            diagnostics: metastore_only_diagnostics(rows_returned, postgres_query_time),
        })
    }

    pub async fn list_threads(&self, query: ThreadListQuery) -> Result<ThreadListPage> {
        let postgres_started = Instant::now();
        let limit = normalized_page_size(query.page_size);
        let rows = query_thread_rows(&self.postgres_url, &query, limit + 1).await?;
        let postgres_query_time = postgres_started.elapsed();
        let (items, next_cursor) =
            page_thread_rows(rows.into_iter().map(thread_summary_from_row), limit);
        let rows_returned = items.len();

        Ok(ThreadListPage {
            items,
            next_cursor,
            diagnostics: metastore_only_diagnostics(rows_returned, postgres_query_time),
        })
    }

    pub async fn list_thread_messages(
        &self,
        project_name: &str,
        thread_id: &str,
        limit: usize,
    ) -> Result<Vec<ThreadMessageSummary>> {
        let (client, connection) = tokio_postgres::connect(&self.postgres_url, NoTls)
            .await
            .context("connect postgres for thread messages")?;
        tokio::spawn(async move {
            if let Err(err) = connection.await {
                tracing::warn!(error = %err, "postgres thread messages connection failed");
            }
        });

        let rows = client
            .query(
                "SELECT
                    thread_id, trace_id, span_id, run_id, role, preview, turn_order,
                    trace_segment_id, row_index, start_time_unix_nano
                FROM thread_messages
                WHERE project_name = $1 AND thread_id = $2
                ORDER BY
                    turn_order ASC,
                    trace_id ASC,
                    span_id ASC,
                    CASE role WHEN 'user' THEN 0 ELSE 1 END ASC,
                    role ASC
                LIMIT $3",
                &[&project_name, &thread_id, &(limit.min(1000) as i64)],
            )
            .await
            .context("list thread messages")?;

        Ok(rows.into_iter().map(thread_message_from_row).collect())
    }
}

async fn query_thread_trace_rows(
    postgres_url: &str,
    query: &ThreadTraceQuery,
    limit: usize,
) -> Result<Vec<Row>> {
    let (client, connection) = tokio_postgres::connect(postgres_url, NoTls)
        .await
        .context("connect postgres for thread trace listing")?;
    tokio::spawn(async move {
        if let Err(err) = connection.await {
            tracing::warn!(error = %err, "postgres thread trace listing connection failed");
        }
    });

    client
        .query(thread_trace_sql(query, limit)?.as_str(), &[])
        .await
        .context("list thread traces")
}

async fn query_thread_rows(
    postgres_url: &str,
    query: &ThreadListQuery,
    limit: usize,
) -> Result<Vec<Row>> {
    let (client, connection) = tokio_postgres::connect(postgres_url, NoTls)
        .await
        .context("connect postgres for thread listing")?;
    tokio::spawn(async move {
        if let Err(err) = connection.await {
            tracing::warn!(error = %err, "postgres thread listing connection failed");
        }
    });

    client
        .query(thread_sql(query, limit)?.as_str(), &[])
        .await
        .context("list threads")
}

fn thread_trace_sql(query: &ThreadTraceQuery, limit: usize) -> Result<String> {
    let mut predicates = vec![
        format!(
            "traces.project_name = {}",
            sql_string_literal(&query.project_name)
        ),
        format!(
            "traces.thread_id = {}",
            sql_string_literal(&query.thread_id)
        ),
    ];
    if let Some(cursor) = &query.cursor {
        let cursor = TraceCursor::decode(cursor)?;
        predicates.push(format!(
            "(traces.start_time_unix_nano > {} OR (traces.start_time_unix_nano = {} AND traces.trace_id > {}))",
            cursor.start_time_unix_nano,
            cursor.start_time_unix_nano,
            sql_string_literal(&cursor.trace_id)
        ));
    }
    if let Some(filter) = &query.filter {
        predicates.push(root_run_filter_sql(
            filter,
            "traces",
            "filter_heads",
            &query.project_name,
        )?);
    }

    Ok(format!(
        "SELECT
            traces.thread_id,
            traces.trace_id,
            traces.root_run_id,
            traces.root_span_id,
            traces.name,
            traces.start_time_unix_nano,
            traces.end_time_unix_nano,
            traces.latency_nanos,
            traces.first_token_time_unix_nano,
            traces.inputs_preview,
            traces.outputs_preview,
            traces.error_preview,
            traces.prompt_tokens,
            traces.completion_tokens,
            traces.total_tokens,
            traces.prompt_cost,
            traces.completion_cost,
            traces.total_cost
        FROM thread_traces traces
        WHERE {}
        ORDER BY traces.start_time_unix_nano ASC, traces.trace_id ASC
        LIMIT {}",
        predicates.join(" AND "),
        limit
    ))
}

fn thread_sql(query: &ThreadListQuery, limit: usize) -> Result<String> {
    let mut predicates = vec![format!(
        "threads.project_name = {}",
        sql_string_literal(&query.project_name)
    )];
    if let Some(min_start_time_unix_nano) = query.min_start_time_unix_nano {
        predicates.push(format!(
            "threads.max_start_time_unix_nano >= {min_start_time_unix_nano}"
        ));
    }
    if let Some(max_start_time_unix_nano) = query.max_start_time_unix_nano {
        predicates.push(format!(
            "threads.min_start_time_unix_nano <= {max_start_time_unix_nano}"
        ));
    }
    if let Some(cursor) = &query.cursor {
        let cursor = ThreadCursor::decode(cursor)?;
        predicates.push(format!(
            "(threads.max_start_time_unix_nano < {} OR (threads.max_start_time_unix_nano = {} AND threads.thread_id > {}))",
            cursor.max_start_time_unix_nano,
            cursor.max_start_time_unix_nano,
            sql_string_literal(&cursor.thread_id)
        ));
    }
    if let Some(filter) = &query.filter {
        predicates.push(thread_root_run_filter_sql(filter, &query.project_name)?);
    }

    Ok(format!(
        "SELECT
            threads.thread_id,
            threads.last_trace_id AS trace_id,
            threads.first_trace_id,
            threads.last_trace_id,
            threads.count,
            threads.min_start_time_unix_nano,
            threads.max_start_time_unix_nano,
            threads.first_inputs,
            threads.last_outputs,
            threads.last_error,
            threads.prompt_tokens,
            threads.completion_tokens,
            threads.total_tokens,
            threads.prompt_cost,
            threads.completion_cost,
            threads.total_cost,
            threads.latency_p50,
            threads.latency_p99,
            threads.num_errored_turns
        FROM threads
        WHERE {}
        ORDER BY threads.max_start_time_unix_nano DESC NULLS LAST, threads.thread_id ASC
        LIMIT {}",
        predicates.join(" AND "),
        limit
    ))
}

fn root_run_filter_sql(
    filter: &FilterExpr,
    trace_alias: &str,
    run_alias: &str,
    project_name: &str,
) -> Result<String> {
    let project_names = vec![project_name.to_owned()];
    let predicate = filter
        .compile_run_head_filter_for_projects(run_alias, &project_names)
        .map_err(|err| anyhow!(err))?
        .predicate_sql;
    Ok(format!(
        "EXISTS (
            SELECT 1
            FROM run_heads {run_alias}
            WHERE {run_alias}.project_name = {trace_alias}.project_name
                AND {run_alias}.trace_id = {trace_alias}.trace_id
                AND {run_alias}.span_id = {trace_alias}.root_span_id
                AND {run_alias}.deleted_at_unix_nano IS NULL
                AND {predicate}
        )"
    ))
}

fn thread_root_run_filter_sql(filter: &FilterExpr, project_name: &str) -> Result<String> {
    let project_names = vec![project_name.to_owned()];
    let predicate = filter
        .compile_run_head_filter_for_projects("filter_heads", &project_names)
        .map_err(|err| anyhow!(err))?
        .predicate_sql;
    Ok(format!(
        "threads.thread_id IN (
            SELECT filter_traces.thread_id
            FROM thread_traces filter_traces
            INNER JOIN run_heads filter_heads
                ON filter_heads.project_name = filter_traces.project_name
                AND filter_heads.trace_id = filter_traces.trace_id
                AND filter_heads.span_id = filter_traces.root_span_id
            WHERE filter_traces.project_name = {}
                AND filter_heads.deleted_at_unix_nano IS NULL
                AND {predicate}
        )",
        sql_string_literal(project_name)
    ))
}

fn page_thread_trace_rows(
    rows: impl Iterator<Item = ThreadTraceSummary>,
    limit: usize,
) -> (Vec<ThreadTraceSummary>, Option<String>) {
    let mut items = rows.collect::<Vec<_>>();
    let next_cursor = if items.len() > limit {
        items.truncate(limit);
        items
            .last()
            .map(TraceCursor::from)
            .map(|cursor| cursor.encode())
    } else {
        None
    };
    (items, next_cursor)
}

fn page_thread_rows(
    rows: impl Iterator<Item = ThreadSummary>,
    limit: usize,
) -> (Vec<ThreadSummary>, Option<String>) {
    let mut items = rows.collect::<Vec<_>>();
    let next_cursor = if items.len() > limit {
        items.truncate(limit);
        items
            .last()
            .and_then(ThreadCursor::from)
            .map(|cursor| cursor.encode())
    } else {
        None
    };
    (items, next_cursor)
}

fn normalized_page_size(page_size: usize) -> usize {
    page_size.clamp(1, MAX_THREAD_PAGE_SIZE)
}

fn metastore_only_diagnostics(
    rows_returned: usize,
    postgres_query_time: std::time::Duration,
) -> RunQueryDiagnostics {
    RunQueryDiagnostics {
        candidate_runs: rows_returned,
        rows_returned,
        postgres_query_time,
        ..RunQueryDiagnostics::default()
    }
}

fn thread_trace_from_row(row: Row) -> ThreadTraceSummary {
    ThreadTraceSummary {
        thread_id: row.get(0),
        trace_id: row.get(1),
        root_run_id: row.get(2),
        root_span_id: row.get(3),
        name: row.get(4),
        start_time_unix_nano: row.get(5),
        end_time_unix_nano: row.get(6),
        latency_nanos: row.get(7),
        first_token_time_unix_nano: row.get(8),
        inputs_preview: row.get(9),
        outputs_preview: row.get(10),
        error_preview: row.get(11),
        prompt_tokens: row.get(12),
        completion_tokens: row.get(13),
        total_tokens: row.get(14),
        prompt_cost: row.get(15),
        completion_cost: row.get(16),
        total_cost: row.get(17),
    }
}

fn thread_summary_from_row(row: Row) -> ThreadSummary {
    ThreadSummary {
        thread_id: row.get(0),
        trace_id: row.get(1),
        first_trace_id: row.get(2),
        last_trace_id: row.get(3),
        count: row.get(4),
        min_start_time_unix_nano: row.get(5),
        max_start_time_unix_nano: row.get(6),
        first_inputs: row.get(7),
        last_outputs: row.get(8),
        last_error: row.get(9),
        prompt_tokens: row.get(10),
        completion_tokens: row.get(11),
        total_tokens: row.get(12),
        total_cost: row.get(15),
        latency_p50: row.get(16),
        latency_p99: row.get(17),
        num_errored_turns: row.get(18),
    }
}

fn thread_message_from_row(row: Row) -> ThreadMessageSummary {
    ThreadMessageSummary {
        thread_id: row.get(0),
        trace_id: row.get(1),
        span_id: row.get(2),
        run_id: row.get(3),
        role: row.get(4),
        preview: row.get(5),
        turn_order: row.get(6),
        trace_segment_id: row.get(7),
        row_index: row.get(8),
        start_time_unix_nano: row.get(9),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TraceCursor {
    start_time_unix_nano: i64,
    trace_id: String,
}

impl TraceCursor {
    fn decode(value: &str) -> Result<Self> {
        let mut parts = value.splitn(3, ':');
        match (parts.next(), parts.next(), parts.next()) {
            (Some("trace"), Some(start), Some(trace_id)) => Ok(Self {
                start_time_unix_nano: start
                    .parse::<i64>()
                    .context("invalid thread trace cursor start time")?,
                trace_id: trace_id.to_owned(),
            }),
            _ => Err(anyhow!("invalid thread trace cursor")),
        }
    }

    fn encode(&self) -> String {
        format!("trace:{}:{}", self.start_time_unix_nano, self.trace_id)
    }
}

impl From<&ThreadTraceSummary> for TraceCursor {
    fn from(trace: &ThreadTraceSummary) -> Self {
        Self {
            start_time_unix_nano: trace.start_time_unix_nano,
            trace_id: trace.trace_id.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ThreadCursor {
    max_start_time_unix_nano: i64,
    thread_id: String,
}

impl ThreadCursor {
    fn decode(value: &str) -> Result<Self> {
        let mut parts = value.splitn(3, ':');
        match (parts.next(), parts.next(), parts.next()) {
            (Some("thread"), Some(start), Some(thread_id)) => Ok(Self {
                max_start_time_unix_nano: start
                    .parse::<i64>()
                    .context("invalid thread cursor start time")?,
                thread_id: thread_id.to_owned(),
            }),
            _ => Err(anyhow!("invalid thread cursor")),
        }
    }

    fn encode(&self) -> String {
        format!(
            "thread:{}:{}",
            self.max_start_time_unix_nano, self.thread_id
        )
    }
}

impl ThreadCursor {
    fn from(thread: &ThreadSummary) -> Option<Self> {
        Some(Self {
            max_start_time_unix_nano: thread.max_start_time_unix_nano?,
            thread_id: thread.thread_id.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_and_decodes_trace_cursors() {
        let cursor = TraceCursor {
            start_time_unix_nano: 42,
            trace_id: "trace-a".to_owned(),
        };

        assert_eq!(
            TraceCursor::decode(&cursor.encode()).expect("decode trace cursor"),
            cursor
        );
    }

    #[test]
    fn thread_trace_sql_uses_root_run_filter_and_cursor() {
        let mut query = ThreadTraceQuery::new("demo", "thread-a");
        query.cursor = Some("trace:10:trace-a".to_owned());
        query.filter = Some(FilterExpr::parse(r#"eq(status, "success")"#).expect("filter"));

        let sql = thread_trace_sql(&query, 21).expect("compile thread trace sql");

        assert!(sql.contains("traces.thread_id = 'thread-a'"));
        assert!(sql.contains("traces.start_time_unix_nano > 10"));
        assert!(sql.contains("FROM run_heads filter_heads"));
        assert!(sql.contains("filter_heads.status = 'success'"));
    }
}
