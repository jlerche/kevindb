use std::collections::{HashMap, HashSet};
use std::sync::Arc;

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
use vortex_datafusion::VortexFormatFactory;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunSummary {
    pub project_name: String,
    pub trace_id: String,
    pub span_id: String,
    pub parent_span_id: Option<String>,
    pub name: String,
    pub run_type: String,
    pub status: String,
    pub start_time_unix_nano: i64,
    pub end_time_unix_nano: i64,
    pub is_root: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunNode {
    pub run: RunSummary,
    pub children: Vec<RunNode>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceTree {
    pub project_name: String,
    pub trace_id: String,
    pub roots: Vec<RunNode>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunQuery {
    pub project_names: Vec<String>,
    pub trace_id: Option<String>,
    pub run_type: Option<String>,
    pub is_root: Option<bool>,
    pub error: Option<bool>,
    pub limit: Option<usize>,
}

impl RunQuery {
    pub fn new(project_name: impl Into<String>) -> Self {
        Self {
            project_names: vec![project_name.into()],
            trace_id: None,
            run_type: None,
            is_root: None,
            error: None,
            limit: None,
        }
    }
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
        self.list_runs(RunQuery {
            project_names: vec![project_name.to_owned()],
            trace_id: Some(trace_id.to_owned()),
            run_type: None,
            is_root: None,
            error: None,
            limit: None,
        })
        .await
    }

    pub async fn list_runs(&self, query: RunQuery) -> Result<Vec<RunSummary>> {
        let segment_uris = load_run_query_segment_uris(&self.postgres_url, &query).await?;
        query_trace_segments_with_datafusion(Arc::clone(&self.object_store), segment_uris, &query)
            .await
    }

    pub async fn load_trace_tree(&self, project_name: &str, trace_id: &str) -> Result<TraceTree> {
        let runs = self.list_runs_in_trace(project_name, trace_id).await?;
        Ok(trace_tree_from_runs(project_name, trace_id, runs))
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

    let mut uris = Vec::new();
    for project_name in &query.project_names {
        let rows = if let Some(trace_id) = &query.trace_id {
            client
                .query(
                    "SELECT DISTINCT trace_segments.uri
                    FROM trace_segments
                    INNER JOIN trace_segment_spans
                        ON trace_segment_spans.trace_segment_id = trace_segments.id
                    WHERE trace_segment_spans.project_name = $1
                        AND trace_segment_spans.trace_id = $2
                    ORDER BY trace_segments.uri",
                    &[project_name, trace_id],
                )
                .await
        } else {
            client
                .query(
                    "SELECT DISTINCT trace_segments.uri
                    FROM trace_segments
                    INNER JOIN trace_segment_spans
                        ON trace_segment_spans.trace_segment_id = trace_segments.id
                    WHERE trace_segment_spans.project_name = $1
                    ORDER BY trace_segments.uri",
                    &[project_name],
                )
                .await
        }
        .context("load run query segment uris")?;

        uris.extend(rows.into_iter().map(|row| row.get(0)));
    }
    uris.sort();
    uris.dedup();
    Ok(uris)
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
    let source_sql = segment_uris
        .iter()
        .map(|uri| format!("SELECT * FROM {}", sql_object_store_path(uri)))
        .collect::<Vec<_>>()
        .join(" UNION ALL ");
    let where_sql = run_query_where_sql(query);
    let limit_sql = query
        .limit
        .map(|limit| format!(" LIMIT {limit}"))
        .unwrap_or_default();

    let sql = format!(
        "SELECT
            project_name, trace_id, span_id, parent_span_id, name, run_type, status,
            start_time_unix_nano, end_time_unix_nano, is_root
        FROM (
            SELECT
                project_name,
                trace_id,
                span_id,
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
                parent_span_id IS NULL AS is_root
            FROM ({source_sql}) AS segment_spans
        ) AS runs
        WHERE {where_sql}
        ORDER BY start_time_unix_nano ASC, span_id ASC{limit_sql}",
    );
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

fn run_query_where_sql(query: &RunQuery) -> String {
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
    if let Some(run_type) = &query.run_type {
        predicates.push(format!("run_type = {}", sql_string_literal(run_type)));
    }
    if let Some(is_root) = query.is_root {
        predicates.push(format!(
            "is_root = {}",
            if is_root { "true" } else { "false" }
        ));
    }
    if let Some(error) = query.error {
        if error {
            predicates.push("status = 'error'".to_owned());
        } else {
            predicates.push("status <> 'error'".to_owned());
        }
    }

    if predicates.is_empty() {
        "true".to_owned()
    } else {
        predicates.join(" AND ")
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

fn trace_tree_from_runs(project_name: &str, trace_id: &str, runs: Vec<RunSummary>) -> TraceTree {
    let ordered_ids = runs
        .iter()
        .map(|run| run.span_id.clone())
        .collect::<Vec<_>>();
    let known_ids = ordered_ids.iter().cloned().collect::<HashSet<_>>();
    let mut children_by_parent: HashMap<Option<String>, Vec<String>> = HashMap::new();

    for run in &runs {
        let parent = run
            .parent_span_id
            .as_ref()
            .filter(|parent| known_ids.contains(parent.as_str()))
            .cloned();
        children_by_parent
            .entry(parent)
            .or_default()
            .push(run.span_id.clone());
    }

    let runs_by_id = runs
        .into_iter()
        .map(|run| (run.span_id.clone(), run))
        .collect::<HashMap<_, _>>();
    let mut visited = HashSet::new();
    let mut active = HashSet::new();
    let mut roots = Vec::new();

    for span_id in children_by_parent.get(&None).into_iter().flatten() {
        if let Some(node) = build_run_node(
            span_id,
            &runs_by_id,
            &children_by_parent,
            &mut visited,
            &mut active,
        ) {
            roots.push(node);
        }
    }
    for span_id in ordered_ids {
        if !visited.contains(&span_id)
            && let Some(node) = build_run_node(
                &span_id,
                &runs_by_id,
                &children_by_parent,
                &mut visited,
                &mut active,
            )
        {
            roots.push(node);
        }
    }

    TraceTree {
        project_name: project_name.to_owned(),
        trace_id: trace_id.to_owned(),
        roots,
    }
}

fn build_run_node(
    span_id: &str,
    runs_by_id: &HashMap<String, RunSummary>,
    children_by_parent: &HashMap<Option<String>, Vec<String>>,
    visited: &mut HashSet<String>,
    active: &mut HashSet<String>,
) -> Option<RunNode> {
    if active.contains(span_id) || !visited.insert(span_id.to_owned()) {
        return None;
    }
    active.insert(span_id.to_owned());

    let children = children_by_parent
        .get(&Some(span_id.to_owned()))
        .into_iter()
        .flatten()
        .filter_map(|child_id| {
            build_run_node(child_id, runs_by_id, children_by_parent, visited, active)
        })
        .collect::<Vec<_>>();
    active.remove(span_id);

    runs_by_id
        .get(span_id)
        .cloned()
        .map(|run| RunNode { run, children })
}

fn run_summaries_from_batches(batches: &[RecordBatch]) -> Result<Vec<RunSummary>> {
    let mut runs = Vec::new();
    for batch in batches {
        let project_names = string_column(batch, 0, "project_name")?;
        let trace_ids = string_column(batch, 1, "trace_id")?;
        let span_ids = string_column(batch, 2, "span_id")?;
        let parent_span_ids = string_column(batch, 3, "parent_span_id")?;
        let names = string_column(batch, 4, "name")?;
        let run_types = string_column(batch, 5, "run_type")?;
        let statuses = string_column(batch, 6, "status")?;
        let start_times = int64_column(batch, 7, "start_time_unix_nano")?;
        let end_times = int64_column(batch, 8, "end_time_unix_nano")?;
        let roots = bool_column(batch, 9, "is_root")?;

        for row in 0..batch.num_rows() {
            runs.push(RunSummary {
                project_name: project_names.value(row).to_owned(),
                trace_id: trace_ids.value(row).to_owned(),
                span_id: span_ids.value(row).to_owned(),
                parent_span_id: if parent_span_ids.is_null(row) {
                    None
                } else {
                    Some(parent_span_ids.value(row).to_owned())
                },
                name: names.value(row).to_owned(),
                run_type: run_types.value(row).to_owned(),
                status: statuses.value(row).to_owned(),
                start_time_unix_nano: start_times.value(row),
                end_time_unix_nano: end_times.value(row),
                is_root: roots.value(row),
            });
        }
    }

    Ok(runs)
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

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::{ObjectStore, ObjectStoreExt, PutPayload};

    use crate::otlp::SpanRecord;
    use crate::segment::encode_span_records;

    const TRACE_ID: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    #[tokio::test]
    async fn datafusion_scans_vortex_segments() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let segment_uri = "projects/demo/trace-segments/test.vortex";
        let records = vec![
            span_record("ignored-project", "other-project", TRACE_ID, None, 1, 2, 1),
            span_record(
                "ignored-trace",
                "demo",
                "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                None,
                2,
                3,
                1,
            ),
            span_record("child", "demo", TRACE_ID, Some("root"), 20, 30, 2),
            span_record("root", "demo", TRACE_ID, None, 10, 40, 1),
        ];
        let payload = encode_span_records(&records)
            .await
            .expect("encode Vortex segment");
        object_store
            .put(&Path::from(segment_uri), PutPayload::from_bytes(payload))
            .await
            .expect("write Vortex segment");

        let result = query_trace_segments_with_datafusion(
            object_store,
            vec![segment_uri.to_owned()],
            &RunQuery {
                project_names: vec!["demo".to_owned()],
                trace_id: Some(TRACE_ID.to_owned()),
                run_type: None,
                is_root: None,
                error: None,
                limit: None,
            },
        )
        .await
        .expect("query trace segments");

        assert_eq!(
            result
                .iter()
                .map(|run| run.name.as_str())
                .collect::<Vec<_>>(),
            vec!["root", "child"]
        );
        assert_eq!(result[0].run_type, "chain");
        assert_eq!(result[0].status, "success");
        assert!(result[0].is_root);
        assert_eq!(result[1].parent_span_id.as_deref(), Some("root"));
        assert_eq!(result[1].run_type, "llm");
        assert_eq!(result[1].status, "error");
    }

    #[tokio::test]
    async fn datafusion_applies_run_query_filters() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let segment_uri = "projects/demo/trace-segments/filtered.vortex";
        let records = vec![
            span_record("root", "demo", TRACE_ID, None, 10, 40, 1),
            span_record("child", "demo", TRACE_ID, Some("root"), 20, 30, 2),
        ];
        let payload = encode_span_records(&records)
            .await
            .expect("encode Vortex segment");
        object_store
            .put(&Path::from(segment_uri), PutPayload::from_bytes(payload))
            .await
            .expect("write Vortex segment");

        let result = query_trace_segments_with_datafusion(
            object_store,
            vec![segment_uri.to_owned()],
            &RunQuery {
                project_names: vec!["demo".to_owned()],
                trace_id: None,
                run_type: Some("llm".to_owned()),
                is_root: Some(false),
                error: Some(true),
                limit: Some(1),
            },
        )
        .await
        .expect("query trace segments");

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].name, "child");
    }

    #[test]
    fn builds_trace_tree_from_runs() {
        let tree = trace_tree_from_runs(
            "demo",
            TRACE_ID,
            vec![
                run("demo", TRACE_ID, "child", Some("root"), 20),
                run("demo", TRACE_ID, "root", None, 10),
                run("demo", TRACE_ID, "orphan", Some("missing"), 30),
            ],
        );

        assert_eq!(tree.project_name, "demo");
        assert_eq!(tree.trace_id, TRACE_ID);
        assert_eq!(tree.roots.len(), 2);
        assert_eq!(tree.roots[0].run.name, "root");
        assert_eq!(tree.roots[0].children.len(), 1);
        assert_eq!(tree.roots[0].children[0].run.name, "child");
        assert_eq!(tree.roots[1].run.name, "orphan");
    }

    #[test]
    fn escapes_sql_string_literals() {
        assert_eq!(sql_string_literal("project's trace"), "'project''s trace'");
        assert_eq!(
            sql_object_store_path("projects/project's/test.vortex"),
            "'/projects/project''s/test.vortex'"
        );
        assert_eq!(
            run_query_where_sql(&RunQuery {
                project_names: vec!["demo".to_owned()],
                trace_id: Some("trace".to_owned()),
                run_type: Some("llm".to_owned()),
                is_root: Some(false),
                error: Some(false),
                limit: None,
            }),
            "project_name IN ('demo') AND trace_id = 'trace' AND run_type = 'llm' AND is_root = false AND status <> 'error'"
        );
    }

    fn run(
        project_name: &str,
        trace_id: &str,
        name: &str,
        parent_span_id: Option<&str>,
        start_time_unix_nano: i64,
    ) -> RunSummary {
        RunSummary {
            project_name: project_name.to_owned(),
            trace_id: trace_id.to_owned(),
            span_id: name.to_owned(),
            parent_span_id: parent_span_id.map(str::to_owned),
            name: name.to_owned(),
            run_type: if parent_span_id.is_some() {
                "llm".to_owned()
            } else {
                "chain".to_owned()
            },
            status: "success".to_owned(),
            start_time_unix_nano,
            end_time_unix_nano: start_time_unix_nano + 10,
            is_root: parent_span_id.is_none(),
        }
    }

    fn span_record(
        name: &str,
        project_name: &str,
        trace_id: &str,
        parent_span_id: Option<&str>,
        start_time_unix_nano: i64,
        end_time_unix_nano: i64,
        status_code: i32,
    ) -> SpanRecord {
        SpanRecord {
            project_name: project_name.to_owned(),
            trace_id: trace_id.to_owned(),
            span_id: name.to_owned(),
            parent_span_id: parent_span_id.map(str::to_owned),
            name: name.to_owned(),
            run_type: if parent_span_id.is_some() {
                "llm".to_owned()
            } else {
                "chain".to_owned()
            },
            start_time_unix_nano,
            end_time_unix_nano,
            status_code,
            attributes_json: "{}".to_owned(),
        }
    }
}
