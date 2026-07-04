use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow};
use arrow_array::cast::AsArray;
use arrow_array::{Array, Float64Array, Int64Array, RecordBatch, StringArray, StringViewArray};
use object_store::ObjectStore;

use super::super::object_store_stats::{
    MeasuringObjectStore, ObjectStoreReadLimits, ObjectStoreReadSnapshot,
    enforce_runtime_object_store_limits,
};
use super::super::{
    DataFusionQueryTiming, MAX_DATAFUSION_SEGMENTS_PER_BATCH, RunKey, RunQuery, SegmentSource,
    run_query_where_sql, run_source_pushdown_where_sql, segment_candidate_rows_where_sql,
    sql_object_store_path, vortex_session_context,
};

pub(super) async fn load_aggregate_rows_with_datafusion(
    object_store: Arc<dyn ObjectStore>,
    segments: Vec<SegmentSource>,
    query: &RunQuery,
    candidate_run_keys: Option<&HashSet<RunKey>>,
) -> Result<(
    Vec<AggregateRunRow>,
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
    let mut rows = Vec::new();
    let mut planning_time = Duration::ZERO;
    let mut execution_time = Duration::ZERO;

    for batch in segments.chunks(MAX_DATAFUSION_SEGMENTS_PER_BATCH) {
        let sql = aggregate_datafusion_sql(batch, query, candidate_run_keys);
        let planning_started = Instant::now();
        let dataframe = context
            .sql(&sql)
            .await
            .context("plan DataFusion aggregate query")?;
        planning_time += planning_started.elapsed();

        let execution_started = Instant::now();
        let batches = dataframe
            .collect()
            .await
            .context("execute DataFusion aggregate query")?;
        execution_time += execution_started.elapsed();
        rows.extend(aggregate_rows_from_batches(&batches)?);
    }

    let object_store_reads = measured_store.snapshot();
    enforce_runtime_object_store_limits(query, object_store_reads)?;
    Ok((
        rows,
        DataFusionQueryTiming {
            planning_time,
            execution_time,
        },
        object_store_reads,
    ))
}

fn aggregate_datafusion_sql(
    segments: &[SegmentSource],
    query: &RunQuery,
    candidate_run_keys: Option<&HashSet<RunKey>>,
) -> String {
    let source_candidate_keys = segments
        .iter()
        .any(|segment| segment.candidate_rows.is_empty())
        .then_some(candidate_run_keys)
        .flatten();
    let source_where_sql = run_source_pushdown_where_sql(query, source_candidate_keys);
    let source_sql = segments
        .iter()
        .map(|segment| aggregate_segment_source_sql(segment, &source_where_sql))
        .collect::<Vec<_>>()
        .join(" UNION ALL ");
    let where_sql = run_query_where_sql(query);

    format!(
        "SELECT
            project_name, run_id, trace_id, span_id, run_type, status,
            start_time_unix_nano, latency_nanos,
            prompt_tokens, completion_tokens, total_tokens,
            prompt_cost, completion_cost, total_cost,
            first_token_latency_nanos, evaluator_score, model_name, provider_name
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
                    latency_nanos,
                    prompt_tokens,
                    completion_tokens,
                    total_tokens,
                    prompt_cost,
                    completion_cost,
                    total_cost,
                    first_token_latency_nanos,
                    evaluator_score,
                    model_name,
                    provider_name
                FROM ({source_sql}) AS segment_spans
            ) AS versioned_runs
        ) AS runs
        WHERE run_version = 1 AND {where_sql}"
    )
}

fn aggregate_segment_source_sql(segment: &SegmentSource, source_where_sql: &str) -> String {
    let source_where_sql =
        if let Some(candidate_rows_sql) = segment_candidate_rows_where_sql(segment) {
            format!("({source_where_sql}) AND ({candidate_rows_sql})")
        } else {
            source_where_sql.to_owned()
        };

    format!(
        "SELECT
            project_name, run_id, trace_id, span_id, run_type,
            start_time_unix_nano, end_time_unix_nano, status_code,
            event_time_unix_nano, row_index,
            latency_nanos, prompt_tokens, completion_tokens, total_tokens,
            prompt_cost, completion_cost, total_cost,
            first_token_latency_nanos, evaluator_score, model_name, provider_name
        FROM {}
        WHERE {source_where_sql}",
        sql_object_store_path(&segment.uri),
    )
}

#[derive(Debug, Clone)]
pub(super) struct AggregateRunRow {
    pub(super) project_name: String,
    pub(super) run_id: Option<String>,
    pub(super) trace_id: String,
    pub(super) span_id: String,
    pub(super) run_type: String,
    pub(super) status: String,
    pub(super) start_time_unix_nano: i64,
    pub(super) latency_nanos: i64,
    pub(super) prompt_tokens: Option<i64>,
    pub(super) completion_tokens: Option<i64>,
    pub(super) total_tokens: Option<i64>,
    pub(super) prompt_cost: Option<f64>,
    pub(super) completion_cost: Option<f64>,
    pub(super) total_cost: Option<f64>,
    pub(super) first_token_latency_nanos: Option<i64>,
    pub(super) evaluator_score: Option<f64>,
    pub(super) model_name: Option<String>,
    pub(super) provider_name: Option<String>,
}

impl AggregateRunRow {
    pub(super) fn run_key(&self) -> RunKey {
        RunKey {
            project_name: self.project_name.clone(),
            trace_id: self.trace_id.clone(),
            span_id: self.span_id.clone(),
        }
    }
}

fn aggregate_rows_from_batches(batches: &[RecordBatch]) -> Result<Vec<AggregateRunRow>> {
    let mut rows = Vec::new();
    for batch in batches {
        let project_name = string_column(batch, "project_name")?;
        let run_id = string_column(batch, "run_id")?;
        let trace_id = string_column(batch, "trace_id")?;
        let span_id = string_column(batch, "span_id")?;
        let run_type = string_column(batch, "run_type")?;
        let status = string_column(batch, "status")?;
        let start_time = i64_column(batch, "start_time_unix_nano")?;
        let latency = i64_column(batch, "latency_nanos")?;
        let prompt_tokens = i64_column(batch, "prompt_tokens")?;
        let completion_tokens = i64_column(batch, "completion_tokens")?;
        let total_tokens = i64_column(batch, "total_tokens")?;
        let prompt_cost = f64_column(batch, "prompt_cost")?;
        let completion_cost = f64_column(batch, "completion_cost")?;
        let total_cost = f64_column(batch, "total_cost")?;
        let first_token_latency = i64_column(batch, "first_token_latency_nanos")?;
        let evaluator_score = f64_column(batch, "evaluator_score")?;
        let model_name = string_column(batch, "model_name")?;
        let provider_name = string_column(batch, "provider_name")?;

        for row in 0..batch.num_rows() {
            rows.push(AggregateRunRow {
                project_name: required_string(&project_name, row, "project_name")?,
                run_id: optional_string(&run_id, row),
                trace_id: required_string(&trace_id, row, "trace_id")?,
                span_id: required_string(&span_id, row, "span_id")?,
                run_type: required_string(&run_type, row, "run_type")?,
                status: required_string(&status, row, "status")?,
                start_time_unix_nano: required_i64(start_time, row, "start_time_unix_nano")?,
                latency_nanos: required_i64(latency, row, "latency_nanos")?,
                prompt_tokens: optional_i64(prompt_tokens, row),
                completion_tokens: optional_i64(completion_tokens, row),
                total_tokens: optional_i64(total_tokens, row),
                prompt_cost: optional_f64(prompt_cost, row),
                completion_cost: optional_f64(completion_cost, row),
                total_cost: optional_f64(total_cost, row),
                first_token_latency_nanos: optional_i64(first_token_latency, row),
                evaluator_score: optional_f64(evaluator_score, row),
                model_name: optional_string(&model_name, row),
                provider_name: optional_string(&provider_name, row),
            });
        }
    }
    Ok(rows)
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

fn string_column<'a>(batch: &'a RecordBatch, name: &str) -> Result<StringColumn<'a>> {
    let column = batch
        .column_by_name(name)
        .ok_or_else(|| anyhow!("missing aggregate column {name}"))?;
    if let Some(column) = column.as_string_opt::<i32>() {
        return Ok(StringColumn::Utf8(column));
    }
    if let Some(column) = column.as_string_view_opt() {
        return Ok(StringColumn::Utf8View(column));
    }
    Err(anyhow!("aggregate column {name} is not utf8 or utf8view"))
}

fn i64_column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a Int64Array> {
    batch
        .column_by_name(name)
        .ok_or_else(|| anyhow!("missing aggregate column {name}"))?
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| anyhow!("aggregate column {name} is not int64"))
}

fn f64_column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a Float64Array> {
    batch
        .column_by_name(name)
        .ok_or_else(|| anyhow!("missing aggregate column {name}"))?
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| anyhow!("aggregate column {name} is not float64"))
}

fn required_string(array: &StringColumn<'_>, row: usize, name: &str) -> Result<String> {
    optional_string(array, row).ok_or_else(|| anyhow!("aggregate column {name} is null"))
}

fn optional_string(array: &StringColumn<'_>, row: usize) -> Option<String> {
    (!array.is_null(row)).then(|| array.value(row).to_owned())
}

fn required_i64(array: &Int64Array, row: usize, name: &str) -> Result<i64> {
    optional_i64(array, row).ok_or_else(|| anyhow!("aggregate column {name} is null"))
}

fn optional_i64(array: &Int64Array, row: usize) -> Option<i64> {
    (!array.is_null(row)).then(|| array.value(row))
}

fn optional_f64(array: &Float64Array, row: usize) -> Option<f64> {
    (!array.is_null(row)).then(|| array.value(row))
}
