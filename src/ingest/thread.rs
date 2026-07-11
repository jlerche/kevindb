use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde_json::Value;

use kevindb_core::SpanRecord;

const MAX_PREVIEW_BYTES: usize = 512;
const MAX_THREAD_ID_BYTES: usize = 256;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct RunPreview {
    inputs_preview: Option<String>,
    outputs_preview: Option<String>,
    error_preview: Option<String>,
    first_token_time_unix_nano: Option<i64>,
}

#[derive(Debug, Clone, PartialEq)]
struct ThreadRunRow {
    span_id: String,
    run_id: String,
    root_run_id: String,
    root_span_id: String,
    name: String,
    status: String,
    is_root: bool,
    start_time_unix_nano: i64,
    end_time_unix_nano: i64,
    prompt_tokens: Option<i64>,
    completion_tokens: Option<i64>,
    total_tokens: Option<i64>,
    prompt_cost: Option<f64>,
    completion_cost: Option<f64>,
    total_cost: Option<f64>,
    trace_segment_id: Option<i64>,
    row_index: Option<i64>,
    thread_id: Option<String>,
    inputs_preview: Option<String>,
    outputs_preview: Option<String>,
    error_preview: Option<String>,
    first_token_time_unix_nano: Option<i64>,
}

#[derive(Debug, Clone, PartialEq)]
struct ThreadTraceUpsert {
    root_run_id: String,
    root_span_id: String,
    name: String,
    start_time_unix_nano: i64,
    end_time_unix_nano: i64,
    latency_nanos: i64,
    first_token_time_unix_nano: Option<i64>,
    inputs_preview: Option<String>,
    outputs_preview: Option<String>,
    error_preview: Option<String>,
    prompt_tokens: Option<i64>,
    completion_tokens: Option<i64>,
    total_tokens: Option<i64>,
    prompt_cost: Option<f64>,
    completion_cost: Option<f64>,
    total_cost: Option<f64>,
}

#[derive(Debug, Clone, PartialEq)]
struct ThreadTraceSummaryRow {
    trace_id: String,
    start_time_unix_nano: i64,
    latency_nanos: i64,
    inputs_preview: Option<String>,
    outputs_preview: Option<String>,
    error_preview: Option<String>,
    prompt_tokens: Option<i64>,
    completion_tokens: Option<i64>,
    total_tokens: Option<i64>,
    prompt_cost: Option<f64>,
    completion_cost: Option<f64>,
    total_cost: Option<f64>,
}

pub(super) async fn replace_run_preview(
    tx: &tokio_postgres::Transaction<'_>,
    record: &SpanRecord,
) -> Result<()> {
    let preview = RunPreview::from_attributes_json(&record.attributes_json);
    tx.execute(
        "INSERT INTO run_previews(
            project_name, trace_id, span_id, inputs_preview, outputs_preview,
            error_preview, first_token_time_unix_nano
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (project_name, trace_id, span_id)
        DO UPDATE SET
            inputs_preview = EXCLUDED.inputs_preview,
            outputs_preview = EXCLUDED.outputs_preview,
            error_preview = EXCLUDED.error_preview,
            first_token_time_unix_nano = EXCLUDED.first_token_time_unix_nano",
        &[
            &record.project_name,
            &record.trace_id,
            &record.span_id,
            &preview.inputs_preview,
            &preview.outputs_preview,
            &preview.error_preview,
            &preview.first_token_time_unix_nano,
        ],
    )
    .await
    .context("upsert run preview")?;
    Ok(())
}

pub(super) async fn refresh_trace_thread_metadata(
    tx: &tokio_postgres::Transaction<'_>,
    project_name: &str,
    trace_id: &str,
) -> Result<()> {
    let mut affected_threads =
        delete_existing_trace_thread_rows(tx, project_name, trace_id).await?;
    let runs = load_trace_thread_runs(tx, project_name, trace_id).await?;
    let mut runs_by_thread = BTreeMap::<String, Vec<ThreadRunRow>>::new();
    for run in runs {
        if let Some(thread_id) = run.thread_id.clone() {
            runs_by_thread.entry(thread_id).or_default().push(run);
        }
    }

    for (thread_id, mut runs) in runs_by_thread {
        runs.sort_by(|left, right| {
            left.start_time_unix_nano
                .cmp(&right.start_time_unix_nano)
                .then_with(|| left.span_id.cmp(&right.span_id))
        });
        affected_threads.insert(thread_id.clone());
        ensure_thread_row(tx, project_name, &thread_id).await?;
        let trace = build_thread_trace_upsert(&runs);
        upsert_thread_trace(tx, project_name, &thread_id, trace_id, &trace).await?;
        replace_thread_messages(tx, project_name, &thread_id, trace_id, &runs).await?;
    }

    for thread_id in affected_threads {
        refresh_thread_summary(tx, project_name, &thread_id).await?;
    }

    Ok(())
}

async fn delete_existing_trace_thread_rows(
    tx: &tokio_postgres::Transaction<'_>,
    project_name: &str,
    trace_id: &str,
) -> Result<BTreeSet<String>> {
    let old_thread_ids = tx
        .query(
            "SELECT DISTINCT thread_id
            FROM thread_traces
            WHERE project_name = $1 AND trace_id = $2",
            &[&project_name, &trace_id],
        )
        .await
        .context("load old trace thread ids")?
        .into_iter()
        .map(|row| row.get(0))
        .collect::<BTreeSet<_>>();

    tx.execute(
        "DELETE FROM thread_messages WHERE project_name = $1 AND trace_id = $2",
        &[&project_name, &trace_id],
    )
    .await
    .context("delete stale thread messages")?;
    tx.execute(
        "DELETE FROM thread_traces WHERE project_name = $1 AND trace_id = $2",
        &[&project_name, &trace_id],
    )
    .await
    .context("delete stale thread traces")?;

    Ok(old_thread_ids)
}

async fn load_trace_thread_runs(
    tx: &tokio_postgres::Transaction<'_>,
    project_name: &str,
    trace_id: &str,
) -> Result<Vec<ThreadRunRow>> {
    let tags_by_span = load_trace_tags(tx, project_name, trace_id).await?;
    let rows = tx
        .query(
            "SELECT
                heads.span_id,
                heads.run_id,
                heads.root_run_id,
                heads.root_span_id,
                heads.name,
                heads.status,
                heads.is_root,
                heads.start_time_unix_nano,
                heads.end_time_unix_nano,
                heads.prompt_tokens,
                heads.completion_tokens,
                heads.total_tokens,
                heads.prompt_cost,
                heads.completion_cost,
                heads.total_cost,
                heads.last_trace_segment_id,
                heads.last_row_index,
                thread_meta.value,
                session_meta.value,
                previews.inputs_preview,
                previews.outputs_preview,
                previews.error_preview,
                previews.first_token_time_unix_nano
            FROM run_heads heads
            LEFT JOIN run_metadata thread_meta
                ON thread_meta.project_name = heads.project_name
                AND thread_meta.trace_id = heads.trace_id
                AND thread_meta.span_id = heads.span_id
                AND thread_meta.key = 'thread_id'
            LEFT JOIN run_metadata session_meta
                ON session_meta.project_name = heads.project_name
                AND session_meta.trace_id = heads.trace_id
                AND session_meta.span_id = heads.span_id
                AND session_meta.key = 'session_id'
            LEFT JOIN run_previews previews
                ON previews.project_name = heads.project_name
                AND previews.trace_id = heads.trace_id
                AND previews.span_id = heads.span_id
            WHERE heads.project_name = $1
                AND heads.trace_id = $2
                AND heads.deleted_at_unix_nano IS NULL
            ORDER BY heads.start_time_unix_nano ASC, heads.span_id ASC",
            &[&project_name, &trace_id],
        )
        .await
        .context("load trace runs for thread refresh")?;

    Ok(rows
        .into_iter()
        .map(|row| {
            let span_id: String = row.get(0);
            let metadata_thread_id = clean_thread_id(row.get::<_, Option<String>>(17));
            let metadata_session_id = clean_thread_id(row.get::<_, Option<String>>(18));
            let tag_thread_id = tags_by_span
                .get(&span_id)
                .and_then(|tags| thread_id_from_tags(tags));
            let thread_id = metadata_thread_id
                .or(metadata_session_id)
                .or(tag_thread_id.and_then(|value| clean_thread_id(Some(value))));
            ThreadRunRow {
                span_id,
                run_id: row.get(1),
                root_run_id: row.get(2),
                root_span_id: row.get(3),
                name: row.get(4),
                status: row.get(5),
                is_root: row.get(6),
                start_time_unix_nano: row.get(7),
                end_time_unix_nano: row.get(8),
                prompt_tokens: row.get(9),
                completion_tokens: row.get(10),
                total_tokens: row.get(11),
                prompt_cost: row.get(12),
                completion_cost: row.get(13),
                total_cost: row.get(14),
                trace_segment_id: row.get(15),
                row_index: row.get(16),
                thread_id,
                inputs_preview: row.get(19),
                outputs_preview: row.get(20),
                error_preview: row.get(21),
                first_token_time_unix_nano: row.get(22),
            }
        })
        .collect())
}

async fn load_trace_tags(
    tx: &tokio_postgres::Transaction<'_>,
    project_name: &str,
    trace_id: &str,
) -> Result<BTreeMap<String, Vec<String>>> {
    let rows = tx
        .query(
            "SELECT span_id, tag
            FROM run_tags
            WHERE project_name = $1 AND trace_id = $2
            ORDER BY span_id, tag",
            &[&project_name, &trace_id],
        )
        .await
        .context("load trace tags for thread refresh")?;

    let mut tags_by_span = BTreeMap::<String, Vec<String>>::new();
    for row in rows {
        tags_by_span.entry(row.get(0)).or_default().push(row.get(1));
    }
    Ok(tags_by_span)
}

fn build_thread_trace_upsert(runs: &[ThreadRunRow]) -> ThreadTraceUpsert {
    let first = runs
        .first()
        .expect("thread trace requires at least one run");
    let root = runs.iter().find(|run| run.is_root).unwrap_or(first);
    let end_time_unix_nano = runs
        .iter()
        .map(|run| run.end_time_unix_nano)
        .max()
        .unwrap_or(first.end_time_unix_nano);
    let start_time_unix_nano = runs
        .iter()
        .map(|run| run.start_time_unix_nano)
        .min()
        .unwrap_or(first.start_time_unix_nano);

    ThreadTraceUpsert {
        root_run_id: stable_run_id(root),
        root_span_id: first_nonempty(&[&root.root_span_id, &root.span_id])
            .unwrap_or(&root.span_id)
            .to_owned(),
        name: root.name.clone(),
        start_time_unix_nano,
        end_time_unix_nano,
        latency_nanos: end_time_unix_nano
            .checked_sub(start_time_unix_nano)
            .filter(|latency| *latency > 0)
            .unwrap_or(0),
        first_token_time_unix_nano: runs
            .iter()
            .filter_map(|run| run.first_token_time_unix_nano)
            .min(),
        inputs_preview: first_preview(runs.iter().map(|run| run.inputs_preview.as_ref())),
        outputs_preview: last_preview(runs.iter().map(|run| run.outputs_preview.as_ref())),
        error_preview: last_preview(runs.iter().map(|run| run.error_preview.as_ref())),
        prompt_tokens: sum_i64(runs.iter().map(|run| run.prompt_tokens)),
        completion_tokens: sum_i64(runs.iter().map(|run| run.completion_tokens)),
        total_tokens: sum_i64(runs.iter().map(|run| run.total_tokens)),
        prompt_cost: sum_f64(runs.iter().map(|run| run.prompt_cost)),
        completion_cost: sum_f64(runs.iter().map(|run| run.completion_cost)),
        total_cost: sum_f64(runs.iter().map(|run| run.total_cost)),
    }
}

async fn ensure_thread_row(
    tx: &tokio_postgres::Transaction<'_>,
    project_name: &str,
    thread_id: &str,
) -> Result<()> {
    tx.execute(
        "INSERT INTO threads(project_name, thread_id)
        VALUES ($1, $2)
        ON CONFLICT (project_name, thread_id) DO NOTHING",
        &[&project_name, &thread_id],
    )
    .await
    .context("ensure thread row")?;
    Ok(())
}

async fn upsert_thread_trace(
    tx: &tokio_postgres::Transaction<'_>,
    project_name: &str,
    thread_id: &str,
    trace_id: &str,
    trace: &ThreadTraceUpsert,
) -> Result<()> {
    tx.execute(
        "INSERT INTO thread_traces(
            project_name, thread_id, trace_id, root_run_id, root_span_id, name,
            start_time_unix_nano, end_time_unix_nano, latency_nanos,
            first_token_time_unix_nano, inputs_preview, outputs_preview, error_preview,
            prompt_tokens, completion_tokens, total_tokens,
            prompt_cost, completion_cost, total_cost
        )
        VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13,
            $14, $15, $16, $17, $18, $19
        )
        ON CONFLICT (project_name, thread_id, trace_id)
        DO UPDATE SET
            root_run_id = EXCLUDED.root_run_id,
            root_span_id = EXCLUDED.root_span_id,
            name = EXCLUDED.name,
            start_time_unix_nano = EXCLUDED.start_time_unix_nano,
            end_time_unix_nano = EXCLUDED.end_time_unix_nano,
            latency_nanos = EXCLUDED.latency_nanos,
            first_token_time_unix_nano = EXCLUDED.first_token_time_unix_nano,
            inputs_preview = EXCLUDED.inputs_preview,
            outputs_preview = EXCLUDED.outputs_preview,
            error_preview = EXCLUDED.error_preview,
            prompt_tokens = EXCLUDED.prompt_tokens,
            completion_tokens = EXCLUDED.completion_tokens,
            total_tokens = EXCLUDED.total_tokens,
            prompt_cost = EXCLUDED.prompt_cost,
            completion_cost = EXCLUDED.completion_cost,
            total_cost = EXCLUDED.total_cost",
        &[
            &project_name,
            &thread_id,
            &trace_id,
            &trace.root_run_id,
            &trace.root_span_id,
            &trace.name,
            &trace.start_time_unix_nano,
            &trace.end_time_unix_nano,
            &trace.latency_nanos,
            &trace.first_token_time_unix_nano,
            &trace.inputs_preview,
            &trace.outputs_preview,
            &trace.error_preview,
            &trace.prompt_tokens,
            &trace.completion_tokens,
            &trace.total_tokens,
            &trace.prompt_cost,
            &trace.completion_cost,
            &trace.total_cost,
        ],
    )
    .await
    .context("upsert thread trace")?;
    Ok(())
}

async fn replace_thread_messages(
    tx: &tokio_postgres::Transaction<'_>,
    project_name: &str,
    thread_id: &str,
    trace_id: &str,
    runs: &[ThreadRunRow],
) -> Result<()> {
    for run in runs {
        if let Some(preview) = &run.inputs_preview {
            upsert_thread_message(tx, project_name, thread_id, trace_id, run, "user", preview)
                .await?;
        }
        if let Some(preview) = &run.outputs_preview {
            upsert_thread_message(
                tx,
                project_name,
                thread_id,
                trace_id,
                run,
                "assistant",
                preview,
            )
            .await?;
        }
    }
    Ok(())
}

async fn upsert_thread_message(
    tx: &tokio_postgres::Transaction<'_>,
    project_name: &str,
    thread_id: &str,
    trace_id: &str,
    run: &ThreadRunRow,
    role: &str,
    preview: &str,
) -> Result<()> {
    tx.execute(
        "INSERT INTO thread_messages(
            project_name, thread_id, trace_id, span_id, run_id, role, preview,
            turn_order, trace_segment_id, row_index, start_time_unix_nano
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        ON CONFLICT (project_name, thread_id, trace_id, span_id, role)
        DO UPDATE SET
            run_id = EXCLUDED.run_id,
            preview = EXCLUDED.preview,
            turn_order = EXCLUDED.turn_order,
            trace_segment_id = EXCLUDED.trace_segment_id,
            row_index = EXCLUDED.row_index,
            start_time_unix_nano = EXCLUDED.start_time_unix_nano",
        &[
            &project_name,
            &thread_id,
            &trace_id,
            &run.span_id,
            &stable_run_id(run),
            &role,
            &preview,
            &run.start_time_unix_nano,
            &run.trace_segment_id,
            &run.row_index,
            &run.start_time_unix_nano,
        ],
    )
    .await
    .context("upsert thread message")?;
    Ok(())
}

async fn refresh_thread_summary(
    tx: &tokio_postgres::Transaction<'_>,
    project_name: &str,
    thread_id: &str,
) -> Result<()> {
    let rows = tx
        .query(
            "SELECT
                trace_id,
                start_time_unix_nano,
                latency_nanos,
                inputs_preview,
                outputs_preview,
                error_preview,
                prompt_tokens,
                completion_tokens,
                total_tokens,
                prompt_cost,
                completion_cost,
                total_cost
            FROM thread_traces
            WHERE project_name = $1 AND thread_id = $2
            ORDER BY start_time_unix_nano ASC, trace_id ASC",
            &[&project_name, &thread_id],
        )
        .await
        .context("load thread trace summaries")?;

    let traces = rows
        .into_iter()
        .map(|row| ThreadTraceSummaryRow {
            trace_id: row.get(0),
            start_time_unix_nano: row.get(1),
            latency_nanos: row.get(2),
            inputs_preview: row.get(3),
            outputs_preview: row.get(4),
            error_preview: row.get(5),
            prompt_tokens: row.get(6),
            completion_tokens: row.get(7),
            total_tokens: row.get(8),
            prompt_cost: row.get(9),
            completion_cost: row.get(10),
            total_cost: row.get(11),
        })
        .collect::<Vec<_>>();

    if traces.is_empty() {
        tx.execute(
            "DELETE FROM threads WHERE project_name = $1 AND thread_id = $2",
            &[&project_name, &thread_id],
        )
        .await
        .context("delete empty thread summary")?;
        return Ok(());
    }

    let count = traces.len() as i64;
    let first = traces.first().expect("nonempty traces");
    let last = traces.last().expect("nonempty traces");
    let mut latencies = traces
        .iter()
        .map(|trace| trace.latency_nanos.max(0))
        .collect::<Vec<_>>();
    latencies.sort_unstable();
    let latency_p50 = percentile_seconds(&latencies, 50);
    let latency_p99 = percentile_seconds(&latencies, 99);
    let num_errored_turns = traces
        .iter()
        .filter(|trace| trace.error_preview.is_some())
        .count() as i64;
    let first_inputs = first_preview(traces.iter().map(|trace| trace.inputs_preview.as_ref()));
    let last_outputs = last_preview(traces.iter().map(|trace| trace.outputs_preview.as_ref()));
    let last_error = last_preview(traces.iter().map(|trace| trace.error_preview.as_ref()));
    let prompt_tokens = sum_i64(traces.iter().map(|trace| trace.prompt_tokens));
    let completion_tokens = sum_i64(traces.iter().map(|trace| trace.completion_tokens));
    let total_tokens = sum_i64(traces.iter().map(|trace| trace.total_tokens));
    let prompt_cost = sum_f64(traces.iter().map(|trace| trace.prompt_cost));
    let completion_cost = sum_f64(traces.iter().map(|trace| trace.completion_cost));
    let total_cost = sum_f64(traces.iter().map(|trace| trace.total_cost));

    tx.execute(
        "UPDATE threads
        SET
            count = $3,
            first_trace_id = $4,
            last_trace_id = $5,
            min_start_time_unix_nano = $6,
            max_start_time_unix_nano = $7,
            first_inputs = $8,
            last_outputs = $9,
            last_error = $10,
            prompt_tokens = $11,
            completion_tokens = $12,
            total_tokens = $13,
            prompt_cost = $14,
            completion_cost = $15,
            total_cost = $16,
            latency_p50 = $17,
            latency_p99 = $18,
            num_errored_turns = $19
        WHERE project_name = $1 AND thread_id = $2",
        &[
            &project_name,
            &thread_id,
            &count,
            &first.trace_id,
            &last.trace_id,
            &first.start_time_unix_nano,
            &last.start_time_unix_nano,
            &first_inputs,
            &last_outputs,
            &last_error,
            &prompt_tokens,
            &completion_tokens,
            &total_tokens,
            &prompt_cost,
            &completion_cost,
            &total_cost,
            &latency_p50,
            &latency_p99,
            &num_errored_turns,
        ],
    )
    .await
    .context("update thread summary")?;
    Ok(())
}

impl RunPreview {
    fn from_attributes_json(attributes_json: &str) -> Self {
        let Ok(root) = serde_json::from_str::<Value>(attributes_json) else {
            return Self::default();
        };

        Self {
            inputs_preview: first_path_value(
                &root,
                &[
                    &["langsmith.inputs"],
                    &["inputs"],
                    &["input"],
                    &["messages"],
                ],
            )
            .and_then(input_preview),
            outputs_preview: first_path_value(
                &root,
                &[
                    &["langsmith.outputs"],
                    &["outputs"],
                    &["output"],
                    &["result"],
                ],
            )
            .and_then(output_preview),
            error_preview: first_path_value(&root, &[&["langsmith.error"], &["error"]])
                .and_then(error_preview),
            first_token_time_unix_nano: first_path_value(
                &root,
                &[
                    &["first_token_time_unix_nano"],
                    &["first_token_time"],
                    &["metrics", "first_token_time_unix_nano"],
                    &["metrics", "first_token_time"],
                    &["langsmith.extra", "metadata", "first_token_time_unix_nano"],
                    &["langsmith.extra", "metadata", "first_token_time"],
                ],
            )
            .and_then(first_token_time_unix_nano),
        }
    }
}

fn input_preview(value: &Value) -> Option<String> {
    message_container_preview(value).or_else(|| {
        object_field_preview(value, &["question", "prompt", "input", "content", "text"])
    })
}

fn output_preview(value: &Value) -> Option<String> {
    choice_message_preview(value).or_else(|| {
        message_container_preview(value)
            .or_else(|| object_field_preview(value, &["content", "text", "output", "result"]))
    })
}

fn error_preview(value: &Value) -> Option<String> {
    value
        .as_str()
        .and_then(bounded_preview)
        .or_else(|| object_field_preview(value, &["message", "error"]))
}

fn message_container_preview(value: &Value) -> Option<String> {
    match value {
        Value::Array(messages) => messages.iter().rev().find_map(message_content_preview),
        Value::Object(object) => object
            .get("messages")
            .or_else(|| object.get("message"))
            .and_then(message_container_preview)
            .or_else(|| message_content_preview(value)),
        _ => scalar_or_compact_preview(value),
    }
}

fn choice_message_preview(value: &Value) -> Option<String> {
    value
        .get("choices")
        .and_then(Value::as_array)
        .and_then(|choices| choices.first())
        .and_then(|choice| choice.get("message").or_else(|| choice.get("delta")))
        .and_then(message_content_preview)
}

fn message_content_preview(value: &Value) -> Option<String> {
    match value {
        Value::String(value) => bounded_preview(value),
        Value::Object(object) => object.get("content").and_then(content_preview),
        _ => None,
    }
}

fn content_preview(value: &Value) -> Option<String> {
    match value {
        Value::String(value) => bounded_preview(value),
        Value::Array(parts) => parts.iter().find_map(|part| {
            part.as_str().and_then(bounded_preview).or_else(|| {
                part.get("text")
                    .and_then(Value::as_str)
                    .and_then(bounded_preview)
            })
        }),
        _ => None,
    }
}

fn object_field_preview(value: &Value, fields: &[&str]) -> Option<String> {
    let object = value.as_object()?;
    fields
        .iter()
        .find_map(|field| object.get(*field).and_then(scalar_or_compact_preview))
        .or_else(|| scalar_or_compact_preview(value))
}

fn scalar_or_compact_preview(value: &Value) -> Option<String> {
    match value {
        Value::String(value) => bounded_preview(value),
        Value::Number(_) | Value::Bool(_) => bounded_preview(&value.to_string()),
        Value::Array(_) | Value::Object(_) => bounded_preview(&value.to_string()),
        Value::Null => None,
    }
}

fn first_path_value<'a>(root: &'a Value, paths: &[&[&str]]) -> Option<&'a Value> {
    paths.iter().find_map(|path| path_value(root, path))
}

fn path_value<'a>(root: &'a Value, path: &[&str]) -> Option<&'a Value> {
    let mut current = root;
    for key in path {
        current = current.get(*key)?;
    }
    Some(current)
}

fn first_token_time_unix_nano(value: &Value) -> Option<i64> {
    match value {
        Value::Number(number) => number.as_i64(),
        Value::String(value) => value
            .parse::<i64>()
            .ok()
            .or_else(|| parse_rfc3339_nanos(value)),
        _ => None,
    }
}

fn parse_rfc3339_nanos(value: &str) -> Option<i64> {
    let datetime = DateTime::parse_from_rfc3339(value).ok()?;
    datetime
        .timestamp()
        .checked_mul(1_000_000_000)?
        .checked_add(i64::from(
            datetime.with_timezone(&Utc).timestamp_subsec_nanos(),
        ))
}

fn thread_id_from_tags(tags: &[String]) -> Option<String> {
    tags.iter()
        .find_map(|tag| tag_value(tag, &["thread_id:", "thread_id=", "thread:", "thread="]))
        .or_else(|| {
            tags.iter()
                .find_map(|tag| tag_value(tag, &["session_id:", "session_id="]))
        })
}

fn tag_value(tag: &str, prefixes: &[&str]) -> Option<String> {
    prefixes
        .iter()
        .find_map(|prefix| tag.strip_prefix(prefix))
        .map(str::to_owned)
}

fn clean_thread_id(value: Option<String>) -> Option<String> {
    value
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty() && value.len() <= MAX_THREAD_ID_BYTES)
}

fn stable_run_id(run: &ThreadRunRow) -> String {
    first_nonempty(&[&run.run_id, &run.root_run_id, &run.span_id])
        .unwrap_or(&run.span_id)
        .to_owned()
}

fn first_nonempty<'a>(values: &[&'a str]) -> Option<&'a str> {
    values.iter().copied().find(|value| !value.is_empty())
}

fn first_preview<'a>(values: impl IntoIterator<Item = Option<&'a String>>) -> Option<String> {
    values
        .into_iter()
        .flatten()
        .find(|value| !value.is_empty())
        .cloned()
}

fn last_preview<'a>(values: impl IntoIterator<Item = Option<&'a String>>) -> Option<String> {
    values
        .into_iter()
        .flatten()
        .filter(|value| !value.is_empty())
        .last()
        .cloned()
}

fn sum_i64(values: impl IntoIterator<Item = Option<i64>>) -> Option<i64> {
    let mut seen = false;
    let sum = values.into_iter().flatten().inspect(|_| seen = true).sum();
    seen.then_some(sum)
}

fn sum_f64(values: impl IntoIterator<Item = Option<f64>>) -> Option<f64> {
    let mut seen = false;
    let sum = values.into_iter().flatten().inspect(|_| seen = true).sum();
    seen.then_some(sum)
}

fn percentile_seconds(sorted_nanos: &[i64], percentile: usize) -> Option<f64> {
    if sorted_nanos.is_empty() {
        return None;
    }
    let index = ((sorted_nanos.len() - 1) * percentile).div_ceil(100);
    Some(sorted_nanos[index.min(sorted_nanos.len() - 1)] as f64 / 1_000_000_000.0)
}

fn bounded_preview(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }
    Some(truncate_utf8(trimmed, MAX_PREVIEW_BYTES))
}

fn truncate_utf8(value: &str, max_bytes: usize) -> String {
    if value.len() <= max_bytes {
        return value.to_owned();
    }
    let mut end = max_bytes;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    value[..end].to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn extracts_thread_previews_from_common_message_shapes() {
        let preview = RunPreview::from_attributes_json(
            &json!({
                "langsmith.inputs": {
                    "messages": [
                        {"role": "system", "content": "ignore"},
                        {"role": "user", "content": "hello"}
                    ]
                },
                "langsmith.outputs": {
                    "choices": [
                        {"message": {"role": "assistant", "content": "world"}}
                    ]
                },
                "first_token_time": "1970-01-01T00:00:00.000000123Z"
            })
            .to_string(),
        );

        assert_eq!(preview.inputs_preview.as_deref(), Some("hello"));
        assert_eq!(preview.outputs_preview.as_deref(), Some("world"));
        assert_eq!(preview.first_token_time_unix_nano, Some(123));
    }

    #[test]
    fn bounds_preview_bytes() {
        let preview = RunPreview::from_attributes_json(
            &json!({
                "inputs": {"prompt": "x".repeat(MAX_PREVIEW_BYTES + 20)}
            })
            .to_string(),
        );

        assert_eq!(
            preview.inputs_preview.expect("preview").len(),
            MAX_PREVIEW_BYTES
        );
    }

    #[test]
    fn extracts_thread_ids_from_tags() {
        assert_eq!(
            thread_id_from_tags(&["prod".to_owned(), "thread_id:abc".to_owned()]).as_deref(),
            Some("abc")
        );
        assert_eq!(
            thread_id_from_tags(&["session_id=def".to_owned()]).as_deref(),
            Some("def")
        );
    }
}
