use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result};
use serde_json::Value;

use crate::metrics::TypedRunMetrics;
use crate::record::SpanRecord;

const MAX_TAGS: usize = 32;
const MAX_TAG_BYTES: usize = 128;
const MAX_METADATA_PAIRS: usize = 32;
const MAX_METADATA_KEY_BYTES: usize = 128;
const MAX_METADATA_VALUE_BYTES: usize = 256;
pub(crate) const AGGREGATE_ROLLUP_BUCKET_UNIX_NANOS: i64 = 60 * 60 * 1_000_000_000;

#[derive(Debug, Clone, PartialEq)]
pub(super) struct ScalarIndexes {
    pub root_run_id: String,
    pub root_span_id: String,
    pub latency_nanos: i64,
    pub prompt_tokens: Option<i64>,
    pub completion_tokens: Option<i64>,
    pub total_tokens: Option<i64>,
    pub prompt_cost: Option<f64>,
    pub completion_cost: Option<f64>,
    pub total_cost: Option<f64>,
    pub first_token_latency_nanos: Option<i64>,
    pub evaluator_score: Option<f64>,
    pub model_name: Option<String>,
    pub provider_name: Option<String>,
    pub tags: Vec<String>,
    pub metadata: Vec<(String, String)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct RootLocator {
    pub run_id: String,
    pub span_id: String,
}

impl ScalarIndexes {
    pub fn from_record(record: &SpanRecord, root: RootLocator) -> Result<Self> {
        let attributes = parse_attributes(&record.attributes_json)?;
        let metrics = TypedRunMetrics::from_record(record);
        Ok(Self {
            root_run_id: root.run_id,
            root_span_id: root.span_id,
            latency_nanos: metrics.latency_nanos,
            prompt_tokens: metrics.prompt_tokens,
            completion_tokens: metrics.completion_tokens,
            total_tokens: metrics.total_tokens,
            prompt_cost: metrics.prompt_cost,
            completion_cost: metrics.completion_cost,
            total_cost: metrics.total_cost,
            first_token_latency_nanos: metrics.first_token_latency_nanos,
            evaluator_score: metrics.evaluator_score,
            model_name: metrics.model_name,
            provider_name: metrics.provider_name,
            tags: extract_tags(&attributes),
            metadata: extract_metadata(&attributes),
        })
    }
}

pub(super) async fn root_locator_for_record(
    tx: &tokio_postgres::Transaction<'_>,
    record: &SpanRecord,
) -> Result<RootLocator> {
    let own_run_id = record.run_id.clone();

    let Some(parent_span_id) = record.parent_span_id.as_deref() else {
        return Ok(RootLocator {
            run_id: own_run_id,
            span_id: record.span_id.clone(),
        });
    };

    let row = tx
        .query_opt(
            "SELECT root_run_id, root_span_id, run_id, span_id
            FROM run_heads
            WHERE project_name = $1 AND trace_id = $2 AND span_id = $3
            LIMIT 1",
            &[&record.project_name, &record.trace_id, &parent_span_id],
        )
        .await
        .context("load parent root locator")?;

    Ok(row
        .map(|row| {
            let root_run_id: String = row.get(0);
            let root_span_id: String = row.get(1);
            let run_id: String = row.get(2);
            let span_id: String = row.get(3);
            RootLocator {
                run_id: first_nonempty(&[&root_run_id, &run_id])
                    .unwrap_or(&own_run_id)
                    .to_owned(),
                span_id: first_nonempty(&[&root_span_id, &span_id])
                    .unwrap_or(parent_span_id)
                    .to_owned(),
            }
        })
        .unwrap_or(RootLocator {
            run_id: own_run_id,
            span_id: record.span_id.clone(),
        }))
}

fn first_nonempty<'a>(values: &[&'a str]) -> Option<&'a str> {
    values.iter().copied().find(|value| !value.is_empty())
}

pub(super) async fn replace_run_scalar_indexes(
    tx: &tokio_postgres::Transaction<'_>,
    record: &SpanRecord,
    indexes: &ScalarIndexes,
) -> Result<()> {
    tx.execute(
        "DELETE FROM run_tags
        WHERE project_name = $1 AND trace_id = $2 AND span_id = $3",
        &[&record.project_name, &record.trace_id, &record.span_id],
    )
    .await
    .context("delete old run tags")?;

    for tag in &indexes.tags {
        tx.execute(
            "INSERT INTO run_tags(project_name, trace_id, span_id, tag, updated_at)
            VALUES ($1, $2, $3, $4, CURRENT_TIMESTAMP)
            ON CONFLICT (project_name, trace_id, span_id, tag)
            DO UPDATE SET updated_at = CURRENT_TIMESTAMP",
            &[
                &record.project_name,
                &record.trace_id,
                &record.span_id,
                &tag,
            ],
        )
        .await
        .context("insert run tag")?;
    }

    tx.execute(
        "DELETE FROM run_metadata
        WHERE project_name = $1 AND trace_id = $2 AND span_id = $3",
        &[&record.project_name, &record.trace_id, &record.span_id],
    )
    .await
    .context("delete old run metadata")?;

    for (key, value) in &indexes.metadata {
        tx.execute(
            "INSERT INTO run_metadata(project_name, trace_id, span_id, key, value, updated_at)
            VALUES ($1, $2, $3, $4, $5, CURRENT_TIMESTAMP)
            ON CONFLICT (project_name, trace_id, span_id, key, value)
            DO UPDATE SET updated_at = CURRENT_TIMESTAMP",
            &[
                &record.project_name,
                &record.trace_id,
                &record.span_id,
                &key,
                &value,
            ],
        )
        .await
        .context("insert run metadata")?;
    }
    Ok(())
}

pub(super) async fn refresh_project_aggregate_rollups(
    tx: &tokio_postgres::Transaction<'_>,
    project_name: &str,
    time_bucket_start_unix_nano: i64,
) -> Result<()> {
    tx.execute(
        "DELETE FROM run_metric_rollups
        WHERE project_name = $1 AND time_bucket_start_unix_nano = $2",
        &[&project_name, &time_bucket_start_unix_nano],
    )
    .await
    .context("delete old run aggregate rollups")?;

    let rows = tx
        .query(
            "SELECT
                run_type, status, start_time_unix_nano, latency_nanos,
                prompt_tokens, completion_tokens, total_tokens,
                prompt_cost, completion_cost, total_cost,
                evaluator_score, model_name
            FROM run_heads heads
            LEFT JOIN run_deletions deletions
                ON deletions.project_name = heads.project_name
                AND deletions.trace_id = heads.trace_id
                AND deletions.span_id = heads.span_id
            WHERE heads.project_name = $1
                AND heads.start_time_unix_nano >= $2
                AND heads.start_time_unix_nano < $3
                AND heads.deleted_at_unix_nano IS NULL
                AND deletions.span_id IS NULL",
            &[
                &project_name,
                &time_bucket_start_unix_nano,
                &time_bucket_start_unix_nano.saturating_add(AGGREGATE_ROLLUP_BUCKET_UNIX_NANOS),
            ],
        )
        .await
        .context("load run heads for aggregate rollups")?;

    let runs = rows
        .into_iter()
        .map(|row| RollupRun {
            run_type: row.get(0),
            status: row.get(1),
            start_time_unix_nano: row.get(2),
            latency_nanos: row.get(3),
            prompt_tokens: row.get(4),
            completion_tokens: row.get(5),
            total_tokens: row.get(6),
            prompt_cost: row.get(7),
            completion_cost: row.get(8),
            total_cost: row.get(9),
            evaluator_score: row.get(10),
            model_name: row.get(11),
        })
        .collect::<Vec<_>>();

    for rollup_kind in ["project", "run_type", "model", "error"] {
        let mut groups = BTreeMap::<(i64, String), RollupAccumulator>::new();
        for run in &runs {
            let group_value = match rollup_kind {
                "project" => project_name.to_owned(),
                "run_type" => run.run_type.clone(),
                "model" => run
                    .model_name
                    .clone()
                    .unwrap_or_else(|| "unknown".to_owned()),
                "error" => (run.status == "error").to_string(),
                _ => unreachable!("known rollup kind"),
            };
            groups
                .entry((rollup_time_bucket(run.start_time_unix_nano), group_value))
                .or_default()
                .push(run);
        }
        for ((time_bucket_start_unix_nano, group_value), accumulator) in groups {
            insert_run_metric_rollup(
                tx,
                project_name,
                rollup_kind,
                time_bucket_start_unix_nano,
                &group_value,
                accumulator.finish(),
            )
            .await?;
        }
    }

    Ok(())
}

async fn insert_run_metric_rollup(
    tx: &tokio_postgres::Transaction<'_>,
    project_name: &str,
    rollup_kind: &str,
    time_bucket_start_unix_nano: i64,
    group_value: &str,
    stats: RollupStats,
) -> Result<()> {
    tx.execute(
        "INSERT INTO run_metric_rollups(
            project_name, bucket_size_unix_nanos, time_bucket_start_unix_nano,
            rollup_kind, group_value, run_count, error_count,
            latency_min_nanos, latency_max_nanos, latency_avg_nanos,
            latency_p50_nanos, latency_p95_nanos, latency_p99_nanos,
            prompt_tokens_sum, prompt_tokens_avg,
            completion_tokens_sum, completion_tokens_avg,
            total_tokens_sum, total_tokens_avg,
            prompt_cost_sum, prompt_cost_avg,
            completion_cost_sum, completion_cost_avg,
            total_cost_sum, total_cost_avg,
            evaluator_score_avg, updated_at
        )
        VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
            $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
            $21, $22, $23, $24, $25, $26, CURRENT_TIMESTAMP
        )",
        &[
            &project_name,
            &AGGREGATE_ROLLUP_BUCKET_UNIX_NANOS,
            &time_bucket_start_unix_nano,
            &rollup_kind,
            &group_value,
            &stats.run_count,
            &stats.error_count,
            &stats.latency_min_nanos,
            &stats.latency_max_nanos,
            &stats.latency_avg_nanos,
            &stats.latency_p50_nanos,
            &stats.latency_p95_nanos,
            &stats.latency_p99_nanos,
            &stats.prompt_tokens_sum,
            &stats.prompt_tokens_avg,
            &stats.completion_tokens_sum,
            &stats.completion_tokens_avg,
            &stats.total_tokens_sum,
            &stats.total_tokens_avg,
            &stats.prompt_cost_sum,
            &stats.prompt_cost_avg,
            &stats.completion_cost_sum,
            &stats.completion_cost_avg,
            &stats.total_cost_sum,
            &stats.total_cost_avg,
            &stats.evaluator_score_avg,
        ],
    )
    .await
    .with_context(|| format!("insert {rollup_kind} aggregate rollup"))?;
    Ok(())
}

#[derive(Debug, Clone)]
struct RollupRun {
    run_type: String,
    status: String,
    start_time_unix_nano: i64,
    latency_nanos: i64,
    prompt_tokens: Option<i64>,
    completion_tokens: Option<i64>,
    total_tokens: Option<i64>,
    prompt_cost: Option<f64>,
    completion_cost: Option<f64>,
    total_cost: Option<f64>,
    evaluator_score: Option<f64>,
    model_name: Option<String>,
}

#[derive(Debug, Default)]
struct RollupAccumulator {
    run_count: i64,
    error_count: i64,
    latency_nanos: Vec<i64>,
    prompt_tokens: Vec<i64>,
    completion_tokens: Vec<i64>,
    total_tokens: Vec<i64>,
    prompt_cost: Vec<f64>,
    completion_cost: Vec<f64>,
    total_cost: Vec<f64>,
    evaluator_score: Vec<f64>,
}

impl RollupAccumulator {
    fn push(&mut self, run: &RollupRun) {
        self.run_count += 1;
        if run.status == "error" {
            self.error_count += 1;
        }
        self.latency_nanos.push(run.latency_nanos);
        push_option(&mut self.prompt_tokens, run.prompt_tokens);
        push_option(&mut self.completion_tokens, run.completion_tokens);
        push_option(&mut self.total_tokens, run.total_tokens);
        push_option_f64(&mut self.prompt_cost, run.prompt_cost);
        push_option_f64(&mut self.completion_cost, run.completion_cost);
        push_option_f64(&mut self.total_cost, run.total_cost);
        push_option_f64(&mut self.evaluator_score, run.evaluator_score);
    }

    fn finish(self) -> RollupStats {
        let mut latency_nanos = self.latency_nanos;
        latency_nanos.sort_unstable();
        RollupStats {
            run_count: self.run_count,
            error_count: self.error_count,
            latency_min_nanos: latency_nanos.first().copied(),
            latency_max_nanos: latency_nanos.last().copied(),
            latency_avg_nanos: avg_i64(&latency_nanos),
            latency_p50_nanos: percentile_i64(&latency_nanos, 50),
            latency_p95_nanos: percentile_i64(&latency_nanos, 95),
            latency_p99_nanos: percentile_i64(&latency_nanos, 99),
            prompt_tokens_sum: sum_i64(&self.prompt_tokens),
            prompt_tokens_avg: avg_i64(&self.prompt_tokens),
            completion_tokens_sum: sum_i64(&self.completion_tokens),
            completion_tokens_avg: avg_i64(&self.completion_tokens),
            total_tokens_sum: sum_i64(&self.total_tokens),
            total_tokens_avg: avg_i64(&self.total_tokens),
            prompt_cost_sum: sum_f64(&self.prompt_cost),
            prompt_cost_avg: avg_f64(&self.prompt_cost),
            completion_cost_sum: sum_f64(&self.completion_cost),
            completion_cost_avg: avg_f64(&self.completion_cost),
            total_cost_sum: sum_f64(&self.total_cost),
            total_cost_avg: avg_f64(&self.total_cost),
            evaluator_score_avg: avg_f64(&self.evaluator_score),
        }
    }
}

#[derive(Debug, Clone)]
struct RollupStats {
    run_count: i64,
    error_count: i64,
    latency_min_nanos: Option<i64>,
    latency_max_nanos: Option<i64>,
    latency_avg_nanos: Option<f64>,
    latency_p50_nanos: Option<f64>,
    latency_p95_nanos: Option<f64>,
    latency_p99_nanos: Option<f64>,
    prompt_tokens_sum: Option<i64>,
    prompt_tokens_avg: Option<f64>,
    completion_tokens_sum: Option<i64>,
    completion_tokens_avg: Option<f64>,
    total_tokens_sum: Option<i64>,
    total_tokens_avg: Option<f64>,
    prompt_cost_sum: Option<f64>,
    prompt_cost_avg: Option<f64>,
    completion_cost_sum: Option<f64>,
    completion_cost_avg: Option<f64>,
    total_cost_sum: Option<f64>,
    total_cost_avg: Option<f64>,
    evaluator_score_avg: Option<f64>,
}

pub(super) fn rollup_time_bucket(start_time_unix_nano: i64) -> i64 {
    start_time_unix_nano.div_euclid(AGGREGATE_ROLLUP_BUCKET_UNIX_NANOS)
        * AGGREGATE_ROLLUP_BUCKET_UNIX_NANOS
}

fn push_option(values: &mut Vec<i64>, value: Option<i64>) {
    if let Some(value) = value {
        values.push(value);
    }
}

fn push_option_f64(values: &mut Vec<f64>, value: Option<f64>) {
    if let Some(value) = value.filter(|value| value.is_finite()) {
        values.push(value);
    }
}

fn sum_i64(values: &[i64]) -> Option<i64> {
    (!values.is_empty()).then(|| values.iter().sum())
}

fn avg_i64(values: &[i64]) -> Option<f64> {
    (!values.is_empty()).then(|| values.iter().sum::<i64>() as f64 / values.len() as f64)
}

fn sum_f64(values: &[f64]) -> Option<f64> {
    (!values.is_empty()).then(|| values.iter().sum())
}

fn avg_f64(values: &[f64]) -> Option<f64> {
    (!values.is_empty()).then(|| values.iter().sum::<f64>() / values.len() as f64)
}

fn percentile_i64(sorted_values: &[i64], percentile: usize) -> Option<f64> {
    if sorted_values.is_empty() {
        return None;
    }
    let index = ((sorted_values.len() - 1) * percentile).div_ceil(100);
    Some(sorted_values[index.min(sorted_values.len() - 1)] as f64)
}

fn parse_attributes(attributes_json: &str) -> Result<Value> {
    serde_json::from_str(attributes_json).context("parse attributes_json for scalar indexes")
}

fn extract_tags(root: &Value) -> Vec<String> {
    let mut tags = BTreeSet::new();
    for path in TAG_PATHS {
        if let Some(Value::Array(values)) = path_value(root, path) {
            for value in values {
                if let Some(tag) = bounded_string(value, MAX_TAG_BYTES) {
                    tags.insert(tag);
                    if tags.len() >= MAX_TAGS {
                        return tags.into_iter().collect();
                    }
                }
            }
        }
    }
    tags.into_iter().collect()
}

fn extract_metadata(root: &Value) -> Vec<(String, String)> {
    let mut metadata = BTreeSet::new();
    for path in METADATA_PATHS {
        if let Some(Value::Object(values)) = path_value(root, path) {
            for (key, value) in values {
                let Some(key) = bounded_raw_string(key, MAX_METADATA_KEY_BYTES) else {
                    continue;
                };
                let Some(value) = scalar_value_string(value, MAX_METADATA_VALUE_BYTES) else {
                    continue;
                };
                metadata.insert((key, value));
                if metadata.len() >= MAX_METADATA_PAIRS {
                    return metadata.into_iter().collect();
                }
            }
        }
    }
    metadata.into_iter().collect()
}

fn bounded_string(value: &Value, max_bytes: usize) -> Option<String> {
    match value {
        Value::String(value) => bounded_raw_string(value, max_bytes),
        Value::Number(_) | Value::Bool(_) => scalar_value_string(value, max_bytes),
        _ => None,
    }
}

fn scalar_value_string(value: &Value, max_bytes: usize) -> Option<String> {
    match value {
        Value::String(value) => bounded_raw_string(value, max_bytes),
        Value::Number(number) => bounded_raw_string(&number.to_string(), max_bytes),
        Value::Bool(value) => bounded_raw_string(&value.to_string(), max_bytes),
        _ => None,
    }
}

fn bounded_raw_string(value: &str, max_bytes: usize) -> Option<String> {
    (!value.is_empty() && value.len() <= max_bytes).then(|| value.to_owned())
}

fn path_value<'a>(root: &'a Value, path: &[&str]) -> Option<&'a Value> {
    let mut current = root;
    for key in path {
        current = current.get(*key)?;
    }
    Some(current)
}

const TAG_PATHS: &[&[&str]] = &[
    &["tags"],
    &["langsmith.tags"],
    &["langsmith.extra", "tags"],
    &["metadata", "tags"],
];

const METADATA_PATHS: &[&[&str]] = &[
    &["metadata"],
    &["langsmith.extra", "metadata"],
    &["extra", "metadata"],
];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::RunEventKind;

    #[test]
    fn extracts_bounded_scalar_indexes() {
        let record = SpanRecord {
            project_name: "demo".to_owned(),
            run_id: "run".to_owned(),
            trace_id: "trace".to_owned(),
            span_id: "span".to_owned(),
            parent_run_id: None,
            parent_span_id: None,
            name: "llm".to_owned(),
            run_type: "llm".to_owned(),
            start_time_unix_nano: 10,
            end_time_unix_nano: 42,
            status_code: 1,
            event_kind: RunEventKind::End,
            attributes_json: serde_json::json!({
                "tags": ["prod", "prod", "llm"],
                "metadata": {
                    "thread_id": "thread-1",
                    "temperature": 0.7,
                    "large": "x".repeat(MAX_METADATA_VALUE_BYTES + 1),
                    "object": {"skip": true}
                },
                "metrics": {
                    "prompt_tokens": 12,
                    "completion_tokens": 4,
                    "total_tokens": 16,
                    "total_cost": 0.002
                },
                "gen_ai.request.model": "gpt-test",
                "gen_ai.system": "openai"
            })
            .to_string(),
            idempotency_key: None,
        };

        let indexes = ScalarIndexes::from_record(
            &record,
            RootLocator {
                run_id: "run".to_owned(),
                span_id: "span".to_owned(),
            },
        )
        .expect("build scalar indexes");

        assert_eq!(indexes.latency_nanos, 32);
        assert_eq!(indexes.tags, vec!["llm", "prod"]);
        assert!(
            indexes
                .metadata
                .contains(&("thread_id".to_owned(), "thread-1".to_owned()))
        );
        assert!(
            indexes
                .metadata
                .contains(&("temperature".to_owned(), "0.7".to_owned()))
        );
        assert!(!indexes.metadata.iter().any(|(key, _)| key == "large"));
        assert_eq!(indexes.prompt_tokens, Some(12));
        assert_eq!(indexes.completion_tokens, Some(4));
        assert_eq!(indexes.total_tokens, Some(16));
        assert_eq!(indexes.total_cost, Some(0.002));
        assert_eq!(indexes.model_name.as_deref(), Some("gpt-test"));
        assert_eq!(indexes.provider_name.as_deref(), Some("openai"));
    }
}
