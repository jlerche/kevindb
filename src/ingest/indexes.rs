use std::collections::BTreeSet;

use anyhow::{Context, Result};
use serde_json::Value;

use crate::otlp::SpanRecord;

const MAX_TAGS: usize = 32;
const MAX_TAG_BYTES: usize = 128;
const MAX_METADATA_PAIRS: usize = 32;
const MAX_METADATA_KEY_BYTES: usize = 128;
const MAX_METADATA_VALUE_BYTES: usize = 256;

#[derive(Debug, Clone, PartialEq)]
pub(super) struct ScalarIndexes {
    pub root_run_id: String,
    pub root_span_id: String,
    pub latency_nanos: i64,
    pub prompt_tokens: Option<i64>,
    pub completion_tokens: Option<i64>,
    pub total_tokens: Option<i64>,
    pub total_cost: Option<f64>,
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
    pub fn from_record(record: &SpanRecord, root: RootLocator) -> Self {
        let attributes = parse_attributes(&record.attributes_json);
        Self {
            root_run_id: root.run_id,
            root_span_id: root.span_id,
            latency_nanos: latency_nanos(record),
            prompt_tokens: scalar_i64(&attributes, PROMPT_TOKEN_PATHS),
            completion_tokens: scalar_i64(&attributes, COMPLETION_TOKEN_PATHS),
            total_tokens: scalar_i64(&attributes, TOTAL_TOKEN_PATHS),
            total_cost: scalar_f64(&attributes, TOTAL_COST_PATHS),
            model_name: scalar_string(&attributes, MODEL_PATHS),
            provider_name: scalar_string(&attributes, PROVIDER_PATHS),
            tags: extract_tags(&attributes),
            metadata: extract_metadata(&attributes),
        }
    }
}

pub(super) async fn root_locator_for_record(
    tx: &tokio_postgres::Transaction<'_>,
    record: &SpanRecord,
    generated_run_id: &str,
) -> Result<RootLocator> {
    let own_run_id = if record.run_id.is_empty() {
        generated_run_id.to_owned()
    } else {
        record.run_id.clone()
    };

    let Some(parent_span_id) = record.parent_span_id.as_deref() else {
        return Ok(RootLocator {
            run_id: own_run_id,
            span_id: record.span_id.clone(),
        });
    };

    let row = tx
        .query_opt(
            "SELECT root_run_id, root_span_id, run_id, generated_run_id, span_id
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
            let generated_run_id: String = row.get(3);
            let span_id: String = row.get(4);
            RootLocator {
                run_id: first_nonempty(&[&root_run_id, &run_id, &generated_run_id])
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

    refresh_project_filter_stats(tx, &record.project_name).await
}

pub(super) async fn refresh_project_filter_stats(
    tx: &tokio_postgres::Transaction<'_>,
    project_name: &str,
) -> Result<()> {
    let stats = [
        (
            "run_type",
            count_distinct_run_head(tx, project_name, "run_type").await?,
        ),
        ("tag", count_distinct_tag(tx, project_name).await?),
        (
            "metadata_key",
            count_distinct_metadata_key(tx, project_name).await?,
        ),
        (
            "model_name",
            count_distinct_run_head(tx, project_name, "model_name").await?,
        ),
        (
            "provider_name",
            count_distinct_run_head(tx, project_name, "provider_name").await?,
        ),
    ];

    for (stat_name, distinct_count) in stats {
        tx.execute(
            "INSERT INTO project_filter_stats(project_name, stat_name, distinct_count, updated_at)
            VALUES ($1, $2, $3, CURRENT_TIMESTAMP)
            ON CONFLICT (project_name, stat_name)
            DO UPDATE SET
                distinct_count = EXCLUDED.distinct_count,
                updated_at = CURRENT_TIMESTAMP",
            &[&project_name, &stat_name, &distinct_count],
        )
        .await
        .context("upsert project filter stat")?;
    }

    Ok(())
}

async fn count_distinct_run_head(
    tx: &tokio_postgres::Transaction<'_>,
    project_name: &str,
    column: &str,
) -> Result<i64> {
    let row = tx
        .query_one(
            format!("SELECT COUNT(DISTINCT {column}) FROM run_heads WHERE project_name = $1")
                .as_str(),
            &[&project_name],
        )
        .await
        .with_context(|| format!("count distinct {column}"))?;
    Ok(row.get(0))
}

async fn count_distinct_tag(
    tx: &tokio_postgres::Transaction<'_>,
    project_name: &str,
) -> Result<i64> {
    let row = tx
        .query_one(
            "SELECT COUNT(DISTINCT tag) FROM run_tags WHERE project_name = $1",
            &[&project_name],
        )
        .await
        .context("count distinct tags")?;
    Ok(row.get(0))
}

async fn count_distinct_metadata_key(
    tx: &tokio_postgres::Transaction<'_>,
    project_name: &str,
) -> Result<i64> {
    let row = tx
        .query_one(
            "SELECT COUNT(DISTINCT key) FROM run_metadata WHERE project_name = $1",
            &[&project_name],
        )
        .await
        .context("count distinct metadata keys")?;
    Ok(row.get(0))
}

fn parse_attributes(attributes_json: &str) -> Value {
    serde_json::from_str(attributes_json).unwrap_or(Value::Null)
}

fn latency_nanos(record: &SpanRecord) -> i64 {
    record
        .end_time_unix_nano
        .checked_sub(record.start_time_unix_nano)
        .filter(|latency| *latency > 0)
        .unwrap_or(0)
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

fn scalar_string(root: &Value, paths: &[&[&str]]) -> Option<String> {
    paths
        .iter()
        .find_map(|path| path_value(root, path).and_then(|value| bounded_string(value, 256)))
}

fn scalar_i64(root: &Value, paths: &[&[&str]]) -> Option<i64> {
    paths.iter().find_map(|path| {
        path_value(root, path).and_then(|value| match value {
            Value::Number(number) => number.as_i64(),
            Value::String(value) => value.parse::<i64>().ok(),
            _ => None,
        })
    })
}

fn scalar_f64(root: &Value, paths: &[&[&str]]) -> Option<f64> {
    paths.iter().find_map(|path| {
        path_value(root, path).and_then(|value| match value {
            Value::Number(number) => number.as_f64(),
            Value::String(value) => value.parse::<f64>().ok(),
            _ => None,
        })
    })
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

const PROMPT_TOKEN_PATHS: &[&[&str]] = &[
    &["prompt_tokens"],
    &["metrics", "prompt_tokens"],
    &["usage", "prompt_tokens"],
    &["langsmith.extra", "metadata", "prompt_tokens"],
];
const COMPLETION_TOKEN_PATHS: &[&[&str]] = &[
    &["completion_tokens"],
    &["metrics", "completion_tokens"],
    &["usage", "completion_tokens"],
    &["langsmith.extra", "metadata", "completion_tokens"],
];
const TOTAL_TOKEN_PATHS: &[&[&str]] = &[
    &["total_tokens"],
    &["metrics", "total_tokens"],
    &["usage", "total_tokens"],
    &["langsmith.extra", "metadata", "total_tokens"],
];
const TOTAL_COST_PATHS: &[&[&str]] = &[
    &["total_cost"],
    &["metrics", "total_cost"],
    &["usage", "total_cost"],
    &["langsmith.extra", "metadata", "total_cost"],
];
const MODEL_PATHS: &[&[&str]] = &[
    &["gen_ai.request.model"],
    &["model"],
    &["model_name"],
    &["metadata", "model"],
    &["metadata", "ls_model_name"],
    &["langsmith.extra", "metadata", "model"],
    &["langsmith.extra", "metadata", "ls_model_name"],
];
const PROVIDER_PATHS: &[&[&str]] = &[
    &["gen_ai.system"],
    &["provider"],
    &["provider_name"],
    &["metadata", "provider"],
    &["metadata", "ls_provider"],
    &["langsmith.extra", "metadata", "provider"],
    &["langsmith.extra", "metadata", "ls_provider"],
];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::otlp::RunEventKind;

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
        };

        let indexes = ScalarIndexes::from_record(
            &record,
            RootLocator {
                run_id: "run".to_owned(),
                span_id: "span".to_owned(),
            },
        );

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
