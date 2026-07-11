use chrono::{DateTime, Utc};
use serde_json::Value;

use crate::record::SpanRecord;

#[derive(Debug, Clone, Default, PartialEq)]
pub struct TypedRunMetrics {
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
}

impl TypedRunMetrics {
    pub fn from_record(record: &SpanRecord) -> Self {
        let root = serde_json::from_str(&record.attributes_json).unwrap_or(Value::Null);
        let prompt_tokens = scalar_i64(&root, PROMPT_TOKEN_PATHS);
        let completion_tokens = scalar_i64(&root, COMPLETION_TOKEN_PATHS);
        let total_tokens = scalar_i64(&root, TOTAL_TOKEN_PATHS)
            .or_else(|| sum_i64(prompt_tokens, completion_tokens));
        let prompt_cost = scalar_f64(&root, PROMPT_COST_PATHS);
        let completion_cost = scalar_f64(&root, COMPLETION_COST_PATHS);

        Self {
            latency_nanos: latency_nanos(record),
            prompt_tokens,
            completion_tokens,
            total_tokens,
            prompt_cost,
            completion_cost,
            total_cost: scalar_f64(&root, TOTAL_COST_PATHS)
                .or_else(|| sum_f64(prompt_cost, completion_cost)),
            first_token_latency_nanos: first_token_latency_nanos(&root, record),
            evaluator_score: scalar_f64(&root, EVALUATOR_SCORE_PATHS),
            model_name: scalar_string(&root, MODEL_PATHS).map(normalize_model_name),
            provider_name: scalar_string(&root, PROVIDER_PATHS).map(normalize_provider_name),
        }
    }
}

fn latency_nanos(record: &SpanRecord) -> i64 {
    record
        .end_time_unix_nano
        .checked_sub(record.start_time_unix_nano)
        .filter(|latency| *latency > 0)
        .unwrap_or(0)
}

fn first_token_latency_nanos(root: &Value, record: &SpanRecord) -> Option<i64> {
    scalar_i64(root, FIRST_TOKEN_LATENCY_NANOS_PATHS)
        .or_else(|| scalar_seconds_as_nanos(root, FIRST_TOKEN_LATENCY_SECONDS_PATHS))
        .or_else(|| {
            first_path_value(root, FIRST_TOKEN_TIME_PATHS)
                .and_then(timestamp_unix_nanos)
                .and_then(|first_token| first_token.checked_sub(record.start_time_unix_nano))
                .filter(|latency| *latency >= 0)
        })
}

fn scalar_string(root: &Value, paths: &[&[&str]]) -> Option<String> {
    paths
        .iter()
        .find_map(|path| path_value(root, path).and_then(|value| bounded_string(value, 256)))
}

fn scalar_i64(root: &Value, paths: &[&[&str]]) -> Option<i64> {
    paths.iter().find_map(|path| {
        path_value(root, path)
            .and_then(value_i64)
            .filter(|value| *value >= 0)
    })
}

fn scalar_f64(root: &Value, paths: &[&[&str]]) -> Option<f64> {
    paths.iter().find_map(|path| {
        path_value(root, path)
            .and_then(value_f64)
            .filter(|value| value.is_finite() && *value >= 0.0)
    })
}

fn scalar_seconds_as_nanos(root: &Value, paths: &[&[&str]]) -> Option<i64> {
    paths.iter().find_map(|path| {
        path_value(root, path)
            .and_then(value_f64)
            .filter(|value| value.is_finite() && *value >= 0.0)
            .and_then(|seconds| f64_to_i64(seconds * 1_000_000_000.0))
    })
}

fn timestamp_unix_nanos(value: &Value) -> Option<i64> {
    match value {
        Value::Number(number) => number.as_i64(),
        Value::String(value) => value
            .parse::<i64>()
            .ok()
            .or_else(|| parse_rfc3339_nanos(value)),
        _ => None,
    }
}

fn value_i64(value: &Value) -> Option<i64> {
    match value {
        Value::Number(number) => number
            .as_i64()
            .or_else(|| number.as_u64().and_then(|value| i64::try_from(value).ok())),
        Value::String(value) => value.trim().parse::<i64>().ok(),
        _ => None,
    }
}

fn value_f64(value: &Value) -> Option<f64> {
    match value {
        Value::Number(number) => number.as_f64(),
        Value::String(value) => value.trim().parse::<f64>().ok(),
        _ => None,
    }
}

fn bounded_string(value: &Value, max_bytes: usize) -> Option<String> {
    let value = match value {
        Value::String(value) => value.trim().to_owned(),
        Value::Number(_) | Value::Bool(_) => value.to_string(),
        _ => return None,
    };
    (!value.is_empty() && value.len() <= max_bytes).then_some(value)
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

fn sum_i64(left: Option<i64>, right: Option<i64>) -> Option<i64> {
    Some(left?.saturating_add(right?))
}

fn sum_f64(left: Option<f64>, right: Option<f64>) -> Option<f64> {
    let sum = left? + right?;
    sum.is_finite().then_some(sum)
}

fn f64_to_i64(value: f64) -> Option<i64> {
    (value >= 0.0 && value <= i64::MAX as f64).then_some(value.round() as i64)
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

fn normalize_model_name(value: String) -> String {
    value.trim().to_ascii_lowercase()
}

fn normalize_provider_name(value: String) -> String {
    match value.trim().to_ascii_lowercase().as_str() {
        "open_ai" | "open-ai" => "openai".to_owned(),
        "anthropic_ai" | "anthropic-ai" => "anthropic".to_owned(),
        value => value.to_owned(),
    }
}

const PROMPT_TOKEN_PATHS: &[&[&str]] = &[
    &["prompt_tokens"],
    &["input_tokens"],
    &["metrics", "prompt_tokens"],
    &["metrics", "input_tokens"],
    &["usage", "prompt_tokens"],
    &["usage", "input_tokens"],
    &["token_usage", "prompt_tokens"],
    &["llm_output", "token_usage", "prompt_tokens"],
    &["gen_ai.usage.input_tokens"],
    &[
        "langsmith.outputs",
        "llm_output",
        "token_usage",
        "prompt_tokens",
    ],
    &["langsmith.extra", "metadata", "prompt_tokens"],
];

const COMPLETION_TOKEN_PATHS: &[&[&str]] = &[
    &["completion_tokens"],
    &["output_tokens"],
    &["metrics", "completion_tokens"],
    &["metrics", "output_tokens"],
    &["usage", "completion_tokens"],
    &["usage", "output_tokens"],
    &["token_usage", "completion_tokens"],
    &["llm_output", "token_usage", "completion_tokens"],
    &["gen_ai.usage.output_tokens"],
    &[
        "langsmith.outputs",
        "llm_output",
        "token_usage",
        "completion_tokens",
    ],
    &["langsmith.extra", "metadata", "completion_tokens"],
];

const TOTAL_TOKEN_PATHS: &[&[&str]] = &[
    &["total_tokens"],
    &["metrics", "total_tokens"],
    &["usage", "total_tokens"],
    &["token_usage", "total_tokens"],
    &["llm_output", "token_usage", "total_tokens"],
    &["gen_ai.usage.total_tokens"],
    &[
        "langsmith.outputs",
        "llm_output",
        "token_usage",
        "total_tokens",
    ],
    &["langsmith.extra", "metadata", "total_tokens"],
];

const PROMPT_COST_PATHS: &[&[&str]] = &[
    &["prompt_cost"],
    &["input_cost"],
    &["metrics", "prompt_cost"],
    &["metrics", "input_cost"],
    &["usage", "prompt_cost"],
    &["usage", "input_cost"],
    &["langsmith.extra", "metadata", "prompt_cost"],
];

const COMPLETION_COST_PATHS: &[&[&str]] = &[
    &["completion_cost"],
    &["output_cost"],
    &["metrics", "completion_cost"],
    &["metrics", "output_cost"],
    &["usage", "completion_cost"],
    &["usage", "output_cost"],
    &["langsmith.extra", "metadata", "completion_cost"],
];

const TOTAL_COST_PATHS: &[&[&str]] = &[
    &["total_cost"],
    &["cost"],
    &["metrics", "total_cost"],
    &["metrics", "cost"],
    &["usage", "total_cost"],
    &["usage", "cost"],
    &["langsmith.extra", "metadata", "total_cost"],
];

const FIRST_TOKEN_LATENCY_NANOS_PATHS: &[&[&str]] = &[
    &["first_token_latency_nanos"],
    &["metrics", "first_token_latency_nanos"],
    &["langsmith.extra", "metadata", "first_token_latency_nanos"],
];

const FIRST_TOKEN_LATENCY_SECONDS_PATHS: &[&[&str]] = &[
    &["first_token_latency"],
    &["metrics", "first_token_latency"],
    &["langsmith.extra", "metadata", "first_token_latency"],
];

const FIRST_TOKEN_TIME_PATHS: &[&[&str]] = &[
    &["first_token_time_unix_nano"],
    &["first_token_time"],
    &["metrics", "first_token_time_unix_nano"],
    &["metrics", "first_token_time"],
    &["langsmith.extra", "metadata", "first_token_time_unix_nano"],
    &["langsmith.extra", "metadata", "first_token_time"],
];

const EVALUATOR_SCORE_PATHS: &[&[&str]] = &[
    &["evaluator_score"],
    &["score"],
    &["metrics", "evaluator_score"],
    &["metrics", "score"],
    &["feedback", "score"],
    &["langsmith.extra", "metadata", "evaluator_score"],
];

const MODEL_PATHS: &[&[&str]] = &[
    &["gen_ai.request.model"],
    &["gen_ai.response.model"],
    &["model"],
    &["model_name"],
    &["metadata", "model"],
    &["metadata", "model_name"],
    &["metadata", "ls_model_name"],
    &["invocation_params", "model"],
    &["invocation_params", "model_name"],
    &["langsmith.extra", "metadata", "model"],
    &["langsmith.extra", "metadata", "ls_model_name"],
    &["langsmith.extra", "invocation_params", "model"],
    &["langsmith.outputs", "llm_output", "model_name"],
];

const PROVIDER_PATHS: &[&[&str]] = &[
    &["gen_ai.system"],
    &["provider"],
    &["provider_name"],
    &["metadata", "provider"],
    &["metadata", "provider_name"],
    &["metadata", "ls_provider"],
    &["langsmith.extra", "metadata", "provider"],
    &["langsmith.extra", "metadata", "ls_provider"],
];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::RunEventKind;
    use serde_json::json;

    #[test]
    fn extracts_langsmith_and_provider_metrics() {
        let record = record(json!({
            "usage": {
                "input_tokens": "12",
                "output_tokens": 4
            },
            "metrics": {
                "prompt_cost": 0.001,
                "completion_cost": "0.002",
                "first_token_latency": 0.25
            },
            "score": "0.9",
            "metadata": {
                "ls_model_name": " GPT-Test ",
                "ls_provider": "Open-AI"
            }
        }));

        let metrics = TypedRunMetrics::from_record(&record);

        assert_eq!(metrics.latency_nanos, 900);
        assert_eq!(metrics.prompt_tokens, Some(12));
        assert_eq!(metrics.completion_tokens, Some(4));
        assert_eq!(metrics.total_tokens, Some(16));
        assert_eq!(metrics.prompt_cost, Some(0.001));
        assert_eq!(metrics.completion_cost, Some(0.002));
        assert_eq!(metrics.total_cost, Some(0.003));
        assert_eq!(metrics.first_token_latency_nanos, Some(250_000_000));
        assert_eq!(metrics.evaluator_score, Some(0.9));
        assert_eq!(metrics.model_name.as_deref(), Some("gpt-test"));
        assert_eq!(metrics.provider_name.as_deref(), Some("openai"));
    }

    #[test]
    fn ignores_missing_malformed_and_negative_metrics() {
        let record = record(json!({
            "prompt_tokens": "nope",
            "completion_tokens": -1,
            "total_cost": "NaN",
            "first_token_time_unix_nano": 99,
            "score": {"nested": true}
        }));

        let metrics = TypedRunMetrics::from_record(&record);

        assert_eq!(metrics.prompt_tokens, None);
        assert_eq!(metrics.completion_tokens, None);
        assert_eq!(metrics.total_tokens, None);
        assert_eq!(metrics.total_cost, None);
        assert_eq!(metrics.first_token_latency_nanos, None);
        assert_eq!(metrics.evaluator_score, None);
    }

    #[test]
    fn derives_first_token_latency_from_rfc3339_time() {
        let mut record = record(json!({
            "first_token_time": "1970-01-01T00:00:01.000000123Z"
        }));
        record.start_time_unix_nano = 1_000_000_000;

        let metrics = TypedRunMetrics::from_record(&record);

        assert_eq!(metrics.first_token_latency_nanos, Some(123));
    }

    fn record(attributes: Value) -> SpanRecord {
        SpanRecord {
            project_name: "demo".to_owned(),
            run_id: "run".to_owned(),
            trace_id: "trace".to_owned(),
            span_id: "span".to_owned(),
            parent_run_id: None,
            parent_span_id: None,
            name: "llm".to_owned(),
            run_type: "llm".to_owned(),
            start_time_unix_nano: 100,
            end_time_unix_nano: 1000,
            status_code: 1,
            event_kind: RunEventKind::End,
            attributes_json: attributes.to_string(),
            idempotency_key: None,
        }
    }
}
