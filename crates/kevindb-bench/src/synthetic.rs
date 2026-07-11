use std::env;

use anyhow::{Context, Result};
use kevindb::{RunEventKind, SpanRecord};
use serde::Serialize;
use serde_json::json;

#[derive(Debug, Clone, Serialize)]
pub struct SyntheticConfig {
    pub project_name: String,
    pub trace_count: usize,
    pub runs_per_trace: usize,
    pub tree_depth: usize,
    pub fanout: usize,
    pub payload_bytes: usize,
    pub thread_count: usize,
    pub traces_per_thread: usize,
    pub feedback_density_per_mille: u16,
    pub error_rate_per_mille: u16,
    pub metric_density_per_mille: u16,
    pub ingest_batch_size: usize,
    pub max_spans_per_segment: usize,
    pub iterations: usize,
}

impl SyntheticConfig {
    pub fn from_env() -> Result<Self> {
        let thread_count = env_usize("KEVINDB_BENCH_THREAD_COUNT", 4)?;
        let traces_per_thread = env_usize("KEVINDB_BENCH_TRACES_PER_THREAD", 6)?;
        Ok(Self {
            project_name: env::var("KEVINDB_BENCH_PROJECT")
                .unwrap_or_else(|_| "bench-demo".to_owned()),
            trace_count: env_usize(
                "KEVINDB_BENCH_TRACE_COUNT",
                thread_count.saturating_mul(traces_per_thread),
            )?,
            runs_per_trace: env_usize("KEVINDB_BENCH_RUNS_PER_TRACE", 8)?,
            tree_depth: env_usize("KEVINDB_BENCH_TREE_DEPTH", 3)?,
            fanout: env_usize("KEVINDB_BENCH_FANOUT", 2)?.max(1),
            payload_bytes: env_usize("KEVINDB_BENCH_PAYLOAD_BYTES", 128)?,
            thread_count: thread_count.max(1),
            traces_per_thread: traces_per_thread.max(1),
            feedback_density_per_mille: env_per_mille(
                "KEVINDB_BENCH_FEEDBACK_DENSITY_PER_MILLE",
                250,
            )?,
            error_rate_per_mille: env_per_mille("KEVINDB_BENCH_ERROR_RATE_PER_MILLE", 100)?,
            metric_density_per_mille: env_per_mille("KEVINDB_BENCH_METRIC_DENSITY_PER_MILLE", 750)?,
            ingest_batch_size: env_usize("KEVINDB_BENCH_INGEST_BATCH_SIZE", 16)?.max(1),
            max_spans_per_segment: env_usize("KEVINDB_BENCH_MAX_SPANS_PER_SEGMENT", 16)?.max(1),
            iterations: env_usize("KEVINDB_BENCH_ITERATIONS", 5)?.max(1),
        })
    }
}

#[derive(Debug, Clone)]
pub struct SyntheticDataset {
    pub config: SyntheticConfig,
    pub records: Vec<SpanRecord>,
    pub selected_trace_id: String,
    pub selected_run_id: String,
}

pub fn generate_dataset(config: SyntheticConfig) -> SyntheticDataset {
    let mut records = Vec::with_capacity(config.trace_count * config.runs_per_trace);
    for trace_index in 0..config.trace_count {
        let trace_id = trace_id(trace_index);
        for run_index in 0..config.runs_per_trace {
            let current_span_id = span_id(trace_index, run_index);
            let parent_index = parent_index(run_index, config.fanout, config.tree_depth);
            let parent_span_id = parent_index.map(|index| span_id(trace_index, index));
            let parent_run_id = parent_index.map(|index| run_id(trace_index, index));
            let status_code = if per_mille_hit(
                trace_index * config.runs_per_trace + run_index,
                config.error_rate_per_mille,
            ) {
                2
            } else {
                1
            };
            let run_type = run_type(run_index, parent_index);
            let start_time = 1_700_000_000_000_000_000
                + (trace_index as i64 * 1_000_000_000)
                + (run_index as i64 * 10_000_000);
            records.push(SpanRecord {
                project_name: config.project_name.clone(),
                run_id: run_id(trace_index, run_index),
                trace_id: trace_id.clone(),
                span_id: current_span_id,
                parent_run_id,
                parent_span_id,
                name: format!("{run_type}.{}", run_index + 1),
                run_type,
                start_time_unix_nano: start_time,
                end_time_unix_nano: start_time + 5_000_000,
                status_code,
                event_kind: RunEventKind::End,
                attributes_json: attributes_json(&config, trace_index, run_index),
                idempotency_key: None,
            });
        }
    }

    let selected_trace = config.trace_count / 2;
    let selected_run = config.runs_per_trace.saturating_sub(1);
    SyntheticDataset {
        selected_trace_id: trace_id(selected_trace),
        selected_run_id: run_id(selected_trace, selected_run),
        config,
        records,
    }
}

pub fn feedback_selected(record_index: usize, density_per_mille: u16) -> bool {
    per_mille_hit(record_index, density_per_mille)
}

fn attributes_json(config: &SyntheticConfig, trace_index: usize, run_index: usize) -> String {
    let payload_len =
        config.payload_bytes + ((trace_index + run_index) % 4) * config.payload_bytes / 3;
    let thread_id = format!(
        "thread-{:04}",
        (trace_index / config.traces_per_thread).min(config.thread_count - 1)
    );
    let include_metrics = per_mille_hit(
        trace_index * config.runs_per_trace + run_index,
        config.metric_density_per_mille,
    );
    let metrics = include_metrics.then(|| {
        json!({
            "prompt_tokens": 100 + run_index,
            "completion_tokens": 40 + trace_index % 17,
            "total_tokens": 140 + run_index + trace_index % 17,
            "prompt_cost": ((run_index + 1) as f64) / 2000.0,
            "completion_cost": ((run_index + 1) as f64) / 2000.0,
            "total_cost": ((run_index + 1) as f64) / 1000.0
        })
    });

    json!({
        "metadata": {
            "thread_id": thread_id,
            "synthetic_trace_index": trace_index,
            "synthetic_run_index": run_index,
            "ls_model_name": format!("synthetic-model-{}", run_index % 3),
            "ls_provider": if run_index.is_multiple_of(2) { "openai" } else { "anthropic" }
        },
        "tags": [format!("depth-{}", run_depth(run_index, config.fanout)), "synthetic"],
        "payload": "x".repeat(payload_len),
        "metrics": metrics
    })
    .to_string()
}

fn parent_index(run_index: usize, fanout: usize, max_depth: usize) -> Option<usize> {
    if run_index == 0 {
        return None;
    }
    let parent = (run_index - 1) / fanout.max(1);
    if run_depth(run_index, fanout) > max_depth.max(1) {
        Some(parent.min(fanout))
    } else {
        Some(parent)
    }
}

fn run_depth(run_index: usize, fanout: usize) -> usize {
    let mut depth = 0;
    let mut index = run_index;
    while index > 0 {
        index = (index - 1) / fanout.max(1);
        depth += 1;
    }
    depth
}

fn run_type(run_index: usize, parent_index: Option<usize>) -> String {
    if parent_index.is_none() {
        "chain".to_owned()
    } else if run_index.is_multiple_of(3) {
        "tool".to_owned()
    } else {
        "llm".to_owned()
    }
}

fn trace_id(trace_index: usize) -> String {
    format!("{:032x}", trace_index + 1)
}

fn span_id(trace_index: usize, run_index: usize) -> String {
    format!(
        "{:016x}",
        ((trace_index as u64) << 32) | (run_index as u64 + 1)
    )
}

fn run_id(trace_index: usize, run_index: usize) -> String {
    format!(
        "00000000-0000-{:04x}-0000-{:012x}",
        trace_index,
        run_index + 1
    )
}

fn per_mille_hit(index: usize, per_mille: u16) -> bool {
    ((index * 37 + 11) % 1000) < per_mille as usize
}

fn env_usize(name: &str, default: usize) -> Result<usize> {
    env::var(name)
        .map(|value| {
            value
                .parse::<usize>()
                .with_context(|| format!("parse {name}={value}"))
        })
        .unwrap_or(Ok(default))
}

fn env_per_mille(name: &str, default: u16) -> Result<u16> {
    let value = env::var(name)
        .map(|value| {
            value
                .parse::<u16>()
                .with_context(|| format!("parse {name}={value}"))
        })
        .unwrap_or(Ok(default))?;
    Ok(value.min(1000))
}
