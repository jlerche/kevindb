use std::collections::BTreeMap;
use std::time::Duration;

use axum::Json;
use axum::extract::State;
use kevindb::query::{
    FeedbackScoreStats, NumericStats, RunAggregateGroup, RunAggregateMetrics, RunAggregateQuery,
    RunAggregateResult, RunAggregateRow, RunAggregateSource, RunQueryLimits,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{ApiError, ServerState};

use super::filter::{parse_filter, parse_tree_filter};
use super::{
    ESTIMATED_OBJECT_STORE_REQUESTS_PER_VORTEX_FILE, MAX_RUN_QUERY_CANDIDATE_SEGMENTS,
    RunQueryDiagnosticsResponse, StringList, parse_time_nanos, query_error,
};

const DEFAULT_AGGREGATE_BUCKET_NANOS: i64 = 60 * 60 * 1_000_000_000;

pub(crate) async fn query_run_aggregates(
    State(state): State<ServerState>,
    Json(request): Json<RunAggregateRequest>,
) -> Result<Json<RunAggregateResponse>, ApiError> {
    let query = request.to_query(&state).await?;
    let debug = request.debug.unwrap_or(false);
    let result = state
        .query_engine()
        .aggregate_runs(query)
        .await
        .map_err(query_error)?;
    crate::metrics::record_aggregate_query(&result.diagnostics);
    Ok(Json(RunAggregateResponse::from_result(result, debug)))
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct RunAggregateRequest {
    #[serde(default, alias = "project_names", alias = "session_name")]
    project_name: Option<StringList>,
    #[serde(default)]
    session: Option<StringList>,
    #[serde(default)]
    group_by: Option<StringList>,
    #[serde(default)]
    time_bucket_nanos: Option<i64>,
    #[serde(default)]
    run_type: Option<String>,
    #[serde(default)]
    error: Option<bool>,
    #[serde(default)]
    start_time: Option<String>,
    #[serde(default)]
    end_time: Option<String>,
    #[serde(default, alias = "start_time_after")]
    start_time_gte: Option<String>,
    #[serde(default, alias = "start_time_before")]
    start_time_lte: Option<String>,
    #[serde(default, alias = "start_time_min")]
    start_time_min: Option<String>,
    #[serde(default, alias = "start_time_max")]
    start_time_max: Option<String>,
    #[serde(default)]
    filter: Option<Value>,
    #[serde(default)]
    trace_filter: Option<Value>,
    #[serde(default)]
    tree_filter: Option<Value>,
    #[serde(default, alias = "feedback_key")]
    feedback_keys: Option<StringList>,
    #[serde(default)]
    debug: Option<bool>,
}

impl RunAggregateRequest {
    async fn to_query(&self, state: &ServerState) -> Result<RunAggregateQuery, ApiError> {
        let mut project_names = self
            .project_name
            .clone()
            .map(StringList::into_vec)
            .unwrap_or_default();
        if let Some(session) = self.session.clone() {
            project_names.extend(state.resolve_project_selectors(session.into_vec()).await?);
        }
        project_names = super::dedupe(project_names);
        if project_names.is_empty() {
            return Err(ApiError::bad_request(
                "project_name or session is required".to_owned(),
            ));
        }

        let group_by = parse_group_by(self.group_by.clone())?;
        let time_bucket_nanos = group_by.contains(&RunAggregateGroup::TimeBucket).then_some(
            self.time_bucket_nanos
                .unwrap_or(DEFAULT_AGGREGATE_BUCKET_NANOS),
        );

        Ok(RunAggregateQuery {
            project_names,
            start_time_min_unix_nano: parse_time_nanos(
                self.start_time_min
                    .as_deref()
                    .or(self.start_time_gte.as_deref())
                    .or(self.start_time.as_deref()),
            )?,
            start_time_max_unix_nano: parse_time_nanos(
                self.start_time_max
                    .as_deref()
                    .or(self.start_time_lte.as_deref())
                    .or(self.end_time.as_deref()),
            )?,
            run_type: self.run_type.clone(),
            error: self.error,
            filter: parse_filter(self.filter.as_ref(), "filter")?,
            trace_filter: parse_filter(self.trace_filter.as_ref(), "trace_filter")?,
            tree_filter: parse_tree_filter(self.tree_filter.as_ref())?,
            group_by,
            time_bucket_nanos,
            feedback_keys: self
                .feedback_keys
                .clone()
                .map(StringList::into_vec)
                .unwrap_or_default(),
            include_deleted: false,
            limits: RunQueryLimits {
                max_candidate_segments: Some(MAX_RUN_QUERY_CANDIDATE_SEGMENTS),
                max_candidate_runs: Some(100_000),
                max_estimated_object_store_requests: Some(
                    MAX_RUN_QUERY_CANDIDATE_SEGMENTS
                        * ESTIMATED_OBJECT_STORE_REQUESTS_PER_VORTEX_FILE,
                ),
                max_candidate_bytes: Some(256 * 1024 * 1024),
                max_wall_time: Some(Duration::from_secs(30)),
            },
        })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunAggregateResponse {
    pub source: String,
    pub rows: Vec<RunAggregateRowResponse>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub diagnostics: Option<RunQueryDiagnosticsResponse>,
}

impl RunAggregateResponse {
    fn from_result(result: RunAggregateResult, include_diagnostics: bool) -> Self {
        Self {
            source: source_name(result.source).to_owned(),
            rows: result
                .rows
                .into_iter()
                .map(RunAggregateRowResponse::from)
                .collect(),
            diagnostics: include_diagnostics
                .then_some(RunQueryDiagnosticsResponse::from(result.diagnostics)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunAggregateRowResponse {
    pub group: BTreeMap<String, String>,
    pub metrics: RunAggregateMetricsResponse,
}

impl From<RunAggregateRow> for RunAggregateRowResponse {
    fn from(row: RunAggregateRow) -> Self {
        Self {
            group: row.group,
            metrics: RunAggregateMetricsResponse::from(row.metrics),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunAggregateMetricsResponse {
    pub count: u64,
    pub error_count: u64,
    pub error_rate: f64,
    pub latency_nanos: Option<NumericStatsResponse>,
    pub prompt_tokens: Option<NumericStatsResponse>,
    pub completion_tokens: Option<NumericStatsResponse>,
    pub total_tokens: Option<NumericStatsResponse>,
    pub prompt_cost: Option<NumericStatsResponse>,
    pub completion_cost: Option<NumericStatsResponse>,
    pub total_cost: Option<NumericStatsResponse>,
    pub first_token_latency_nanos: Option<NumericStatsResponse>,
    pub evaluator_score: Option<NumericStatsResponse>,
    pub feedback_scores: BTreeMap<String, FeedbackScoreStatsResponse>,
}

impl From<RunAggregateMetrics> for RunAggregateMetricsResponse {
    fn from(metrics: RunAggregateMetrics) -> Self {
        Self {
            count: metrics.count,
            error_count: metrics.error_count,
            error_rate: metrics.error_rate,
            latency_nanos: metrics.latency_nanos.map(NumericStatsResponse::from),
            prompt_tokens: metrics.prompt_tokens.map(NumericStatsResponse::from),
            completion_tokens: metrics.completion_tokens.map(NumericStatsResponse::from),
            total_tokens: metrics.total_tokens.map(NumericStatsResponse::from),
            prompt_cost: metrics.prompt_cost.map(NumericStatsResponse::from),
            completion_cost: metrics.completion_cost.map(NumericStatsResponse::from),
            total_cost: metrics.total_cost.map(NumericStatsResponse::from),
            first_token_latency_nanos: metrics
                .first_token_latency_nanos
                .map(NumericStatsResponse::from),
            evaluator_score: metrics.evaluator_score.map(NumericStatsResponse::from),
            feedback_scores: metrics
                .feedback_scores
                .into_iter()
                .map(|(key, stats)| (key, FeedbackScoreStatsResponse::from(stats)))
                .collect(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NumericStatsResponse {
    pub count: u64,
    pub sum: Option<f64>,
    pub avg: Option<f64>,
    pub min: Option<f64>,
    pub max: Option<f64>,
    pub p50: Option<f64>,
    pub p95: Option<f64>,
    pub p99: Option<f64>,
}

impl From<NumericStats> for NumericStatsResponse {
    fn from(stats: NumericStats) -> Self {
        Self {
            count: stats.count,
            sum: stats.sum,
            avg: stats.avg,
            min: stats.min,
            max: stats.max,
            p50: stats.p50,
            p95: stats.p95,
            p99: stats.p99,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FeedbackScoreStatsResponse {
    pub count: u64,
    pub avg: Option<f64>,
    pub min: Option<f64>,
    pub max: Option<f64>,
    pub p50: Option<f64>,
    pub p95: Option<f64>,
    pub p99: Option<f64>,
    pub distribution: BTreeMap<String, u64>,
}

impl From<FeedbackScoreStats> for FeedbackScoreStatsResponse {
    fn from(stats: FeedbackScoreStats) -> Self {
        Self {
            count: stats.count,
            avg: stats.avg,
            min: stats.min,
            max: stats.max,
            p50: stats.p50,
            p95: stats.p95,
            p99: stats.p99,
            distribution: stats.distribution,
        }
    }
}

fn parse_group_by(group_by: Option<StringList>) -> Result<Vec<RunAggregateGroup>, ApiError> {
    let values = group_by
        .map(StringList::into_vec)
        .unwrap_or_else(|| vec!["project".to_owned(), "time_bucket".to_owned()]);
    let mut groups = Vec::new();
    for value in values {
        let group = match value.to_ascii_lowercase().as_str() {
            "project" | "project_name" | "session" | "session_name" => RunAggregateGroup::Project,
            "time" | "time_bucket" | "time_bucket_start_unix_nano" => RunAggregateGroup::TimeBucket,
            "run_type" => RunAggregateGroup::RunType,
            "tag" | "tags" => RunAggregateGroup::Tag,
            "model" | "model_name" | "ls_model_name" => RunAggregateGroup::Model,
            "provider" | "provider_name" | "ls_provider" => RunAggregateGroup::Provider,
            "error" | "errored" => RunAggregateGroup::Error,
            "feedback_key" => RunAggregateGroup::FeedbackKey,
            _ => {
                return Err(ApiError::bad_request(format!(
                    "unsupported aggregate group_by field: {value}"
                )));
            }
        };
        if !groups.contains(&group) {
            groups.push(group);
        }
    }
    Ok(groups)
}

fn source_name(source: RunAggregateSource) -> &'static str {
    match source {
        RunAggregateSource::Rollup => "rollup",
        RunAggregateSource::Vortex => "vortex",
        RunAggregateSource::FeedbackRollup => "feedback_rollup",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_aggregate_group_by_aliases() {
        let groups = parse_group_by(Some(StringList::Many(vec![
            "project_name".to_owned(),
            "time_bucket".to_owned(),
            "model".to_owned(),
            "feedback_key".to_owned(),
        ])))
        .expect("parse groups");

        assert_eq!(
            groups,
            vec![
                RunAggregateGroup::Project,
                RunAggregateGroup::TimeBucket,
                RunAggregateGroup::Model,
                RunAggregateGroup::FeedbackKey
            ]
        );
    }
}
