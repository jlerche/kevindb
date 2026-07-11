use std::collections::HashSet;

use axum::Json;
use axum::extract::{Path, RawQuery, State};
use kevindb::query::{
    ThreadListPage, ThreadListQuery, ThreadSummary, ThreadTracePage, ThreadTraceQuery,
    ThreadTraceSummary,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::{ApiError, ServerState};

use super::filter::parse_filter;
use super::{otel_trace_id_to_uuid, parse_time_nanos, query_error, unix_nano_to_rfc3339};

impl ServerState {
    async fn resolve_thread_project_name(
        &self,
        project_id: Option<&str>,
        project_name: Option<&str>,
    ) -> Result<String, ApiError> {
        if let Some(project_name) = project_name.filter(|name| !name.trim().is_empty()) {
            return Ok(project_name.to_owned());
        }
        let Some(project_id) = project_id.filter(|id| !id.trim().is_empty()) else {
            return Err(ApiError::bad_request(
                "project_id or project_name is required".to_owned(),
            ));
        };
        let mut projects = self
            .resolve_project_selectors(vec![project_id.to_owned()])
            .await?;
        projects
            .pop()
            .ok_or_else(|| ApiError::not_found("project not found".to_owned()))
    }
}

pub(crate) async fn query_threads(
    State(state): State<ServerState>,
    Json(request): Json<ThreadsQueryRequest>,
) -> Result<Json<ThreadsQueryResponse>, ApiError> {
    let project_name = state
        .resolve_thread_project_name(
            request.project_id.as_deref(),
            request.project_name.as_deref(),
        )
        .await?;
    let filter_value = request
        .filter
        .as_ref()
        .map(|filter| Value::String(filter.clone()));
    let query = ThreadListQuery {
        project_name,
        filter: parse_filter(filter_value.as_ref(), "filter")?,
        min_start_time_unix_nano: parse_time_nanos(request.min_start_time.as_deref())?,
        max_start_time_unix_nano: parse_time_nanos(request.max_start_time.as_deref())?,
        page_size: request.page_size.unwrap_or(20),
        cursor: request.cursor,
    };
    let page = state
        .query_engine()
        .list_threads(query)
        .await
        .map_err(query_error)?;

    Ok(Json(ThreadsQueryResponse::try_from_page(page)?))
}

pub(crate) async fn query_thread_traces(
    State(state): State<ServerState>,
    Path(thread_id): Path<String>,
    RawQuery(raw_query): RawQuery,
) -> Result<Json<ThreadTracesResponse>, ApiError> {
    let request = ThreadTracesQuery::from_raw_query(raw_query.as_deref())?;
    let project_name = state
        .resolve_thread_project_name(
            request.project_id.as_deref(),
            request.project_name.as_deref(),
        )
        .await?;
    let filter_value = request
        .filter
        .as_ref()
        .map(|filter| Value::String(filter.clone()));
    let query = ThreadTraceQuery {
        project_name,
        thread_id,
        filter: parse_filter(filter_value.as_ref(), "filter")?,
        page_size: request.page_size.unwrap_or(20),
        cursor: request.cursor,
    };
    let page = state
        .query_engine()
        .list_thread_traces(query)
        .await
        .map_err(query_error)?;
    let selects = ThreadTraceSelects::from_values(request.selects);

    Ok(Json(ThreadTracesResponse::try_from_page(page, &selects)?))
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct ThreadTracesQuery {
    pub project_id: Option<String>,
    pub project_name: Option<String>,
    pub cursor: Option<String>,
    pub filter: Option<String>,
    pub page_size: Option<usize>,
    #[serde(default)]
    pub selects: Vec<String>,
}

impl ThreadTracesQuery {
    fn from_raw_query(raw_query: Option<&str>) -> Result<Self, ApiError> {
        let mut query = Self {
            project_id: None,
            project_name: None,
            cursor: None,
            filter: None,
            page_size: None,
            selects: Vec::new(),
        };

        for (key, value) in url::form_urlencoded::parse(raw_query.unwrap_or("").as_bytes()) {
            match key.as_ref() {
                "project_id" => query.project_id = Some(value.into_owned()),
                "project_name" => query.project_name = Some(value.into_owned()),
                "cursor" => query.cursor = Some(value.into_owned()),
                "filter" => query.filter = Some(value.into_owned()),
                "page_size" => {
                    let raw = value.into_owned();
                    query.page_size = Some(raw.parse::<usize>().map_err(|error| {
                        ApiError::bad_request(format!("page_size must be an integer: {error}"))
                    })?);
                }
                "selects" | "select" => query.selects.push(value.into_owned()),
                _ => {}
            }
        }

        Ok(query)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct ThreadsQueryRequest {
    pub project_id: Option<String>,
    pub project_name: Option<String>,
    pub cursor: Option<String>,
    pub filter: Option<String>,
    pub min_start_time: Option<String>,
    pub max_start_time: Option<String>,
    pub page_size: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ThreadTracesResponse {
    pub items: Vec<ThreadTraceResponse>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

impl ThreadTracesResponse {
    fn try_from_page(
        page: ThreadTracePage,
        selects: &ThreadTraceSelects,
    ) -> Result<Self, ApiError> {
        Ok(Self {
            items: page
                .items
                .into_iter()
                .map(|trace| ThreadTraceResponse::from_summary(trace, selects))
                .collect::<Result<Vec<_>, _>>()?,
            next_cursor: page.next_cursor,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ThreadsQueryResponse {
    pub items: Vec<ThreadResponse>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

impl ThreadsQueryResponse {
    fn try_from_page(page: ThreadListPage) -> Result<Self, ApiError> {
        Ok(Self {
            items: page
                .items
                .into_iter()
                .map(ThreadResponse::try_from_summary)
                .collect::<Result<Vec<_>, _>>()?,
            next_cursor: page.next_cursor,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ThreadTraceResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completion_cost: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completion_cost_details: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completion_token_details: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completion_tokens: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end_time: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_preview: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub first_token_time: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub inputs_preview: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latency: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub op: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub outputs_preview: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prompt_cost: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prompt_cost_details: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prompt_token_details: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prompt_tokens: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_time: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_cost: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_tokens: Option<i64>,
    pub trace_id: String,
}

impl ThreadTraceResponse {
    fn from_summary(
        trace: ThreadTraceSummary,
        selects: &ThreadTraceSelects,
    ) -> Result<Self, ApiError> {
        Ok(Self {
            completion_cost: selects
                .include("COMPLETION_COST")
                .then_some(trace.completion_cost)
                .flatten(),
            completion_cost_details: selects
                .include("COMPLETION_COST_DETAILS")
                .then_some(json!({})),
            completion_token_details: selects
                .include("COMPLETION_TOKEN_DETAILS")
                .then_some(json!({})),
            completion_tokens: selects
                .include("COMPLETION_TOKENS")
                .then_some(trace.completion_tokens)
                .flatten(),
            end_time: selects
                .include("END_TIME")
                .then(|| unix_nano_to_optional_rfc3339(trace.end_time_unix_nano))
                .flatten(),
            error_preview: selects
                .include("ERROR_PREVIEW")
                .then_some(trace.error_preview)
                .flatten(),
            first_token_time: selects
                .include("FIRST_TOKEN_TIME")
                .then(|| trace.first_token_time_unix_nano.map(unix_nano_to_rfc3339))
                .flatten(),
            inputs_preview: selects
                .include("INPUTS_PREVIEW")
                .then_some(trace.inputs_preview)
                .flatten(),
            latency: selects
                .include("LATENCY")
                .then_some(nanos_to_seconds(trace.latency_nanos)),
            name: selects.include("NAME").then_some(trace.name).flatten(),
            op: None,
            outputs_preview: selects
                .include("OUTPUTS_PREVIEW")
                .then_some(trace.outputs_preview)
                .flatten(),
            prompt_cost: selects
                .include("PROMPT_COST")
                .then_some(trace.prompt_cost)
                .flatten(),
            prompt_cost_details: selects.include("PROMPT_COST_DETAILS").then_some(json!({})),
            prompt_token_details: selects.include("PROMPT_TOKEN_DETAILS").then_some(json!({})),
            prompt_tokens: selects
                .include("PROMPT_TOKENS")
                .then_some(trace.prompt_tokens)
                .flatten(),
            start_time: selects
                .include("START_TIME")
                .then_some(unix_nano_to_rfc3339(trace.start_time_unix_nano)),
            thread_id: selects.include("THREAD_ID").then_some(trace.thread_id),
            total_cost: selects
                .include("TOTAL_COST")
                .then_some(trace.total_cost)
                .flatten(),
            total_tokens: selects
                .include("TOTAL_TOKENS")
                .then_some(trace.total_tokens)
                .flatten(),
            trace_id: otel_trace_id_to_uuid(&trace.trace_id)?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ThreadResponse {
    pub thread_id: String,
    pub trace_id: Option<String>,
    pub first_trace_id: Option<String>,
    pub last_trace_id: Option<String>,
    pub count: i64,
    pub min_start_time: Option<String>,
    pub max_start_time: Option<String>,
    pub start_time: Option<String>,
    pub first_inputs: Option<String>,
    pub last_outputs: Option<String>,
    pub last_error: Option<String>,
    pub prompt_tokens: Option<i64>,
    pub completion_tokens: Option<i64>,
    pub total_tokens: Option<i64>,
    pub total_cost: Option<f64>,
    pub total_cost_details: Value,
    pub total_token_details: Value,
    pub feedback_stats: Value,
    pub latency_p50: Option<f64>,
    pub latency_p99: Option<f64>,
    pub num_errored_turns: i64,
}

impl ThreadResponse {
    fn try_from_summary(thread: ThreadSummary) -> Result<Self, ApiError> {
        Ok(Self {
            thread_id: thread.thread_id,
            trace_id: thread
                .trace_id
                .as_deref()
                .map(otel_trace_id_to_uuid)
                .transpose()?,
            first_trace_id: thread
                .first_trace_id
                .as_deref()
                .map(otel_trace_id_to_uuid)
                .transpose()?,
            last_trace_id: thread
                .last_trace_id
                .as_deref()
                .map(otel_trace_id_to_uuid)
                .transpose()?,
            count: thread.count,
            min_start_time: thread.min_start_time_unix_nano.map(unix_nano_to_rfc3339),
            max_start_time: thread.max_start_time_unix_nano.map(unix_nano_to_rfc3339),
            start_time: thread.min_start_time_unix_nano.map(unix_nano_to_rfc3339),
            first_inputs: thread.first_inputs,
            last_outputs: thread.last_outputs,
            last_error: thread.last_error,
            prompt_tokens: thread.prompt_tokens,
            completion_tokens: thread.completion_tokens,
            total_tokens: thread.total_tokens,
            total_cost: thread.total_cost,
            total_cost_details: json!({}),
            total_token_details: json!({}),
            feedback_stats: json!({}),
            latency_p50: thread.latency_p50,
            latency_p99: thread.latency_p99,
            num_errored_turns: thread.num_errored_turns,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ThreadTraceSelects {
    all: bool,
    fields: HashSet<String>,
}

impl ThreadTraceSelects {
    fn from_values(values: Vec<String>) -> Self {
        let fields = values
            .into_iter()
            .filter(|value| !value.trim().is_empty())
            .map(|value| value.trim().to_ascii_uppercase())
            .collect::<HashSet<_>>();
        Self {
            all: fields.is_empty(),
            fields,
        }
    }

    fn include(&self, field: &str) -> bool {
        self.all || self.fields.contains(field)
    }
}

fn nanos_to_seconds(nanos: i64) -> f64 {
    nanos.max(0) as f64 / 1_000_000_000.0
}

fn unix_nano_to_optional_rfc3339(nanos: i64) -> Option<String> {
    (nanos > 0).then(|| unix_nano_to_rfc3339(nanos))
}
