use std::sync::Arc;

use anyhow::Context;
use axum::Json;
use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use kevindb::ingest::{FlushReceipt, IngestConfig, IngestReceipt, Ingestor};
use kevindb::query::QueryEngine;
use kevindb_config::ServiceRole;
use object_store::ObjectStore;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use prost::Message;
use serde::{Deserialize, Serialize};
use tokio_postgres::NoTls;

pub mod cache;
mod langsmith;
mod routes;
mod routing;
pub use routes::app;

pub use langsmith::{
    FeedbackResponse, ProjectResponse, RunResponse, RunsQueryRequest, RunsResponse, StringList,
    ThreadResponse, ThreadTraceResponse, ThreadTracesResponse, ThreadsQueryRequest,
    ThreadsQueryResponse, TraceResponse,
};
use langsmith::{
    create_feedback, create_run, list_feedback, list_run_feedback, list_sessions, query_runs,
    query_thread_traces, query_threads, read_feedback, read_project_trace, read_run, update_run,
};
use routing::read_project_route;

#[derive(Clone)]
pub struct ServerState {
    postgres_url: String,
    object_store: Arc<dyn ObjectStore>,
    ingestor: Arc<Ingestor>,
    service_role: ServiceRole,
}

impl ServerState {
    pub fn new(
        postgres_url: impl Into<String>,
        object_store: Arc<dyn ObjectStore>,
        ingest_config: IngestConfig,
    ) -> Self {
        Self::new_with_node_id(postgres_url, object_store, ingest_config, None)
    }

    pub fn new_with_node_id(
        postgres_url: impl Into<String>,
        object_store: Arc<dyn ObjectStore>,
        ingest_config: IngestConfig,
        node_id: Option<String>,
    ) -> Self {
        Self::new_with_role(
            postgres_url,
            object_store,
            ingest_config,
            ServiceRole::All,
            node_id,
        )
    }

    pub fn new_with_role(
        postgres_url: impl Into<String>,
        object_store: Arc<dyn ObjectStore>,
        ingest_config: IngestConfig,
        service_role: ServiceRole,
        node_id: Option<String>,
    ) -> Self {
        let postgres_url = postgres_url.into();
        let ingestor = Arc::new(Ingestor::with_node_id(
            postgres_url.clone(),
            Arc::clone(&object_store),
            ingest_config,
            node_id,
        ));

        Self {
            postgres_url,
            object_store,
            ingestor,
            service_role,
        }
    }

    fn query_engine(&self) -> QueryEngine {
        QueryEngine::new(self.postgres_url.clone(), Arc::clone(&self.object_store))
    }

    pub async fn flush_pending_ingest(&self) -> anyhow::Result<Vec<FlushReceipt>> {
        self.ingestor.flush().await
    }

    fn service_role(&self) -> ServiceRole {
        self.service_role
    }

    async fn check_ready(&self) -> anyhow::Result<()> {
        let (client, connection) = tokio_postgres::connect(&self.postgres_url, NoTls)
            .await
            .context("connect postgres for readiness check")?;
        tokio::spawn(async move {
            if let Err(err) = connection.await {
                tracing::warn!(error = %err, "postgres readiness connection failed");
            }
        });

        client
            .simple_query("SELECT 1")
            .await
            .context("run readiness query")?;
        Ok(())
    }
}

async fn healthz(State(state): State<ServerState>) -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok".to_owned(),
        role: state.service_role().as_str().to_owned(),
    })
}

async fn readyz(State(state): State<ServerState>) -> Result<Json<HealthResponse>, ApiError> {
    state.check_ready().await?;
    Ok(Json(HealthResponse {
        status: "ok".to_owned(),
        role: state.service_role().as_str().to_owned(),
    }))
}

async fn ingest_trace(
    State(state): State<ServerState>,
    Path(project_name): Path<String>,
    body: Bytes,
) -> Result<Json<IngestResponse>, ApiError> {
    let request = ExportTraceServiceRequest::decode(body)
        .map_err(|error| ApiError::bad_request(format!("invalid OTLP protobuf: {error}")))?;
    let receipt = state.ingestor.ingest_otlp(project_name, request).await?;
    Ok(Json(IngestResponse::from(receipt)))
}

async fn list_trace_runs(
    State(state): State<ServerState>,
    Path((project_name, trace_id)): Path<(String, String)>,
) -> Result<Json<RunsResponse>, ApiError> {
    let runs = state
        .query_engine()
        .list_runs_in_trace(&project_name, &trace_id)
        .await
        .with_context(|| format!("list runs for trace {trace_id}"))?;
    Ok(Json(RunsResponse::new(
        runs.into_iter().map(RunResponse::from).collect(),
    )))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HealthResponse {
    pub status: String,
    pub role: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IngestResponse {
    pub accepted_spans: usize,
    pub flushed_segments: usize,
    pub flush: Option<FlushResponse>,
    pub flushes: Vec<FlushResponse>,
}

impl From<IngestReceipt> for IngestResponse {
    fn from(receipt: IngestReceipt) -> Self {
        Self {
            accepted_spans: receipt.accepted_spans,
            flushed_segments: receipt.flushed_segments,
            flush: receipt.flush.map(FlushResponse::from),
            flushes: receipt
                .flushes
                .into_iter()
                .map(FlushResponse::from)
                .collect(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlushResponse {
    pub segment_uri: String,
    pub span_count: usize,
    pub total_bytes: usize,
}

impl From<FlushReceipt> for FlushResponse {
    fn from(receipt: FlushReceipt) -> Self {
        Self {
            segment_uri: receipt.segment_uri,
            span_count: receipt.span_count,
            total_bytes: receipt.total_bytes,
        }
    }
}

#[derive(Debug)]
enum ApiError {
    BadRequest(String),
    NotFound(String),
    Internal(anyhow::Error),
}

impl ApiError {
    fn bad_request(message: String) -> Self {
        Self::BadRequest(message)
    }

    fn not_found(message: String) -> Self {
        Self::NotFound(message)
    }
}

impl From<anyhow::Error> for ApiError {
    fn from(error: anyhow::Error) -> Self {
        Self::Internal(error)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            Self::BadRequest(message) => (StatusCode::BAD_REQUEST, message),
            Self::NotFound(message) => (StatusCode::NOT_FOUND, message),
            Self::Internal(error) => {
                tracing::error!(error = %error, "request failed");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal server error".to_owned(),
                )
            }
        };

        (status, Json(ErrorResponse { error: message })).into_response()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ErrorResponse {
    error: String,
}

#[cfg(test)]
mod tests {
    use std::net::TcpListener;
    use std::process::Stdio;
    use std::time::{Duration as StdDuration, Instant};

    use anyhow::{Result, anyhow};
    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode};
    use object_store::memory::InMemory;
    use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
    use opentelemetry_proto::tonic::resource::v1::Resource;
    use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span, Status, status};
    use serde_json::{Value, json};
    use tokio::process::{Child, Command};
    use tokio::time::sleep;
    use tower::ServiceExt;
    use uuid::Uuid;

    use super::*;
    use kevindb::db::run_migrations;

    #[tokio::test]
    async fn serves_otlp_ingest_and_trace_run_query() -> Result<()> {
        let _ = tracing_subscriber::fmt().with_test_writer().try_init();
        let mockgres = Mockgres::start().await?;
        run_migrations(mockgres.postgres_url()).await?;

        let state = ServerState::new(
            mockgres.postgres_url().to_owned(),
            Arc::new(InMemory::new()),
            IngestConfig {
                max_spans_per_segment: 64,
                max_flush_delay: std::time::Duration::ZERO,
            },
        );
        let app = app(state);

        let health_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/healthz")
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(health_response.status(), StatusCode::OK);
        let health_body: HealthResponse = decode_response(health_response.into_body()).await?;
        assert_eq!(health_body.status, "ok");

        let ready_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/readyz")
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(ready_response.status(), StatusCode::OK);
        let ready_body: HealthResponse = decode_response(ready_response.into_body()).await?;
        assert_eq!(ready_body.status, "ok");

        let ingest_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/projects/demo/traces")
                    .body(Body::from(sample_export().encode_to_vec()))?,
            )
            .await?;
        assert_eq!(ingest_response.status(), StatusCode::OK);
        let ingest_body: IngestResponse = decode_response(ingest_response.into_body()).await?;
        assert_eq!(ingest_body.accepted_spans, 2);
        assert_eq!(ingest_body.flushed_segments, 1);
        assert_eq!(ingest_body.flushes.len(), 1);

        let sessions_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/sessions?name=demo&limit=1&include_stats=false")
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(sessions_response.status(), StatusCode::OK);
        let sessions_body: Vec<ProjectResponse> =
            decode_response(sessions_response.into_body()).await?;
        assert_eq!(sessions_body.len(), 1);
        assert_eq!(sessions_body[0].name, "demo");
        assert!(Uuid::parse_str(&sessions_body[0].tenant_id).is_ok());

        let runs_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/v1/projects/demo/traces/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/runs")
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(runs_response.status(), StatusCode::OK);
        let runs_body: RunsResponse = decode_response(runs_response.into_body()).await?;
        assert_eq!(
            runs_body
                .runs
                .iter()
                .map(|run| run.name.as_str())
                .collect::<Vec<_>>(),
            vec!["agent.run", "llm.call"]
        );
        assert!(Uuid::parse_str(&runs_body.runs[0].id).is_ok());
        assert_eq!(runs_body.runs[0].session_id, sessions_body[0].id);
        assert!(runs_body.runs[0].start_time.starts_with("1970-01-01T"));
        assert_eq!(
            runs_body.runs[1].parent_span_id.as_deref(),
            Some("1111111111111111")
        );
        assert!(runs_body.runs[1].parent_run_id.is_some());

        let trace_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/v1/projects/demo/traces/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(trace_response.status(), StatusCode::OK);
        let trace_body: TraceResponse = decode_response(trace_response.into_body()).await?;
        assert_eq!(trace_body.root_run_ids, vec![trace_body.runs[0].id.clone()]);
        assert_eq!(
            trace_body.runs[0].child_run_ids,
            vec![trace_body.runs[1].id.clone()]
        );
        let child_run_id = trace_body.runs[1].id.clone();

        let direct_run_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/runs/query")
                    .header("content-type", "application/json")
                    .body(json_body(json!({
                        "run_ids": [child_run_id.clone()],
                        "select": ["id", "name"]
                    })))?,
            )
            .await?;
        assert_eq!(direct_run_response.status(), StatusCode::OK);
        let direct_run_body: RunsResponse =
            decode_response(direct_run_response.into_body()).await?;
        assert_eq!(direct_run_body.runs.len(), 1);
        assert_eq!(direct_run_body.runs[0].id, child_run_id);
        assert_eq!(direct_run_body.runs[0].inputs, json!({}));

        let create_feedback_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/feedback")
                    .header("content-type", "application/json")
                    .body(json_body(json!({
                        "run_id": child_run_id.clone(),
                        "key": "quality",
                        "score": 1.0,
                        "value": "pass",
                        "comment": "looks good",
                        "extra": {"source": "unit-test"}
                    })))?,
            )
            .await?;
        assert_eq!(create_feedback_response.status(), StatusCode::ACCEPTED);

        let feedback_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(format!("/feedback?run={child_run_id}&key=quality"))
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(feedback_response.status(), StatusCode::OK);
        let feedback_body: Vec<FeedbackResponse> =
            decode_response(feedback_response.into_body()).await?;
        assert_eq!(feedback_body.len(), 1);
        assert_eq!(
            feedback_body[0].run_id.as_deref(),
            Some(child_run_id.as_str())
        );
        assert_eq!(feedback_body[0].key, "quality");
        assert_eq!(feedback_body[0].score, Some(json!(1.0)));
        assert_eq!(feedback_body[0].value, Some(json!("pass")));
        assert_eq!(feedback_body[0].comment.as_deref(), Some("looks good"));

        let indexed_feedback_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/feedback?project_name=demo&trace_id=aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa&score_min=1&score_max=1&value=pass&created_at_min=1970-01-01T00:00:00Z&created_at_max=2100-01-01T00:00:00Z")
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(indexed_feedback_response.status(), StatusCode::OK);
        let indexed_feedback_body: Vec<FeedbackResponse> =
            decode_response(indexed_feedback_response.into_body()).await?;
        assert_eq!(indexed_feedback_body.len(), 1);
        assert_eq!(indexed_feedback_body[0].id, feedback_body[0].id);

        let read_feedback_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(format!("/feedback/{}", feedback_body[0].id))
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(read_feedback_response.status(), StatusCode::OK);

        let run_feedback_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(format!("/runs/{child_run_id}/feedback"))
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(run_feedback_response.status(), StatusCode::OK);
        let run_feedback_body: Vec<FeedbackResponse> =
            decode_response(run_feedback_response.into_body()).await?;
        assert_eq!(run_feedback_body.len(), 1);

        let langsmith_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/runs/query")
                    .header("content-type", "application/json")
                    .body(json_body(json!({
                        "session": [sessions_body[0].id],
                        "run_type": "llm",
                        "limit": 1
                    })))?,
            )
            .await?;
        assert_eq!(langsmith_response.status(), StatusCode::OK);
        let langsmith_body: RunsResponse = decode_response(langsmith_response.into_body()).await?;
        assert_eq!(langsmith_body.runs.len(), 1);
        assert_eq!(langsmith_body.runs[0].name, "llm.call");
        assert_eq!(langsmith_body.runs[0].inputs, json!({}));

        let phase2_filter_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/runs/query")
                    .header("content-type", "application/json")
                    .body(json_body(json!({
                        "project_name": "demo",
                        "filter": "and(eq(model, \"gpt-test\"), eq(feedback_key, \"quality\"), eq(feedback_score, 1.0))",
                        "select": ["id", "name"],
                        "debug": true
                    })))?,
            )
            .await?;
        assert_eq!(phase2_filter_response.status(), StatusCode::OK);
        let phase2_filter_body: RunsResponse =
            decode_response(phase2_filter_response.into_body()).await?;
        assert_eq!(phase2_filter_body.runs.len(), 1);
        assert_eq!(phase2_filter_body.runs[0].name, "llm.call");
        assert_eq!(phase2_filter_body.runs[0].inputs, json!({}));
        let diagnostics = phase2_filter_body
            .diagnostics
            .expect("debug query should include diagnostics");
        assert_eq!(diagnostics.candidate_runs, 1);
        assert_eq!(diagnostics.estimated_object_store_requests, 48);
        assert!(diagnostics.actual_object_store_requests > 0);
        assert!(diagnostics.actual_object_store_bytes_read > 0);

        let search_filter_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/runs/query")
                    .header("content-type", "application/json")
                    .body(json_body(json!({
                        "project_name": "demo",
                        "filter": "search(\"payload\")"
                    })))?,
            )
            .await?;
        assert_eq!(search_filter_response.status(), StatusCode::OK);
        let search_filter_body: RunsResponse =
            decode_response(search_filter_response.into_body()).await?;
        assert!(search_filter_body.runs.is_empty());

        let filtered_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/runs/query")
                    .header("content-type", "application/json")
                    .body(json_body(json!({
                        "project_name": "demo",
                        "parent_span_id": "1111111111111111",
                        "start_time_gte": "1970-01-01T00:00:00.000000002Z"
                    })))?,
            )
            .await?;
        assert_eq!(filtered_response.status(), StatusCode::OK);
        let filtered_body: RunsResponse = decode_response(filtered_response.into_body()).await?;
        assert_eq!(
            filtered_body
                .runs
                .iter()
                .map(|run| run.name.as_str())
                .collect::<Vec<_>>(),
            vec!["llm.call"]
        );

        let first_page_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/runs/query")
                    .header("content-type", "application/json")
                    .body(json_body(json!({
                        "project_name": "demo",
                        "limit": 1
                    })))?,
            )
            .await?;
        assert_eq!(first_page_response.status(), StatusCode::OK);
        let first_page: RunsResponse = decode_response(first_page_response.into_body()).await?;
        assert_eq!(first_page.runs[0].name, "llm.call");
        assert_eq!(first_page.cursors.next.as_deref(), Some("1"));

        let second_page_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/runs/query")
                    .header("content-type", "application/json")
                    .body(json_body(json!({
                        "project_name": "demo",
                        "limit": 1,
                        "cursor": first_page.cursors.next
                    })))?,
            )
            .await?;
        assert_eq!(second_page_response.status(), StatusCode::OK);
        let second_page: RunsResponse = decode_response(second_page_response.into_body()).await?;
        assert_eq!(second_page.runs[0].name, "agent.run");
        assert_eq!(second_page.cursors.next, None);

        let root_response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/runs/query")
                    .header("content-type", "application/json")
                    .body(json_body(json!({
                        "project_name": "demo",
                        "is_root": true
                    })))?,
            )
            .await?;
        assert_eq!(root_response.status(), StatusCode::OK);
        let root_body: RunsResponse = decode_response(root_response.into_body()).await?;
        assert_eq!(
            root_body
                .runs
                .iter()
                .map(|run| run.name.as_str())
                .collect::<Vec<_>>(),
            vec!["agent.run"]
        );

        mockgres.stop().await?;
        Ok(())
    }

    #[tokio::test]
    async fn rejects_invalid_otlp_payloads() -> Result<()> {
        let state = ServerState::new(
            "postgresql://127.0.0.1:1/postgres",
            Arc::new(InMemory::new()),
            IngestConfig {
                max_spans_per_segment: 64,
                max_flush_delay: std::time::Duration::ZERO,
            },
        );

        let response = app(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/projects/demo/traces")
                    .body(Body::from("not protobuf"))?,
            )
            .await?;

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body: ErrorResponse = decode_response(response.into_body()).await?;
        assert!(body.error.contains("invalid OTLP protobuf"));
        Ok(())
    }

    #[tokio::test]
    async fn serves_langsmith_thread_endpoints() -> Result<()> {
        let mockgres = Mockgres::start().await?;
        run_migrations(mockgres.postgres_url()).await?;

        let state = ServerState::new(
            mockgres.postgres_url().to_owned(),
            Arc::new(InMemory::new()),
            IngestConfig {
                max_spans_per_segment: 64,
                max_flush_delay: std::time::Duration::ZERO,
            },
        );
        let app = app(state);

        for (run_id, trace_id, start, tokens, input, output) in [
            (
                "11111111-1111-4111-8111-111111111111",
                "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
                "2026-01-01T00:00:00Z",
                42,
                "hello one",
                "answer one",
            ),
            (
                "22222222-2222-4222-8222-222222222222",
                "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
                "2026-01-01T00:01:00Z",
                58,
                "hello two",
                "answer two",
            ),
        ] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/runs")
                        .header("content-type", "application/json")
                        .body(json_body(json!({
                            "id": run_id,
                            "name": "chat",
                            "run_type": "chain",
                            "project_name": "demo",
                            "trace_id": trace_id,
                            "start_time": start,
                            "end_time": "2026-01-01T00:02:00Z",
                            "inputs": {
                                "messages": [{"role": "user", "content": input}]
                            },
                            "outputs": {
                                "choices": [{"message": {"role": "assistant", "content": output}}]
                            },
                            "extra": {
                                "metadata": {
                                    "thread_id": "thread-http",
                                    "total_tokens": tokens,
                                    "total_cost": 0.01
                                }
                            }
                        })))?,
                )
                .await?;
            assert_eq!(response.status(), StatusCode::ACCEPTED);
        }

        let sessions_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/sessions?name=demo")
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(sessions_response.status(), StatusCode::OK);
        let sessions: Vec<ProjectResponse> = decode_response(sessions_response.into_body()).await?;
        let project_id = sessions[0].id.clone();

        let threads_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v2/threads/query")
                    .header("content-type", "application/json")
                    .body(json_body(json!({
                        "project_id": project_id,
                        "page_size": 20
                    })))?,
            )
            .await?;
        assert_eq!(threads_response.status(), StatusCode::OK);
        let threads: ThreadsQueryResponse = decode_response(threads_response.into_body()).await?;
        assert_eq!(threads.items.len(), 1);
        assert_eq!(threads.items[0].thread_id, "thread-http");
        assert_eq!(threads.items[0].count, 2);
        assert_eq!(threads.items[0].total_tokens, Some(100));
        assert_eq!(threads.items[0].first_inputs.as_deref(), Some("hello one"));
        assert_eq!(threads.items[0].last_outputs.as_deref(), Some("answer two"));

        let first_traces_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(format!(
                        "/v2/threads/thread-http/traces?project_id={project_id}&page_size=1&selects=TRACE_ID&selects=THREAD_ID&selects=INPUTS_PREVIEW&selects=TOTAL_TOKENS"
                    ))
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(first_traces_response.status(), StatusCode::OK);
        let first_traces: ThreadTracesResponse =
            decode_response(first_traces_response.into_body()).await?;
        assert_eq!(first_traces.items.len(), 1);
        assert_eq!(
            first_traces.items[0].trace_id,
            "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
        );
        assert_eq!(
            first_traces.items[0].thread_id.as_deref(),
            Some("thread-http")
        );
        assert_eq!(
            first_traces.items[0].inputs_preview.as_deref(),
            Some("hello one")
        );
        assert_eq!(first_traces.items[0].total_tokens, Some(42));
        assert_eq!(first_traces.items[0].outputs_preview, None);
        let next_cursor = first_traces.next_cursor.expect("cursor");

        let second_traces_response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(format!(
                        "/v2/threads/thread-http/traces?project_id={project_id}&page_size=1&cursor={next_cursor}"
                    ))
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(second_traces_response.status(), StatusCode::OK);
        let second_traces: ThreadTracesResponse =
            decode_response(second_traces_response.into_body()).await?;
        assert_eq!(
            second_traces.items[0].trace_id,
            "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
        );
        assert_eq!(
            second_traces.items[0].outputs_preview.as_deref(),
            Some("answer two")
        );

        mockgres.stop().await?;
        Ok(())
    }

    async fn decode_response<T>(body: Body) -> Result<T>
    where
        T: for<'de> Deserialize<'de>,
    {
        let bytes = to_bytes(body, usize::MAX).await?;
        Ok(serde_json::from_slice(&bytes)?)
    }

    fn json_body(value: Value) -> Body {
        Body::from(serde_json::to_vec(&value).expect("serialize json request body"))
    }

    fn sample_export() -> ExportTraceServiceRequest {
        ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: Some(Resource {
                    attributes: vec![string_attr("service.name", "agent-api")],
                    dropped_attributes_count: 0,
                    entity_refs: vec![],
                }),
                scope_spans: vec![ScopeSpans {
                    scope: None,
                    spans: vec![
                        Span {
                            trace_id: repeated_bytes(0xAA, 16),
                            span_id: repeated_bytes(0x11, 8),
                            parent_span_id: vec![],
                            name: "agent.run".to_owned(),
                            start_time_unix_nano: 1,
                            end_time_unix_nano: 10,
                            status: Some(Status {
                                message: String::new(),
                                code: status::StatusCode::Ok as i32,
                            }),
                            ..Default::default()
                        },
                        Span {
                            trace_id: repeated_bytes(0xAA, 16),
                            span_id: repeated_bytes(0x22, 8),
                            parent_span_id: repeated_bytes(0x11, 8),
                            name: "llm.call".to_owned(),
                            attributes: vec![string_attr("gen_ai.request.model", "gpt-test")],
                            start_time_unix_nano: 2,
                            end_time_unix_nano: 9,
                            status: Some(Status {
                                message: String::new(),
                                code: status::StatusCode::Ok as i32,
                            }),
                            ..Default::default()
                        },
                    ],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        }
    }

    fn string_attr(key: &str, value: &str) -> KeyValue {
        KeyValue {
            key: key.to_owned(),
            key_strindex: 0,
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(value.to_owned())),
            }),
        }
    }

    fn repeated_bytes(byte: u8, len: usize) -> Vec<u8> {
        vec![byte; len]
    }

    struct Mockgres {
        child: Child,
        postgres_url: String,
    }

    impl Mockgres {
        async fn start() -> Result<Self> {
            let port = portpicker::pick_unused_port()
                .ok_or_else(|| anyhow!("could not reserve mockgres port"))?;
            let postgres_url = format!("postgresql://127.0.0.1:{port}/postgres");
            let child = Command::new("mockgres")
                .arg("--host")
                .arg("127.0.0.1")
                .arg("--port")
                .arg(port.to_string())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()?;
            let mockgres = Self {
                child,
                postgres_url,
            };
            mockgres.wait_until_ready().await?;
            Ok(mockgres)
        }

        fn postgres_url(&self) -> &str {
            &self.postgres_url
        }

        async fn stop(mut self) -> Result<()> {
            self.child.start_kill()?;
            let _ = self.child.wait().await?;
            Ok(())
        }

        async fn wait_until_ready(&self) -> Result<()> {
            let deadline = Instant::now() + StdDuration::from_secs(5);
            loop {
                match tokio_postgres::connect(&self.postgres_url, NoTls).await {
                    Ok((client, connection)) => {
                        tokio::spawn(async move {
                            let _ = connection.await;
                        });
                        if client.simple_query("SELECT 1").await.is_ok() {
                            return Ok(());
                        }
                    }
                    Err(_) if Instant::now() >= deadline => {
                        return Err(anyhow!(
                            "mockgres did not become ready on {}",
                            self.postgres_url
                        ));
                    }
                    Err(_) => {}
                }

                sleep(std::time::Duration::from_millis(50)).await;
            }
        }
    }

    impl Drop for Mockgres {
        fn drop(&mut self) {
            let _ = self.child.start_kill();
        }
    }

    #[test]
    fn reserve_port_smoke_test() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
        assert!(listener.local_addr().expect("local addr").port() > 0);
    }
}
