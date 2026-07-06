use axum::Router;
use axum::routing::{get, post};
use kevindb_config::ServiceRole;

use crate::langsmith::aggregates::query_run_aggregates;
use crate::metrics::metrics_snapshot;
use crate::{
    ServerState, create_feedback, create_run, healthz, ingest_trace, list_feedback,
    list_run_feedback, list_sessions, list_trace_runs, query_runs, query_thread_traces,
    query_threads, read_feedback, read_project_route, read_project_trace, read_run, readyz,
    update_feedback, update_run,
};

pub fn app(state: ServerState) -> Router {
    let role = state.service_role();
    let router = Router::new()
        .route("/healthz", get(healthz))
        .route("/metrics", get(metrics_snapshot))
        .route("/readyz", get(readyz));
    let router = match role {
        ServiceRole::All => all_routes(router),
        ServiceRole::Ingest => ingest_routes(router),
        ServiceRole::Query => query_routes(router),
        ServiceRole::Compaction => router,
        ServiceRole::Coordinator => coordination_routes(router),
    };
    router.with_state(state)
}

fn all_routes(router: Router<ServerState>) -> Router<ServerState> {
    router
        .route("/sessions", get(list_sessions))
        .route("/v1/sessions", get(list_sessions))
        .route("/runs", post(create_run))
        .route("/v1/runs", post(create_run))
        .route("/runs/{run_id}", get(read_run).patch(update_run))
        .route("/v1/runs/{run_id}", get(read_run).patch(update_run))
        .route("/runs/{run_id}/feedback", get(list_run_feedback))
        .route("/v1/runs/{run_id}/feedback", get(list_run_feedback))
        .route("/runs/query", post(query_runs))
        .route("/v1/runs/query", post(query_runs))
        .route("/runs/aggregate", post(query_run_aggregates))
        .route("/v1/runs/aggregate", post(query_run_aggregates))
        .route("/v2/threads/query", post(query_threads))
        .route("/v2/threads/{thread_id}/traces", get(query_thread_traces))
        .route("/feedback", get(list_feedback).post(create_feedback))
        .route("/v1/feedback", get(list_feedback).post(create_feedback))
        .route(
            "/feedback/{feedback_id}",
            get(read_feedback).patch(update_feedback),
        )
        .route(
            "/v1/feedback/{feedback_id}",
            get(read_feedback).patch(update_feedback),
        )
        .route("/v1/projects/{project_name}/traces", post(ingest_trace))
        .route(
            "/v1/projects/{project_name}/traces/{trace_id}",
            get(read_project_trace),
        )
        .route(
            "/v1/projects/{project_name}/traces/{trace_id}/runs",
            get(list_trace_runs),
        )
        .route("/v1/projects/{project_name}/route", get(read_project_route))
}

fn ingest_routes(router: Router<ServerState>) -> Router<ServerState> {
    router
        .route("/runs", post(create_run))
        .route("/v1/runs", post(create_run))
        .route("/runs/{run_id}", axum::routing::patch(update_run))
        .route("/v1/runs/{run_id}", axum::routing::patch(update_run))
        .route("/feedback", post(create_feedback))
        .route("/v1/feedback", post(create_feedback))
        .route(
            "/feedback/{feedback_id}",
            axum::routing::patch(update_feedback),
        )
        .route(
            "/v1/feedback/{feedback_id}",
            axum::routing::patch(update_feedback),
        )
        .route("/v1/projects/{project_name}/traces", post(ingest_trace))
}

fn query_routes(router: Router<ServerState>) -> Router<ServerState> {
    router
        .route("/sessions", get(list_sessions))
        .route("/v1/sessions", get(list_sessions))
        .route("/runs/{run_id}", get(read_run))
        .route("/v1/runs/{run_id}", get(read_run))
        .route("/runs/{run_id}/feedback", get(list_run_feedback))
        .route("/v1/runs/{run_id}/feedback", get(list_run_feedback))
        .route("/runs/query", post(query_runs))
        .route("/v1/runs/query", post(query_runs))
        .route("/runs/aggregate", post(query_run_aggregates))
        .route("/v1/runs/aggregate", post(query_run_aggregates))
        .route("/v2/threads/query", post(query_threads))
        .route("/v2/threads/{thread_id}/traces", get(query_thread_traces))
        .route("/feedback", get(list_feedback))
        .route("/v1/feedback", get(list_feedback))
        .route("/feedback/{feedback_id}", get(read_feedback))
        .route("/v1/feedback/{feedback_id}", get(read_feedback))
        .route(
            "/v1/projects/{project_name}/traces/{trace_id}",
            get(read_project_trace),
        )
        .route(
            "/v1/projects/{project_name}/traces/{trace_id}/runs",
            get(list_trace_runs),
        )
}

fn coordination_routes(router: Router<ServerState>) -> Router<ServerState> {
    router.route("/v1/projects/{project_name}/route", get(read_project_route))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode};
    use kevindb::ingest::IngestConfig;
    use object_store::memory::InMemory;
    use serde_json::Value;
    use tower::ServiceExt;

    use super::*;

    #[tokio::test]
    async fn service_roles_expose_scoped_routes_and_health_role() {
        let query_app = app(state(ServiceRole::Query));
        let health = query_app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/healthz")
                    .body(Body::empty())
                    .expect("health request"),
            )
            .await
            .expect("health response");
        assert_eq!(health.status(), StatusCode::OK);
        let body = to_bytes(health.into_body(), 1024)
            .await
            .expect("health body");
        let body: Value = serde_json::from_slice(&body).expect("health json");
        assert_eq!(body["role"], "query");

        let ingest_response = query_app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/projects/demo/traces")
                    .body(Body::empty())
                    .expect("ingest request"),
            )
            .await
            .expect("query app ingest response");
        assert_eq!(ingest_response.status(), StatusCode::NOT_FOUND);

        let ingest_app = app(state(ServiceRole::Ingest));
        let query_response = ingest_app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/runs/query")
                    .body(Body::empty())
                    .expect("query request"),
            )
            .await
            .expect("ingest app query response");
        assert_eq!(query_response.status(), StatusCode::METHOD_NOT_ALLOWED);
    }

    fn state(role: ServiceRole) -> ServerState {
        ServerState::new_with_role(
            "postgresql://127.0.0.1:1/postgres",
            Arc::new(InMemory::new()),
            IngestConfig {
                max_spans_per_segment: 64,
                max_flush_delay: Duration::ZERO,
            },
            role,
            None,
        )
    }

    #[test]
    fn openapi_snapshot_documents_langsmith_compat_routes() {
        let spec: Value = serde_json::from_str(include_str!(
            "../../../docs/openapi/langsmith-compat.openapi.json"
        ))
        .expect("openapi snapshot json");
        assert_eq!(spec["openapi"], "3.1.0");
        let paths = spec["paths"].as_object().expect("openapi paths");
        for (path, method) in [
            ("/healthz", "get"),
            ("/metrics", "get"),
            ("/readyz", "get"),
            ("/sessions", "get"),
            ("/v1/sessions", "get"),
            ("/runs", "post"),
            ("/v1/runs", "post"),
            ("/runs/{run_id}", "get"),
            ("/runs/{run_id}", "patch"),
            ("/v1/runs/{run_id}", "get"),
            ("/v1/runs/{run_id}", "patch"),
            ("/runs/{run_id}/feedback", "get"),
            ("/v1/runs/{run_id}/feedback", "get"),
            ("/runs/query", "post"),
            ("/v1/runs/query", "post"),
            ("/runs/aggregate", "post"),
            ("/v1/runs/aggregate", "post"),
            ("/feedback", "get"),
            ("/feedback", "post"),
            ("/v1/feedback", "get"),
            ("/v1/feedback", "post"),
            ("/feedback/{feedback_id}", "get"),
            ("/feedback/{feedback_id}", "patch"),
            ("/v1/feedback/{feedback_id}", "get"),
            ("/v1/feedback/{feedback_id}", "patch"),
            ("/v1/projects/{project_name}/traces", "post"),
            ("/v1/projects/{project_name}/traces/{trace_id}", "get"),
            ("/v1/projects/{project_name}/traces/{trace_id}/runs", "get"),
            ("/v1/projects/{project_name}/route", "get"),
            ("/v2/threads/query", "post"),
            ("/v2/threads/{thread_id}/traces", "get"),
        ] {
            assert!(
                openapi_path_item(paths, path)
                    .and_then(|path| path.get(method))
                    .is_some(),
                "missing {method} {path} from OpenAPI snapshot"
            );
        }
    }

    fn openapi_path_item<'a>(
        paths: &'a serde_json::Map<String, Value>,
        path: &str,
    ) -> Option<&'a Value> {
        let item = paths.get(path)?;
        let Some(reference) = item.get("$ref").and_then(Value::as_str) else {
            return Some(item);
        };
        let path = reference
            .strip_prefix("#/paths/")
            .map(|path| path.replace("~1", "/").replace("~0", "~"))?;
        paths.get(&path)
    }
}
