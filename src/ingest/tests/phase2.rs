use super::*;
use crate::query::filter::FilterExpr;

use serde_json::json;

fn phase2_records() -> Vec<SpanRecord> {
    let mut root = sample_record("1111111111111111", 100);
    root.run_id = "root-run".to_owned();
    root.name = "agent.root".to_owned();
    root.attributes_json = json!({
        "tags": ["prod", "agent"],
        "metadata": {
            "thread_id": "thread-a",
            "large": "x".repeat(300),
            "nested": {"skip": true}
        }
    })
    .to_string();

    let mut child = sample_record("2222222222222222", 120);
    child.run_id = "child-run".to_owned();
    child.parent_run_id = Some("root-run".to_owned());
    child.parent_span_id = Some("1111111111111111".to_owned());
    child.name = "llm.call".to_owned();
    child.run_type = "llm".to_owned();
    child.end_time_unix_nano = 220;
    child.attributes_json = json!({
        "tags": ["prod", "llm"],
        "metadata": {
            "thread_id": "thread-a",
            "temperature": 0.7,
            "large": "x".repeat(300)
        },
        "metrics": {
            "prompt_tokens": 12,
            "completion_tokens": 5,
            "total_tokens": 17,
            "total_cost": 0.003
        },
        "gen_ai.request.model": "gpt-test",
        "gen_ai.system": "openai"
    })
    .to_string();

    vec![root, child]
}

#[tokio::test]
async fn scalar_indexes_are_materialized_and_bounded() {
    let mockgres = Mockgres::start().await.expect("start mockgres");
    run_migrations(mockgres.postgres_url())
        .await
        .expect("run migrations");

    let ingestor = Ingestor::new(
        mockgres.postgres_url().to_owned(),
        Arc::new(InMemory::new()),
        IngestConfig {
            max_spans_per_segment: 64,
            max_flush_delay: Duration::ZERO,
        },
    );
    ingestor
        .ingest_records(phase2_records())
        .await
        .expect("ingest scalar-indexed records");

    let (client, connection) = tokio_postgres::connect(mockgres.postgres_url(), NoTls)
        .await
        .expect("connect postgres");
    tokio::spawn(async move {
        let _ = connection.await;
    });

    let row = client
        .query_one(
            "SELECT root_run_id, root_span_id, latency_nanos, prompt_tokens,
                completion_tokens, total_tokens, total_cost, model_name, provider_name
            FROM run_heads
            WHERE span_id = '2222222222222222'",
            &[],
        )
        .await
        .expect("load scalar run head");
    assert_eq!(row.get::<_, String>(0), "root-run");
    assert_eq!(row.get::<_, String>(1), "1111111111111111");
    assert_eq!(row.get::<_, i64>(2), 100);
    assert_eq!(row.get::<_, Option<i64>>(3), Some(12));
    assert_eq!(row.get::<_, Option<i64>>(4), Some(5));
    assert_eq!(row.get::<_, Option<i64>>(5), Some(17));
    assert_eq!(row.get::<_, Option<f64>>(6), Some(0.003));
    assert_eq!(row.get::<_, Option<String>>(7).as_deref(), Some("gpt-test"));
    assert_eq!(row.get::<_, Option<String>>(8).as_deref(), Some("openai"));

    let tags = client
        .query(
            "SELECT tag FROM run_tags
            WHERE project_name = 'demo' AND span_id = '2222222222222222'
            ORDER BY tag",
            &[],
        )
        .await
        .expect("load tags")
        .into_iter()
        .map(|row| row.get::<_, String>(0))
        .collect::<Vec<_>>();
    assert_eq!(tags, vec!["llm", "prod"]);

    let metadata = client
        .query(
            "SELECT key, value FROM run_metadata
            WHERE project_name = 'demo' AND span_id = '2222222222222222'
            ORDER BY key, value",
            &[],
        )
        .await
        .expect("load metadata")
        .into_iter()
        .map(|row| (row.get::<_, String>(0), row.get::<_, String>(1)))
        .collect::<Vec<_>>();
    assert!(metadata.contains(&("thread_id".to_owned(), "thread-a".to_owned())));
    assert!(metadata.contains(&("temperature".to_owned(), "0.7".to_owned())));
    assert!(!metadata.iter().any(|(key, _)| key == "large"));

    let stat_count: i64 = client
        .query_one(
            "SELECT count(*) FROM project_filter_stats WHERE project_name = 'demo'",
            &[],
        )
        .await
        .expect("count filter stats")
        .get(0);
    assert!(stat_count >= 5);

    mockgres.stop().await.expect("stop mockgres");
}

#[tokio::test]
async fn planner_rejects_queries_that_exceed_fanout_limits() {
    let mockgres = Mockgres::start().await.expect("start mockgres");
    run_migrations(mockgres.postgres_url())
        .await
        .expect("run migrations");

    let object_store = Arc::new(InMemory::new());
    let ingestor = Ingestor::new(
        mockgres.postgres_url().to_owned(),
        object_store.clone(),
        IngestConfig {
            max_spans_per_segment: 64,
            max_flush_delay: Duration::ZERO,
        },
    );
    ingestor
        .ingest_records(phase2_records())
        .await
        .expect("ingest scalar-indexed records");

    let mut segment_limited = RunQuery::new("demo");
    segment_limited.limits.max_candidate_segments = Some(0);
    let err = QueryEngine::new(mockgres.postgres_url().to_owned(), object_store.clone())
        .list_runs_with_diagnostics(segment_limited)
        .await
        .expect_err("candidate segment limit should reject");
    assert!(
        err.to_string()
            .contains("candidate segments 1 exceed limit 0")
    );

    let mut request_limited = RunQuery::new("demo");
    request_limited.limits.max_estimated_object_store_requests = Some(0);
    let err = QueryEngine::new(mockgres.postgres_url().to_owned(), object_store.clone())
        .list_runs_with_diagnostics(request_limited)
        .await
        .expect_err("object-store request limit should reject");
    assert!(
        err.to_string()
            .contains("estimated object-store requests 48 exceed limit 0")
    );

    let mut bytes_limited = RunQuery::new("demo");
    bytes_limited.limits.max_candidate_bytes = Some(1);
    let err = QueryEngine::new(mockgres.postgres_url().to_owned(), object_store)
        .list_runs_with_diagnostics(bytes_limited)
        .await
        .expect_err("candidate byte limit should reject");
    assert!(err.to_string().contains("candidate bytes"));
    assert!(err.to_string().contains("exceed limit 1"));

    mockgres.stop().await.expect("stop mockgres");
}

#[tokio::test]
async fn filters_use_scalar_indexes_feedback_and_projection() {
    let mockgres = Mockgres::start().await.expect("start mockgres");
    run_migrations(mockgres.postgres_url())
        .await
        .expect("run migrations");

    let object_store = Arc::new(InMemory::new());
    let ingestor = Ingestor::new(
        mockgres.postgres_url().to_owned(),
        object_store.clone(),
        IngestConfig {
            max_spans_per_segment: 64,
            max_flush_delay: Duration::ZERO,
        },
    );
    ingestor
        .ingest_records(phase2_records())
        .await
        .expect("ingest scalar-indexed records");

    let (client, connection) = tokio_postgres::connect(mockgres.postgres_url(), NoTls)
        .await
        .expect("connect postgres");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .execute(
            "INSERT INTO feedback(
                id, run_id, trace_id, project_name, key,
                score_json, value_json, score_number, value_text,
                created_at_unix_nano, modified_at_unix_nano
            )
            VALUES (
                'feedback-a', 'child-run', 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa',
                'demo', 'correctness', '0.9', '\"pass\"', 0.9, 'pass', 300, 300
            )",
            &[],
        )
        .await
        .expect("insert feedback scalar index");

    let mut query = RunQuery::new("demo");
    query.filter = Some(
        FilterExpr::parse(
            r#"and(has(tags, "llm"), eq(metadata_key, "temperature"), eq(metadata_value, "0.7"), eq(model, "gpt-test"), gte(total_tokens, 17), eq(feedback_key, "correctness"), gte(feedback_score, 0.9), eq(feedback_value, "pass"))"#,
        )
        .expect("parse phase2 filter"),
    );
    query.include_payload = false;
    query.limits.max_candidate_segments = Some(1);
    query.limits.max_estimated_object_store_requests = Some(48);

    let result = QueryEngine::new(mockgres.postgres_url().to_owned(), object_store)
        .list_runs_with_diagnostics(query)
        .await
        .expect("run phase2 filtered query");

    assert_eq!(result.runs.len(), 1);
    assert_eq!(result.runs[0].span_id, "2222222222222222");
    assert_eq!(result.runs[0].attributes_json, "{}");
    assert_eq!(result.diagnostics.candidate_runs, 1);
    assert_eq!(result.diagnostics.candidate_segments, 1);
    assert_eq!(result.diagnostics.estimated_object_store_requests, 48);
    assert!(result.diagnostics.candidate_bytes > 0);

    mockgres.stop().await.expect("stop mockgres");
}
