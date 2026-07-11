use super::*;
use crate::query::{RunAggregateGroup, RunAggregateQuery, RunAggregateSource};
use kevindb_metastore_postgres::{FeedbackRecord, PostgresMetastore};

use serde_json::json;

const HOUR_NANOS: i64 = 60 * 60 * 1_000_000_000;

#[tokio::test]
async fn concurrent_workers_serialize_project_rollup_refreshes() {
    let mockgres = Mockgres::start().await.expect("start mockgres");
    run_migrations(mockgres.postgres_url())
        .await
        .expect("run migrations");
    let (client, connection) = tokio_postgres::connect(mockgres.postgres_url(), NoTls)
        .await
        .expect("connect postgres");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .execute(
            "INSERT INTO projects(name, id) VALUES ('demo', 'demo-id')",
            &[],
        )
        .await
        .expect("seed concurrent ingest project");
    let first = Ingestor::new(
        mockgres.postgres_url().to_owned(),
        Arc::new(InMemory::new()),
        IngestConfig {
            max_spans_per_segment: 1,
            max_flush_delay: Duration::ZERO,
        },
    );
    let second = Ingestor::new(
        mockgres.postgres_url().to_owned(),
        Arc::new(InMemory::new()),
        IngestConfig {
            max_spans_per_segment: 1,
            max_flush_delay: Duration::ZERO,
        },
    );

    let (first_result, second_result) = tokio::join!(
        first.ingest_records(vec![sample_record("1111111111111111", 10)]),
        second.ingest_records(vec![sample_record("2222222222222222", 20)]),
    );
    first_result.expect("first concurrent ingest");
    second_result.expect("second concurrent ingest");

    let run_count: i64 = client
        .query_one(
            "SELECT run_count
            FROM run_metric_rollups
            WHERE project_name = 'demo' AND rollup_kind = 'project'",
            &[],
        )
        .await
        .expect("load serialized project rollup")
        .get(0);
    assert_eq!(run_count, 2);

    mockgres.stop().await.expect("stop mockgres");
}

#[tokio::test]
async fn aggregates_use_rollups_and_typed_vortex_columns() {
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
        .ingest_records(vec![
            metric_record(MetricRecordArgs {
                span_id: "1111111111111111",
                run_type: "llm",
                start_time_unix_nano: HOUR_NANOS + 10,
                end_time_unix_nano: HOUR_NANOS + 110,
                status_code: 1,
                prompt_tokens: 10,
                completion_tokens: 5,
                prompt_cost: 0.01,
                completion_cost: 0.02,
                model: "GPT-Test",
                provider: "Open-AI",
                evaluator_score: 0.9,
            }),
            metric_record(MetricRecordArgs {
                span_id: "2222222222222222",
                run_type: "llm",
                start_time_unix_nano: HOUR_NANOS + 20,
                end_time_unix_nano: HOUR_NANOS + 220,
                status_code: 2,
                prompt_tokens: 20,
                completion_tokens: 10,
                prompt_cost: 0.02,
                completion_cost: 0.03,
                model: "gpt-test",
                provider: "openai",
                evaluator_score: 0.1,
            }),
            metric_record(MetricRecordArgs {
                span_id: "3333333333333333",
                run_type: "tool",
                start_time_unix_nano: HOUR_NANOS + 30,
                end_time_unix_nano: HOUR_NANOS + 80,
                status_code: 1,
                prompt_tokens: 1,
                completion_tokens: 2,
                prompt_cost: 0.001,
                completion_cost: 0.002,
                model: "tool-model",
                provider: "local",
                evaluator_score: 1.0,
            }),
        ])
        .await
        .expect("ingest metric records");

    let (client, connection) = tokio_postgres::connect(mockgres.postgres_url(), NoTls)
        .await
        .expect("connect postgres");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    let row = client
        .query_one(
            "SELECT prompt_cost, completion_cost, total_cost,
                first_token_latency_nanos, evaluator_score, model_name, provider_name
            FROM run_heads
            WHERE span_id = '1111111111111111'",
            &[],
        )
        .await
        .expect("load typed metric run head");
    assert_eq!(row.get::<_, Option<f64>>(0), Some(0.01));
    assert_eq!(row.get::<_, Option<f64>>(1), Some(0.02));
    assert_close(row.get::<_, Option<f64>>(2).expect("total cost"), 0.03);
    assert_eq!(row.get::<_, Option<i64>>(3), Some(7));
    assert_eq!(row.get::<_, Option<f64>>(4), Some(0.9));
    assert_eq!(row.get::<_, Option<String>>(5).as_deref(), Some("gpt-test"));
    assert_eq!(row.get::<_, Option<String>>(6).as_deref(), Some("openai"));

    let query_engine = QueryEngine::new(mockgres.postgres_url().to_owned(), object_store);
    let rollup = query_engine
        .aggregate_runs(RunAggregateQuery {
            project_names: vec!["demo".to_owned()],
            group_by: vec![
                RunAggregateGroup::Project,
                RunAggregateGroup::TimeBucket,
                RunAggregateGroup::RunType,
            ],
            time_bucket_nanos: Some(HOUR_NANOS),
            ..RunAggregateQuery::new("demo")
        })
        .await
        .expect("aggregate from rollups");
    assert_eq!(rollup.source, RunAggregateSource::Rollup);
    assert_eq!(rollup.diagnostics.actual_object_store_requests, 0);
    let llm_rollup = rollup
        .rows
        .iter()
        .find(|row| {
            row.group
                .get("run_type")
                .is_some_and(|value| value == "llm")
        })
        .expect("llm rollup row");
    assert_eq!(llm_rollup.metrics.count, 2);
    assert_eq!(llm_rollup.metrics.error_count, 1);
    assert_eq!(
        llm_rollup
            .metrics
            .total_tokens
            .as_ref()
            .and_then(|stats| stats.sum),
        Some(45.0)
    );
    assert_close(
        llm_rollup
            .metrics
            .total_cost
            .as_ref()
            .and_then(|stats| stats.sum)
            .expect("total cost sum"),
        0.08,
    );

    let vortex = query_engine
        .aggregate_runs(RunAggregateQuery {
            project_names: vec!["demo".to_owned()],
            group_by: vec![RunAggregateGroup::Model],
            ..RunAggregateQuery::new("demo")
        })
        .await
        .expect("aggregate from Vortex");
    assert_eq!(vortex.source, RunAggregateSource::Vortex);
    assert!(vortex.diagnostics.actual_object_store_requests > 0);
    let model = vortex
        .rows
        .iter()
        .find(|row| {
            row.group
                .get("model_name")
                .is_some_and(|value| value == "gpt-test")
        })
        .expect("model aggregate row");
    assert_eq!(model.metrics.count, 2);
    assert_close(
        model
            .metrics
            .prompt_cost
            .as_ref()
            .and_then(|stats| stats.sum)
            .expect("prompt cost sum"),
        0.03,
    );
    assert_eq!(
        model
            .metrics
            .first_token_latency_nanos
            .as_ref()
            .and_then(|stats| stats.p50),
        Some(7.0)
    );

    assert!(
        query_engine
            .delete_run(
                "demo",
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "1111111111111111",
                Some("aggregate_refresh_test"),
            )
            .await
            .expect("delete metric run")
    );
    let after_delete = query_engine
        .aggregate_runs(RunAggregateQuery {
            project_names: vec!["demo".to_owned()],
            group_by: vec![
                RunAggregateGroup::Project,
                RunAggregateGroup::TimeBucket,
                RunAggregateGroup::RunType,
            ],
            time_bucket_nanos: Some(HOUR_NANOS),
            ..RunAggregateQuery::new("demo")
        })
        .await
        .expect("aggregate refreshed rollups after delete");
    let llm_after_delete = after_delete
        .rows
        .iter()
        .find(|row| {
            row.group
                .get("run_type")
                .is_some_and(|value| value == "llm")
        })
        .expect("remaining llm rollup row");
    assert_eq!(llm_after_delete.metrics.count, 1);

    PostgresMetastore::new(mockgres.postgres_url())
        .insert_feedback(&FeedbackRecord {
            id: "generated-feedback".to_owned(),
            run_id: Some("3333333333333333".to_owned()),
            trace_id: Some("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa".to_owned()),
            project_name: Some("demo".to_owned()),
            key: "quality".to_owned(),
            score: Some(json!(0.75)),
            value: Some(json!("pass")),
            correction: None,
            comment: None,
            feedback_source: None,
            extra: None,
            created_at_unix_nano: 1,
            modified_at_unix_nano: 1,
        })
        .await
        .expect("insert generated-id feedback");
    let feedback = query_engine
        .aggregate_runs(RunAggregateQuery {
            project_names: vec!["demo".to_owned()],
            group_by: vec![RunAggregateGroup::FeedbackKey],
            feedback_keys: vec!["quality".to_owned()],
            ..RunAggregateQuery::new("demo")
        })
        .await
        .expect("aggregate generated-id feedback");
    let quality = feedback
        .rows
        .iter()
        .find(|row| {
            row.group
                .get("feedback_key")
                .is_some_and(|value| value == "quality")
        })
        .expect("quality feedback aggregate row");
    assert_eq!(quality.metrics.count, 1);
    assert_eq!(quality.metrics.feedback_scores["quality"].count, 1);

    mockgres.stop().await.expect("stop mockgres");
}

struct MetricRecordArgs<'a> {
    span_id: &'a str,
    run_type: &'a str,
    start_time_unix_nano: i64,
    end_time_unix_nano: i64,
    status_code: i32,
    prompt_tokens: i64,
    completion_tokens: i64,
    prompt_cost: f64,
    completion_cost: f64,
    model: &'a str,
    provider: &'a str,
    evaluator_score: f64,
}

fn metric_record(args: MetricRecordArgs<'_>) -> SpanRecord {
    let mut record = sample_record(args.span_id, args.start_time_unix_nano);
    record.run_type = args.run_type.to_owned();
    record.end_time_unix_nano = args.end_time_unix_nano;
    record.status_code = args.status_code;
    record.attributes_json = json!({
        "metrics": {
            "prompt_tokens": args.prompt_tokens,
            "completion_tokens": args.completion_tokens,
            "prompt_cost": args.prompt_cost,
            "completion_cost": args.completion_cost,
            "first_token_time_unix_nano": args.start_time_unix_nano + 7,
            "evaluator_score": args.evaluator_score
        },
        "metadata": {
            "ls_model_name": args.model,
            "ls_provider": args.provider
        }
    })
    .to_string();
    record
}

fn assert_close(actual: f64, expected: f64) {
    assert!(
        (actual - expected).abs() < 0.000001,
        "expected {actual} to be close to {expected}"
    );
}
