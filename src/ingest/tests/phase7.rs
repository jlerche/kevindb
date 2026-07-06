use std::time::Duration;

use super::*;

#[tokio::test]
async fn retention_policy_expires_old_runs_and_pushes_delete_masks() {
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
            sample_record("1111111111111111", 10),
            sample_record("2222222222222222", 40),
        ])
        .await
        .expect("ingest retention candidates");

    let query_engine = QueryEngine::new(mockgres.postgres_url().to_owned(), object_store);
    query_engine
        .set_project_retention_policy("demo", Duration::from_nanos(20))
        .await
        .expect("set retention policy");
    let policy = query_engine
        .load_project_retention_policy("demo")
        .await
        .expect("load retention policy")
        .expect("policy exists");
    assert_eq!(policy.retention_period_nanos, 20);

    let receipt = query_engine
        .enforce_project_retention_policies_at(50)
        .await
        .expect("enforce retention");
    assert_eq!(receipt.projects_checked, 1);
    assert_eq!(receipt.expired_runs, 1);

    let runs = query_engine
        .list_runs(RunQuery::new("demo"))
        .await
        .expect("list retained runs");
    assert_eq!(
        runs.iter()
            .map(|run| run.span_id.as_str())
            .collect::<Vec<_>>(),
        vec!["2222222222222222"]
    );

    let trace = query_engine
        .load_trace_with_diagnostics("demo", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
        .await
        .expect("load retained trace");
    assert_eq!(
        trace
            .runs
            .iter()
            .map(|run| run.span_id.as_str())
            .collect::<Vec<_>>(),
        vec!["2222222222222222"]
    );
    assert_eq!(trace.diagnostics.candidate_runs, 1);

    let (client, connection) = tokio_postgres::connect(mockgres.postgres_url(), NoTls)
        .await
        .expect("connect postgres");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    let delete_vectors: i64 = client
        .query_one(
            "SELECT count(*)
            FROM trace_segment_delete_vectors
            WHERE project_name = 'demo'
                AND trace_id = 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
                AND span_id = '1111111111111111'",
            &[],
        )
        .await
        .expect("count delete vectors")
        .get(0);
    assert_eq!(delete_vectors, 1);

    mockgres.stop().await.expect("stop mockgres");
}
