use std::time::Duration;

use super::*;

#[tokio::test]
async fn restart_preserves_ingest_idempotency_and_durable_ack_boundaries() {
    let mockgres = Mockgres::start().await.expect("start mockgres");
    run_migrations(mockgres.postgres_url())
        .await
        .expect("run migrations");

    let object_store = Arc::new(InMemory::new());
    let first_ingestor = Ingestor::new(
        mockgres.postgres_url().to_owned(),
        object_store.clone(),
        IngestConfig {
            max_spans_per_segment: 64,
            max_flush_delay: Duration::ZERO,
        },
    );
    let first = sample_record("1111111111111111", 10);
    let first_receipt = first_ingestor
        .ingest_records(vec![first.clone()])
        .await
        .expect("initial ingest");
    assert_eq!(first_receipt.flushed_segments, 1);

    let restarted_ingestor = Ingestor::new(
        mockgres.postgres_url().to_owned(),
        object_store.clone(),
        IngestConfig {
            max_spans_per_segment: 64,
            max_flush_delay: Duration::ZERO,
        },
    );
    let retry_receipt = restarted_ingestor
        .ingest_records(vec![first])
        .await
        .expect("retry after restart");
    assert_eq!(retry_receipt.accepted_spans, 1);
    assert_eq!(retry_receipt.flushed_segments, 0);

    let runs = QueryEngine::new(mockgres.postgres_url().to_owned(), object_store)
        .list_runs(RunQuery::new("demo"))
        .await
        .expect("list runs after restart");
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0].span_id, "1111111111111111");

    mockgres.stop().await.expect("stop mockgres");
}

#[tokio::test]
async fn restart_preserves_delete_vectors_and_retention_masks() {
    let mockgres = Mockgres::start().await.expect("start mockgres");
    run_migrations(mockgres.postgres_url())
        .await
        .expect("run migrations");

    let object_store = Arc::new(InMemory::new());
    Ingestor::new(
        mockgres.postgres_url().to_owned(),
        object_store.clone(),
        IngestConfig {
            max_spans_per_segment: 64,
            max_flush_delay: Duration::ZERO,
        },
    )
    .ingest_records(vec![
        sample_record("1111111111111111", 10),
        sample_record("2222222222222222", 20),
    ])
    .await
    .expect("ingest delete-vector records");

    QueryEngine::new(mockgres.postgres_url().to_owned(), object_store.clone())
        .delete_run(
            "demo",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "1111111111111111",
            Some("phase10_restart"),
        )
        .await
        .expect("delete run before restart");

    let restarted_query = QueryEngine::new(mockgres.postgres_url().to_owned(), object_store);
    let trace = restarted_query
        .load_trace_with_diagnostics("demo", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
        .await
        .expect("load trace after restart");
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
                AND span_id = '1111111111111111'
                AND reason = 'phase10_restart'",
            &[],
        )
        .await
        .expect("count delete vectors")
        .get(0);
    assert_eq!(delete_vectors, 1);

    mockgres.stop().await.expect("stop mockgres");
}

#[tokio::test]
async fn restart_preserves_compaction_metadata_and_reclaimability() {
    let mockgres = Mockgres::start().await.expect("start mockgres");
    run_migrations(mockgres.postgres_url())
        .await
        .expect("run migrations");

    let object_store = Arc::new(InMemory::new());
    let ingestor = Ingestor::new(
        mockgres.postgres_url().to_owned(),
        object_store.clone(),
        IngestConfig {
            max_spans_per_segment: 1,
            max_flush_delay: Duration::ZERO,
        },
    );
    ingestor
        .ingest_records(vec![sample_record("1111111111111111", 10)])
        .await
        .expect("ingest first segment");
    ingestor
        .ingest_records(vec![sample_record("2222222222222222", 20)])
        .await
        .expect("ingest second segment");
    let compacted = ingestor
        .compact_project("demo")
        .await
        .expect("compact demo");
    assert_eq!(compacted.compacted_segments, 2);
    assert!(compacted.written_segments >= 1);

    let restarted_query = QueryEngine::new(mockgres.postgres_url().to_owned(), object_store);
    let runs = restarted_query
        .list_runs(RunQuery::new("demo"))
        .await
        .expect("list compacted runs after restart");
    assert_eq!(runs.len(), 2);
    let dry_run = restarted_query
        .cleanup_compacted_objects_at(i64::MAX, Duration::ZERO, true)
        .await
        .expect("dry-run cleanup after restart");
    assert_eq!(dry_run.candidates.len(), 2);
    assert_eq!(dry_run.deleted_objects, 0);

    mockgres.stop().await.expect("stop mockgres");
}
