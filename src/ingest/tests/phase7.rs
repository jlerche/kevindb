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

#[tokio::test]
async fn compacted_object_cleanup_supports_dry_run_and_reclaims_objects() {
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
    let first = ingestor
        .ingest_records(vec![sample_record("1111111111111111", 10)])
        .await
        .expect("ingest first")
        .flush
        .expect("first flush");
    let second = ingestor
        .ingest_records(vec![sample_record("2222222222222222", 20)])
        .await
        .expect("ingest second")
        .flush
        .expect("second flush");
    let compacted = ingestor
        .compact_project("demo")
        .await
        .expect("compact demo");
    assert_eq!(compacted.compacted_segments, 2);

    let query_engine = QueryEngine::new(mockgres.postgres_url().to_owned(), object_store.clone());
    let dry_run = query_engine
        .cleanup_compacted_objects_at(i64::MAX, Duration::ZERO, true)
        .await
        .expect("dry-run compacted cleanup");
    assert!(dry_run.dry_run);
    assert_eq!(dry_run.candidates.len(), 2);
    assert_eq!(dry_run.deleted_objects, 0);
    object_store
        .head(&Path::from(first.segment_uri.as_str()))
        .await
        .expect("dry run keeps first compacted object");

    let applied = query_engine
        .cleanup_compacted_objects_at(i64::MAX, Duration::ZERO, false)
        .await
        .expect("apply compacted cleanup");
    assert!(!applied.dry_run);
    assert_eq!(applied.candidates.len(), 2);
    assert_eq!(applied.deleted_objects, 4);
    assert!(
        object_store
            .head(&Path::from(first.segment_uri.as_str()))
            .await
            .is_err()
    );
    assert!(
        object_store
            .head(&Path::from(second.segment_uri.as_str()))
            .await
            .is_err()
    );

    let repeated = query_engine
        .cleanup_compacted_objects_at(i64::MAX, Duration::ZERO, false)
        .await
        .expect("repeat compacted cleanup");
    assert!(repeated.candidates.is_empty());
    assert_eq!(repeated.deleted_objects, 0);

    mockgres.stop().await.expect("stop mockgres");
}

#[tokio::test]
async fn compaction_service_respects_project_leases() {
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
        .ingest_records(vec![sample_record("1111111111111111", 10)])
        .await
        .expect("ingest first segment");
    ingestor
        .ingest_records(vec![sample_record("2222222222222222", 20)])
        .await
        .expect("ingest second segment");

    let (client, connection) = tokio_postgres::connect(mockgres.postgres_url(), NoTls)
        .await
        .expect("connect postgres");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .execute(
            "INSERT INTO compaction_leases(
                project_name, holder_id, lease_expires_at_unix_nano
            )
            VALUES ('demo', 'other-node', $1)",
            &[&i64::MAX],
        )
        .await
        .expect("insert competing lease");

    let blocked = ingestor
        .run_compaction_service_once(CompactionServiceConfig {
            holder_id: "node-a".to_owned(),
            lease_duration: Duration::from_secs(60),
        })
        .await
        .expect("run blocked compaction pass");
    assert_eq!(blocked.projects_scanned, 1);
    assert_eq!(blocked.leases_acquired, 0);
    assert_eq!(blocked.compacted_segments, 0);

    let compacted = ingestor
        .run_compaction_service_once(CompactionServiceConfig {
            holder_id: "other-node".to_owned(),
            lease_duration: Duration::from_secs(60),
        })
        .await
        .expect("run lease-holder compaction pass");
    assert_eq!(compacted.projects_scanned, 1);
    assert_eq!(compacted.leases_acquired, 1);
    assert_eq!(compacted.compacted_segments, 2);
    assert_eq!(compacted.written_segments, 1);

    let active_leases: i64 = client
        .query_one("SELECT count(*) FROM compaction_leases", &[])
        .await
        .expect("count compaction leases")
        .get(0);
    assert_eq!(active_leases, 0);
    let runs = QueryEngine::new(mockgres.postgres_url().to_owned(), object_store)
        .list_runs(RunQuery::new("demo"))
        .await
        .expect("list compacted runs");
    assert_eq!(runs.len(), 2);

    mockgres.stop().await.expect("stop mockgres");
}
