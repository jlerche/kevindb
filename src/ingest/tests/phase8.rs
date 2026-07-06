use std::time::Duration;

use super::*;
use crate::query::{
    DistributedQueryCancellation, DistributedQueryConfig, RunAggregateGroup, RunAggregateQuery,
};

fn phase8_record(span_id: &str, start_time_unix_nano: i64) -> SpanRecord {
    let mut record = sample_record(span_id, start_time_unix_nano);
    record.end_time_unix_nano = start_time_unix_nano + 10;
    record
}

#[tokio::test]
async fn distributed_query_partitions_segments_and_merges_pages_deterministically() {
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
        .ingest_records(vec![
            phase8_record("1111111111111111", 10),
            phase8_record("2222222222222222", 20),
            phase8_record("3333333333333333", 30),
            phase8_record("4444444444444444", 40),
        ])
        .await
        .expect("ingest partitionable records");

    let mut query = RunQuery::new("demo");
    query.offset = Some(2);
    query.limit = Some(2);
    query.limits.max_candidate_segments = Some(4);
    query.limits.max_estimated_object_store_requests = Some(4 * 48);

    let result = QueryEngine::new(mockgres.postgres_url().to_owned(), object_store)
        .list_runs_distributed_with_diagnostics(
            query,
            DistributedQueryConfig {
                worker_count: 2,
                max_segments_per_partition: 1,
                max_in_flight_partitions: 2,
                max_queued_partitions: Some(4),
                ..DistributedQueryConfig::default()
            },
        )
        .await
        .expect("run distributed query");

    assert_eq!(
        result
            .runs
            .iter()
            .map(|run| run.span_id.as_str())
            .collect::<Vec<_>>(),
        vec!["3333333333333333", "4444444444444444"]
    );
    assert_eq!(result.diagnostics.candidate_segments, 4);
    assert_eq!(result.diagnostics.rows_returned, 2);
    assert_eq!(result.distributed.workers_configured, 2);
    assert_eq!(result.distributed.partitions_planned, 4);
    assert_eq!(result.distributed.partitions_executed, 4);
    assert_eq!(result.distributed.max_in_flight_partitions, 2);
    assert!(result.diagnostics.actual_object_store_requests > 0);
    assert!(
        result
            .distributed
            .partitions
            .iter()
            .all(|partition| partition.segment_count == 1)
    );
    assert_eq!(
        result
            .distributed
            .partitions
            .iter()
            .map(|partition| partition.worker_index)
            .collect::<std::collections::BTreeSet<_>>(),
        [0, 1].into_iter().collect()
    );

    mockgres.stop().await.expect("stop mockgres");
}

#[tokio::test]
async fn distributed_query_enforces_worker_budgets_and_load_sheds() {
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
        .ingest_records(vec![
            phase8_record("1111111111111111", 10),
            phase8_record("2222222222222222", 20),
        ])
        .await
        .expect("ingest budget records");

    let query_engine = QueryEngine::new(mockgres.postgres_url().to_owned(), object_store.clone());
    let worker_budget_err = query_engine
        .list_runs_distributed_with_diagnostics(
            RunQuery::new("demo"),
            DistributedQueryConfig {
                worker_count: 2,
                max_segments_per_partition: 1,
                max_in_flight_partitions: 2,
                max_estimated_object_store_requests_per_worker: Some(47),
                ..DistributedQueryConfig::default()
            },
        )
        .await
        .expect_err("worker request budget should reject");
    assert!(
        worker_budget_err
            .to_string()
            .contains("estimated object-store requests 48 exceed per-worker limit 47")
    );

    let load_shed_err = QueryEngine::new(mockgres.postgres_url().to_owned(), object_store)
        .list_runs_distributed_with_diagnostics(
            RunQuery::new("demo"),
            DistributedQueryConfig {
                worker_count: 2,
                max_segments_per_partition: 1,
                max_in_flight_partitions: 2,
                max_queued_partitions: Some(1),
                ..DistributedQueryConfig::default()
            },
        )
        .await
        .expect_err("queue limit should shed");
    assert!(
        load_shed_err
            .to_string()
            .contains("query load-shed: planned partitions 2 exceed queue limit 1")
    );

    mockgres.stop().await.expect("stop mockgres");
}

#[tokio::test]
async fn distributed_query_honors_coordinator_cancellation() {
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
        .ingest_records(vec![phase8_record("1111111111111111", 10)])
        .await
        .expect("ingest cancellable record");

    let cancellation = DistributedQueryCancellation::new();
    cancellation.cancel();
    let err = QueryEngine::new(mockgres.postgres_url().to_owned(), object_store)
        .list_runs_distributed_with_cancellation(
            RunQuery::new("demo"),
            DistributedQueryConfig {
                worker_count: 2,
                max_segments_per_partition: 1,
                max_in_flight_partitions: 2,
                ..DistributedQueryConfig::default()
            },
            cancellation,
        )
        .await
        .expect_err("pre-cancelled query should reject");
    assert!(err.to_string().contains("query cancelled by coordinator"));

    mockgres.stop().await.expect("stop mockgres");
}

#[tokio::test]
async fn distributed_aggregates_merge_partition_rows_deterministically() {
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
    let mut first = phase8_record("1111111111111111", 10);
    first.run_type = "chain".to_owned();
    let mut second = phase8_record("2222222222222222", 20);
    second.run_type = "llm".to_owned();
    let mut third = phase8_record("3333333333333333", 30);
    third.run_type = "llm".to_owned();
    ingestor
        .ingest_records(vec![first, second, third])
        .await
        .expect("ingest aggregate records");

    let mut query = RunAggregateQuery::new("demo");
    query.group_by = vec![RunAggregateGroup::RunType];
    query.limits.max_candidate_segments = Some(3);
    query.limits.max_estimated_object_store_requests = Some(3 * 48);

    let result = QueryEngine::new(mockgres.postgres_url().to_owned(), object_store)
        .aggregate_runs_distributed(
            query,
            DistributedQueryConfig {
                worker_count: 2,
                max_segments_per_partition: 1,
                max_in_flight_partitions: 2,
                ..DistributedQueryConfig::default()
            },
        )
        .await
        .expect("run distributed aggregate query");
    let counts = result
        .rows
        .iter()
        .map(|row| {
            (
                row.group["run_type"].clone(),
                row.metrics.count,
                row.metrics.error_count,
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        counts,
        vec![("chain".to_owned(), 1, 0), ("llm".to_owned(), 2, 0)]
    );
    assert_eq!(result.diagnostics.candidate_segments, 3);
    assert_eq!(result.distributed.partitions_planned, 3);
    assert_eq!(result.distributed.partitions_executed, 3);

    mockgres.stop().await.expect("stop mockgres");
}
