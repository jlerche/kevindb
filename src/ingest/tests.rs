use std::net::TcpListener;
use std::process::Stdio;
use std::time::{Duration as StdDuration, Instant};

use object_store::ObjectStoreExt;
use tokio::process::{Child, Command};
use tokio::time::{sleep, timeout};

use super::*;
use crate::query::{QueryEngine, RunProjection, RunQuery};
use kevindb_core::generated_run_id;
use kevindb_metastore_postgres::run_migrations;

#[test]
fn deduplicates_idempotency_keys_within_a_flush() {
    let mut first = sample_record("1111111111111111", 10);
    first.idempotency_key = Some("same-request".to_owned());
    let mut duplicate = sample_record("2222222222222222", 20);
    duplicate.idempotency_key = Some("same-request".to_owned());

    let records = deduplicate_records(vec![first.clone(), duplicate]);

    assert_eq!(records, vec![first]);
}

#[tokio::test]
async fn invalid_record_does_not_poison_pending_ingest() {
    let ingestor = Ingestor::in_memory("postgresql://invalid");
    let mut invalid = sample_record("1111111111111111", 10);
    invalid.attributes_json = "{".to_owned();

    let error = ingestor
        .ingest_records(vec![invalid])
        .await
        .expect_err("invalid JSON must fail before buffering");
    assert!(error.to_string().contains("attributes_json"));

    let receipt = ingestor
        .ingest_records(Vec::new())
        .await
        .expect("empty ingest remains usable after validation failure");
    assert_eq!(receipt.accepted_spans, 0);
    assert!(ingestor.pending.lock().await.buffers.is_empty());
}

mod phase1;
mod phase10;
mod phase2;
mod phase3;
mod phase4;
mod phase5;
mod phase6;
mod phase7;
mod phase8;

#[tokio::test]
async fn ingest_records_flushes_to_object_store_and_postgres() {
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

    let receipt = ingestor
        .ingest_records(sample_records())
        .await
        .expect("ingest records");
    assert_eq!(receipt.accepted_spans, 2);
    let flush = receipt.flush.expect("ingest should flush");
    assert_eq!(receipt.flushes, vec![flush.clone()]);
    assert_eq!(flush.span_count, 2);
    assert!(flush.total_bytes > 0);
    assert!(
        object_store
            .head(&Path::from(flush.segment_uri.as_str()))
            .await
            .is_ok()
    );

    let (client, connection) = tokio_postgres::connect(mockgres.postgres_url(), NoTls)
        .await
        .expect("connect postgres");
    tokio::spawn(async move {
        let _ = connection.await;
    });

    let segment_count: i64 = client
        .query_one("SELECT count(*) FROM trace_segments", &[])
        .await
        .expect("count segments")
        .get(0);
    let span_count: i64 = client
        .query_one("SELECT count(*) FROM trace_segment_spans", &[])
        .await
        .expect("count spans")
        .get(0);
    let run_head_count: i64 = client
        .query_one("SELECT count(*) FROM run_heads", &[])
        .await
        .expect("count run heads")
        .get(0);
    let run_locator_count: i64 = client
        .query_one("SELECT count(*) FROM run_locators", &[])
        .await
        .expect("count run locators")
        .get(0);
    let trace_locator_count: i64 = client
        .query_one("SELECT count(*) FROM trace_locators", &[])
        .await
        .expect("count trace locators")
        .get(0);
    let schema_version: i64 = client
        .query_one("SELECT schema_version FROM trace_segments", &[])
        .await
        .expect("load trace segment schema version")
        .get(0);
    let search_index_uri: String = client
        .query_one("SELECT search_index_uri FROM trace_segments", &[])
        .await
        .expect("load search index uri")
        .get(0);
    let (last_row_index, last_run_event_id): (i64, Option<i64>) = {
        let row = client
            .query_one(
                "SELECT last_row_index, last_run_event_id
                FROM run_heads
                WHERE span_id = '2222222222222222'",
                &[],
            )
            .await
            .expect("load run head locator");
        (row.get(0), row.get(1))
    };

    assert_eq!(segment_count, 1);
    assert_eq!(span_count, 2);
    assert_eq!(run_head_count, 2);
    assert_eq!(run_locator_count, 2);
    assert_eq!(trace_locator_count, 2);
    assert_eq!(schema_version, crate::segment::SPAN_SEGMENT_SCHEMA_VERSION);
    assert!(
        object_store
            .head(&Path::from(search_index_uri.as_str()))
            .await
            .is_ok()
    );
    assert_eq!(last_row_index, 1);
    assert!(last_run_event_id.is_some());

    let generated_child_id = generated_run_id(
        "demo",
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "2222222222222222",
    );
    let query_engine = QueryEngine::new(mockgres.postgres_url().to_owned(), object_store);
    let child = query_engine
        .load_run_by_id(&generated_child_id)
        .await
        .expect("load generated run id")
        .expect("generated run should exist");
    assert_eq!(child.run_id, generated_child_id);
    assert_eq!(child.span_id, "2222222222222222");

    mockgres.stop().await.expect("stop mockgres");
}

#[tokio::test]
async fn project_route_tracks_recent_ingest_node() {
    let mockgres = Mockgres::start().await.expect("start mockgres");
    run_migrations(mockgres.postgres_url())
        .await
        .expect("run migrations");

    let object_store = Arc::new(InMemory::new());
    let node_a = Ingestor::with_node_id(
        mockgres.postgres_url().to_owned(),
        object_store.clone(),
        IngestConfig {
            max_spans_per_segment: 64,
            max_flush_delay: Duration::ZERO,
        },
        Some("node-a".to_owned()),
    );
    let first_receipt = node_a
        .ingest_records(vec![sample_record("1111111111111111", 10)])
        .await
        .expect("ingest on node a");
    let first_segment = first_receipt.flush.expect("node a flush").segment_uri;

    let query_engine = QueryEngine::new(mockgres.postgres_url().to_owned(), object_store.clone());
    let route = query_engine
        .load_project_route("demo")
        .await
        .expect("load node a route")
        .expect("route exists");
    assert_eq!(route.node_id, "node-a");
    assert_eq!(route.last_segment_uri, first_segment);

    let node_b = Ingestor::with_node_id(
        mockgres.postgres_url().to_owned(),
        object_store.clone(),
        IngestConfig {
            max_spans_per_segment: 64,
            max_flush_delay: Duration::ZERO,
        },
        Some("node-b".to_owned()),
    );
    let second_receipt = node_b
        .ingest_records(vec![sample_record("2222222222222222", 20)])
        .await
        .expect("ingest on node b");
    let second_segment = second_receipt.flush.expect("node b flush").segment_uri;

    let route = query_engine
        .load_project_route("demo")
        .await
        .expect("load node b route")
        .expect("route exists");
    assert_eq!(route.node_id, "node-b");
    assert_eq!(route.last_segment_uri, second_segment);

    mockgres.stop().await.expect("stop mockgres");
}

#[tokio::test]
async fn query_diagnostics_report_segment_fanout() {
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
        .ingest_records(sample_records())
        .await
        .expect("ingest records");

    let query_engine = QueryEngine::new(mockgres.postgres_url().to_owned(), object_store);
    let result = query_engine
        .list_runs_with_diagnostics(RunQuery {
            project_names: vec!["demo".to_owned()],
            trace_id: Some("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned()),
            parent_run_id: None,
            parent_span_id: None,
            run_type: None,
            is_root: None,
            error: None,
            start_time_min_unix_nano: None,
            start_time_max_unix_nano: None,
            limit: None,
            offset: None,
            retention_cutoff_unix_nano: None,
            include_deleted: false,
            filter: None,
            trace_filter: None,
            tree_filter: None,
            include_payload: true,
            newest_first: false,
            limits: Default::default(),
        })
        .await
        .expect("query with diagnostics");

    assert_eq!(result.runs.len(), 2);
    assert_eq!(result.diagnostics.rows_returned, 2);
    assert_eq!(result.diagnostics.candidate_segments, 1);
    assert_eq!(result.diagnostics.vortex_files_opened, 1);

    mockgres.stop().await.expect("stop mockgres");
}

#[tokio::test]
async fn trace_query_diagnostics_reject_project_wide_fanout_when_trace_is_known() {
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
    let first = sample_record("1111111111111111", 10);
    let mut second = sample_record("2222222222222222", 20);
    second.trace_id = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_owned();
    ingestor
        .ingest_records(vec![first, second])
        .await
        .expect("ingest traces");

    let query_engine = QueryEngine::new(mockgres.postgres_url().to_owned(), object_store);
    let result = query_engine
        .load_trace_with_diagnostics("demo", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
        .await
        .expect("query trace diagnostics");

    assert_eq!(result.runs.len(), 1);
    assert_eq!(result.diagnostics.candidate_segments, 1);

    mockgres.stop().await.expect("stop mockgres");
}

#[tokio::test]
async fn project_time_filter_diagnostics_reject_full_project_fanout() {
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
            sample_record("1111111111111111", 10),
            sample_record("2222222222222222", 20),
            sample_record("3333333333333333", 30),
        ])
        .await
        .expect("ingest project runs");

    let query_engine = QueryEngine::new(mockgres.postgres_url().to_owned(), object_store);
    let result = query_engine
        .list_runs_with_diagnostics(RunQuery {
            project_names: vec!["demo".to_owned()],
            trace_id: None,
            parent_run_id: None,
            parent_span_id: None,
            run_type: None,
            is_root: None,
            error: None,
            start_time_min_unix_nano: Some(20),
            start_time_max_unix_nano: Some(20),
            limit: None,
            offset: None,
            retention_cutoff_unix_nano: None,
            include_deleted: false,
            filter: None,
            trace_filter: None,
            tree_filter: None,
            include_payload: true,
            newest_first: false,
            limits: Default::default(),
        })
        .await
        .expect("query project time filter diagnostics");

    assert_eq!(result.runs.len(), 1);
    assert_eq!(result.runs[0].span_id, "2222222222222222");
    assert_eq!(result.diagnostics.candidate_segments, 1);

    mockgres.stop().await.expect("stop mockgres");
}

#[tokio::test]
async fn empty_ingest_does_not_flush() {
    let ingestor = Ingestor::in_memory("postgresql://127.0.0.1:1/postgres");

    let receipt = ingestor
        .ingest_records(Vec::new())
        .await
        .expect("empty ingest should not connect to postgres");

    assert_eq!(
        receipt,
        IngestReceipt {
            accepted_spans: 0,
            flushed_segments: 0,
            flush: None,
            flushes: Vec::new(),
        }
    );
}

#[tokio::test]
async fn splits_flushes_by_max_spans_per_segment() {
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

    let receipt = ingestor
        .ingest_records(sample_records())
        .await
        .expect("ingest records");
    assert_eq!(receipt.accepted_spans, 2);
    assert_eq!(receipt.flushed_segments, 2);
    assert_eq!(receipt.flushes.len(), 2);
    assert!(receipt.flushes.iter().all(|flush| flush.span_count == 1));

    for flush in &receipt.flushes {
        assert!(
            object_store
                .head(&Path::from(flush.segment_uri.as_str()))
                .await
                .is_ok()
        );
    }

    let (client, connection) = tokio_postgres::connect(mockgres.postgres_url(), NoTls)
        .await
        .expect("connect postgres");
    tokio::spawn(async move {
        let _ = connection.await;
    });

    let segment_count: i64 = client
        .query_one("SELECT count(*) FROM trace_segments", &[])
        .await
        .expect("count segments")
        .get(0);
    assert_eq!(segment_count, 2);

    mockgres.stop().await.expect("stop mockgres");
}

#[tokio::test]
async fn concurrent_ingests_wait_for_shared_durable_flush() {
    let mockgres = Mockgres::start().await.expect("start mockgres");
    run_migrations(mockgres.postgres_url())
        .await
        .expect("run migrations");

    let ingestor = Arc::new(Ingestor::new(
        mockgres.postgres_url().to_owned(),
        Arc::new(InMemory::new()),
        IngestConfig {
            max_spans_per_segment: 2,
            max_flush_delay: Duration::from_secs(60),
        },
    ));

    let first_ingestor = Arc::clone(&ingestor);
    let first = tokio::spawn(async move {
        first_ingestor
            .ingest_records(vec![sample_record("1111111111111111", 1)])
            .await
    });

    wait_for_pending_records(&ingestor, 1).await;

    let second = ingestor
        .ingest_records(vec![sample_record("2222222222222222", 2)])
        .await
        .expect("second ingest");
    let first = first.await.expect("join first").expect("first ingest");

    assert_eq!(first.accepted_spans, 1);
    assert_eq!(second.accepted_spans, 1);
    assert_eq!(first.flushed_segments, 1);
    assert_eq!(second.flushed_segments, 1);
    assert_eq!(first.flushes, second.flushes);
    assert_eq!(first.flushes[0].span_count, 2);

    mockgres.stop().await.expect("stop mockgres");
}

#[tokio::test]
async fn manual_flush_drains_pending_records_before_delay() {
    let mockgres = Mockgres::start().await.expect("start mockgres");
    run_migrations(mockgres.postgres_url())
        .await
        .expect("run migrations");

    let ingestor = Arc::new(Ingestor::new(
        mockgres.postgres_url().to_owned(),
        Arc::new(InMemory::new()),
        IngestConfig {
            max_spans_per_segment: 64,
            max_flush_delay: Duration::from_secs(60),
        },
    ));

    let ingest_task = {
        let ingestor = Arc::clone(&ingestor);
        tokio::spawn(async move {
            ingestor
                .ingest_records(vec![sample_record("1111111111111111", 1)])
                .await
        })
    };

    wait_for_pending_records(&ingestor, 1).await;

    let flushes = ingestor.flush().await.expect("manual flush");
    assert_eq!(flushes.len(), 1);
    assert_eq!(flushes[0].span_count, 1);

    let receipt = timeout(Duration::from_secs(1), ingest_task)
        .await
        .expect("ingest should finish after manual flush")
        .expect("join ingest")
        .expect("ingest");
    assert_eq!(receipt.flushes, flushes);

    mockgres.stop().await.expect("stop mockgres");
}

#[tokio::test]
async fn failed_flush_restores_pending_records() {
    let ingestor = Ingestor::new(
        "postgresql://127.0.0.1:1/postgres",
        Arc::new(InMemory::new()),
        IngestConfig {
            max_spans_per_segment: 1,
            max_flush_delay: Duration::ZERO,
        },
    );

    let error = ingestor
        .ingest_records(sample_records())
        .await
        .expect_err("postgres connection should fail");
    assert!(
        error
            .to_string()
            .contains("connect postgres for ingest idempotency check")
    );

    let pending = ingestor.pending.lock().await;
    assert_eq!(
        pending
            .buffers
            .values()
            .map(|buffer| buffer.records.len())
            .sum::<usize>(),
        2
    );
}

#[test]
fn segment_uri_escapes_project_name() {
    let record = SpanRecord {
        project_name: "demo/project".to_owned(),
        run_id: String::new(),
        trace_id: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
        span_id: "1111111111111111".to_owned(),
        parent_run_id: None,
        parent_span_id: None,
        name: "root".to_owned(),
        run_type: "span".to_owned(),
        start_time_unix_nano: 1,
        end_time_unix_nano: 2,
        status_code: 1,
        event_kind: RunEventKind::End,
        attributes_json: "{}".to_owned(),
        idempotency_key: None,
    };
    let partition = record_partition_key(&record);
    let uri = segment_uri(&partition, &[record]).expect("segment uri");

    assert!(uri.starts_with("projects/demo%2Fproject/time-buckets/0/trace-segments/"));
    assert!(uri.ends_with("-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.vortex"));
}

#[tokio::test]
async fn partitions_flushes_by_project() {
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

    let mut other_project = sample_record("2222222222222222", 2);
    other_project.project_name = "other".to_owned();
    let receipt = ingestor
        .ingest_records(vec![sample_record("1111111111111111", 1), other_project])
        .await
        .expect("ingest partitioned records");

    assert_eq!(receipt.accepted_spans, 2);
    assert_eq!(receipt.flushed_segments, 2);

    let (client, connection) = tokio_postgres::connect(mockgres.postgres_url(), NoTls)
        .await
        .expect("connect postgres");
    tokio::spawn(async move {
        let _ = connection.await;
    });

    let projects = client
        .query(
            "SELECT project_name, span_count FROM trace_segments ORDER BY project_name",
            &[],
        )
        .await
        .expect("load trace segments")
        .into_iter()
        .map(|row| (row.get::<_, String>(0), row.get::<_, i64>(1)))
        .collect::<Vec<_>>();

    assert_eq!(
        projects,
        vec![("demo".to_owned(), 1), ("other".to_owned(), 1)]
    );

    mockgres.stop().await.expect("stop mockgres");
}

#[tokio::test]
async fn compacts_and_respects_deletes_and_retention() {
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

    let mut first = sample_record("1111111111111111", 10);
    first.attributes_json =
        r#"{"langsmith.extra":{"tier":"gold"},"prompt":"hello world"}"#.to_owned();
    let mut second = sample_record("2222222222222222", 30);
    second.name = "tool.run".to_owned();
    second.attributes_json =
        r#"{"langsmith.extra":{"tier":"silver"},"prompt":"goodbye"}"#.to_owned();

    ingestor
        .ingest_records(vec![first, second])
        .await
        .expect("ingest indexed records");

    let query_engine = QueryEngine::new(mockgres.postgres_url().to_owned(), object_store);
    let initial = query_engine
        .list_runs(RunQuery {
            project_names: vec!["demo".to_owned()],
            trace_id: Some("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned()),
            parent_run_id: None,
            parent_span_id: None,
            run_type: None,
            is_root: None,
            error: None,
            start_time_min_unix_nano: None,
            start_time_max_unix_nano: None,
            limit: None,
            offset: None,
            retention_cutoff_unix_nano: None,
            include_deleted: false,
            filter: None,
            trace_filter: None,
            tree_filter: None,
            include_payload: true,
            newest_first: false,
            limits: Default::default(),
        })
        .await
        .expect("query initial runs");
    assert_eq!(initial.len(), 2);

    let compacted = ingestor
        .compact_project("demo")
        .await
        .expect("compact project");
    assert_eq!(compacted.compacted_runs, 2);
    assert_eq!(compacted.compacted_segments, 2);
    assert_eq!(compacted.written_segments, 1);

    let compacted_run = query_engine
        .load_run_by_id("2222222222222222")
        .await
        .expect("load run after compaction")
        .expect("compacted run should exist");
    assert_eq!(compacted_run.span_id, "2222222222222222");
    let compacted_trace = query_engine
        .load_trace_with_diagnostics("demo", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
        .await
        .expect("load trace after compaction");
    assert_eq!(compacted_trace.runs.len(), 2);
    assert_eq!(compacted_trace.diagnostics.candidate_segments, 1);

    assert!(
        query_engine
            .delete_run(
                "demo",
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "1111111111111111",
                Some("unit-test"),
            )
            .await
            .expect("delete run")
    );
    let active = query_engine
        .list_runs_in_trace("demo", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
        .await
        .expect("query active runs");
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].span_id, "2222222222222222");

    assert_eq!(
        query_engine
            .expire_project_runs_before("demo", 50)
            .await
            .expect("expire runs"),
        1
    );
    assert!(
        query_engine
            .list_runs_in_trace("demo", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
            .await
            .expect("query after expiration")
            .is_empty()
    );

    mockgres.stop().await.expect("stop mockgres");
}

#[tokio::test]
async fn repeated_compaction_preserves_active_runs() {
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
            sample_record("1111111111111111", 10),
            sample_record("2222222222222222", 20),
        ])
        .await
        .expect("ingest compactable records");

    let query_engine = QueryEngine::new(mockgres.postgres_url().to_owned(), object_store);
    for expected in [(2, 2, 1), (0, 0, 0)] {
        let compacted = ingestor
            .compact_project("demo")
            .await
            .expect("compact project");
        assert_eq!(compacted.compacted_runs, expected.0);
        assert_eq!(compacted.compacted_segments, expected.1);
        assert_eq!(compacted.written_segments, expected.2);

        let active = query_engine
            .list_runs_in_trace("demo", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
            .await
            .expect("query active runs after compaction");
        assert_eq!(
            active
                .iter()
                .map(|run| run.span_id.as_str())
                .collect::<Vec<_>>(),
            vec!["1111111111111111", "2222222222222222"]
        );
    }

    mockgres.stop().await.expect("stop mockgres");
}

async fn wait_for_pending_records(ingestor: &Ingestor, expected_records: usize) {
    let deadline = Instant::now() + StdDuration::from_secs(1);
    loop {
        {
            let pending = ingestor.pending.lock().await;
            let pending_records = pending
                .buffers
                .values()
                .map(|buffer| buffer.records.len())
                .sum::<usize>();
            if pending_records == expected_records {
                return;
            }
        }

        assert!(
            Instant::now() < deadline,
            "timed out waiting for {expected_records} pending records"
        );
        sleep(Duration::from_millis(10)).await;
    }
}

fn sample_record(span_id: &str, start_time_unix_nano: i64) -> SpanRecord {
    SpanRecord {
        project_name: "demo".to_owned(),
        run_id: span_id.to_owned(),
        trace_id: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
        span_id: span_id.to_owned(),
        parent_run_id: None,
        parent_span_id: None,
        name: "agent.run".to_owned(),
        run_type: "chain".to_owned(),
        start_time_unix_nano,
        end_time_unix_nano: start_time_unix_nano + 1,
        status_code: 1,
        event_kind: RunEventKind::End,
        attributes_json: "{}".to_owned(),
        idempotency_key: None,
    }
}

fn sample_records() -> Vec<SpanRecord> {
    vec![
        SpanRecord {
            project_name: "demo".to_owned(),
            run_id: String::new(),
            trace_id: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
            span_id: "1111111111111111".to_owned(),
            parent_run_id: None,
            parent_span_id: None,
            name: "agent.run".to_owned(),
            run_type: "span".to_owned(),
            start_time_unix_nano: 1,
            end_time_unix_nano: 10,
            status_code: 1,
            event_kind: RunEventKind::End,
            attributes_json: r#"{"resource.service.name":"agent-api"}"#.to_owned(),
            idempotency_key: None,
        },
        SpanRecord {
            project_name: "demo".to_owned(),
            run_id: String::new(),
            trace_id: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
            span_id: "2222222222222222".to_owned(),
            parent_run_id: None,
            parent_span_id: Some("1111111111111111".to_owned()),
            name: "llm.call".to_owned(),
            run_type: "llm".to_owned(),
            start_time_unix_nano: 2,
            end_time_unix_nano: 9,
            status_code: 1,
            event_kind: RunEventKind::End,
            attributes_json:
                r#"{"gen_ai.request.model":"gpt-test","resource.service.name":"agent-api"}"#
                    .to_owned(),
            idempotency_key: None,
        },
    ]
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
            .spawn()
            .context("spawn mockgres")?;
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

            sleep(Duration::from_millis(50)).await;
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
