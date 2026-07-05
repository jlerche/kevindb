use super::*;

#[tokio::test]
async fn direct_run_lookup_uses_current_locator_after_stale_segment_deleted() {
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

    let mut start = sample_record("1111111111111111", 10);
    start.end_time_unix_nano = 0;
    start.status_code = 0;
    start.event_kind = RunEventKind::Start;
    start.attributes_json = r#"{"version":"start"}"#.to_owned();
    let first_flush = ingestor
        .ingest_records(vec![start])
        .await
        .expect("ingest start")
        .flush
        .expect("start should flush");

    let mut end = sample_record("1111111111111111", 10);
    end.end_time_unix_nano = 40;
    end.status_code = 1;
    end.event_kind = RunEventKind::End;
    end.attributes_json = r#"{"version":"end"}"#.to_owned();
    ingestor
        .ingest_records(vec![end])
        .await
        .expect("ingest end");

    object_store
        .delete(&Path::from(first_flush.segment_uri.as_str()))
        .await
        .expect("delete stale segment");

    let query_engine = QueryEngine::new(mockgres.postgres_url().to_owned(), object_store);
    let result = query_engine
        .load_run_by_id_with_diagnostics("1111111111111111")
        .await
        .expect("load run by id");
    assert_eq!(result.diagnostics.candidate_segments, 1);
    assert_eq!(result.diagnostics.vortex_files_opened, 1);
    let run = result.run.expect("run should exist");
    assert_eq!(run.status, "success");
    assert_eq!(run.end_time_unix_nano, 40);
    assert_eq!(run.attributes_json, r#"{"version":"end"}"#);

    let events = query_engine
        .load_run_events_by_id("1111111111111111")
        .await
        .expect("load run events");
    assert_eq!(
        events
            .iter()
            .map(|event| event.event_type.as_str())
            .collect::<Vec<_>>(),
        vec!["start", "end"]
    );
    let replayed = query_engine
        .replay_run_by_id("1111111111111111")
        .await
        .expect("replay run")
        .expect("replayed run should exist");
    assert_eq!(replayed, run);

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
    let tombstone_events = query_engine
        .load_run_events_by_id("1111111111111111")
        .await
        .expect("load tombstone run events");
    assert_eq!(
        tombstone_events
            .last()
            .map(|event| event.event_type.as_str()),
        Some("tombstone")
    );
    assert!(
        query_engine
            .load_run_by_id("1111111111111111")
            .await
            .expect("load deleted run")
            .is_none()
    );
    assert!(
        query_engine
            .replay_run_by_id("1111111111111111")
            .await
            .expect("replay deleted run")
            .is_none()
    );

    mockgres.stop().await.expect("stop mockgres");
}

#[tokio::test]
async fn duplicate_event_retry_does_not_write_duplicate_metadata_or_object() {
    let mockgres = Mockgres::start().await.expect("start mockgres");
    run_migrations(mockgres.postgres_url())
        .await
        .expect("run migrations");

    let object_store = Arc::new(InMemory::new());
    let ingestor = Ingestor::new(
        mockgres.postgres_url().to_owned(),
        object_store,
        IngestConfig {
            max_spans_per_segment: 1,
            max_flush_delay: Duration::ZERO,
        },
    );
    let record = sample_record("1111111111111111", 10);

    let first = ingestor
        .ingest_records(vec![record.clone()])
        .await
        .expect("first ingest");
    let retry = ingestor
        .ingest_records(vec![record])
        .await
        .expect("retry ingest");
    assert_eq!(first.flushed_segments, 1);
    assert_eq!(retry.flushed_segments, 0);

    let (client, connection) = tokio_postgres::connect(mockgres.postgres_url(), NoTls)
        .await
        .expect("connect postgres");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    let segment_count: i64 = client
        .query_one("SELECT count(*) FROM trace_segments", &[])
        .await
        .expect("count trace segments")
        .get(0);
    let event_count: i64 = client
        .query_one("SELECT count(*) FROM run_events", &[])
        .await
        .expect("count run events")
        .get(0);
    assert_eq!(segment_count, 1);
    assert_eq!(event_count, 1);

    mockgres.stop().await.expect("stop mockgres");
}

#[tokio::test]
async fn event_ordering_preserves_streaming_updates_errors_and_late_arrivals() {
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

    let mut start = sample_record("1111111111111111", 10);
    start.end_time_unix_nano = 0;
    start.status_code = 0;
    start.event_kind = RunEventKind::Start;
    start.attributes_json = r#"{"stage":"start"}"#.to_owned();
    ingestor
        .ingest_records(vec![start])
        .await
        .expect("ingest start");

    let mut streaming_update = sample_record("1111111111111111", 20);
    streaming_update.end_time_unix_nano = 0;
    streaming_update.status_code = 0;
    streaming_update.event_kind = RunEventKind::Update;
    streaming_update.attributes_json = r#"{"stage":"streaming"}"#.to_owned();
    ingestor
        .ingest_records(vec![streaming_update])
        .await
        .expect("ingest streaming update");

    let query_engine = QueryEngine::new(mockgres.postgres_url().to_owned(), object_store.clone());
    let pending = query_engine
        .load_run_by_id("1111111111111111")
        .await
        .expect("load pending run")
        .expect("pending run should exist");
    assert_eq!(pending.status, "pending");
    assert_eq!(pending.attributes_json, r#"{"stage":"streaming"}"#);

    let mut end = sample_record("1111111111111111", 30);
    end.end_time_unix_nano = 60;
    end.status_code = 2;
    end.event_kind = RunEventKind::End;
    end.attributes_json = r#"{"stage":"error"}"#.to_owned();
    ingestor
        .ingest_records(vec![end])
        .await
        .expect("ingest error end");

    let mut late = sample_record("1111111111111111", 5);
    late.end_time_unix_nano = 15;
    late.status_code = 1;
    late.event_kind = RunEventKind::Update;
    late.attributes_json = r#"{"stage":"late"}"#.to_owned();
    ingestor
        .ingest_records(vec![late])
        .await
        .expect("ingest late event");

    let final_run = query_engine
        .load_run_by_id("1111111111111111")
        .await
        .expect("load final run")
        .expect("final run should exist");
    assert_eq!(final_run.status, "error");
    assert_eq!(final_run.end_time_unix_nano, 60);
    assert_eq!(final_run.attributes_json, r#"{"stage":"error"}"#);

    let replayed = query_engine
        .replay_run_by_id("1111111111111111")
        .await
        .expect("replay final run")
        .expect("replayed final run should exist");
    assert_eq!(replayed, final_run);

    mockgres.stop().await.expect("stop mockgres");
}

#[tokio::test]
async fn error_update_without_end_time_is_not_reported_as_pending() {
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

    let mut start = sample_record("1111111111111111", 10);
    start.end_time_unix_nano = 0;
    start.status_code = 0;
    start.event_kind = RunEventKind::Start;
    ingestor
        .ingest_records(vec![start])
        .await
        .expect("ingest start");

    let mut error_update = sample_record("1111111111111111", 20);
    error_update.end_time_unix_nano = 0;
    error_update.status_code = 2;
    error_update.event_kind = RunEventKind::Update;
    error_update.attributes_json = r#"{"stage":"error-update"}"#.to_owned();
    ingestor
        .ingest_records(vec![error_update])
        .await
        .expect("ingest error update");

    let query_engine = QueryEngine::new(mockgres.postgres_url().to_owned(), object_store);
    let loaded = query_engine
        .load_run_by_id("1111111111111111")
        .await
        .expect("load run")
        .expect("run exists");
    assert_eq!(loaded.status, "error");
    assert_eq!(loaded.end_time_unix_nano, 0);

    let errored = query_engine
        .list_runs(RunQuery {
            error: Some(true),
            ..RunQuery::new("demo")
        })
        .await
        .expect("list errored runs");
    assert_eq!(errored.len(), 1);
    assert_eq!(errored[0].status, "error");

    mockgres.stop().await.expect("stop mockgres");
}

#[tokio::test]
async fn trace_load_uses_row_index_for_same_time_updates_and_supports_projections() {
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

    let mut first = sample_record("1111111111111111", 10);
    first.end_time_unix_nano = 0;
    first.status_code = 0;
    first.event_kind = RunEventKind::Update;
    first.attributes_json = r#"{"stage":"first"}"#.to_owned();
    let mut second = first.clone();
    second.attributes_json = r#"{"stage":"second"}"#.to_owned();

    ingestor
        .ingest_records(vec![first, second])
        .await
        .expect("ingest same-time updates");

    let query_engine = QueryEngine::new(mockgres.postgres_url().to_owned(), object_store);
    let trace_runs = query_engine
        .list_runs_in_trace("demo", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
        .await
        .expect("load trace");
    assert_eq!(trace_runs.len(), 1);
    assert_eq!(trace_runs[0].attributes_json, r#"{"stage":"second"}"#);

    let summary = query_engine
        .load_run_by_id_with_projection("1111111111111111", RunProjection::Summary)
        .await
        .expect("load summary projection")
        .run
        .expect("summary run");
    assert_eq!(summary.attributes_json, "{}");

    let full = query_engine
        .load_run_by_id_with_projection("1111111111111111", RunProjection::FullPayload)
        .await
        .expect("load full projection")
        .run
        .expect("full run");
    assert_eq!(full.attributes_json, r#"{"stage":"second"}"#);

    let events = query_engine
        .load_run_by_id_with_projection("1111111111111111", RunProjection::Events)
        .await
        .expect("load events projection");
    assert_eq!(
        events
            .events
            .iter()
            .map(|event| event.event_type.as_str())
            .collect::<Vec<_>>(),
        vec!["update", "update"]
    );
    assert_eq!(
        events.run.expect("events projection run").attributes_json,
        "{}"
    );

    let trace_events = query_engine
        .load_trace_with_projection(
            "demo",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            RunProjection::Events,
        )
        .await
        .expect("load trace events projection");
    assert_eq!(trace_events.events.len(), 2);
    assert_eq!(trace_events.runs[0].attributes_json, "{}");

    mockgres.stop().await.expect("stop mockgres");
}

#[tokio::test]
async fn duplicate_conflict_after_object_write_rolls_back_visible_segment() {
    let mockgres = Mockgres::start().await.expect("start mockgres");
    run_migrations(mockgres.postgres_url())
        .await
        .expect("run migrations");

    let ingestor = Ingestor::new(
        mockgres.postgres_url().to_owned(),
        Arc::new(InMemory::new()),
        IngestConfig {
            max_spans_per_segment: 1,
            max_flush_delay: Duration::ZERO,
        },
    );
    let record = sample_record("1111111111111111", 10);

    let first = ingestor
        .ingest_records(vec![record.clone()])
        .await
        .expect("first ingest");
    assert_eq!(first.flushed_segments, 1);

    let conflict = ingestor
        .persist_segment(&record_partition_key(&record), vec![record])
        .await
        .expect("persist duplicate segment");
    assert!(matches!(
        conflict,
        PersistSegmentResult::DuplicateConflict(_)
    ));

    let (client, connection) = tokio_postgres::connect(mockgres.postgres_url(), NoTls)
        .await
        .expect("connect postgres");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    let segment_count: i64 = client
        .query_one("SELECT count(*) FROM trace_segments", &[])
        .await
        .expect("count trace segments")
        .get(0);
    let event_count: i64 = client
        .query_one("SELECT count(*) FROM run_events", &[])
        .await
        .expect("count run events")
        .get(0);
    let span_count: i64 = client
        .query_one("SELECT count(*) FROM trace_segment_spans", &[])
        .await
        .expect("count trace segment spans")
        .get(0);
    assert_eq!(segment_count, 1);
    assert_eq!(event_count, 1);
    assert_eq!(span_count, 1);

    mockgres.stop().await.expect("stop mockgres");
}
