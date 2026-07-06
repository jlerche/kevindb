#[path = "support/mockgres.rs"]
mod mockgres_support;

use std::sync::Arc;

use anyhow::Result;
use object_store::ObjectStoreExt;
use object_store::memory::InMemory;
use object_store::path::Path;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span, Status, status};
use tokio_postgres::NoTls;

use kevindb::ingest::{IngestConfig, Ingestor};
use kevindb::otlp::{RunEventKind, SpanRecord};
use kevindb::query::{QueryEngine, RunQuery, generated_run_id};
use kevindb::segment::read_span_count;
use mockgres_support::start_mockgres_with_migrations;
use tokio::time::Duration;

const TRACE_ID: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

#[tokio::test]
async fn ingests_otlp_spans_to_vortex_segment_and_postgres_indexes() -> Result<()> {
    let mockgres = start_mockgres_with_migrations().await?;
    let object_store = Arc::new(InMemory::new());
    let ingestor = Ingestor::new(
        mockgres.postgres_url().to_owned(),
        object_store.clone(),
        IngestConfig {
            max_spans_per_segment: 64,
            max_flush_delay: Duration::ZERO,
        },
    );

    let records = kevindb_otlp::span_records_from_export("demo", sample_export())?;
    let receipt = ingestor.ingest_records(records).await?;
    assert_eq!(receipt.accepted_spans, 2);
    assert_eq!(receipt.flushed_segments, 1);
    assert_eq!(receipt.flushes.len(), 1);

    let flush = receipt.flush.expect("ingest should flush before returning");
    assert_eq!(flush.span_count, 2);
    assert!(flush.total_bytes > 0);

    let payload = object_store
        .get(&Path::from(flush.segment_uri.as_str()))
        .await?;
    assert_eq!(read_span_count(payload.bytes().await?).await?, 2);

    let (client, connection) = tokio_postgres::connect(mockgres.postgres_url(), NoTls).await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });

    let segment_count: i64 = client
        .query_one("SELECT count(*) FROM trace_segments", &[])
        .await?
        .get(0);
    let span_count: i64 = client
        .query_one("SELECT count(*) FROM trace_segment_spans", &[])
        .await?
        .get(0);
    let run_head_count: i64 = client
        .query_one("SELECT count(*) FROM run_heads", &[])
        .await?
        .get(0);
    let run_locator_count: i64 = client
        .query_one("SELECT count(*) FROM run_locators", &[])
        .await?
        .get(0);
    let trace_locator_count: i64 = client
        .query_one("SELECT count(*) FROM trace_locators", &[])
        .await?
        .get(0);
    let schema_version: i64 = client
        .query_one("SELECT schema_version FROM trace_segments", &[])
        .await?
        .get(0);
    let (child_last_row_index, child_last_run_event_id): (i64, Option<i64>) = {
        let row = client
            .query_one(
                "SELECT last_row_index, last_run_event_id
                FROM run_heads
                WHERE span_id = '2222222222222222'",
                &[],
            )
            .await?;
        (row.get(0), row.get(1))
    };

    assert_eq!(segment_count, 1);
    assert_eq!(span_count, 2);
    assert_eq!(run_head_count, 2);
    assert_eq!(run_locator_count, 2);
    assert_eq!(trace_locator_count, 2);
    assert_eq!(
        schema_version,
        kevindb::segment::SPAN_SEGMENT_SCHEMA_VERSION
    );
    assert_eq!(child_last_row_index, 1);
    assert!(child_last_run_event_id.is_some());

    let parent: Option<String> = client
        .query_one(
            "SELECT parent_span_id FROM run_heads WHERE name = 'llm.call'",
            &[],
        )
        .await?
        .get(0);
    assert_eq!(parent.as_deref(), Some("1111111111111111"));

    let query_engine = QueryEngine::new(mockgres.postgres_url().to_owned(), object_store.clone());
    let runs = query_engine
        .list_runs_in_trace("demo", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
        .await?;
    assert_eq!(
        runs.iter().map(|run| run.name.as_str()).collect::<Vec<_>>(),
        vec!["agent.run", "llm.call"]
    );
    assert_eq!(runs[0].status, "success");
    assert_eq!(runs[1].run_type, "llm");

    let trace_result = query_engine
        .load_trace_with_diagnostics("demo", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
        .await?;
    assert_eq!(trace_result.runs.len(), 2);
    assert_eq!(trace_result.diagnostics.candidate_segments, 1);
    assert_eq!(trace_result.diagnostics.vortex_files_opened, 1);

    let generated_child_id = generated_run_id(
        "demo",
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "2222222222222222",
    );
    let child = query_engine
        .load_run_by_id_with_diagnostics(&generated_child_id)
        .await?;
    assert_eq!(child.diagnostics.candidate_segments, 1);
    assert_eq!(child.diagnostics.vortex_files_opened, 1);
    let child = child.run.expect("generated child run should exist");
    assert_eq!(child.run_id, None);
    assert_eq!(child.name, "llm.call");

    let child_events = query_engine
        .load_run_events_by_id(&generated_child_id)
        .await?;
    assert_eq!(child_events.len(), 1);
    assert_eq!(child_events[0].generated_run_id, generated_child_id);
    assert_eq!(child_events[0].run_id, None);
    assert_eq!(child_events[0].row_index, 1);

    let trace = query_engine
        .load_trace_tree("demo", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
        .await?;
    assert_eq!(trace.roots.len(), 1);
    assert_eq!(trace.roots[0].run.name, "agent.run");
    assert_eq!(trace.roots[0].children.len(), 1);
    assert_eq!(trace.roots[0].children[0].run.name, "llm.call");

    mockgres.stop().await?;
    Ok(())
}

#[tokio::test]
async fn duplicate_retry_skips_duplicate_metadata_and_object_write() -> Result<()> {
    let mockgres = start_mockgres_with_migrations().await?;
    let object_store = Arc::new(InMemory::new());
    let ingestor = Ingestor::new(
        mockgres.postgres_url().to_owned(),
        object_store.clone(),
        IngestConfig {
            max_spans_per_segment: 1,
            max_flush_delay: Duration::ZERO,
        },
    );
    let record = span_record("agent.run", 10, 20, 1);

    let first = ingestor.ingest_records(vec![record.clone()]).await?;
    let retry = ingestor.ingest_records(vec![record]).await?;

    assert_eq!(first.accepted_spans, 1);
    assert_eq!(first.flushed_segments, 1);
    assert_eq!(retry.accepted_spans, 1);
    assert_eq!(retry.flushed_segments, 0);
    assert!(retry.flushes.is_empty());

    let first_uri = first.flush.expect("first ingest should flush").segment_uri;
    object_store.head(&Path::from(first_uri.as_str())).await?;

    let (client, connection) = tokio_postgres::connect(mockgres.postgres_url(), NoTls).await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });

    let segment_count: i64 = client
        .query_one("SELECT count(*) FROM trace_segments", &[])
        .await?
        .get(0);
    let event_count: i64 = client
        .query_one("SELECT count(*) FROM run_events", &[])
        .await?
        .get(0);
    let span_index_count: i64 = client
        .query_one("SELECT count(*) FROM trace_segment_spans", &[])
        .await?
        .get(0);
    let run_locator_count: i64 = client
        .query_one("SELECT count(*) FROM run_locators", &[])
        .await?
        .get(0);

    assert_eq!(segment_count, 1);
    assert_eq!(event_count, 1);
    assert_eq!(span_index_count, 1);
    assert_eq!(run_locator_count, 1);

    mockgres.stop().await?;
    Ok(())
}

#[tokio::test]
async fn compaction_deletes_and_retention_work_together() -> Result<()> {
    let mockgres = start_mockgres_with_migrations().await?;
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
            span_record_with_attrs(
                "agent.run",
                "1111111111111111",
                10,
                20,
                1,
                r#"{"langsmith.extra":{"tier":"gold"},"prompt":"hello world"}"#,
            ),
            span_record_with_attrs(
                "tool.run",
                "2222222222222222",
                30,
                40,
                1,
                r#"{"langsmith.extra":{"tier":"silver"},"prompt":"goodbye"}"#,
            ),
        ])
        .await?;

    let query_engine = QueryEngine::new(mockgres.postgres_url().to_owned(), object_store.clone());
    let initial = query_engine
        .list_runs(RunQuery {
            project_names: vec!["demo".to_owned()],
            trace_id: Some(TRACE_ID.to_owned()),
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
        .await?;
    assert_eq!(
        initial
            .iter()
            .map(|run| run.span_id.as_str())
            .collect::<Vec<_>>(),
        vec!["1111111111111111", "2222222222222222"]
    );

    let compacted = ingestor.compact_project("demo").await?;
    assert_eq!(compacted.compacted_runs, 2);
    assert_eq!(compacted.compacted_segments, 2);
    assert_eq!(compacted.written_segments, 2);

    assert!(
        query_engine
            .delete_run("demo", TRACE_ID, "1111111111111111", Some("test-delete"))
            .await?
    );
    let after_delete = query_engine
        .list_runs_in_trace("demo", TRACE_ID)
        .await?
        .into_iter()
        .map(|run| run.span_id)
        .collect::<Vec<_>>();
    assert_eq!(after_delete, vec!["2222222222222222"]);

    let expired = query_engine.expire_project_runs_before("demo", 50).await?;
    assert_eq!(expired, 1);
    assert!(
        query_engine
            .list_runs_in_trace("demo", TRACE_ID)
            .await?
            .is_empty()
    );

    let inclusive = query_engine
        .list_runs(RunQuery {
            project_names: vec!["demo".to_owned()],
            trace_id: Some(TRACE_ID.to_owned()),
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
            include_deleted: true,
            filter: None,
            trace_filter: None,
            tree_filter: None,
            include_payload: true,
            newest_first: false,
            limits: Default::default(),
        })
        .await?;
    assert_eq!(inclusive.len(), 2);

    mockgres.stop().await?;
    Ok(())
}

#[tokio::test]
async fn query_uses_run_head_manifests_to_skip_stale_segments() -> Result<()> {
    let mockgres = start_mockgres_with_migrations().await?;
    let object_store = Arc::new(InMemory::new());
    let ingestor = Ingestor::new(
        mockgres.postgres_url().to_owned(),
        object_store.clone(),
        IngestConfig {
            max_spans_per_segment: 64,
            max_flush_delay: Duration::ZERO,
        },
    );

    let mut start = span_record("agent.run", 10, 0, 0);
    start.event_kind = RunEventKind::Start;
    let first_receipt = ingestor.ingest_records(vec![start]).await?;
    let stale_segment_uri = first_receipt
        .flush
        .expect("first ingest should flush")
        .segment_uri;
    let mut end = span_record("agent.run", 10, 40, 2);
    end.event_kind = RunEventKind::End;
    ingestor.ingest_records(vec![end]).await?;

    object_store
        .delete(&Path::from(stale_segment_uri.as_str()))
        .await?;

    let query_engine = QueryEngine::new(mockgres.postgres_url().to_owned(), object_store);
    let runs = query_engine.list_runs_in_trace("demo", TRACE_ID).await?;

    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0].name, "agent.run");
    assert_eq!(runs[0].status, "error");
    assert_eq!(runs[0].end_time_unix_nano, 40);

    let direct = query_engine
        .load_run_by_id_with_diagnostics("11111111-1111-1111-1111-111111111111")
        .await?;
    assert_eq!(direct.diagnostics.candidate_segments, 1);
    assert_eq!(direct.diagnostics.vortex_files_opened, 1);
    assert_eq!(direct.diagnostics.rows_returned, 1);
    assert_eq!(direct.run.as_ref(), runs.first());

    let events = query_engine
        .load_run_events_by_id("11111111-1111-1111-1111-111111111111")
        .await?;
    assert_eq!(
        events
            .iter()
            .map(|event| event.event_type.as_str())
            .collect::<Vec<_>>(),
        vec!["start", "end"]
    );

    let replayed = query_engine
        .replay_run_by_id("11111111-1111-1111-1111-111111111111")
        .await?;
    assert_eq!(replayed.as_ref(), runs.first());

    let trace_result = query_engine
        .load_trace_with_diagnostics("demo", TRACE_ID)
        .await?;
    assert_eq!(trace_result.runs, runs);
    assert_eq!(trace_result.diagnostics.candidate_segments, 1);
    assert_eq!(trace_result.diagnostics.vortex_files_opened, 1);

    mockgres.stop().await?;
    Ok(())
}

fn sample_export() -> ExportTraceServiceRequest {
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "service.name".to_owned(),
                    key_strindex: 0,
                    value: Some(AnyValue {
                        value: Some(any_value::Value::StringValue("agent-api".to_owned())),
                    }),
                }],
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
                        start_time_unix_nano: 1_700_000_000_000_000_000,
                        end_time_unix_nano: 1_700_000_000_100_000_000,
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
                        start_time_unix_nano: 1_700_000_000_010_000_000,
                        end_time_unix_nano: 1_700_000_000_090_000_000,
                        attributes: vec![KeyValue {
                            key: "gen_ai.request.model".to_owned(),
                            key_strindex: 0,
                            value: Some(AnyValue {
                                value: Some(any_value::Value::StringValue("gpt-test".to_owned())),
                            }),
                        }],
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

fn repeated_bytes(byte: u8, len: usize) -> Vec<u8> {
    vec![byte; len]
}

fn span_record(
    name: &str,
    start_time_unix_nano: i64,
    end_time_unix_nano: i64,
    status_code: i32,
) -> SpanRecord {
    SpanRecord {
        project_name: "demo".to_owned(),
        run_id: "11111111-1111-1111-1111-111111111111".to_owned(),
        trace_id: TRACE_ID.to_owned(),
        span_id: "1111111111111111".to_owned(),
        parent_run_id: None,
        parent_span_id: None,
        name: name.to_owned(),
        run_type: "chain".to_owned(),
        start_time_unix_nano,
        end_time_unix_nano,
        status_code,
        event_kind: RunEventKind::End,
        attributes_json: "{}".to_owned(),
        idempotency_key: None,
    }
}

fn span_record_with_attrs(
    name: &str,
    span_id: &str,
    start_time_unix_nano: i64,
    end_time_unix_nano: i64,
    status_code: i32,
    attributes_json: &str,
) -> SpanRecord {
    let mut record = span_record(name, start_time_unix_nano, end_time_unix_nano, status_code);
    record.span_id = span_id.to_owned();
    record.run_id = span_id.to_owned();
    record.attributes_json = attributes_json.to_owned();
    record.event_kind = if end_time_unix_nano > 0 {
        RunEventKind::End
    } else {
        RunEventKind::Start
    };
    record
}
