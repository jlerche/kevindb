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
use kevindb::query::{QueryEngine, RunQuery};
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

    let receipt = ingestor.ingest_otlp("demo", sample_export()).await?;
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

    assert_eq!(segment_count, 1);
    assert_eq!(span_count, 2);
    assert_eq!(run_head_count, 2);

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

    let first_receipt = ingestor
        .ingest_records(vec![span_record("agent.run", 10, 0, 1)])
        .await?;
    let stale_segment_uri = first_receipt
        .flush
        .expect("first ingest should flush")
        .segment_uri;
    ingestor
        .ingest_records(vec![span_record("agent.run", 10, 40, 2)])
        .await?;

    object_store
        .delete(&Path::from(stale_segment_uri.as_str()))
        .await?;

    let query_engine = QueryEngine::new(mockgres.postgres_url().to_owned(), object_store);
    let runs = query_engine.list_runs_in_trace("demo", TRACE_ID).await?;

    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0].name, "agent.run");
    assert_eq!(runs[0].status, "error");
    assert_eq!(runs[0].end_time_unix_nano, 40);

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
