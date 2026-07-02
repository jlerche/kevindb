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
use kevindb::query::QueryEngine;
use kevindb::segment::read_span_count;
use mockgres_support::start_mockgres_with_migrations;
use tokio::time::Duration;

#[tokio::test]
async fn ingests_otlp_spans_to_vortex_segment_and_postgres_indexes() -> Result<()> {
    let mockgres = start_mockgres_with_migrations().await?;
    let object_store = Arc::new(InMemory::new());
    let ingestor = Ingestor::new(
        mockgres.postgres_url().to_owned(),
        object_store.clone(),
        IngestConfig {
            max_spans_per_segment: 64,
            flush_interval: Duration::ZERO,
        },
    );

    let receipt = ingestor.ingest_otlp("demo", sample_export()).await?;
    assert_eq!(receipt.accepted_spans, 2);
    assert_eq!(receipt.flushed_segments, 1);

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
