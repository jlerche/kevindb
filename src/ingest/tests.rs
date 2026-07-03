use std::net::TcpListener;
use std::process::Stdio;
use std::time::{Duration as StdDuration, Instant};

use object_store::ObjectStoreExt;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span, Status, status};
use tokio::process::{Child, Command};
use tokio::time::{sleep, timeout};

use super::*;
use crate::db::run_migrations;

#[tokio::test]
async fn ingest_otlp_flushes_to_object_store_and_postgres() {
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
        .ingest_otlp("demo", sample_export())
        .await
        .expect("ingest otlp");
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

    assert_eq!(segment_count, 1);
    assert_eq!(span_count, 2);
    assert_eq!(run_head_count, 2);

    mockgres.stop().await.expect("stop mockgres");
}

#[tokio::test]
async fn empty_ingest_does_not_flush() {
    let ingestor = Ingestor::in_memory("postgresql://127.0.0.1:1/postgres");

    let receipt = ingestor
        .ingest_otlp("demo", ExportTraceServiceRequest::default())
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
        .ingest_otlp("demo", sample_export())
        .await
        .expect("ingest otlp");
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
        .ingest_otlp("demo", sample_export())
        .await
        .expect_err("postgres connection should fail");
    assert!(
        error
            .to_string()
            .contains("connect postgres for ingest metadata")
    );

    let pending = ingestor.pending.lock().await;
    assert_eq!(pending.records.len(), 2);
}

#[test]
fn segment_uri_escapes_project_name() {
    let uri = segment_uri(&[SpanRecord {
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
        attributes_json: "{}".to_owned(),
    }])
    .expect("segment uri");

    assert!(uri.starts_with("projects/demo%2Fproject/trace-segments/"));
    assert!(uri.ends_with("-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.vortex"));
}

async fn wait_for_pending_records(ingestor: &Ingestor, expected_records: usize) {
    let deadline = Instant::now() + StdDuration::from_secs(1);
    loop {
        {
            let pending = ingestor.pending.lock().await;
            if pending.records.len() == expected_records {
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
        attributes_json: "{}".to_owned(),
    }
}

fn sample_export() -> ExportTraceServiceRequest {
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![string_attr("service.name", "agent-api")],
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
                        start_time_unix_nano: 1,
                        end_time_unix_nano: 10,
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
                        start_time_unix_nano: 2,
                        end_time_unix_nano: 9,
                        attributes: vec![string_attr("gen_ai.request.model", "gpt-test")],
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

fn string_attr(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_owned(),
        key_strindex: 0,
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue(value.to_owned())),
        }),
    }
}

fn repeated_bytes(byte: u8, len: usize) -> Vec<u8> {
    vec![byte; len]
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
