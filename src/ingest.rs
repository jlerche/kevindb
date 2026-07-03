use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow};
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use tokio::sync::Mutex;
use tokio::time::{Duration, sleep};
use tokio_postgres::NoTls;

use crate::otlp::{SpanRecord, span_records_from_export};
use crate::segment::encode_span_records;

#[derive(Debug, Clone)]
pub struct IngestConfig {
    pub max_spans_per_segment: usize,
    pub flush_interval: Duration,
}

impl Default for IngestConfig {
    fn default() -> Self {
        Self {
            max_spans_per_segment: 1024,
            flush_interval: Duration::from_millis(500),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngestReceipt {
    pub accepted_spans: usize,
    pub flushed_segments: usize,
    pub flush: Option<FlushReceipt>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlushReceipt {
    pub segment_uri: String,
    pub span_count: usize,
    pub total_bytes: usize,
}

#[derive(Debug, Default)]
struct PendingBuffer {
    records: Vec<SpanRecord>,
}

pub struct Ingestor {
    postgres_url: String,
    object_store: Arc<dyn ObjectStore>,
    config: IngestConfig,
    pending: Mutex<PendingBuffer>,
}

impl Ingestor {
    pub fn new(
        postgres_url: impl Into<String>,
        object_store: Arc<dyn ObjectStore>,
        config: IngestConfig,
    ) -> Self {
        Self {
            postgres_url: postgres_url.into(),
            object_store,
            config,
            pending: Mutex::new(PendingBuffer::default()),
        }
    }

    pub fn in_memory(postgres_url: impl Into<String>) -> Self {
        Self::new(
            postgres_url,
            Arc::new(InMemory::new()),
            IngestConfig::default(),
        )
    }

    pub async fn ingest_otlp(
        &self,
        project_name: impl Into<String>,
        request: ExportTraceServiceRequest,
    ) -> Result<IngestReceipt> {
        let records = span_records_from_export(project_name, request)?;
        self.ingest_records(records).await
    }

    pub async fn ingest_records(&self, records: Vec<SpanRecord>) -> Result<IngestReceipt> {
        let accepted_spans = records.len();
        if accepted_spans == 0 {
            return Ok(IngestReceipt {
                accepted_spans: 0,
                flushed_segments: 0,
                flush: None,
            });
        }

        let should_flush_now = {
            let mut pending = self.pending.lock().await;
            pending.records.extend(records);
            pending.records.len() >= self.config.max_spans_per_segment
        };

        if !should_flush_now && !self.config.flush_interval.is_zero() {
            sleep(self.config.flush_interval).await;
        }

        let flush = self.flush().await?;
        let flushed_segments = usize::from(flush.is_some());

        Ok(IngestReceipt {
            accepted_spans,
            flushed_segments,
            flush,
        })
    }

    pub async fn flush(&self) -> Result<Option<FlushReceipt>> {
        let records = {
            let mut pending = self.pending.lock().await;
            if pending.records.is_empty() {
                return Ok(None);
            }
            std::mem::take(&mut pending.records)
        };

        match self.persist_records(records).await {
            Ok(receipt) => Ok(Some(receipt)),
            Err(err) => {
                let mut pending = self.pending.lock().await;
                let mut unflushed_records = err.records;
                unflushed_records.append(&mut pending.records);
                pending.records = unflushed_records;
                Err(err.error)
            }
        }
    }

    async fn persist_records(
        &self,
        records: Vec<SpanRecord>,
    ) -> std::result::Result<FlushReceipt, PersistError> {
        let payload = encode_span_records(&records)
            .await
            .map_err(|error| PersistError {
                records: records.clone(),
                error,
            })?;
        let segment_uri = segment_uri(&records).map_err(|error| PersistError {
            records: records.clone(),
            error,
        })?;
        let path = Path::from(segment_uri.as_str());
        let put_result = self
            .object_store
            .put(&path, PutPayload::from_bytes(payload.clone()))
            .await
            .context("write Vortex segment to object store")
            .map_err(|error| PersistError {
                records: records.clone(),
                error,
            })?;

        let (mut client, connection) = tokio_postgres::connect(&self.postgres_url, NoTls)
            .await
            .context("connect postgres for ingest metadata")
            .map_err(|error| PersistError {
                records: records.clone(),
                error,
            })?;
        tokio::spawn(async move {
            if let Err(err) = connection.await {
                tracing::warn!(error = %err, "postgres ingest metadata connection failed");
            }
        });

        let tx = client
            .transaction()
            .await
            .context("begin ingest metadata transaction")
            .map_err(|error| PersistError {
                records: records.clone(),
                error,
            })?;
        persist_metadata(
            &tx,
            &segment_uri,
            put_result.e_tag.as_deref().unwrap_or(""),
            payload.len(),
            &records,
        )
        .await
        .map_err(|error| PersistError {
            records: records.clone(),
            error,
        })?;
        tx.commit()
            .await
            .context("commit ingest metadata transaction")
            .map_err(|error| PersistError {
                records: records.clone(),
                error,
            })?;

        Ok(FlushReceipt {
            segment_uri,
            span_count: records.len(),
            total_bytes: payload.len(),
        })
    }
}

struct PersistError {
    records: Vec<SpanRecord>,
    error: anyhow::Error,
}

async fn persist_metadata(
    tx: &tokio_postgres::Transaction<'_>,
    segment_uri: &str,
    etag: &str,
    total_bytes: usize,
    records: &[SpanRecord],
) -> Result<()> {
    let first = records
        .first()
        .ok_or_else(|| anyhow!("cannot persist empty segment"))?;
    let min_start = records
        .iter()
        .map(|record| record.start_time_unix_nano)
        .min()
        .unwrap_or(0);
    let max_end = records
        .iter()
        .map(|record| record.end_time_unix_nano)
        .max()
        .unwrap_or(0);

    tx.execute(
        "INSERT INTO projects(name) VALUES ($1) ON CONFLICT (name) DO NOTHING",
        &[&first.project_name],
    )
    .await
    .context("upsert project")?;

    let row = tx
        .query_one(
            "INSERT INTO trace_segments(
                project_name, uri, etag, total_bytes, span_count,
                min_start_time_unix_nano, max_end_time_unix_nano
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            RETURNING id",
            &[
                &first.project_name,
                &segment_uri,
                &etag,
                &(total_bytes as i64),
                &(records.len() as i64),
                &min_start,
                &max_end,
            ],
        )
        .await
        .context("insert trace segment")?;
    let segment_id: i64 = row.get(0);

    for (row_index, record) in records.iter().enumerate() {
        tx.execute(
            "INSERT INTO trace_segment_spans(
                trace_segment_id, project_name, run_id, trace_id, span_id,
                parent_run_id, parent_span_id,
                name, run_type, start_time_unix_nano, end_time_unix_nano,
                status_code, status, is_root, row_index
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)",
            &[
                &segment_id,
                &record.project_name,
                &record.run_id,
                &record.trace_id,
                &record.span_id,
                &record.parent_run_id,
                &record.parent_span_id,
                &record.name,
                &record.run_type,
                &record.start_time_unix_nano,
                &record.end_time_unix_nano,
                &record.status_code,
                &status_from_record(record),
                &record.parent_span_id.is_none(),
                &(row_index as i64),
            ],
        )
        .await
        .context("insert trace segment span")?;

        tx.execute(
            "INSERT INTO run_heads(
                project_name, run_id, trace_id, span_id, parent_run_id, parent_span_id,
                name, run_type,
                start_time_unix_nano, end_time_unix_nano, status_code, status, is_root,
                last_trace_segment_id, updated_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, CURRENT_TIMESTAMP)
            ON CONFLICT (project_name, trace_id, span_id)
            DO UPDATE SET
                run_id = EXCLUDED.run_id,
                parent_run_id = EXCLUDED.parent_run_id,
                parent_span_id = EXCLUDED.parent_span_id,
                name = EXCLUDED.name,
                run_type = EXCLUDED.run_type,
                start_time_unix_nano = EXCLUDED.start_time_unix_nano,
                end_time_unix_nano = EXCLUDED.end_time_unix_nano,
                status_code = EXCLUDED.status_code,
                status = EXCLUDED.status,
                is_root = EXCLUDED.is_root,
                last_trace_segment_id = EXCLUDED.last_trace_segment_id,
                updated_at = CURRENT_TIMESTAMP",
            &[
                &record.project_name,
                &record.run_id,
                &record.trace_id,
                &record.span_id,
                &record.parent_run_id,
                &record.parent_span_id,
                &record.name,
                &record.run_type,
                &record.start_time_unix_nano,
                &record.end_time_unix_nano,
                &record.status_code,
                &status_from_record(record),
                &record.parent_span_id.is_none(),
                &segment_id,
            ],
        )
        .await
        .context("upsert run head")?;
    }

    Ok(())
}

fn status_from_record(record: &SpanRecord) -> String {
    if record.end_time_unix_nano == 0 {
        "pending".to_owned()
    } else if record.status_code == 2 {
        "error".to_owned()
    } else {
        "success".to_owned()
    }
}

fn segment_uri(records: &[SpanRecord]) -> Result<String> {
    let first = records
        .first()
        .ok_or_else(|| anyhow!("cannot build segment uri for empty records"))?;
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before unix epoch")?
        .as_millis();
    Ok(format!(
        "projects/{}/trace-segments/{}-{}.vortex",
        escape_path_component(&first.project_name),
        now_ms,
        first.trace_id
    ))
}

fn escape_path_component(value: &str) -> String {
    value
        .bytes()
        .flat_map(|byte| match byte {
            b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'-' | b'_' | b'.' => {
                vec![byte as char]
            }
            _ => format!("%{byte:02X}").chars().collect(),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::net::TcpListener;
    use std::process::Stdio;
    use std::time::{Duration as StdDuration, Instant};

    use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
    use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
    use opentelemetry_proto::tonic::resource::v1::Resource;
    use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span, Status, status};
    use tokio::process::{Child, Command};
    use tokio::time::sleep;

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
                flush_interval: Duration::ZERO,
            },
        );

        let receipt = ingestor
            .ingest_otlp("demo", sample_export())
            .await
            .expect("ingest otlp");
        assert_eq!(receipt.accepted_spans, 2);
        let flush = receipt.flush.expect("ingest should flush");
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
            }
        );
    }

    #[tokio::test]
    async fn failed_flush_restores_pending_records() {
        let ingestor = Ingestor::new(
            "postgresql://127.0.0.1:1/postgres",
            Arc::new(InMemory::new()),
            IngestConfig {
                max_spans_per_segment: 1,
                flush_interval: Duration::ZERO,
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
}
