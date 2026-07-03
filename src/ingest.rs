use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow};
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use tokio::sync::Mutex;
use tokio::sync::Notify;
use tokio::sync::oneshot;
use tokio::time::{Duration, sleep};
use tokio_postgres::NoTls;

use crate::otlp::{SpanRecord, span_records_from_export};
use crate::segment::encode_span_records;

#[derive(Debug, Clone)]
pub struct IngestConfig {
    pub max_spans_per_segment: usize,
    pub max_flush_delay: Duration,
}

impl Default for IngestConfig {
    fn default() -> Self {
        Self {
            max_spans_per_segment: 1024,
            max_flush_delay: Duration::from_millis(500),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngestReceipt {
    pub accepted_spans: usize,
    pub flushed_segments: usize,
    pub flush: Option<FlushReceipt>,
    pub flushes: Vec<FlushReceipt>,
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
    waiters: Vec<FlushWaiter>,
    flushing: bool,
}

type FlushWaiter = oneshot::Sender<WaiterFlushResult>;
type WaiterFlushResult = std::result::Result<Vec<FlushReceipt>, String>;

struct FlushBatch {
    records: Vec<SpanRecord>,
    waiters: Vec<FlushWaiter>,
}

pub struct Ingestor {
    postgres_url: String,
    object_store: Arc<dyn ObjectStore>,
    config: IngestConfig,
    pending: Mutex<PendingBuffer>,
    flush_finished: Notify,
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
            flush_finished: Notify::new(),
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
                flushes: Vec::new(),
            });
        }

        let (mut flush_finished, should_flush_now) = {
            let mut pending = self.pending.lock().await;
            pending.records.extend(records);
            let (sender, receiver) = oneshot::channel();
            pending.waiters.push(sender);

            (
                receiver,
                pending.records.len() >= self.max_spans_per_segment()
                    || self.config.max_flush_delay.is_zero(),
            )
        };

        if should_flush_now {
            self.flush().await?;
        } else if !self.config.max_flush_delay.is_zero() {
            tokio::select! {
                result = &mut flush_finished => {
                    let flushes = flushes_from_waiter_result(result)?;
                    return Ok(receipt_from_flushes(accepted_spans, flushes));
                }
                _ = sleep(self.config.max_flush_delay) => {}
            }
        }

        self.flush().await?;
        let flushes = flushes_from_waiter_result(flush_finished.await)?;
        Ok(receipt_from_flushes(accepted_spans, flushes))
    }

    pub async fn flush(&self) -> Result<Vec<FlushReceipt>> {
        let mut receipts = Vec::new();

        loop {
            let batch = {
                let mut pending = self.pending.lock().await;

                if pending.flushing {
                    let notified = self.flush_finished.notified();
                    drop(pending);
                    notified.await;
                    continue;
                }

                if pending.records.is_empty() {
                    return Ok(receipts);
                }

                pending.flushing = true;
                FlushBatch {
                    records: std::mem::take(&mut pending.records),
                    waiters: std::mem::take(&mut pending.waiters),
                }
            };

            match self.persist_records(batch.records).await {
                Ok(batch_receipts) => {
                    {
                        let mut pending = self.pending.lock().await;
                        pending.flushing = false;
                    }
                    self.flush_finished.notify_waiters();
                    notify_waiters(batch.waiters, Ok(batch_receipts.clone()));
                    receipts.extend(batch_receipts);
                }
                Err(err) => {
                    let (waiters, error) = {
                        let mut pending = self.pending.lock().await;
                        let mut unflushed_records = err.records;
                        unflushed_records.append(&mut pending.records);
                        pending.records = unflushed_records;

                        let mut waiters = batch.waiters;
                        waiters.append(&mut pending.waiters);
                        pending.flushing = false;

                        (waiters, err.error)
                    };

                    self.flush_finished.notify_waiters();
                    notify_waiters(waiters, Err(error.to_string()));
                    return Err(error);
                }
            }
        }
    }

    async fn persist_records(
        &self,
        records: Vec<SpanRecord>,
    ) -> std::result::Result<Vec<FlushReceipt>, PersistError> {
        let mut remaining_records = records;
        let mut receipts = Vec::new();

        while !remaining_records.is_empty() {
            let segment_span_count = self.max_spans_per_segment().min(remaining_records.len());
            let segment_records = remaining_records
                .drain(..segment_span_count)
                .collect::<Vec<_>>();

            match self.persist_segment(segment_records).await {
                Ok(receipt) => receipts.push(receipt),
                Err(mut error) => {
                    error.records.append(&mut remaining_records);
                    return Err(error);
                }
            }
        }

        Ok(receipts)
    }

    async fn persist_segment(
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

    fn max_spans_per_segment(&self) -> usize {
        self.config.max_spans_per_segment.max(1)
    }
}

struct PersistError {
    records: Vec<SpanRecord>,
    error: anyhow::Error,
}

fn notify_waiters(waiters: Vec<FlushWaiter>, result: WaiterFlushResult) {
    for waiter in waiters {
        let _ = waiter.send(result.clone());
    }
}

fn flushes_from_waiter_result(
    result: std::result::Result<WaiterFlushResult, oneshot::error::RecvError>,
) -> Result<Vec<FlushReceipt>> {
    result
        .context("ingest flush waiter dropped")?
        .map_err(|error| anyhow!(error))
}

fn receipt_from_flushes(accepted_spans: usize, flushes: Vec<FlushReceipt>) -> IngestReceipt {
    IngestReceipt {
        accepted_spans,
        flushed_segments: flushes.len(),
        flush: flushes.first().cloned(),
        flushes,
    }
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
    static NEXT_SEGMENT_SEQUENCE: AtomicU64 = AtomicU64::new(1);

    let first = records
        .first()
        .ok_or_else(|| anyhow!("cannot build segment uri for empty records"))?;
    let now_ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before unix epoch")?
        .as_nanos();
    let sequence = NEXT_SEGMENT_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    Ok(format!(
        "projects/{}/trace-segments/{}-{}-{}.vortex",
        escape_path_component(&first.project_name),
        now_ns,
        sequence,
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
mod tests;
