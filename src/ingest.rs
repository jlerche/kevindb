use std::collections::BTreeMap;
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

use crate::otlp::{RunEventKind, SpanRecord, span_records_from_export};
use crate::query::{QueryEngine, RunQuery, RunSummary};
use crate::segment::encode_span_records;

const INGEST_TIME_BUCKET_UNIX_NANOS: i64 = 60 * 60 * 1_000_000_000;

mod indexes;
mod metadata;
mod tree;
use metadata::persist_metadata;

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactReceipt {
    pub compacted_runs: usize,
    pub compacted_segments: usize,
    pub written_segments: usize,
    pub flushes: Vec<FlushReceipt>,
}

#[derive(Debug, Default)]
struct PendingState {
    buffers: BTreeMap<PartitionKey, PendingBuffer>,
}

#[derive(Debug, Default)]
struct PendingBuffer {
    records: Vec<SpanRecord>,
    waiters: Vec<FlushWaiter>,
    flushing: bool,
}

type FlushWaiter = oneshot::Sender<WaiterFlushResult>;
type WaiterFlushResult = std::result::Result<Vec<FlushReceipt>, String>;
type FlushReceiver = oneshot::Receiver<WaiterFlushResult>;

struct FlushBatch {
    partition: PartitionKey,
    records: Vec<SpanRecord>,
    waiters: Vec<FlushWaiter>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct PartitionKey {
    project_name: String,
    time_bucket_start_unix_nano: i64,
}

pub struct Ingestor {
    postgres_url: String,
    object_store: Arc<dyn ObjectStore>,
    config: IngestConfig,
    pending: Mutex<PendingState>,
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
            pending: Mutex::new(PendingState::default()),
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

        let grouped_records = group_records_by_partition(records);
        let (flush_receivers, should_flush_now) = {
            let mut pending = self.pending.lock().await;
            let mut flush_receivers = Vec::new();
            let mut should_flush_now = false;

            for (partition, records) in grouped_records {
                let buffer = pending.buffers.entry(partition).or_default();
                buffer.records.extend(records);
                let (sender, receiver) = oneshot::channel();
                buffer.waiters.push(sender);
                flush_receivers.push(receiver);
                should_flush_now |= buffer.records.len() >= self.max_spans_per_segment()
                    || self.config.max_flush_delay.is_zero();
            }

            (flush_receivers, should_flush_now)
        };

        let flushes = wait_for_flush_receivers(flush_receivers);
        tokio::pin!(flushes);

        if should_flush_now {
            self.flush().await?;
        } else if !self.config.max_flush_delay.is_zero() {
            tokio::select! {
                result = &mut flushes => {
                    let flushes = result?;
                    return Ok(receipt_from_flushes(accepted_spans, flushes));
                }
                _ = sleep(self.config.max_flush_delay) => {}
            }
        }

        self.flush().await?;
        let flushes = flushes.await?;
        Ok(receipt_from_flushes(accepted_spans, flushes))
    }

    pub async fn flush(&self) -> Result<Vec<FlushReceipt>> {
        let mut receipts = Vec::new();

        loop {
            let batch = {
                let mut pending = self.pending.lock().await;

                let next_partition = pending
                    .buffers
                    .iter()
                    .find(|(_, buffer)| !buffer.flushing && !buffer.records.is_empty())
                    .map(|(partition, _)| partition.clone());
                if let Some(partition) = next_partition {
                    let buffer = pending
                        .buffers
                        .get_mut(&partition)
                        .expect("partition selected from pending buffers");
                    buffer.flushing = true;
                    FlushBatch {
                        partition,
                        records: std::mem::take(&mut buffer.records),
                        waiters: std::mem::take(&mut buffer.waiters),
                    }
                } else if pending.buffers.values().any(|buffer| buffer.flushing) {
                    let notified = self.flush_finished.notified();
                    drop(pending);
                    notified.await;
                    continue;
                } else {
                    return Ok(receipts);
                }
            };

            match self
                .persist_partition_records(&batch.partition, batch.records)
                .await
            {
                Ok(batch_receipts) => {
                    {
                        let mut pending = self.pending.lock().await;
                        if let Some(buffer) = pending.buffers.get_mut(&batch.partition) {
                            buffer.flushing = false;
                        }
                        if pending.buffers.get(&batch.partition).is_some_and(|buffer| {
                            buffer.records.is_empty() && buffer.waiters.is_empty()
                        }) {
                            pending.buffers.remove(&batch.partition);
                        }
                    }
                    self.flush_finished.notify_waiters();
                    notify_waiters(batch.waiters, Ok(batch_receipts.clone()));
                    receipts.extend(batch_receipts);
                }
                Err(err) => {
                    let (waiters, error) = {
                        let mut pending = self.pending.lock().await;
                        let buffer = pending.buffers.entry(batch.partition).or_default();
                        let mut unflushed_records = err.records;
                        unflushed_records.append(&mut buffer.records);
                        buffer.records = unflushed_records;

                        let mut waiters = batch.waiters;
                        waiters.append(&mut buffer.waiters);
                        buffer.flushing = false;

                        (waiters, err.error)
                    };

                    self.flush_finished.notify_waiters();
                    notify_waiters(waiters, Err(error.to_string()));
                    return Err(error);
                }
            }
        }
    }

    pub async fn compact_project(&self, project_name: &str) -> Result<CompactReceipt> {
        let old_segment_ids = self.active_project_segment_ids(project_name).await?;
        let runs = QueryEngine::new(self.postgres_url.clone(), Arc::clone(&self.object_store))
            .list_runs(RunQuery::new(project_name))
            .await
            .context("load project runs for compaction")?;

        if runs.is_empty() || old_segment_ids.len() <= 1 {
            return Ok(CompactReceipt {
                compacted_runs: runs.len(),
                compacted_segments: 0,
                written_segments: 0,
                flushes: Vec::new(),
            });
        }

        let grouped_records = group_records_by_partition(
            runs.iter()
                .map(span_record_from_run_summary)
                .collect::<Vec<_>>(),
        );
        let mut flushes = Vec::new();
        for (partition, records) in grouped_records {
            flushes.extend(self.persist_partition_records(&partition, records).await.map_err(
                |error| {
                    anyhow!(
                        "compact project {project_name} failed while writing replacement segment: {}",
                        error.error
                    )
                },
            )?);
        }

        let compacted_segments = self.mark_segments_compacted(&old_segment_ids).await?;

        Ok(CompactReceipt {
            compacted_runs: runs.len(),
            compacted_segments,
            written_segments: flushes.len(),
            flushes,
        })
    }

    async fn persist_partition_records(
        &self,
        partition: &PartitionKey,
        records: Vec<SpanRecord>,
    ) -> std::result::Result<Vec<FlushReceipt>, PersistError> {
        let mut remaining_records = records;
        let mut receipts = Vec::new();

        loop {
            remaining_records = self
                .filter_duplicate_records(remaining_records)
                .await
                .map_err(|(records, error)| PersistError { records, error })?;
            if remaining_records.is_empty() {
                return Ok(receipts);
            }

            let segment_span_count = self.max_spans_per_segment().min(remaining_records.len());
            let segment_records = remaining_records
                .drain(..segment_span_count)
                .collect::<Vec<_>>();

            match self.persist_segment(partition, segment_records).await {
                Ok(PersistSegmentResult::Written(receipt)) => receipts.push(receipt),
                Ok(PersistSegmentResult::DuplicateConflict(mut records)) => {
                    records.append(&mut remaining_records);
                    remaining_records = records;
                }
                Err(mut error) => {
                    error.records.append(&mut remaining_records);
                    return Err(error);
                }
            }
        }
    }

    async fn filter_duplicate_records(
        &self,
        records: Vec<SpanRecord>,
    ) -> std::result::Result<Vec<SpanRecord>, (Vec<SpanRecord>, anyhow::Error)> {
        if records.is_empty() {
            return Ok(records);
        }

        let keys = records
            .iter()
            .map(run_event_idempotency_key)
            .collect::<Vec<_>>();
        let project_name = records[0].project_name.clone();
        let (client, connection) = tokio_postgres::connect(&self.postgres_url, NoTls)
            .await
            .context("connect postgres for ingest idempotency check")
            .map_err(|error| (records.clone(), error))?;
        tokio::spawn(async move {
            if let Err(err) = connection.await {
                tracing::warn!(error = %err, "postgres ingest idempotency connection failed");
            }
        });

        let sql = format!(
            "SELECT idempotency_key
            FROM run_events
            WHERE project_name = {}
                AND idempotency_key IN ({})",
            sql_string_literal(&project_name),
            keys.iter()
                .map(|key| sql_string_literal(key))
                .collect::<Vec<_>>()
                .join(", ")
        );
        let existing = client
            .query(sql.as_str(), &[])
            .await
            .context("load existing run event idempotency keys")
            .map_err(|error| (records.clone(), error))?
            .into_iter()
            .map(|row| row.get::<_, String>(0))
            .collect::<std::collections::HashSet<_>>();

        Ok(records
            .into_iter()
            .zip(keys)
            .filter_map(|(record, key)| (!existing.contains(&key)).then_some(record))
            .collect())
    }

    async fn persist_segment(
        &self,
        partition: &PartitionKey,
        records: Vec<SpanRecord>,
    ) -> std::result::Result<PersistSegmentResult, PersistError> {
        if let Err(error) = validate_partition_records(partition, &records) {
            return Err(PersistError { records, error });
        }
        let payload = encode_span_records(&records)
            .await
            .map_err(|error| PersistError {
                records: records.clone(),
                error,
            })?;
        let segment_uri = segment_uri(partition, &records).map_err(|error| PersistError {
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
        let metadata_inserted = persist_metadata(
            &tx,
            partition,
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
        if !metadata_inserted {
            tx.rollback()
                .await
                .context("rollback duplicate ingest metadata transaction")
                .map_err(|error| PersistError {
                    records: records.clone(),
                    error,
                })?;
            return Ok(PersistSegmentResult::DuplicateConflict(records));
        }
        tx.commit()
            .await
            .context("commit ingest metadata transaction")
            .map_err(|error| PersistError {
                records: records.clone(),
                error,
            })?;

        Ok(PersistSegmentResult::Written(FlushReceipt {
            segment_uri,
            span_count: records.len(),
            total_bytes: payload.len(),
        }))
    }

    fn max_spans_per_segment(&self) -> usize {
        self.config.max_spans_per_segment.max(1)
    }

    async fn active_project_segment_ids(&self, project_name: &str) -> Result<Vec<i64>> {
        let (client, connection) = tokio_postgres::connect(&self.postgres_url, NoTls)
            .await
            .context("connect postgres for compaction segment lookup")?;
        tokio::spawn(async move {
            if let Err(err) = connection.await {
                tracing::warn!(error = %err, "postgres compaction lookup connection failed");
            }
        });

        let rows = client
            .query(
                "SELECT id
                FROM trace_segments
                WHERE project_name = $1 AND compacted_at IS NULL
                ORDER BY id",
                &[&project_name],
            )
            .await
            .context("load active project segments")?;

        Ok(rows.into_iter().map(|row| row.get(0)).collect())
    }

    async fn mark_segments_compacted(&self, segment_ids: &[i64]) -> Result<usize> {
        if segment_ids.is_empty() {
            return Ok(0);
        }

        let (client, connection) = tokio_postgres::connect(&self.postgres_url, NoTls)
            .await
            .context("connect postgres for compaction marker")?;
        tokio::spawn(async move {
            if let Err(err) = connection.await {
                tracing::warn!(error = %err, "postgres compaction marker connection failed");
            }
        });

        let sql = format!(
            "UPDATE trace_segments
            SET compacted_at = CURRENT_TIMESTAMP
            WHERE id IN ({}) AND compacted_at IS NULL",
            segment_ids
                .iter()
                .map(i64::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        );
        let updated = client
            .execute(sql.as_str(), &[])
            .await
            .context("mark segments compacted")?;
        Ok(updated as usize)
    }
}

#[derive(Debug)]
struct PersistError {
    records: Vec<SpanRecord>,
    error: anyhow::Error,
}

#[derive(Debug)]
enum PersistSegmentResult {
    Written(FlushReceipt),
    DuplicateConflict(Vec<SpanRecord>),
}

async fn wait_for_flush_receivers(receivers: Vec<FlushReceiver>) -> Result<Vec<FlushReceipt>> {
    let mut flushes = Vec::new();
    for receiver in receivers {
        flushes.extend(flushes_from_waiter_result(receiver.await)?);
    }
    flushes.sort_by(|left, right| left.segment_uri.cmp(&right.segment_uri));
    Ok(flushes)
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

fn span_record_from_run_summary(run: &RunSummary) -> SpanRecord {
    SpanRecord {
        project_name: run.project_name.clone(),
        run_id: run.run_id.clone().unwrap_or_default(),
        trace_id: run.trace_id.clone(),
        span_id: run.span_id.clone(),
        parent_run_id: run.parent_run_id.clone(),
        parent_span_id: run.parent_span_id.clone(),
        name: run.name.clone(),
        run_type: run.run_type.clone(),
        start_time_unix_nano: run.start_time_unix_nano,
        end_time_unix_nano: run.end_time_unix_nano,
        status_code: if run.status == "error" {
            2
        } else if run.end_time_unix_nano == 0 {
            0
        } else {
            1
        },
        event_kind: RunEventKind::Compact,
        attributes_json: run.attributes_json.clone(),
    }
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

pub(crate) fn event_time_unix_nano(record: &SpanRecord) -> i64 {
    if record.end_time_unix_nano > 0 {
        record.end_time_unix_nano
    } else {
        record.start_time_unix_nano
    }
}

fn group_records_by_partition(records: Vec<SpanRecord>) -> BTreeMap<PartitionKey, Vec<SpanRecord>> {
    let mut grouped = BTreeMap::new();
    for record in records {
        grouped
            .entry(record_partition_key(&record))
            .or_insert_with(Vec::new)
            .push(record);
    }
    grouped
}

fn record_partition_key(record: &SpanRecord) -> PartitionKey {
    PartitionKey {
        project_name: record.project_name.clone(),
        time_bucket_start_unix_nano: record
            .start_time_unix_nano
            .div_euclid(INGEST_TIME_BUCKET_UNIX_NANOS)
            .saturating_mul(INGEST_TIME_BUCKET_UNIX_NANOS),
    }
}

fn validate_partition_records(partition: &PartitionKey, records: &[SpanRecord]) -> Result<()> {
    for record in records {
        let record_partition = record_partition_key(record);
        if &record_partition != partition {
            return Err(anyhow!(
                "record partition mismatch: expected {:?}, got {:?}",
                partition,
                record_partition
            ));
        }
    }

    Ok(())
}

fn run_event_idempotency_key(record: &SpanRecord) -> String {
    format!(
        "{}:{}:{}:{}:{}:{}:{}:{}:{}:{:016x}",
        record.run_id,
        record.trace_id,
        record.span_id,
        record.event_kind.as_str(),
        event_time_unix_nano(record),
        record.start_time_unix_nano,
        record.end_time_unix_nano,
        record.status_code,
        record.name,
        stable_hash(record.attributes_json.as_bytes())
    )
}

fn stable_hash(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

fn sql_string_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn segment_uri(partition: &PartitionKey, records: &[SpanRecord]) -> Result<String> {
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
        "projects/{}/time-buckets/{}/trace-segments/{}-{}-{}.vortex",
        escape_path_component(&partition.project_name),
        partition.time_bucket_start_unix_nano,
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
