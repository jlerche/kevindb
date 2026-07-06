use std::sync::atomic::{AtomicU64, Ordering};

use axum::Json;
use kevindb::ingest::IngestReceipt;
use kevindb::query::RunQueryDiagnostics;
use serde::{Deserialize, Serialize};

const SLOW_QUERY_NANOS: u128 = 1_000_000_000;

static INGEST_REQUESTS: AtomicU64 = AtomicU64::new(0);
static INGEST_ACCEPTED_SPANS: AtomicU64 = AtomicU64::new(0);
static INGEST_FLUSHED_SEGMENTS: AtomicU64 = AtomicU64::new(0);
static QUERY_REQUESTS: AtomicU64 = AtomicU64::new(0);
static AGGREGATE_QUERY_REQUESTS: AtomicU64 = AtomicU64::new(0);
static SLOW_QUERY_REQUESTS: AtomicU64 = AtomicU64::new(0);
static QUERY_CANDIDATE_SEGMENTS: AtomicU64 = AtomicU64::new(0);
static QUERY_CANDIDATE_RUNS: AtomicU64 = AtomicU64::new(0);
static QUERY_ESTIMATED_OBJECT_STORE_REQUESTS: AtomicU64 = AtomicU64::new(0);
static QUERY_ACTUAL_OBJECT_STORE_REQUESTS: AtomicU64 = AtomicU64::new(0);
static QUERY_ACTUAL_OBJECT_STORE_BYTES_READ: AtomicU64 = AtomicU64::new(0);
static QUERY_ROWS_RETURNED: AtomicU64 = AtomicU64::new(0);
static QUERY_POSTGRES_TIME_NANOS: AtomicU64 = AtomicU64::new(0);
static QUERY_DATAFUSION_PLANNING_TIME_NANOS: AtomicU64 = AtomicU64::new(0);
static QUERY_DATAFUSION_EXECUTION_TIME_NANOS: AtomicU64 = AtomicU64::new(0);
static CACHE_HITS: AtomicU64 = AtomicU64::new(0);
static CACHE_MISSES: AtomicU64 = AtomicU64::new(0);
static CACHE_WRITES: AtomicU64 = AtomicU64::new(0);
static CACHE_INVALIDATIONS: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServerMetricsSnapshot {
    pub ingest_requests: u64,
    pub ingest_accepted_spans: u64,
    pub ingest_flushed_segments: u64,
    pub query_requests: u64,
    pub aggregate_query_requests: u64,
    pub slow_query_requests: u64,
    pub query_candidate_segments: u64,
    pub query_candidate_runs: u64,
    pub query_estimated_object_store_requests: u64,
    pub query_actual_object_store_requests: u64,
    pub query_actual_object_store_bytes_read: u64,
    pub query_rows_returned: u64,
    pub query_postgres_time_nanos: u64,
    pub query_datafusion_planning_time_nanos: u64,
    pub query_datafusion_execution_time_nanos: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub cache_writes: u64,
    pub cache_invalidations: u64,
}

pub(crate) async fn metrics_snapshot() -> Json<ServerMetricsSnapshot> {
    Json(snapshot())
}

pub(crate) fn record_ingest(receipt: &IngestReceipt) {
    add(&INGEST_REQUESTS, 1);
    add(&INGEST_ACCEPTED_SPANS, receipt.accepted_spans as u64);
    add(&INGEST_FLUSHED_SEGMENTS, receipt.flushed_segments as u64);
    tracing::debug!(
        accepted_spans = receipt.accepted_spans,
        flushed_segments = receipt.flushed_segments,
        "ingest metrics"
    );
}

pub(crate) fn record_run_query(diagnostics: &RunQueryDiagnostics) {
    add(&QUERY_REQUESTS, 1);
    record_query_diagnostics("run_query", diagnostics);
}

pub(crate) fn record_aggregate_query(diagnostics: &RunQueryDiagnostics) {
    add(&AGGREGATE_QUERY_REQUESTS, 1);
    record_query_diagnostics("aggregate_query", diagnostics);
}

pub(crate) fn record_cache_hit() {
    add(&CACHE_HITS, 1);
}

pub(crate) fn record_cache_miss() {
    add(&CACHE_MISSES, 1);
}

pub(crate) fn record_cache_write() {
    add(&CACHE_WRITES, 1);
}

pub(crate) fn record_cache_invalidation(count: u64) {
    add(&CACHE_INVALIDATIONS, count);
}

fn record_query_diagnostics(operation: &'static str, diagnostics: &RunQueryDiagnostics) {
    add(
        &QUERY_CANDIDATE_SEGMENTS,
        diagnostics.candidate_segments as u64,
    );
    add(&QUERY_CANDIDATE_RUNS, diagnostics.candidate_runs as u64);
    add(
        &QUERY_ESTIMATED_OBJECT_STORE_REQUESTS,
        diagnostics.estimated_object_store_requests as u64,
    );
    add(
        &QUERY_ACTUAL_OBJECT_STORE_REQUESTS,
        diagnostics.actual_object_store_requests,
    );
    add(
        &QUERY_ACTUAL_OBJECT_STORE_BYTES_READ,
        diagnostics.actual_object_store_bytes_read,
    );
    add(&QUERY_ROWS_RETURNED, diagnostics.rows_returned as u64);
    add(
        &QUERY_POSTGRES_TIME_NANOS,
        duration_nanos(diagnostics.postgres_query_time),
    );
    add(
        &QUERY_DATAFUSION_PLANNING_TIME_NANOS,
        duration_nanos(diagnostics.datafusion_planning_time),
    );
    add(
        &QUERY_DATAFUSION_EXECUTION_TIME_NANOS,
        duration_nanos(diagnostics.datafusion_execution_time),
    );

    let total_nanos = diagnostics
        .postgres_query_time
        .saturating_add(diagnostics.datafusion_planning_time)
        .saturating_add(diagnostics.datafusion_execution_time)
        .as_nanos();
    tracing::debug!(
        operation,
        candidate_segments = diagnostics.candidate_segments,
        candidate_runs = diagnostics.candidate_runs,
        estimated_object_store_requests = diagnostics.estimated_object_store_requests,
        actual_object_store_requests = diagnostics.actual_object_store_requests,
        actual_object_store_bytes_read = diagnostics.actual_object_store_bytes_read,
        rows_returned = diagnostics.rows_returned,
        postgres_query_time_nanos = duration_nanos(diagnostics.postgres_query_time),
        datafusion_planning_time_nanos = duration_nanos(diagnostics.datafusion_planning_time),
        datafusion_execution_time_nanos = duration_nanos(diagnostics.datafusion_execution_time),
        total_time_nanos = total_nanos,
        "query diagnostics"
    );
    if total_nanos >= SLOW_QUERY_NANOS {
        add(&SLOW_QUERY_REQUESTS, 1);
        tracing::warn!(
            operation,
            total_time_nanos = total_nanos,
            candidate_segments = diagnostics.candidate_segments,
            candidate_runs = diagnostics.candidate_runs,
            actual_object_store_requests = diagnostics.actual_object_store_requests,
            actual_object_store_bytes_read = diagnostics.actual_object_store_bytes_read,
            "slow query"
        );
    }
}

fn snapshot() -> ServerMetricsSnapshot {
    ServerMetricsSnapshot {
        ingest_requests: load(&INGEST_REQUESTS),
        ingest_accepted_spans: load(&INGEST_ACCEPTED_SPANS),
        ingest_flushed_segments: load(&INGEST_FLUSHED_SEGMENTS),
        query_requests: load(&QUERY_REQUESTS),
        aggregate_query_requests: load(&AGGREGATE_QUERY_REQUESTS),
        slow_query_requests: load(&SLOW_QUERY_REQUESTS),
        query_candidate_segments: load(&QUERY_CANDIDATE_SEGMENTS),
        query_candidate_runs: load(&QUERY_CANDIDATE_RUNS),
        query_estimated_object_store_requests: load(&QUERY_ESTIMATED_OBJECT_STORE_REQUESTS),
        query_actual_object_store_requests: load(&QUERY_ACTUAL_OBJECT_STORE_REQUESTS),
        query_actual_object_store_bytes_read: load(&QUERY_ACTUAL_OBJECT_STORE_BYTES_READ),
        query_rows_returned: load(&QUERY_ROWS_RETURNED),
        query_postgres_time_nanos: load(&QUERY_POSTGRES_TIME_NANOS),
        query_datafusion_planning_time_nanos: load(&QUERY_DATAFUSION_PLANNING_TIME_NANOS),
        query_datafusion_execution_time_nanos: load(&QUERY_DATAFUSION_EXECUTION_TIME_NANOS),
        cache_hits: load(&CACHE_HITS),
        cache_misses: load(&CACHE_MISSES),
        cache_writes: load(&CACHE_WRITES),
        cache_invalidations: load(&CACHE_INVALIDATIONS),
    }
}

fn add(counter: &AtomicU64, value: u64) {
    counter.fetch_add(value, Ordering::Relaxed);
}

fn load(counter: &AtomicU64) -> u64 {
    counter.load(Ordering::Relaxed)
}

fn duration_nanos(duration: std::time::Duration) -> u64 {
    duration.as_nanos().min(u64::MAX as u128) as u64
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn records_query_diagnostics_and_cache_counters() {
        let before = snapshot();
        record_run_query(&RunQueryDiagnostics {
            candidate_segments: 2,
            candidate_runs: 3,
            estimated_object_store_requests: 96,
            actual_object_store_requests: 7,
            actual_object_store_bytes_read: 1024,
            rows_returned: 2,
            postgres_query_time: Duration::from_nanos(10),
            datafusion_planning_time: Duration::from_nanos(20),
            datafusion_execution_time: Duration::from_nanos(30),
            ..RunQueryDiagnostics::default()
        });
        record_cache_hit();
        record_cache_miss();
        record_cache_write();
        record_cache_invalidation(2);

        let after = snapshot();
        assert!(after.query_requests > before.query_requests);
        assert!(
            after.query_actual_object_store_requests
                >= before.query_actual_object_store_requests + 7
        );
        assert!(after.cache_hits > before.cache_hits);
        assert!(after.cache_misses > before.cache_misses);
        assert!(after.cache_writes > before.cache_writes);
        assert!(after.cache_invalidations >= before.cache_invalidations + 2);
    }
}
