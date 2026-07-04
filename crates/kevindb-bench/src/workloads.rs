use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use kevindb::ingest::{IngestConfig, Ingestor};
use kevindb::query::filter::FilterExpr;
use kevindb::query::{
    QueryEngine, RunQuery, RunQueryDiagnostics, ThreadTraceQuery, TreeFilterExpr,
};
use kevindb_metastore_postgres::{FeedbackFilter, FeedbackRecord, PostgresMetastore};
use object_store::ObjectStore;
use object_store::memory::InMemory;
use serde::Serialize;
use serde_json::json;
use tokio_postgres::NoTls;

use crate::counting_object_store::{CountingObjectStore, ObjectStoreSnapshot};
use crate::mockgres::MockgresInstance;
use crate::synthetic::{SyntheticConfig, SyntheticDataset, feedback_selected, generate_dataset};

#[derive(Debug, Serialize)]
pub struct BenchReport {
    pub config: SyntheticConfig,
    pub record_count: usize,
    pub segment_count: usize,
    pub feedback_count: usize,
    pub results: Vec<WorkloadResult>,
}

#[derive(Debug, Serialize)]
pub struct WorkloadResult {
    pub name: &'static str,
    pub status: &'static str,
    pub iterations: usize,
    pub rows_returned: usize,
    pub latency_nanos: Option<LatencySummary>,
    pub candidate_segments_max: usize,
    pub vortex_files_opened_max: usize,
    pub object_store_requests: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
    pub postgres_query_nanos: u128,
    pub datafusion_planning_nanos: u128,
    pub datafusion_execution_nanos: u128,
    pub note: Option<&'static str>,
}

#[derive(Debug, Serialize)]
pub struct LatencySummary {
    pub p50: u128,
    pub p95: u128,
    pub p99: u128,
}

pub async fn run_core_benchmarks() -> Result<BenchReport> {
    let config = SyntheticConfig::from_env()?;
    let dataset = generate_dataset(config.clone());
    let mockgres = MockgresInstance::start_with_migrations().await?;
    let counting_store = Arc::new(CountingObjectStore::new(Arc::new(InMemory::new())));
    let object_store: Arc<dyn ObjectStore> = counting_store.clone();
    let ingestor = Ingestor::new(
        mockgres.postgres_url().to_owned(),
        Arc::clone(&object_store),
        IngestConfig {
            max_spans_per_segment: config.max_spans_per_segment,
            max_flush_delay: Duration::ZERO,
        },
    );
    let query_engine = QueryEngine::new(mockgres.postgres_url().to_owned(), object_store);
    let metastore = PostgresMetastore::new(mockgres.postgres_url().to_owned());

    let ingest = run_ingest_workload(&ingestor, &dataset, &counting_store).await?;
    let feedback_count = insert_feedback(&metastore, &dataset).await?;
    let segment_count = load_segment_count(mockgres.postgres_url()).await?;

    let mut results = vec![ingest];
    results.push(
        run_single_run_load_current(
            &query_engine,
            mockgres.postgres_url(),
            &dataset,
            &counting_store,
        )
        .await?,
    );
    results.push(run_trace_tree_load(&query_engine, &dataset, &counting_store).await?);
    results.push(run_project_run_filtering(&query_engine, &dataset, &counting_store).await?);
    results.push(run_selective_scalar_filtering(&query_engine, &dataset, &counting_store).await?);
    results
        .push(run_nonselective_scalar_filtering(&query_engine, &dataset, &counting_store).await?);
    results.push(run_feedback_filtering(&metastore, &dataset.config).await?);
    results.push(run_root_tree_predicate(&query_engine, &dataset, &counting_store).await?);
    results.push(run_child_tree_predicate(&query_engine, &dataset, &counting_store).await?);
    results.push(run_thread_trace_listing(&query_engine, &dataset, &counting_store).await?);
    results.push(run_unsupported_rejection(
        "aggregate-scans",
        dataset.config.iterations,
        &counting_store,
        "aggregate API and typed rollups are not implemented yet",
    ));

    mockgres.stop().await?;

    Ok(BenchReport {
        config,
        record_count: dataset.records.len(),
        segment_count,
        feedback_count,
        results,
    })
}

async fn run_ingest_workload(
    ingestor: &Ingestor,
    dataset: &SyntheticDataset,
    store: &CountingObjectStore,
) -> Result<WorkloadResult> {
    let before = store.counters().snapshot();
    let mut latencies = Vec::new();
    let mut flushed_segments = 0;
    for batch in dataset.records.chunks(dataset.config.ingest_batch_size) {
        let started = Instant::now();
        let receipt = ingestor.ingest_records(batch.to_vec()).await?;
        latencies.push(started.elapsed());
        flushed_segments += receipt.flushed_segments;
    }
    let object_delta = store.counters().snapshot().delta_since(before);

    Ok(WorkloadResult {
        name: "ingest-throughput-and-ack-latency",
        status: "ok",
        iterations: latencies.len(),
        rows_returned: dataset.records.len(),
        latency_nanos: Some(latency_summary(&latencies)),
        candidate_segments_max: flushed_segments,
        vortex_files_opened_max: 0,
        object_store_requests: object_delta.request_count(),
        bytes_read: object_delta.bytes_read,
        bytes_written: object_delta.bytes_written,
        postgres_query_nanos: 0,
        datafusion_planning_nanos: 0,
        datafusion_execution_nanos: 0,
        note: None,
    })
}

async fn run_single_run_load_current(
    query_engine: &QueryEngine,
    _postgres_url: &str,
    dataset: &SyntheticDataset,
    store: &CountingObjectStore,
) -> Result<WorkloadResult> {
    let mut stats = WorkloadStats::new("single-run-load");
    for _ in 0..dataset.config.iterations {
        let before = store.counters().snapshot();
        let started = Instant::now();
        let result = query_engine
            .load_run_by_id_with_diagnostics(&dataset.selected_run_id)
            .await?;
        let rows = usize::from(result.run.is_some());
        let object_delta = store.counters().snapshot().delta_since(before);
        stats.record(
            started.elapsed(),
            rows,
            &result.diagnostics,
            object_delta,
            Duration::ZERO,
        );
    }
    Ok(stats.finish(None))
}

async fn run_trace_tree_load(
    query_engine: &QueryEngine,
    dataset: &SyntheticDataset,
    store: &CountingObjectStore,
) -> Result<WorkloadResult> {
    let mut stats = WorkloadStats::new("trace-tree-load");
    for _ in 0..dataset.config.iterations {
        let before = store.counters().snapshot();
        let started = Instant::now();
        let result = query_engine
            .load_trace_tree_with_diagnostics(
                &dataset.config.project_name,
                &dataset.selected_trace_id,
            )
            .await?;
        let object_delta = store.counters().snapshot().delta_since(before);
        stats.record(
            started.elapsed(),
            result.diagnostics.rows_returned,
            &result.diagnostics,
            object_delta,
            Duration::ZERO,
        );
    }
    Ok(stats.finish(None))
}

async fn run_project_run_filtering(
    query_engine: &QueryEngine,
    dataset: &SyntheticDataset,
    store: &CountingObjectStore,
) -> Result<WorkloadResult> {
    run_query_workload(
        "project-run-filtering",
        query_engine,
        dataset,
        store,
        RunQuery {
            project_names: vec![dataset.config.project_name.clone()],
            trace_id: None,
            parent_run_id: None,
            parent_span_id: None,
            run_type: Some("llm".to_owned()),
            is_root: None,
            error: Some(false),
            start_time_min_unix_nano: None,
            start_time_max_unix_nano: None,
            limit: Some(100),
            offset: None,
            retention_cutoff_unix_nano: None,
            include_deleted: false,
            filter: None,
            trace_filter: None,
            tree_filter: None,
            include_payload: true,
            newest_first: false,
            limits: Default::default(),
        },
    )
    .await
}

async fn run_selective_scalar_filtering(
    query_engine: &QueryEngine,
    dataset: &SyntheticDataset,
    store: &CountingObjectStore,
) -> Result<WorkloadResult> {
    let selected_trace_index = dataset.config.trace_count / 2;
    let filter = FilterExpr::parse(&format!(
        r#"and(eq(metadata_key, "synthetic_trace_index"), eq(metadata_value, "{selected_trace_index}"))"#
    ))
    .context("parse selective scalar filter")?;
    let mut query = RunQuery::new(dataset.config.project_name.clone());
    query.filter = Some(filter);
    query.limit = Some(100);
    query.include_payload = false;
    query.newest_first = true;

    run_query_workload(
        "selective-scalar-filtering",
        query_engine,
        dataset,
        store,
        query,
    )
    .await
}

async fn run_nonselective_scalar_filtering(
    query_engine: &QueryEngine,
    dataset: &SyntheticDataset,
    store: &CountingObjectStore,
) -> Result<WorkloadResult> {
    let mut query = RunQuery::new(dataset.config.project_name.clone());
    query.filter = Some(FilterExpr::parse(r#"has(tags, "synthetic")"#)?);
    query.limit = Some(100);
    query.include_payload = false;
    query.newest_first = true;

    run_query_workload(
        "nonselective-scalar-filtering",
        query_engine,
        dataset,
        store,
        query,
    )
    .await
}

async fn run_root_tree_predicate(
    query_engine: &QueryEngine,
    dataset: &SyntheticDataset,
    store: &CountingObjectStore,
) -> Result<WorkloadResult> {
    let selected_trace_index = dataset.config.trace_count / 2;
    let mut query = RunQuery::new(dataset.config.project_name.clone());
    query.is_root = Some(true);
    query.tree_filter = Some(TreeFilterExpr::parse(&format!(
        r#"and(eq(run_type, "tool"), eq(metadata_key, "synthetic_trace_index"), eq(metadata_value, "{selected_trace_index}"))"#
    ))?);
    query.limit = Some(100);
    query.include_payload = false;
    run_query_workload("root-tree-predicate", query_engine, dataset, store, query).await
}

async fn run_child_tree_predicate(
    query_engine: &QueryEngine,
    dataset: &SyntheticDataset,
    store: &CountingObjectStore,
) -> Result<WorkloadResult> {
    let selected_trace_index = dataset.config.trace_count / 2;
    let mut query = RunQuery::new(dataset.config.project_name.clone());
    query.run_type = Some("llm".to_owned());
    query.tree_filter = Some(TreeFilterExpr::parse(&format!(
        r#"child(and(eq(run_type, "tool"), eq(metadata_key, "synthetic_trace_index"), eq(metadata_value, "{selected_trace_index}")))"#
    ))?);
    query.limit = Some(100);
    query.include_payload = false;
    run_query_workload("child-tree-predicate", query_engine, dataset, store, query).await
}

async fn run_query_workload(
    name: &'static str,
    query_engine: &QueryEngine,
    dataset: &SyntheticDataset,
    store: &CountingObjectStore,
    query: RunQuery,
) -> Result<WorkloadResult> {
    let mut stats = WorkloadStats::new(name);
    for _ in 0..dataset.config.iterations {
        let before = store.counters().snapshot();
        let started = Instant::now();
        let result = query_engine
            .list_runs_with_diagnostics(query.clone())
            .await?;
        let object_delta = store.counters().snapshot().delta_since(before);
        stats.record(
            started.elapsed(),
            result.runs.len(),
            &result.diagnostics,
            object_delta,
            Duration::ZERO,
        );
    }
    Ok(stats.finish(None))
}

async fn run_feedback_filtering(
    metastore: &PostgresMetastore,
    config: &SyntheticConfig,
) -> Result<WorkloadResult> {
    let mut latencies = Vec::new();
    let mut rows = 0;
    for _ in 0..config.iterations {
        let started = Instant::now();
        let feedback = metastore
            .list_feedback(FeedbackFilter {
                run_ids: Vec::new(),
                keys: vec!["quality".to_owned()],
                limit: 100,
                offset: 0,
                ..FeedbackFilter::default()
            })
            .await?;
        latencies.push(started.elapsed());
        rows = feedback.len();
    }

    Ok(WorkloadResult {
        name: "feedback-filtering",
        status: "ok",
        iterations: config.iterations,
        rows_returned: rows,
        latency_nanos: Some(latency_summary(&latencies)),
        candidate_segments_max: 0,
        vortex_files_opened_max: 0,
        object_store_requests: 0,
        bytes_read: 0,
        bytes_written: 0,
        postgres_query_nanos: latencies.iter().map(Duration::as_nanos).sum(),
        datafusion_planning_nanos: 0,
        datafusion_execution_nanos: 0,
        note: Some("feedback filtering is metastore-only"),
    })
}

async fn run_thread_trace_listing(
    query_engine: &QueryEngine,
    dataset: &SyntheticDataset,
    store: &CountingObjectStore,
) -> Result<WorkloadResult> {
    let selected_trace_index = dataset.config.trace_count / 2;
    let thread_id = format!(
        "thread-{:04}",
        (selected_trace_index / dataset.config.traces_per_thread)
            .min(dataset.config.thread_count - 1)
    );
    let page_size = dataset.config.traces_per_thread.clamp(1, 10);
    let mut latencies = Vec::new();
    let mut rows_returned = 0;
    let mut object_store_requests = 0;
    let mut bytes_read = 0;
    let mut bytes_written = 0;
    let mut postgres_query_nanos = 0;

    for _ in 0..dataset.config.iterations {
        let before = store.counters().snapshot();
        let started = Instant::now();
        let mut cursor = None;
        let mut iteration_rows = 0;
        for _ in 0..2 {
            let page = query_engine
                .list_thread_traces(ThreadTraceQuery {
                    project_name: dataset.config.project_name.clone(),
                    thread_id: thread_id.clone(),
                    filter: None,
                    page_size,
                    cursor,
                })
                .await?;
            postgres_query_nanos += page.diagnostics.postgres_query_time.as_nanos();
            iteration_rows += page.items.len();
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }
        latencies.push(started.elapsed());
        rows_returned = iteration_rows;
        let object_delta = store.counters().snapshot().delta_since(before);
        object_store_requests += object_delta.request_count();
        bytes_read += object_delta.bytes_read;
        bytes_written += object_delta.bytes_written;
    }

    Ok(WorkloadResult {
        name: "thread-trace-listing",
        status: "ok",
        iterations: dataset.config.iterations,
        rows_returned,
        latency_nanos: Some(latency_summary(&latencies)),
        candidate_segments_max: 0,
        vortex_files_opened_max: 0,
        object_store_requests,
        bytes_read,
        bytes_written,
        postgres_query_nanos,
        datafusion_planning_nanos: 0,
        datafusion_execution_nanos: 0,
        note: Some("thread trace pagination is metastore-only"),
    })
}

async fn insert_feedback(
    metastore: &PostgresMetastore,
    dataset: &SyntheticDataset,
) -> Result<usize> {
    let mut inserted = 0;
    for (index, record) in dataset.records.iter().enumerate() {
        if !feedback_selected(index, dataset.config.feedback_density_per_mille) {
            continue;
        }
        metastore
            .insert_feedback(&FeedbackRecord {
                id: format!("feedback-{:08}", index),
                run_id: Some(record.run_id.clone()),
                trace_id: Some(record.trace_id.clone()),
                project_name: Some(record.project_name.clone()),
                key: "quality".to_owned(),
                score: Some(json!(if record.status_code == 2 { 0.0 } else { 1.0 })),
                value: None,
                correction: None,
                comment: Some("synthetic".to_owned()),
                feedback_source: None,
                extra: None,
                created_at_unix_nano: record.end_time_unix_nano + 1,
                modified_at_unix_nano: record.end_time_unix_nano + 1,
            })
            .await?;
        inserted += 1;
    }
    Ok(inserted)
}

async fn load_segment_count(postgres_url: &str) -> Result<usize> {
    let (client, connection) = tokio_postgres::connect(postgres_url, NoTls)
        .await
        .context("connect postgres for segment count")?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    let count: i64 = client
        .query_one(
            "SELECT count(*) FROM trace_segments WHERE compacted_at IS NULL",
            &[],
        )
        .await?
        .get(0);
    Ok(count as usize)
}

#[derive(Debug)]
struct WorkloadStats {
    name: &'static str,
    latencies: Vec<Duration>,
    rows_returned: usize,
    candidate_segments_max: usize,
    vortex_files_opened_max: usize,
    object_store_requests: u64,
    bytes_read: u64,
    bytes_written: u64,
    postgres_query_nanos: u128,
    datafusion_planning_nanos: u128,
    datafusion_execution_nanos: u128,
}

impl WorkloadStats {
    fn new(name: &'static str) -> Self {
        Self {
            name,
            latencies: Vec::new(),
            rows_returned: 0,
            candidate_segments_max: 0,
            vortex_files_opened_max: 0,
            object_store_requests: 0,
            bytes_read: 0,
            bytes_written: 0,
            postgres_query_nanos: 0,
            datafusion_planning_nanos: 0,
            datafusion_execution_nanos: 0,
        }
    }

    fn record(
        &mut self,
        latency: Duration,
        rows_returned: usize,
        diagnostics: &RunQueryDiagnostics,
        object_store: ObjectStoreSnapshot,
        extra_postgres_time: Duration,
    ) {
        self.latencies.push(latency);
        self.rows_returned = rows_returned;
        self.candidate_segments_max = self
            .candidate_segments_max
            .max(diagnostics.candidate_segments);
        self.vortex_files_opened_max = self
            .vortex_files_opened_max
            .max(diagnostics.vortex_files_opened);
        self.object_store_requests += object_store.request_count();
        self.bytes_read += object_store.bytes_read;
        self.bytes_written += object_store.bytes_written;
        self.postgres_query_nanos +=
            diagnostics.postgres_query_time.as_nanos() + extra_postgres_time.as_nanos();
        self.datafusion_planning_nanos += diagnostics.datafusion_planning_time.as_nanos();
        self.datafusion_execution_nanos += diagnostics.datafusion_execution_time.as_nanos();
    }

    fn finish(self, note: Option<&'static str>) -> WorkloadResult {
        WorkloadResult {
            name: self.name,
            status: "ok",
            iterations: self.latencies.len(),
            rows_returned: self.rows_returned,
            latency_nanos: Some(latency_summary(&self.latencies)),
            candidate_segments_max: self.candidate_segments_max,
            vortex_files_opened_max: self.vortex_files_opened_max,
            object_store_requests: self.object_store_requests,
            bytes_read: self.bytes_read,
            bytes_written: self.bytes_written,
            postgres_query_nanos: self.postgres_query_nanos,
            datafusion_planning_nanos: self.datafusion_planning_nanos,
            datafusion_execution_nanos: self.datafusion_execution_nanos,
            note,
        }
    }
}

fn run_unsupported_rejection(
    name: &'static str,
    iterations: usize,
    store: &CountingObjectStore,
    note: &'static str,
) -> WorkloadResult {
    let mut latencies = Vec::new();
    let before = store.counters().snapshot();
    for _ in 0..iterations {
        let started = Instant::now();
        std::hint::black_box(note);
        latencies.push(started.elapsed());
    }
    let object_delta = store.counters().snapshot().delta_since(before);

    WorkloadResult {
        name,
        status: "ok",
        iterations,
        rows_returned: 0,
        latency_nanos: Some(latency_summary(&latencies)),
        candidate_segments_max: 0,
        vortex_files_opened_max: 0,
        object_store_requests: object_delta.request_count(),
        bytes_read: object_delta.bytes_read,
        bytes_written: object_delta.bytes_written,
        postgres_query_nanos: 0,
        datafusion_planning_nanos: 0,
        datafusion_execution_nanos: 0,
        note: Some(note),
    }
}

fn latency_summary(values: &[Duration]) -> LatencySummary {
    let mut nanos = values.iter().map(Duration::as_nanos).collect::<Vec<_>>();
    nanos.sort_unstable();
    LatencySummary {
        p50: percentile(&nanos, 50),
        p95: percentile(&nanos, 95),
        p99: percentile(&nanos, 99),
    }
}

fn percentile(sorted_values: &[u128], percentile: usize) -> u128 {
    if sorted_values.is_empty() {
        return 0;
    }
    let index = ((sorted_values.len() - 1) * percentile).div_ceil(100);
    sorted_values[index.min(sorted_values.len() - 1)]
}
