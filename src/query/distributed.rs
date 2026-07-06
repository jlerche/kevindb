use std::collections::{BTreeSet, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use futures_util::StreamExt;
use tokio::sync::Notify;

use super::object_store_stats::{
    ObjectStoreReadSnapshot, enforce_runtime_object_store_limits, page_datafusion_runs,
};
use super::planner::{RunQueryPlan, estimate_vortex_object_store_requests};
use super::{
    DataFusionQueryTiming, QueryEngine, RunKey, RunQuery, RunQueryDiagnostics, RunSummary,
    SegmentSource, load_deleted_run_keys, load_run_query_plan,
    query_segment_sources_with_datafusion_timed, query_without_wall_time_limit,
    run_matches_retention_filter, search,
};
use super::{RunAggregateRow, RunAggregateSource};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DistributedQueryConfig {
    pub worker_count: usize,
    pub max_segments_per_partition: usize,
    pub max_in_flight_partitions: usize,
    pub max_queued_partitions: Option<usize>,
    pub max_estimated_object_store_requests_per_worker: Option<usize>,
    pub max_candidate_bytes_per_worker: Option<i64>,
}

impl Default for DistributedQueryConfig {
    fn default() -> Self {
        Self {
            worker_count: 1,
            max_segments_per_partition: 8,
            max_in_flight_partitions: 1,
            max_queued_partitions: None,
            max_estimated_object_store_requests_per_worker: None,
            max_candidate_bytes_per_worker: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct DistributedQueryCancellation {
    cancelled: Arc<AtomicBool>,
    notify: Arc<Notify>,
}

impl DistributedQueryCancellation {
    pub fn new() -> Self {
        Self {
            cancelled: Arc::new(AtomicBool::new(false)),
            notify: Arc::new(Notify::new()),
        }
    }

    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
        self.notify.notify_waiters();
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }

    pub(crate) async fn cancelled(&self) {
        if self.is_cancelled() {
            return;
        }
        self.notify.notified().await;
    }
}

impl Default for DistributedQueryCancellation {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DistributedRunQueryResult {
    pub runs: Vec<RunSummary>,
    pub diagnostics: RunQueryDiagnostics,
    pub distributed: DistributedQueryDiagnostics,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DistributedRunAggregateResult {
    pub rows: Vec<RunAggregateRow>,
    pub diagnostics: RunQueryDiagnostics,
    pub distributed: DistributedQueryDiagnostics,
    pub source: RunAggregateSource,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DistributedQueryDiagnostics {
    pub workers_configured: usize,
    pub partitions_planned: usize,
    pub partitions_executed: usize,
    pub max_in_flight_partitions: usize,
    pub load_shed: bool,
    pub cancelled: bool,
    pub partitions: Vec<DistributedQueryPartitionDiagnostics>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DistributedQueryPartitionDiagnostics {
    pub worker_index: usize,
    pub project_names: Vec<String>,
    pub segment_count: usize,
    pub candidate_runs: usize,
    pub candidate_bytes: i64,
    pub estimated_object_store_requests: usize,
    pub rows_returned: usize,
    pub actual_object_store_requests: u64,
    pub actual_object_store_bytes_read: u64,
    pub datafusion_planning_time: Duration,
    pub datafusion_execution_time: Duration,
}

struct DistributedQueryPartition {
    worker_index: usize,
    project_names: Vec<String>,
    segments: Vec<SegmentSource>,
    candidate_runs: usize,
    candidate_bytes: i64,
    estimated_object_store_requests: usize,
}

struct PartitionExecutionResult {
    runs: Vec<RunSummary>,
    diagnostics: DistributedQueryPartitionDiagnostics,
    timing: DataFusionQueryTiming,
    reads: ObjectStoreReadSnapshot,
}

impl QueryEngine {
    pub async fn list_runs_distributed_with_diagnostics(
        &self,
        query: RunQuery,
        config: DistributedQueryConfig,
    ) -> Result<DistributedRunQueryResult> {
        self.list_runs_distributed_with_cancellation(
            query,
            config,
            DistributedQueryCancellation::new(),
        )
        .await
    }

    pub async fn list_runs_distributed_with_cancellation(
        &self,
        query: RunQuery,
        config: DistributedQueryConfig,
        cancellation: DistributedQueryCancellation,
    ) -> Result<DistributedRunQueryResult> {
        validate_distributed_config(&config)?;
        if let Some(max_wall_time) = query.limits.max_wall_time {
            let query = query_without_wall_time_limit(query);
            return tokio::time::timeout(
                max_wall_time,
                self.list_runs_distributed_without_wall_time(query, config, cancellation),
            )
            .await
            .context("query exceeded max wall clock")?;
        }

        self.list_runs_distributed_without_wall_time(query, config, cancellation)
            .await
    }

    async fn list_runs_distributed_without_wall_time(
        &self,
        query: RunQuery,
        config: DistributedQueryConfig,
        cancellation: DistributedQueryCancellation,
    ) -> Result<DistributedRunQueryResult> {
        let cancellation_for_select = cancellation.clone();
        tokio::select! {
            biased;
            _ = cancellation_for_select.cancelled() => Err(anyhow!("query cancelled by coordinator")),
            result = self.list_runs_distributed_inner(query, config, cancellation) => result,
        }
    }

    async fn list_runs_distributed_inner(
        &self,
        query: RunQuery,
        config: DistributedQueryConfig,
        cancellation: DistributedQueryCancellation,
    ) -> Result<DistributedRunQueryResult> {
        reject_if_cancelled(&cancellation)?;
        let postgres_started = Instant::now();
        let plan = load_run_query_plan(&self.postgres_url, &query).await?;
        let deleted_runs = if query.include_deleted {
            HashSet::new()
        } else {
            load_deleted_run_keys(&self.postgres_url, &query).await?
        };
        let postgres_query_time = postgres_started.elapsed();

        reject_if_cancelled(&cancellation)?;
        let (plan, search_index_reads) =
            search::apply_phase6_search_indexes(Arc::clone(&self.object_store), plan, &query)
                .await?;
        let candidate_segments = plan.segments.len();
        let candidate_runs = plan.candidate_runs;
        let candidate_bytes = plan.candidate_bytes;
        let estimated_object_store_requests = plan.estimated_object_store_requests;
        let candidate_run_keys = Arc::new(plan.candidate_run_keys.clone());
        let partitions = plan_partitions(plan, &config)?;
        shed_if_overloaded(partitions.len(), &config)?;
        enforce_worker_budgets(&partitions, &config)?;

        reject_if_cancelled(&cancellation)?;
        let partition_count = partitions.len();
        let worker_query = worker_query(&query);
        let object_store = Arc::clone(&self.object_store);
        let worker_limit = config
            .max_in_flight_partitions
            .min(config.worker_count)
            .max(1);
        let worker_results = futures_util::stream::iter(partitions.into_iter().map(|partition| {
            let object_store = Arc::clone(&object_store);
            let query = worker_query.clone();
            let candidate_run_keys = Arc::clone(&candidate_run_keys);
            let cancellation = cancellation.clone();
            async move {
                reject_if_cancelled(&cancellation)?;
                execute_partition(object_store, partition, query, candidate_run_keys).await
            }
        }))
        .buffer_unordered(worker_limit)
        .collect::<Vec<_>>()
        .await;

        let mut runs = Vec::new();
        let mut partition_diagnostics = Vec::new();
        let mut datafusion_planning_time = Duration::ZERO;
        let mut datafusion_execution_time = Duration::ZERO;
        let mut object_store_reads = ObjectStoreReadSnapshot::default();

        for result in worker_results {
            let result = result?;
            runs.extend(result.runs);
            datafusion_planning_time += result.timing.planning_time;
            datafusion_execution_time += result.timing.execution_time;
            object_store_reads = add_snapshots(object_store_reads, result.reads);
            partition_diagnostics.push(result.diagnostics);
        }

        let filtered_runs = runs
            .into_iter()
            .filter(|run| candidate_run_keys.contains(&RunKey::from(run)))
            .filter(|run| !deleted_runs.contains(&RunKey::from(run)))
            .filter(|run| run_matches_retention_filter(run, &query))
            .collect::<Vec<_>>();
        let runs = page_datafusion_runs(filtered_runs, &query);
        let total_reads = add_snapshots(object_store_reads, search_index_reads);
        enforce_runtime_object_store_limits(&query, total_reads)?;

        Ok(DistributedRunQueryResult {
            diagnostics: RunQueryDiagnostics {
                candidate_segments,
                candidate_runs,
                candidate_bytes,
                estimated_object_store_requests,
                actual_object_store_requests: total_reads.request_count(),
                actual_object_store_bytes_read: total_reads.bytes_read,
                vortex_files_opened: candidate_segments,
                rows_returned: runs.len(),
                postgres_query_time,
                datafusion_planning_time,
                datafusion_execution_time,
            },
            distributed: DistributedQueryDiagnostics {
                workers_configured: config.worker_count,
                partitions_planned: partition_count,
                partitions_executed: partition_diagnostics.len(),
                max_in_flight_partitions: worker_limit,
                load_shed: false,
                cancelled: false,
                partitions: partition_diagnostics,
            },
            runs,
        })
    }
}

pub(crate) fn validate_distributed_config(config: &DistributedQueryConfig) -> Result<()> {
    if config.worker_count == 0 {
        bail!("distributed query config requires at least one worker");
    }
    if config.max_segments_per_partition == 0 {
        bail!("distributed query config requires max_segments_per_partition > 0");
    }
    if config.max_in_flight_partitions == 0 {
        bail!("distributed query config requires max_in_flight_partitions > 0");
    }
    Ok(())
}

pub(crate) fn reject_if_cancelled(cancellation: &DistributedQueryCancellation) -> Result<()> {
    if cancellation.is_cancelled() {
        bail!("query cancelled by coordinator");
    }
    Ok(())
}

pub(crate) fn shed_if_overloaded(partitions: usize, config: &DistributedQueryConfig) -> Result<()> {
    if let Some(limit) = config.max_queued_partitions
        && partitions > limit
    {
        bail!("query load-shed: planned partitions {partitions} exceed queue limit {limit}");
    }
    Ok(())
}

fn enforce_worker_budgets(
    partitions: &[DistributedQueryPartition],
    config: &DistributedQueryConfig,
) -> Result<()> {
    for partition in partitions {
        if let Some(limit) = config.max_estimated_object_store_requests_per_worker
            && partition.estimated_object_store_requests > limit
        {
            bail!(
                "query rejected: worker {} estimated object-store requests {} exceed per-worker limit {}",
                partition.worker_index,
                partition.estimated_object_store_requests,
                limit
            );
        }
        if let Some(limit) = config.max_candidate_bytes_per_worker
            && partition.candidate_bytes > limit
        {
            bail!(
                "query rejected: worker {} candidate bytes {} exceed per-worker limit {}",
                partition.worker_index,
                partition.candidate_bytes,
                limit
            );
        }
    }
    Ok(())
}

fn plan_partitions(
    plan: RunQueryPlan,
    config: &DistributedQueryConfig,
) -> Result<Vec<DistributedQueryPartition>> {
    plan.segments
        .chunks(config.max_segments_per_partition)
        .enumerate()
        .map(|(partition_index, segments)| {
            let segments = segments.to_vec();
            Ok(DistributedQueryPartition {
                worker_index: partition_index % config.worker_count,
                project_names: partition_project_names(&segments),
                candidate_runs: partition_candidate_runs(&segments),
                candidate_bytes: segments.iter().map(|segment| segment.total_bytes).sum(),
                estimated_object_store_requests: estimate_vortex_object_store_requests(
                    segments.len(),
                ),
                segments,
            })
        })
        .collect()
}

fn partition_project_names(segments: &[SegmentSource]) -> Vec<String> {
    segments
        .iter()
        .flat_map(|segment| {
            segment
                .candidate_rows
                .iter()
                .map(|row| row.project_name.clone())
        })
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn partition_candidate_runs(segments: &[SegmentSource]) -> usize {
    segments
        .iter()
        .flat_map(|segment| {
            segment.candidate_rows.iter().map(|row| RunKey {
                project_name: row.project_name.clone(),
                trace_id: row.trace_id.clone(),
                span_id: row.span_id.clone(),
            })
        })
        .collect::<HashSet<_>>()
        .len()
}

fn worker_query(query: &RunQuery) -> RunQuery {
    let mut worker_query = query.clone();
    worker_query.limit = query
        .limit
        .map(|limit| limit.saturating_add(query.offset.unwrap_or(0)));
    worker_query.offset = None;
    worker_query
}

async fn execute_partition(
    object_store: Arc<dyn object_store::ObjectStore>,
    partition: DistributedQueryPartition,
    query: RunQuery,
    candidate_run_keys: Arc<HashSet<RunKey>>,
) -> Result<PartitionExecutionResult> {
    let segment_count = partition.segments.len();
    let candidate_runs = partition.candidate_runs;
    let candidate_bytes = partition.candidate_bytes;
    let estimated_object_store_requests = partition.estimated_object_store_requests;
    let worker_index = partition.worker_index;
    let project_names = partition.project_names;
    let (runs, timing, reads) = query_segment_sources_with_datafusion_timed(
        object_store,
        partition.segments,
        &query,
        query.include_payload,
        Some(candidate_run_keys.as_ref()),
    )
    .await?;
    let rows_returned = runs.len();

    Ok(PartitionExecutionResult {
        runs,
        diagnostics: DistributedQueryPartitionDiagnostics {
            worker_index,
            project_names,
            segment_count,
            candidate_runs,
            candidate_bytes,
            estimated_object_store_requests,
            rows_returned,
            actual_object_store_requests: reads.request_count(),
            actual_object_store_bytes_read: reads.bytes_read,
            datafusion_planning_time: timing.planning_time,
            datafusion_execution_time: timing.execution_time,
        },
        timing,
        reads,
    })
}

pub(crate) fn add_snapshots(
    left: ObjectStoreReadSnapshot,
    right: ObjectStoreReadSnapshot,
) -> ObjectStoreReadSnapshot {
    ObjectStoreReadSnapshot {
        get_requests: left.get_requests.saturating_add(right.get_requests),
        get_ranges_requests: left
            .get_ranges_requests
            .saturating_add(right.get_ranges_requests),
        head_requests: left.head_requests.saturating_add(right.head_requests),
        list_requests: left.list_requests.saturating_add(right.list_requests),
        bytes_read: left.bytes_read.saturating_add(right.bytes_read),
    }
}
