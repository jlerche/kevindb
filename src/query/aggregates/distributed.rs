use std::collections::{BTreeSet, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use futures_util::StreamExt;

use super::scan::{AggregateRunRow, load_aggregate_rows_with_datafusion};
use super::{
    RunAggregateQuery, RunAggregateSource, aggregate_query_without_wall_time_limit, aggregate_rows,
    load_feedback_scores, load_tags, reject_old_metric_segments, rollups, validate_aggregate_query,
};
use crate::query::distributed::{
    DistributedQueryCancellation, DistributedQueryConfig, DistributedQueryDiagnostics,
    DistributedQueryPartitionDiagnostics, DistributedRunAggregateResult, add_snapshots,
    reject_if_cancelled, shed_if_overloaded, validate_distributed_config,
};
use crate::query::object_store_stats::{
    ObjectStoreReadSnapshot, enforce_runtime_object_store_limits,
};
use crate::query::planner::estimate_vortex_object_store_requests;
use crate::query::{
    DataFusionQueryTiming, QueryEngine, RunKey, RunQueryDiagnostics, SegmentSource,
    load_run_query_plan,
};

struct AggregatePartition {
    worker_index: usize,
    project_names: Vec<String>,
    segments: Vec<SegmentSource>,
    candidate_runs: usize,
    candidate_bytes: i64,
    estimated_object_store_requests: usize,
}

struct AggregatePartitionResult {
    rows: Vec<AggregateRunRow>,
    diagnostics: DistributedQueryPartitionDiagnostics,
    timing: DataFusionQueryTiming,
    reads: ObjectStoreReadSnapshot,
}

impl QueryEngine {
    pub async fn aggregate_runs_distributed(
        &self,
        query: RunAggregateQuery,
        config: DistributedQueryConfig,
    ) -> Result<DistributedRunAggregateResult> {
        self.aggregate_runs_distributed_with_cancellation(
            query,
            config,
            DistributedQueryCancellation::new(),
        )
        .await
    }

    pub async fn aggregate_runs_distributed_with_cancellation(
        &self,
        query: RunAggregateQuery,
        config: DistributedQueryConfig,
        cancellation: DistributedQueryCancellation,
    ) -> Result<DistributedRunAggregateResult> {
        validate_distributed_config(&config)?;
        if let Some(max_wall_time) = query.limits.max_wall_time {
            let query = aggregate_query_without_wall_time_limit(query);
            return tokio::time::timeout(
                max_wall_time,
                self.aggregate_runs_distributed_without_wall_time(query, config, cancellation),
            )
            .await
            .context("aggregate query exceeded max wall clock")?;
        }

        self.aggregate_runs_distributed_without_wall_time(query, config, cancellation)
            .await
    }

    async fn aggregate_runs_distributed_without_wall_time(
        &self,
        query: RunAggregateQuery,
        config: DistributedQueryConfig,
        cancellation: DistributedQueryCancellation,
    ) -> Result<DistributedRunAggregateResult> {
        let cancellation_for_select = cancellation.clone();
        tokio::select! {
            biased;
            _ = cancellation_for_select.cancelled() => Err(anyhow!("query cancelled by coordinator")),
            result = self.aggregate_runs_distributed_inner(query, config, cancellation) => result,
        }
    }

    async fn aggregate_runs_distributed_inner(
        &self,
        query: RunAggregateQuery,
        config: DistributedQueryConfig,
        cancellation: DistributedQueryCancellation,
    ) -> Result<DistributedRunAggregateResult> {
        validate_aggregate_query(&query)?;
        reject_if_cancelled(&cancellation)?;
        if let Some(result) = rollups::try_rollup_aggregate(&self.postgres_url, &query).await? {
            return Ok(DistributedRunAggregateResult {
                rows: result.rows,
                diagnostics: result.diagnostics,
                distributed: DistributedQueryDiagnostics {
                    workers_configured: config.worker_count,
                    max_in_flight_partitions: config.max_in_flight_partitions,
                    ..DistributedQueryDiagnostics::default()
                },
                source: result.source,
            });
        }

        let run_query = query.to_run_query();
        let postgres_started = Instant::now();
        let plan = load_run_query_plan(&self.postgres_url, &run_query).await?;
        let postgres_query_time = postgres_started.elapsed();
        let (plan, search_index_reads) = super::super::search::apply_phase6_search_indexes(
            Arc::clone(&self.object_store),
            plan,
            &run_query,
        )
        .await?;
        reject_old_metric_segments(&plan.segments)?;

        let candidate_segments = plan.segments.len();
        let candidate_runs = plan.candidate_runs;
        let candidate_bytes = plan.candidate_bytes;
        let estimated_object_store_requests = plan.estimated_object_store_requests;
        let candidate_run_keys = Arc::new(plan.candidate_run_keys);
        let partitions = plan_aggregate_partitions(plan.segments, &config);
        shed_if_overloaded(partitions.len(), &config)?;
        enforce_aggregate_worker_budgets(&partitions, &config)?;

        reject_if_cancelled(&cancellation)?;
        let partition_count = partitions.len();
        let worker_limit = config
            .max_in_flight_partitions
            .min(config.worker_count)
            .max(1);
        let object_store = Arc::clone(&self.object_store);
        let worker_results = futures_util::stream::iter(partitions.into_iter().map(|partition| {
            let object_store = Arc::clone(&object_store);
            let run_query = run_query.clone();
            let candidate_run_keys = Arc::clone(&candidate_run_keys);
            let cancellation = cancellation.clone();
            async move {
                reject_if_cancelled(&cancellation)?;
                execute_aggregate_partition(object_store, partition, run_query, candidate_run_keys)
                    .await
            }
        }))
        .buffer_unordered(worker_limit)
        .collect::<Vec<_>>()
        .await;

        let mut rows = Vec::new();
        let mut partition_diagnostics = Vec::new();
        let mut datafusion_planning_time = Duration::ZERO;
        let mut datafusion_execution_time = Duration::ZERO;
        let mut object_store_reads = ObjectStoreReadSnapshot::default();

        for result in worker_results {
            let result = result?;
            rows.extend(result.rows);
            datafusion_planning_time += result.timing.planning_time;
            datafusion_execution_time += result.timing.execution_time;
            object_store_reads = add_snapshots(object_store_reads, result.reads);
            partition_diagnostics.push(result.diagnostics);
        }

        let total_reads = add_snapshots(object_store_reads, search_index_reads);
        enforce_runtime_object_store_limits(&run_query, total_reads)?;
        let rows = rows
            .into_iter()
            .filter(|row| candidate_run_keys.contains(&row.run_key()))
            .collect::<Vec<_>>();
        let tags = if query.group_by.contains(&super::RunAggregateGroup::Tag) {
            load_tags(&self.postgres_url, &candidate_run_keys).await?
        } else {
            std::collections::HashMap::new()
        };
        let feedback_scores = if query
            .group_by
            .contains(&super::RunAggregateGroup::FeedbackKey)
            || !query.feedback_keys.is_empty()
        {
            load_feedback_scores(&self.postgres_url, &query, &rows).await?
        } else {
            std::collections::HashMap::new()
        };
        let aggregate_rows = aggregate_rows(rows, &query, &tags, &feedback_scores);

        Ok(DistributedRunAggregateResult {
            diagnostics: RunQueryDiagnostics {
                candidate_segments,
                candidate_runs,
                candidate_bytes,
                estimated_object_store_requests,
                actual_object_store_requests: total_reads.request_count(),
                actual_object_store_bytes_read: total_reads.bytes_read,
                vortex_files_opened: candidate_segments,
                rows_returned: aggregate_rows.len(),
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
            rows: aggregate_rows,
            source: RunAggregateSource::Vortex,
        })
    }
}

fn plan_aggregate_partitions(
    segments: Vec<SegmentSource>,
    config: &DistributedQueryConfig,
) -> Vec<AggregatePartition> {
    segments
        .chunks(config.max_segments_per_partition)
        .enumerate()
        .map(|(partition_index, segments)| {
            let segments = segments.to_vec();
            AggregatePartition {
                worker_index: partition_index % config.worker_count,
                project_names: partition_project_names(&segments),
                candidate_runs: partition_candidate_runs(&segments),
                candidate_bytes: segments.iter().map(|segment| segment.total_bytes).sum(),
                estimated_object_store_requests: estimate_vortex_object_store_requests(
                    segments.len(),
                ),
                segments,
            }
        })
        .collect()
}

fn enforce_aggregate_worker_budgets(
    partitions: &[AggregatePartition],
    config: &DistributedQueryConfig,
) -> Result<()> {
    for partition in partitions {
        if let Some(limit) = config.max_estimated_object_store_requests_per_worker
            && partition.estimated_object_store_requests > limit
        {
            bail!(
                "aggregate query rejected: worker {} estimated object-store requests {} exceed per-worker limit {}",
                partition.worker_index,
                partition.estimated_object_store_requests,
                limit
            );
        }
        if let Some(limit) = config.max_candidate_bytes_per_worker
            && partition.candidate_bytes > limit
        {
            bail!(
                "aggregate query rejected: worker {} candidate bytes {} exceed per-worker limit {}",
                partition.worker_index,
                partition.candidate_bytes,
                limit
            );
        }
    }
    Ok(())
}

async fn execute_aggregate_partition(
    object_store: Arc<dyn object_store::ObjectStore>,
    partition: AggregatePartition,
    query: crate::query::RunQuery,
    candidate_run_keys: Arc<HashSet<RunKey>>,
) -> Result<AggregatePartitionResult> {
    let segment_count = partition.segments.len();
    let candidate_runs = partition.candidate_runs;
    let candidate_bytes = partition.candidate_bytes;
    let estimated_object_store_requests = partition.estimated_object_store_requests;
    let worker_index = partition.worker_index;
    let project_names = partition.project_names;
    let (rows, timing, reads) = load_aggregate_rows_with_datafusion(
        object_store,
        partition.segments,
        &query,
        Some(candidate_run_keys.as_ref()),
    )
    .await?;
    let rows_returned = rows.len();

    Ok(AggregatePartitionResult {
        rows,
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
