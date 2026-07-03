# Performance

Performance is part of the storage contract. Query work should be planned from
Postgres metadata before object storage is opened, and unsupported search modes
must fail clearly instead of falling back to broad payload scans.

## Local MVP Budgets

These budgets target the default synthetic benchmark dataset on a developer
laptop using mockgres and the in-memory object store. They are guardrails, not
published parity claims.

| Workload | Latency budget | Fanout budget |
| --- | --- | --- |
| Ingest batch ack | p99 <= 150 ms | Object PUTs <= flushed segments |
| Single run load | p99 <= 150 ms | Candidate segments <= 1 |
| Trace tree load | p99 <= 150 ms | Candidate segments <= runs in trace |
| Selective project filter | p99 <= 200 ms | Candidate segments < project total when time filters narrow |
| Feedback filter | p99 <= 20 ms | Object-store requests = 0 |

SmithDB parity targets remain tracked in `SMITHDB_PARITY_PLAN.md`; these local
budgets are intentionally smaller-scope checks for day-to-day development.

## Phase 0 Baseline

Command:

```bash
just bench-core
```

Date: 2026-07-03

Dataset:

| Setting | Value |
| --- | ---: |
| traces | 24 |
| runs per trace | 8 |
| records | 192 |
| active segments | 12 |
| feedback records | 52 |
| iterations | 5 |

Results:

| Workload | p50 | p95/p99 | Candidate segments | Vortex files | Object requests | Bytes read |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| ingest ack | 83.8 ms | 88.2 ms | 12 | 0 | 12 | 0 |
| single run load | 74.5 ms | 80.1 ms | 1 | 1 | 50 | 163,260 |
| trace tree load | 62.9 ms | 67.6 ms | 8 | 8 | 140 | 496,200 |
| project run filtering | 89.3 ms | 97.7 ms | 12 | 12 | 360 | 1,890,160 |
| feedback filtering | 1.0 ms | 1.3 ms | 0 | 0 | 0 | 0 |
| root tree predicate | 91.0 ms | 94.5 ms | 12 | 12 | 360 | 1,890,160 |
| child tree predicate | 90.2 ms | 93.2 ms | 12 | 12 | 360 | 1,890,160 |

Unsupported workloads are deliberately reported as unsupported:

| Workload | Reason |
| --- | --- |
| thread trace listing | Thread materialization is not implemented; no payload metadata scan fallback. |
| aggregate scans | Aggregate API and typed rollups are not implemented yet. |

## Planner Snapshots

Single run load:

```text
Postgres:
  run_locators(run_id or generated_run_id)
  -> trace_segments(id = trace_segment_id, compacted_at IS NULL)
  -> run_heads/run_deletions for delete filtering
DataFusion:
  open exactly the locator segment
  filter project_name, trace_id, run_id/span_id, row_index
Budget assertion:
  candidate_segments = 1
```

Trace tree load:

```text
Postgres:
  trace_locators(project_name, trace_id)
  -> active trace_segment URIs
  -> run_deletions for delete filtering
DataFusion:
  open trace-relevant current segments only
  reconstruct latest run heads and tree in memory
Budget assertion:
  candidate_segments <= current runs in the trace
```

Selective project/time filtering:

```text
Postgres:
  run_heads(project_name, start_time_unix_nano, trace_segment_id)
  -> trace_segments(compacted_at IS NULL)
DataFusion:
  open candidate segments returned by metastore pruning
Budget assertion:
  narrowed time filters do not read every project segment
```

Feedback filtering:

```text
Postgres:
  feedback table filters by run_id/key/offset/limit
Object storage:
  no reads
Budget assertion:
  candidate_segments = 0 and object_store_requests = 0
```

## Acceptance Tests

Current query fanout assertions live in library tests:

- `query_diagnostics_report_segment_fanout`
- `trace_query_diagnostics_reject_project_wide_fanout_when_trace_is_known`
- `project_time_filter_diagnostics_reject_full_project_fanout`
- `direct_run_lookup_uses_current_locator_after_stale_segment_deleted`

New query features should add a similar assertion or a benchmark note before
being considered complete.
