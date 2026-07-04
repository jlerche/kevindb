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
| Selective scalar filter | p99 <= 200 ms | Candidate segments scale with indexed candidates |
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
| ingest ack | 78.0 ms | 85.4 ms | 12 | 0 | 12 | 0 |
| single run load | 66.6 ms | 68.7 ms | 1 | 1 | 45 | 162,660 |
| trace tree load | 41.6 ms | 42.5 ms | 1 | 1 | 35 | 158,800 |
| project run filtering | 87.9 ms | 88.7 ms | 12 | 12 | 360 | 1,890,160 |
| feedback filtering | 0.9 ms | 1.2 ms | 0 | 0 | 0 | 0 |
| root tree predicate | 86.9 ms | 89.1 ms | 12 | 12 | 360 | 1,890,160 |
| child tree predicate | 87.5 ms | 91.1 ms | 12 | 12 | 360 | 1,890,160 |
| thread trace listing rejection | 0.00002 ms | 0.00005 ms | 0 | 0 | 0 | 0 |
| aggregate scan rejection | 0.00002 ms | 0.00008 ms | 0 | 0 | 0 | 0 |

Thread trace listing and aggregate scans are deliberately benchmarked as
measured rejection paths until their storage models exist:

| Workload | Reason |
| --- | --- |
| thread trace listing | Thread materialization is not implemented; no payload metadata scan fallback. |
| aggregate scans | Aggregate API and typed rollups are not implemented yet. |

## Phase 2 Filtering Snapshot

Command:

```bash
just bench-core
```

Date: 2026-07-04

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
| ingest ack | 150.7 ms | 156.2 ms | 12 | 0 | 12 | 0 |
| single run load | 69.0 ms | 72.7 ms | 1 | 1 | 45 | 162,660 |
| trace tree load | 43.5 ms | 44.9 ms | 1 | 1 | 35 | 158,800 |
| project run filtering | 96.5 ms | 99.5 ms | 12 | 12 | 420 | 1,912,720 |
| selective scalar filtering | 49.7 ms | 51.9 ms | 1 | 1 | 30 | 156,640 |
| nonselective scalar filtering | 82.8 ms | 83.6 ms | 7 | 7 | 210 | 1,106,160 |
| feedback filtering | 0.8 ms | 1.4 ms | 0 | 0 | 0 | 0 |
| root tree predicate | 90.1 ms | 93.7 ms | 12 | 12 | 360 | 1,890,160 |
| child tree predicate | 92.9 ms | 95.2 ms | 12 | 12 | 420 | 1,912,720 |
| thread trace listing rejection | 0.00002 ms | 0.00014 ms | 0 | 0 | 0 | 0 |
| aggregate scan rejection | 0.00002 ms | 0.00002 ms | 0 | 0 | 0 | 0 |

The selective scalar filter uses indexed metadata and projects payload fields
out of the Vortex scan. The nonselective scalar filter uses an indexed tag that
matches most runs, so fanout is bounded by the newest-first limit rather than by
a full project payload scan.

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

Interactive scalar filtering:

```text
Postgres:
  run_heads(project_name, start_time_unix_nano, trace_segment_id)
  -> run_tags/run_metadata/feedback scalar indexes when filters reference them
  -> trace_segments(compacted_at IS NULL)
DataFusion:
  open candidate segments returned by metastore pruning
  push project/trace/run_type/start_time predicates into each Vortex source
  omit attributes_json when select excludes payload fields
Budget assertion:
  candidate segment/request/byte limits are enforced before object-store reads
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
- `ingest::tests::phase2::filters_use_scalar_indexes_feedback_and_projection`
- `ingest::tests::phase2::planner_rejects_queries_that_exceed_fanout_limits`
- `query::tests::datafusion_sql_pushes_projection_and_source_predicates`

New query features should add a similar assertion or a benchmark note before
being considered complete.

The full local gate also runs the cheap benchmark smoke mode from
`kevindb-bench`, which verifies benchmark wiring without requiring the full
mockgres-backed core benchmark on every check.
