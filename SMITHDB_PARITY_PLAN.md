# SmithDB Parity Plan

This plan describes how KevinDB gets to practical SmithDB parity using public
SmithDB and LangSmith material. Performance is treated as a product feature:
features should not be considered complete until their latency, object-store
request count, bytes read, and fanout behavior are measured and bounded.

## Source Baseline

Primary sources:

- SmithDB architecture and performance:
  <https://www.langchain.com/blog/introducing-smithdb>
- SmithDB inverted index design:
  <https://www.langchain.com/blog/full-text-search-in-smithdb-designing-an-inverted-index-for-object-storage>
- SmithDB inverted index construction/query path:
  <https://www.langchain.com/blog/full-text-search-in-smithdb-constructing-and-querying-our-inverted-index-pt-2>
- LangSmith trace querying:
  <https://docs.langchain.com/langsmith/export-traces>
- LangSmith UI filtering behavior:
  <https://docs.langchain.com/langsmith/filter-traces-in-application>
- LangSmith run data format:
  <https://docs.langchain.com/langsmith/run-data-format>
- LangSmith feedback data format:
  <https://docs.langchain.com/langsmith/feedback-data-format>
- LangSmith thread traces API:
  <https://docs.langchain.com/langsmith/smith-api/threads/query-thread-traces>

SmithDB public architecture claims:

- Rust implementation.
- Apache DataFusion query engine.
- Vortex file toolkit for object-storage-backed trace data.
- Object storage as the durable trace payload layer.
- Postgres as a small metastore for segment metadata.
- Stateless ingestion, query, and compaction services.
- Workloads: random access, interactive filtering, full-text search, JSON
  filtering, tree-aware queries, thread reconstruction, and aggregations.

Published SmithDB performance targets:

| Workload | Published SmithDB latency |
| --- | --- |
| Trace tree load | P50 92 ms, P99 595 ms |
| Single run load | P50 71 ms, P99 358 ms |
| Runs filtering | P50 82 ms, P99 434 ms |
| Trace ingestion | P50 630 ms, P99 1.47 s |
| Full-text search | P50 400 ms, P99 870 ms |
| Threads filtering | P50 131 ms, P95 268 ms |

These are parity targets, not immediate local MVP requirements. Early phases
should still add benchmarks and budget checks so regressions are visible before
the system grows.

## Current Position

Implemented or in progress:

- OTLP-compatible ingest boundary.
- In-memory object store for local development and configurable cached object
  store wrapping.
- Vortex segment writes and DataFusion query execution.
- Postgres metastore for projects, trace segments, span rows, run heads, and
  feedback.
- LangSmith-compatible initial runs/query/feedback APIs.
- Partitioned ingest buffers, run event metadata, segment pruning, compaction,
  delete vectors, and retention hooks.
- Benchmark harness, query diagnostics, and object-store accounting for local
  performance visibility, including measured rejection fixtures for deferred
  thread and aggregate workloads.
- Direct single-run and trace row-locator lookup through `run_locators` and
  `trace_locators` plus Vortex segment row indexes and summary/full/events
  projection options.
- Idempotent run event writes, tombstone event lineage, and replay tests for
  current-head decisions, including rollback of metadata-visible segments when
  an idempotency conflict is discovered after object write.

Important gaps: no production fanout planner, LangSmith query language parser,
tree predicate index, thread materialization, aggregate API, or rollup storage.
Full-text and JSON filtering are intentionally deferred until we implement an
object-store-aware inverted index design. Large payload scans are not an
acceptable fallback.

## Design Principles

- Performance is a feature. Every query path gets latency, bytes-read, segment
  fanout, row-count, and object-request instrumentation.
- Object storage is the durable payload layer. Postgres stores metadata,
  indexes, manifests, coordination state, and materialized heads.
- DataFusion stays the default query engine. Special fast paths are allowed only
  after benchmarks show the general path is insufficient.
- Query planning must happen before object-store reads whenever possible.
- Do not scan large `inputs`, `outputs`, or arbitrary metadata payloads for
  user-facing filters.
- Full-text and JSON filters require a real object-store index: postings,
  positions where needed, row-index masks, bounded GET sizes, compaction-aware
  merge, and DataFusion/Vortex predicate pushdown.
- Every new public API needs compatibility tests against the LangSmith SDK or
  documented HTTP contracts when available.
- Every new storage feature needs crash/retry/idempotency tests around the
  object-store-before-metastore write order.

## [x] Phase 0: Measurement And Guardrails

Goal: make performance visible before adding more query features.

### [x] Epic 0.1: Benchmark Harness

Tasks:

- [x] Add `crates/kevindb-bench` or `benches/` with deterministic data
  generation.
- [x] Generate synthetic projects with configurable:
  - trace count
  - runs per trace
  - tree depth and fanout
  - payload size distribution
  - thread count and traces per thread
  - feedback density
  - error rate
  - token/cost metadata density
- [x] Add benchmark fixtures for:
  - single run load
  - trace tree load
  - project run filtering
  - feedback filtering
  - root and child tree predicates
  - thread trace listing rejection until thread materialization exists
  - aggregate scan rejection until typed rollups exist
  - ingest throughput and ack latency
- [x] Record:
  - p50/p95/p99 latency
  - number of candidate segments
  - number of Vortex files opened
  - bytes read from object store
  - object-store request count
  - DataFusion planning time
  - DataFusion execution time
  - Postgres query time

Subtasks:

- [x] Add a local object-store wrapper that counts GET/HEAD/PUT/LIST
  operations.
- [x] Add a DataFusion execution observer for planning and collection timing.
- [x] Emit benchmark results as JSON for trend comparison.
- [x] Add non-blocking benchmark scripts to `just`.
- [x] Add a small benchmark smoke test to CI-style checks without making local
  development slow.

Exit criteria:

- [x] A developer can run one command and see the current latency and fanout
  profile for the core workloads.
- [x] The benchmark output can be checked into issue/PR discussions.

### [x] Epic 0.2: Performance Budgets

Tasks:

- [x] Define local MVP budgets on a known laptop dataset.
- [x] Define parity targets based on the published SmithDB numbers.
- [x] Add query-plan assertions for common paths:
  - no project query reads all segments when time/project filters exist
  - single run load reads a bounded number of segments
  - trace load reads only trace-relevant segments
  - feedback filter runs in SQL, not Rust after full table load
- [x] Add a "no payload scan" policy for full-text and JSON filters until the
  real index exists.

Subtasks:

- [x] Add a `Performance.md` section or extend this document with current
  measured baselines after each major phase.
- [x] Store query explain/planner snapshots for representative queries.

Exit criteria:

- [x] New features have an explicit performance acceptance test or benchmark
  note.

## [x] Phase 1: Storage Layout And Random Access

Goal: make individual run and trace loads fast without broad segment scans.

### [x] Epic 1.1: Stable Run Event Model

Tasks:

- [x] Finalize internal event types:
  - start
  - update
  - end
  - compacted snapshot
  - tombstone
- [x] Normalize LangSmith and OTLP writes into append-only run events.
- [x] Preserve partial update semantics for long-running runs.
- [x] Track event order using event time plus ingestion sequence.
- [x] Add idempotency keys for retried writes.

Subtasks:

- [x] Add unit tests for:
  - start then end
  - update before end
  - duplicate event retry
  - late event arrival
  - error update
  - streaming events
- [x] Store event lineage in Postgres enough to debug last-head decisions.

Exit criteria:

- [x] `run_heads` is a materialized projection, not the source of truth.
- [x] Replaying events for a run produces the same current head.

### [x] Epic 1.2: Segment Manifests And Row Locators

Tasks:

- [x] Extend segment metadata to record row locators:
  - segment URI
  - row index
  - row group if available
  - event kind
  - project, trace, run/span IDs
  - min/max time
  - root/parent IDs
- [x] Add a `run_locators` or equivalent table keyed by run ID and generated
  run ID.
- [x] Add a `trace_locators` path for trace-level loading.
- [x] Persist per-segment schema version.

Subtasks:

- [x] Add migration and backfill behavior for existing local data.
- [x] Add object-store missing-object detection around row-locator reads.
- [x] Add unit tests for stale locators after compaction.

Exit criteria:

- [x] Loading a single run does not require scanning all current trace
  segments.
- [x] Trace tree load starts from a trace-level manifest, not project-wide
  segments.

### [x] Epic 1.3: Random Access APIs

Tasks:

- [x] Implement direct `QueryEngine::load_run(run_id)`.
- [x] Implement direct `QueryEngine::load_trace(project, trace_id)`.
- [x] Update server `GET /runs/{run_id}` to use direct random access.
- [x] Update trace endpoints to use trace manifests.
- [x] Add optional projection support:
  - summary fields only
  - full payload
  - events

Subtasks:

- [x] Add integration tests deleting stale segments to verify direct loads use
  only current locators.
- [x] Add latency benchmarks for single run and trace tree load.

Exit criteria:

- [x] Single run and trace tree loads have bounded segment fanout.
- [x] Trace tree and single run benchmark baselines are recorded.

## [ ] Phase 2: Interactive Filtering And Fanout Control

Goal: support fast user-facing filters without accidental broad scans.

### [ ] Epic 2.1: Query Language And Filter AST

Tasks:

- [ ] Implement a LangSmith-compatible filter AST.
- [ ] Support documented filter families:
  - equality and inequality
  - contains and does-not-contain for indexed scalar fields only
  - `is one of`
  - numeric comparison
  - boolean filters
  - time ranges
  - error state
  - run type
  - root state
  - tags
  - metadata fields that are materialized as scalar indexes
  - feedback key/score/value filters
- [ ] Explicitly reject full-text and arbitrary JSON predicates until Phase 6.

Subtasks:

- [ ] Parse copied LangSmith query strings.
- [ ] Add structured API filter support for SDK compatibility.
- [ ] Add clear API errors for unsupported operators.
- [ ] Add golden tests using examples from the LangSmith trace query docs.

Exit criteria:

- [ ] The server can accept the common LangSmith filter surface without
  stringly typed ad hoc handling.

### [ ] Epic 2.2: Metadata Indexes

Tasks:

- [ ] Materialize cheap scalar fields into Postgres and/or compact Vortex
  columns:
  - project/session
  - trace ID
  - run ID
  - parent run/span ID
  - root run ID
  - run type
  - name
  - status/error
  - start/end time
  - latency
  - tags
  - selected metadata keys
  - model/provider fields when present
  - token counts and cost fields
- [ ] Add feedback join indexes:
  - run ID
  - trace ID
  - feedback key
  - score/value
  - created time

Subtasks:

- [ ] Define limits for scalar metadata materialization.
- [ ] Avoid copying large payload values into Postgres.
- [ ] Add per-project index cardinality stats.
- [ ] Add migrations and mockgres tests.

Exit criteria:

- [ ] Interactive filters for ordinary trace exploration can be answered by
  metastore pruning plus Vortex scans over narrow candidate segments.

### [ ] Epic 2.3: Fanout-Aware Query Planner

Tasks:

- [ ] Add a planner stage that turns filters into:
  - project partitions
  - time partitions
  - candidate traces
  - candidate run locators
  - candidate segment URIs
- [ ] Estimate fanout before reading object storage.
- [ ] Add adaptive execution:
  - newest-first scans for UI lists
  - early exit for limit/top-K
  - bounded concurrency
  - cancellation
  - backpressure
- [ ] Add hard query safety limits:
  - max candidate segments
  - max object-store requests
  - max bytes read
  - max wall clock

Subtasks:

- [ ] Add planner diagnostics to responses in debug mode.
- [ ] Add tests for high-fanout rejection and partial page behavior.
- [ ] Add benchmarks for selective and nonselective filters.

Exit criteria:

- [ ] Query latency scales with selected partitions and filters, not total
  project size.
- [ ] High-fanout queries fail or degrade explicitly instead of silently
  becoming payload scans.

### [ ] Epic 2.4: DataFusion/Vortex Pushdown

Tasks:

- [ ] Ensure projection pushdown only reads requested columns.
- [ ] Ensure time/project/run-type filters are pushed to DataFusion.
- [ ] Use segment stats to skip row groups before reading data bytes.
- [ ] Add physical-plan tests for critical queries.

Subtasks:

- [ ] Build helpers to assert expected DataFusion plans.
- [ ] Compare Vortex scan bytes with and without projection.

Exit criteria:

- [ ] Common interactive filters read a bounded subset of columns and row
  groups.

## [ ] Phase 3: Tree-Aware Queries

Goal: support filters over root, child, descendant, and trace tree context.

### [ ] Epic 3.1: Tree Index Model

Tasks:

- [ ] Add trace tree metadata:
  - root run ID
  - parent-child edges
  - depth
  - sibling order
  - subtree start/end order or nested-set interval
  - descendant count
- [ ] Maintain this model on ingest and compaction.
- [ ] Support orphan and late parent resolution.

Subtasks:

- [ ] Add migration for tree edge and closure/nested-set metadata.
- [ ] Add tests for:
  - simple tree
  - multiple roots
  - orphan repair
  - child arrives before parent
  - cyclic parent data rejected or guarded

Exit criteria:

- [ ] Trace tree reconstruction does not require reading every run payload.

### [ ] Epic 3.2: Tree Predicate Planning

Tasks:

- [ ] Support documented tree filters:
  - root run has property
  - child/descendant run has property
  - trace contains a matching node
  - return filtered only vs show all vs most relevant
- [ ] Compile tree filters into metastore predicates first.
- [ ] Fetch row data only after candidate trace/run IDs are narrowed.

Subtasks:

- [ ] Add query AST nodes for root and descendant scopes.
- [ ] Add tests based on documented LangSmith tree filter examples.
- [ ] Add benchmark for "trace contains child tool call" over large projects.

Exit criteria:

- [ ] Tree filters avoid scanning all runs in all traces.

## [ ] Phase 4: Thread Reconstruction

Goal: rebuild long-running conversations across many traces quickly.

### [ ] Epic 4.1: Thread Identity And Ingest

Tasks:

- [ ] Extract `thread_id` from metadata/tags according to LangSmith
  conventions.
- [ ] Store thread membership in Postgres:
  - project
  - thread ID
  - trace ID
  - root run ID
  - start/end time
  - preview fields
  - cost/token summaries
  - error preview
- [ ] Support traces without thread IDs as ordinary traces.

Subtasks:

- [ ] Add migration for `threads`, `thread_traces`, and thread-level summaries.
- [ ] Add tests for multi-trace thread ingestion.
- [ ] Add backfill from existing run metadata.

Exit criteria:

- [ ] A thread can be listed without scanning all traces in a project.

### [ ] Epic 4.2: Thread APIs

Tasks:

- [ ] Implement `GET /v2/threads/{thread_id}/traces`.
- [ ] Add thread list/query endpoint compatible with public docs where
  possible.
- [ ] Return fields documented by the thread traces API:
  - token counts
  - cost fields
  - latency
  - previews
  - first token time when available
  - cursor pagination

Subtasks:

- [ ] Add SDK or HTTP integration tests.
- [ ] Add cursor stability tests.
- [ ] Add pagination benchmarks.

Exit criteria:

- [ ] Thread trace listing has bounded Postgres and segment fanout.

### [ ] Epic 4.3: Message/Turn Reconstruction

Tasks:

- [ ] Materialize message previews from runs where possible.
- [ ] Maintain turn ordering across traces in a thread.
- [ ] Store enough metadata to render a Messages view without loading all
  payloads.

Subtasks:

- [ ] Define message extraction rules for common LLM input/output formats.
- [ ] Store preview and locator, not full payload, in Postgres.
- [ ] Add tests for long threads.

Exit criteria:

- [ ] A thread overview loads quickly while full payloads remain in object
  storage.

## [ ] Phase 5: Aggregations And Stats

Goal: compute cost, latency, token usage, evaluator scores, and counts under
filters without full scans.

### [ ] Epic 5.1: Typed Metrics Extraction

Tasks:

- [ ] Extract typed numeric metrics during ingest:
  - latency
  - prompt tokens
  - completion tokens
  - total tokens
  - prompt cost
  - completion cost
  - total cost
  - first token latency
  - evaluator score
- [ ] Normalize provider/model metadata where available.
- [ ] Store typed metrics in run heads and Vortex columns.

Subtasks:

- [ ] Define conversion rules from LangSmith payload fields.
- [ ] Add tests for missing, malformed, and provider-specific values.

Exit criteria:

- [ ] Aggregation inputs are typed columns, not parsed out of JSON at query
  time.

### [ ] Epic 5.2: Aggregate Query Engine

Tasks:

- [ ] Add `RunAggregateQuery`.
- [ ] Support:
  - count
  - error count/rate
  - latency min/max/avg/p50/p95/p99
  - token sums/averages
  - cost sums/averages
  - feedback score averages and distributions
  - group by project, time bucket, run type, tag, model/provider, feedback key
- [ ] Execute through DataFusion for Vortex-backed columns.
- [ ] Join feedback through metastore when selective.

Subtasks:

- [ ] Add API endpoint and internal query API.
- [ ] Add DataFusion aggregation tests.
- [ ] Add fanout-aware planning for aggregate queries.

Exit criteria:

- [ ] Common aggregate queries finish from typed columns with measured fanout.

### [ ] Epic 5.3: Rollups And Sketches

Tasks:

- [ ] Add rollup tables/files for common dashboards:
  - project/time/run_type
  - project/time/model
  - feedback key/time
  - error/time
- [ ] Consider sketches:
  - t-digest for latency percentiles
  - HyperLogLog for approximate cardinality
  - top-K heavy hitters for tags/models/errors

Subtasks:

- [ ] Define freshness requirements.
- [ ] Update rollups during compaction or background jobs.
- [ ] Add invalidation for deletes/retention.

Exit criteria:

- [ ] Dashboard-like stats avoid raw segment scans for common time windows.

## [ ] Phase 6: Full-Text And JSON Filtering

Goal: implement real object-store-aware search and JSON filtering. This phase is
deferred until the core storage, planner, and benchmarks are mature.

### [ ] Epic 6.1: Query Semantics

Tasks:

- [ ] Support the three public SmithDB search shapes:
  - `json_key`: path existence and path pattern search
  - `json_key_search`: path plus value search, including phrase adjacency
  - `search`: full-text search over text columns or JSON values
- [ ] Define operator compatibility with LangSmith filters:
  - full-text input/output filters
  - key-value filters
  - contains and does-not-contain
  - negative filters
  - multi-term filters
- [ ] Define limits:
  - indexed token length
  - key count per run
  - value preview length
  - stop words
  - minimum token length

Subtasks:

- [ ] Document exact deviations from LangSmith where public behavior is
  unclear.
- [ ] Add parser tests before storage implementation.

Exit criteria:

- [ ] The query semantics are stable before index bytes are designed.

### [ ] Epic 6.2: Index Construction

Tasks:

- [ ] Build a streaming JSON flattener/tape:
  - dotted paths
  - leaf values only
  - arrays collapsed to parent path
  - minimal allocation
  - no unnecessary numeric conversion
- [ ] Tokenize values:
  - lowercase
  - split on non-alphanumeric
  - stop-word removal
  - token length cap
- [ ] Intern strings per batch/project.
- [ ] Emit flat occurrence rows:
  - doc ID as Vortex row index
  - term rank
  - position
  - path
- [ ] Sort by term using radix sort or equivalent.

Subtasks:

- [ ] Benchmark JSON parsing separately from index writing.
- [ ] Benchmark std hash vs `ahash`/hashbrown if adopted.
- [ ] Add memory ceiling tests with large payloads.

Exit criteria:

- [ ] Index construction overhead is measured and bounded on ingest.

### [ ] Epic 6.3: Object-Store Index Layout

Tasks:

- [ ] Write sibling index files per indexed column.
- [ ] Use Vortex-compatible custom layout or binary columns.
- [ ] Store row-group dictionaries as FSTs.
- [ ] Store postings and positions as block-bitpacked delta lists.
- [ ] Keep postings and positions in separate byte ranges.
- [ ] Use byte-sized row group thresholds, not term-count-only thresholds.
- [ ] Align postings and positions chunks where phrase queries need both.

Subtasks:

- [ ] Define thresholds for:
  - row group postings bytes
  - term count
  - raw term bytes
  - aligned chunk bytes
  - mid-term position spill
- [ ] Add decode tests against known postings/positions.
- [ ] Add worst-case Zipfian term tests.

Exit criteria:

- [ ] A selective term lookup has bounded GET count and bounded bytes read.

### [ ] Epic 6.4: Query Integration

Tasks:

- [ ] Add DataFusion/Vortex predicate pushdown for search expressions.
- [ ] Route each predicate per segment:
  - use index
  - scan typed/core column if index absent and column is safe to scan
  - short-circuit no-match
  - reject unsafe payload scan
- [ ] Compose core file and sibling index files as one logical segment.
- [ ] Return row-index masks into the core scan.

Subtasks:

- [ ] Add physical-plan tests proving predicates use indexes.
- [ ] Add object-store request-count tests.
- [ ] Add fallbacks only for small/safe columns.

Exit criteria:

- [ ] Full-text/JSON queries never silently scan large payloads.

### [ ] Epic 6.5: Index Compaction And L0/L1 Freshness

Tasks:

- [ ] Write L0 indexes inline during ingestion.
- [ ] Keep recent indexes local to the writer node when a cluster manager
  exists.
- [ ] Promote L0 to durable object storage.
- [ ] Merge index files during compaction with streaming memory behavior.
- [ ] Keep doc IDs aligned to compacted Vortex row order.

Subtasks:

- [ ] Add index merge tests with doc ID remapping.
- [ ] Add crash tests for core segment written but index not written.
- [ ] Add query routing behavior for missing index files.

Exit criteria:

- [ ] Recently ingested data becomes searchable quickly without an inconsistent
  separate live-tail path.

## [ ] Phase 7: Compaction, Retention, And Lifecycle

Goal: make immutable object storage practical over time.

### [ ] Epic 7.1: Compaction Service

Tasks:

- [ ] Move explicit compaction into a service loop.
- [ ] Use Postgres leases for compaction work.
- [ ] Compact by project/time partition.
- [ ] Rewrite small segments into query-optimized segments.
- [ ] Merge run events into compacted snapshots when safe.
- [ ] Merge or rebuild indexes when Phase 6 exists.

Subtasks:

- [ ] Add lease migration and mockgres tests.
- [ ] Add compaction idempotency.
- [ ] Add orphaned object reconciliation.

Exit criteria:

- [ ] Compaction can be run concurrently without corrupting manifests.

### [ ] Epic 7.2: Retention And Deletes

Tasks:

- [ ] Enforce project retention policies.
- [x] Materialize delete vectors per segment.
- [ ] Apply delete masks in query planning and DataFusion scans.
- [ ] Reclaim compacted/deleted objects after a grace period.

Subtasks:

- [ ] Add retention service tests.
- [ ] Add delete-vector pushdown tests.
- [ ] Add object cleanup dry-run mode.

Exit criteria:

- [ ] Deletes and retention do not require rewriting every affected object
  immediately.

## [ ] Phase 8: Distributed Query And Cluster Manager

Goal: scale compute while preserving object-storage durability and low fanout.

### [ ] Epic 8.1: Service Roles

Tasks:

- [ ] Define stateless service roles:
  - ingestion
  - query
  - compaction
  - coordination/cluster manager
- [ ] Add process-level config for each role.
- [ ] Add health/readiness checks per role.

Subtasks:

- [ ] Split heavy crates further to keep compile times controlled.
- [ ] Add local multi-process integration tests.

Exit criteria:

- [ ] Local development can run services separately.

### [ ] Epic 8.2: Sticky Routing And L0

Tasks:

- [ ] Route project/tenant queries to the node that recently ingested that
  scope.
- [ ] Expose local L0 segments/indexes to queries on that node.
- [ ] Fall back to object-store L1 segments for older data.
- [ ] Keep query semantics identical across tiers.

Subtasks:

- [ ] Add routing metadata to Postgres or a simple coordinator.
- [ ] Add query tests mixing L0 and L1.
- [ ] Add failure tests when the writer node disappears.

Exit criteria:

- [ ] Recent data is queryable quickly without waiting for durable index
  promotion.

### [ ] Epic 8.3: Distributed Fanout

Tasks:

- [ ] Add distributed query execution for high-cardinality scans.
- [ ] Split work by project/time/segment partitions.
- [ ] Merge pages and aggregates deterministically.
- [ ] Bound per-node and total object-store requests.

Subtasks:

- [ ] Add coordinator-side cancellation.
- [ ] Add per-query budget enforcement.
- [ ] Add load-shedding behavior.

Exit criteria:

- [ ] Large project queries scale by adding query workers, not by broadening a
  single worker's object-store fanout.

## [ ] Phase 9: API Parity And Compatibility

Goal: make KevinDB usable by existing LangSmith SDK clients and compatible
frontends where public contracts are available.

### [ ] Epic 9.1: Runs And Traces API

Tasks:

- [ ] Expand `/runs/query` compatibility:
  - IDs
  - project/session selectors
  - trace filters
  - time filters
  - root filters
  - error filters
  - run type
  - tags
  - metadata scalar filters
  - feedback filters
  - cursor pagination
- [ ] Add response fields from the public run data format:
  - inputs
  - outputs
  - extra
  - events
  - tags
  - attachments if supported later

Subtasks:

- [ ] Add Python SDK compatibility tests for each query family.
- [ ] Add OpenAPI snapshots.

Exit criteria:

- [ ] Common `Client.list_runs(...)` usage works without custom client code.

### [ ] Epic 9.2: Feedback API

Tasks:

- [ ] Complete feedback data format parity.
- [ ] Support filtering feedback by project/session, run, trace, key, score,
  value, source, and time.
- [ ] Support feedback aggregations in Phase 5.

Subtasks:

- [ ] Add SDK tests for create/list/read/update feedback if public SDK supports
  it.

Exit criteria:

- [ ] Feedback filters are usable in run queries and aggregate queries.

### [ ] Epic 9.3: Threads API

Tasks:

- [ ] Add thread list/query endpoints.
- [ ] Add thread trace endpoint.
- [ ] Add cursor pagination.
- [ ] Include preview and cost/token fields.

Subtasks:

- [ ] Add tests using documented `/v2/threads/{thread_id}/traces` shape.

Exit criteria:

- [ ] Thread reconstruction can support a LangSmith-like thread UI.

## [ ] Phase 10: Operations, Reliability, And Release Readiness

Goal: make the system credible as a deployable database, not only a local demo.

### [ ] Epic 10.1: Reliability

Tasks:

- [ ] Add crash/restart tests around:
  - object write succeeds, metadata fails
  - metadata commit succeeds, ack fails
  - compaction partially succeeds
  - delete vector written, object cleanup fails
- [ ] Add orphan object reconciliation.
- [ ] Add migration compatibility tests.
- [ ] Add backup/restore notes for Postgres metastore plus object storage.

Exit criteria:

- [ ] Known failure modes are documented and tested.

### [ ] Epic 10.2: Observability

Tasks:

- [ ] Emit metrics:
  - ingest ack latency
  - flush latency
  - segment bytes written
  - compaction throughput
  - query planner time
  - DataFusion execution time
  - object-store requests
  - object-store bytes read
  - query fanout
  - cache hit rate
- [ ] Add structured tracing for query plans.
- [ ] Add slow query logging.

Exit criteria:

- [ ] Users can explain why a query is slow.

### [ ] Epic 10.3: Packaging

Tasks:

- [ ] Add production config files.
- [ ] Add Docker image.
- [ ] Add local compose setup with Postgres-compatible storage and object store.
- [ ] Add S3 object store support.
- [ ] Add self-hosted deployment docs.

Exit criteria:

- [ ] A user can run KevinDB locally and in a basic self-hosted environment.

## Recommended Implementation Order

1. Phase 0: measurement and performance guardrails.
2. Phase 1: row locators and direct random access.
3. Phase 2: filter AST, scalar metadata filtering, fanout planner.
4. Phase 3: tree-aware query indexes.
5. Phase 4: thread reconstruction.
6. Phase 5: aggregations and rollups.
7. Phase 7: production compaction/retention lifecycle.
8. Phase 9: API parity expansion in parallel with phases 2-5.
9. Phase 6: full-text and JSON filtering once the planner, compaction, and
   benchmark infrastructure are mature.
10. Phase 8 and Phase 10: distributed execution and release hardening.

## Explicit Non-Goals Until Phase 6

Until the Phase 6 index exists, do not add payload scan fallbacks for
full-text search, Postgres JSONB storage for large `inputs` or `outputs`,
production token tables, phrase search without positions, JSON path/value
search without path-aware postings, or index formats that cannot be compacted
with Vortex row-order changes.

## Near-Term Next Slices

Recommended next engineering slices:

1. Add a LangSmith filter AST and reject unsupported search/JSON predicates
   explicitly.
2. Add scalar metadata and feedback filters with fanout diagnostics.
3. Add fanout-aware planning limits for high-cardinality filters.
4. Add tree index metadata for root/descendant filters.
5. Add thread materialization for bounded thread trace listing.
