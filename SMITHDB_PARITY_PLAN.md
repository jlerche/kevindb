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
- LangSmith filter-string parsing for indexed scalar fields, explicit rejection
  for deferred search/JSON filters, scalar metadata/tag/feedback indexes, and a
  fanout-aware run-query planner with debug diagnostics and hard limits.
- Thread reconstruction through `threads`, `thread_traces`, and
  `thread_messages`, including bounded previews, cursor pagination, and
  LangSmith-compatible thread query endpoints.

Phase 6 now includes an object-store-aware FST sibling index for full-text and
path-aware JSON predicates. Large payload scans remain an unacceptable fallback.

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

- [x] Use a current-only development schema for row locators; existing legacy
  local data must be reset instead of silently backfilled with incomplete
  generated run IDs or idempotency keys.
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

## [x] Phase 2: Interactive Filtering And Fanout Control

Goal: support fast user-facing filters without accidental broad scans.

### [x] Epic 2.1: Query Language And Filter AST

Tasks:

- [x] Implement a LangSmith-compatible filter AST.
- [x] Support documented filter families:
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
- [x] Explicitly reject full-text and arbitrary JSON predicates until Phase 6.

Subtasks:

- [x] Parse copied LangSmith query strings.
- [x] Add structured API filter support for SDK compatibility.
- [x] Add clear API errors for unsupported operators.
- [x] Add golden tests using examples from the LangSmith trace query docs.

Exit criteria:

- [x] The server can accept the common LangSmith filter surface without
  stringly typed ad hoc handling.

### [x] Epic 2.2: Metadata Indexes

Tasks:

- [x] Materialize cheap scalar fields into Postgres and/or compact Vortex
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
- [x] Add feedback join indexes:
  - run ID
  - trace ID
  - feedback key
  - score/value
  - created time

Subtasks:

- [x] Define limits for scalar metadata materialization.
- [x] Avoid copying large payload values into Postgres.
- [x] Add per-project index cardinality stats.
- [x] Add migrations and mockgres tests.

Exit criteria:

- [x] Interactive filters for ordinary trace exploration can be answered by
  metastore pruning plus Vortex scans over narrow candidate segments.

### [x] Epic 2.3: Fanout-Aware Query Planner

Tasks:

- [x] Add a planner stage that turns filters into:
  - project partitions
  - time partitions
  - candidate traces
  - candidate run locators
  - candidate segment URIs
- [x] Estimate fanout before reading object storage.
- [x] Add adaptive execution:
  - newest-first scans for UI lists
  - early exit for limit/top-K
  - bounded concurrency
  - cancellation
  - backpressure
- [x] Add hard query safety limits:
  - max candidate segments
  - max object-store requests
  - max bytes read
  - max wall clock

Subtasks:

- [x] Add planner diagnostics to responses in debug mode.
- [x] Add tests for high-fanout rejection and partial page behavior.
- [x] Add benchmarks for selective and nonselective filters.

Exit criteria:

- [x] Query latency scales with selected partitions and filters, not total
  project size.
- [x] High-fanout queries fail or degrade explicitly instead of silently
  becoming payload scans.

### [x] Epic 2.4: DataFusion/Vortex Pushdown

Tasks:

- [x] Ensure projection pushdown only reads requested columns.
- [x] Ensure time/project/run-type filters are pushed to DataFusion.
- [x] Push source predicates so DataFusion/Vortex can use segment statistics
  before reading unneeded row data.
- [x] Add SQL/plan-shape tests for critical queries.

Subtasks:

- [x] Build helpers to assert expected DataFusion SQL/plan shape.
- [x] Record Vortex scan bytes for projection-aware filter benchmarks.

Exit criteria:

- [x] Common interactive filters read bounded columns and current row locators;
  row-group pushdown can be added when Vortex exposes row-group metadata.

Phase 2 evidence:

- `src/query/filter.rs` and `src/query/filter/parser.rs` implement the filter
  AST/parser/compiler for the indexed LangSmith filter surface.
- `src/db/migrations/V11__add_phase2_filter_indexes.sql` adds scalar run-head,
  tag, metadata, feedback, and project-cardinality indexes.
- `src/ingest/indexes.rs` materializes bounded scalar indexes and refreshes
  `project_filter_stats` with `COUNT(DISTINCT ...)`.
- `src/query/planner.rs` plans candidate runs, row locators, and segments in
  Postgres, estimates fanout, and enforces pre-read run/segment/request/byte
  limits.
- `src/query/object_store_stats.rs` records actual query read requests and bytes;
  debug diagnostics expose both estimates and measured object-store IO, and the
  wrapper rejects over-budget reads while they are being consumed.
- `src/query.rs` executes candidate segments in bounded DataFusion batches and
  pushes row-locator predicates into each segment source.
- `crates/kevindb-server/src/langsmith.rs` accepts `filter`, `trace_filter`,
  `tree_filter`, `select`, direct `run_ids`, and debug diagnostics while
  rejecting Phase 6 search/JSON predicates clearly.
- Focused tests cover documented filter examples, negative metadata/feedback
  semantics, scalar materialization limits, feedback joins, payload projection,
  high-fanout rejection, DataFusion source-predicate pushdown, server debug
  diagnostics, and unsupported full-text rejection.

## [x] Phase 3: Tree-Aware Queries

Goal: support filters over root, child, descendant, and trace tree context.

### [x] Epic 3.1: Tree Index Model

Tasks:

- [x] Add trace tree metadata:
  - root run ID
  - parent-child edges
  - depth
  - sibling order
  - subtree start/end order or nested-set interval
  - descendant count
- [x] Maintain this model on ingest and compaction.
- [x] Support orphan and late parent resolution.

Subtasks:

- [x] Add migration for tree edge and closure/nested-set metadata.
- [x] Add tests for:
  - simple tree
  - multiple roots
  - orphan repair
  - child arrives before parent
  - cyclic parent data rejected or guarded

Exit criteria:

- [x] Trace tree reconstruction does not require reading every run payload.

### [x] Epic 3.2: Tree Predicate Planning

Tasks:

- [x] Support documented tree filters:
  - root run has property
  - child/descendant run has property
  - trace contains a matching node
  - return filtered only vs show all vs most relevant
- [x] Compile tree filters into metastore predicates first.
- [x] Fetch row data only after candidate trace/run IDs are narrowed.

Subtasks:

- [x] Add query AST nodes for root and descendant scopes.
- [x] Add tests based on documented LangSmith tree filter examples.
- [x] Add benchmark for "trace contains child tool call" over large projects.

Exit criteria:

- [x] Tree filters avoid scanning all runs in all traces.

Phase 3 evidence:
- `V12__add_phase3_tree_indexes.sql` adds tree nodes, edges, intervals, depth, sibling order, descendant counts, and guard flags.
- `src/ingest/tree.rs` refreshes trace tree metadata from current heads on ingest and compaction, and `src/query/tree_access.rs` reconstructs trace trees from metadata without Vortex reads.
- `src/query/tree_filter.rs` and `src/query/planner.rs` compile tree filters into metastore candidate-key predicates before Vortex reads.
- `src/ingest/tests/phase3.rs` covers late parent repair, multiple roots, descendant filters, and cycle guarding; bench workloads now use real tree filters.

## [x] Phase 4: Thread Reconstruction

Goal: rebuild long-running conversations across many traces quickly.

### [x] Epic 4.1: Thread Identity And Ingest

Tasks:

- [x] Extract `thread_id` from metadata/tags according to LangSmith
  conventions.
- [x] Store thread membership in Postgres:
  - project
  - thread ID
  - trace ID
  - root run ID
  - start/end time
  - preview fields
  - cost/token summaries
  - error preview
- [x] Support traces without thread IDs as ordinary traces.

Subtasks:

- [x] Add migration for `threads`, `thread_traces`, and thread-level summaries.
- [x] Add tests for multi-trace thread ingestion.
- [x] Add backfill from existing run metadata.

Exit criteria:

- [x] A thread can be listed without scanning all traces in a project.

### [x] Epic 4.2: Thread APIs

Tasks:

- [x] Implement `GET /v2/threads/{thread_id}/traces`.
- [x] Add thread list/query endpoint compatible with public docs where
  possible.
- [x] Return fields documented by the thread traces API:
  - token counts
  - cost fields
  - latency
  - previews
  - first token time when available
  - cursor pagination

Subtasks:

- [x] Add SDK or HTTP integration tests.
- [x] Add cursor stability tests.
- [x] Add pagination benchmarks.

Exit criteria:

- [x] Thread trace listing has bounded Postgres and segment fanout.

### [x] Epic 4.3: Message/Turn Reconstruction

Tasks:

- [x] Materialize message previews from runs where possible.
- [x] Maintain turn ordering across traces in a thread.
- [x] Store enough metadata to render a Messages view without loading all
  payloads.

Subtasks:

- [x] Define message extraction rules for common LLM input/output formats.
- [x] Store preview and locator, not full payload, in Postgres.
- [x] Add tests for long threads.

Exit criteria:

- [x] A thread overview loads quickly while full payloads remain in object
  storage.

Phase 4 evidence:
- `V13__add_phase4_thread_indexes.sql` adds thread tables, preview locators,
  pagination indexes, and a best-effort backfill from `run_metadata`.
- `src/ingest/thread.rs` extracts `thread_id`/`session_id`, bounded previews,
  message previews, and thread summaries inside the ingest metadata transaction.
- `src/query/threads.rs` implements metastore-only thread list, trace list, and
  message listing with tuple cursors and zero Vortex/object-store fanout.
- `crates/kevindb-server/src/langsmith/threads.rs` exposes the two `/v2/threads`
  contracts with documented fields, select handling, and cursor pagination.
- Phase 4 tests cover multi-trace ingestion, fallback IDs, ordinary traces,
  cursor stability, root-run filtering, long threads, and HTTP compatibility.
- The 2026-07-04 benchmark records real `thread-trace-listing`: p50 0.62 ms,
  p99 1.12 ms, zero candidate segments, and zero object-store requests.

## [x] Phase 5: Aggregations And Stats

Goal: compute cost, latency, token usage, evaluator scores, and counts under
filters without full scans.
### [x] Epic 5.1: Typed Metrics Extraction

Tasks:
- [x] Extract typed numeric metrics during ingest:
  - latency
  - prompt tokens
  - completion tokens
  - total tokens
  - prompt cost
  - completion cost
  - total cost
  - first token latency
  - evaluator score
- [x] Normalize provider/model metadata where available.
- [x] Store typed metrics in run heads and Vortex columns.

Subtasks:
- [x] Define conversion rules from LangSmith payload fields.
- [x] Add tests for missing, malformed, and provider-specific values.

Exit criteria:

- [x] Aggregation inputs are typed columns, not parsed out of JSON at query
  time.

### [x] Epic 5.2: Aggregate Query Engine

Tasks:

- [x] Add `RunAggregateQuery`.
- [x] Support:
  - count
  - error count/rate
  - latency min/max/avg/p50/p95/p99
  - token sums/averages
  - cost sums/averages
  - feedback score averages and distributions
  - group by project, time bucket, run type, tag, model/provider, feedback key
- [x] Execute through DataFusion for Vortex-backed columns.
- [x] Join feedback through metastore when selective.

Subtasks:

- [x] Add API endpoint and internal query API.
- [x] Add DataFusion aggregation tests.
- [x] Add fanout-aware planning for aggregate queries.

Exit criteria:

- [x] Common aggregate queries finish from typed columns with measured fanout.

### [x] Epic 5.3: Rollups And Sketches

Tasks:

- [x] Add rollup tables/files for common dashboards:
  - project/time/run_type
  - project/time/model
  - feedback key/time
  - error/time
- [x] Consider sketches; exact rollup percentiles are used until approximate
  cardinality/top-K needs appear.

Subtasks:

- [x] Define freshness requirements.
- [x] Update rollups during ingest/compaction-visible metadata refresh.
- [x] Add invalidation for deletes/retention.

Exit criteria:

- [x] Dashboard-like stats avoid raw segment scans for common time windows.

Phase 5 evidence: typed metrics live in `src/metrics.rs`, Vortex columns, and
run heads; `src/query/aggregates/`, `V14__add_phase5_aggregates.sql`, feedback
rollups, and `/v1/runs/aggregate` cover aggregate APIs, diagnostics, fanout
limits, rollups, feedback scores, and tests.

## [x] Phase 6: Full-Text And JSON Filtering

Goal: implement real object-store-aware search and JSON filtering.

### [x] Epic 6.1: Query Semantics

- [x] Support `json_key`, `json_key_search`, and `search`.
- [x] Accept SmithDB blog-compatible field-scoped forms
  (`json_key(inputs, "...")`, `json_key_search(inputs, "...", "...")`,
  `search(inputs, "...")`) plus KevinDB's existing shorthand forms.
- [x] Support LangSmith-style payload `eq`, `neq`, `contains`,
  `does_not_contain`, `in`, key existence, negative filters, phrases, and
  multi-term filters for `inputs`, `outputs`, `extra`, and `attributes_json`.
- [x] Define limits in `src/search/`: token length, minimum token length,
  stop words, max indexed value bytes, and max JSON leaf keys per run.
- [x] Reject mixed scalar/search `or(...)` filters rather than evaluating an
  unsafe partial predicate.

### [x] Epic 6.2: Index Construction

- [x] Flatten JSON to dotted leaf paths, collapse arrays to parent paths, and
  avoid numeric conversion beyond scalar string indexing.
- [x] Build a JSON tape with contiguous path/value byte storage instead of
  materializing a `serde_json::Value` tree for indexing.
- [x] Tokenize by lowercasing ASCII alphanumerics, splitting on non-
  alphanumerics, removing stop words, and capping token bytes.
- [x] Intern paths/terms per segment and emit sorted occurrence rows containing
  Vortex row index, path, and position.
- [x] Cover construction limits and phrase/path behavior with unit tests.

### [x] Epic 6.3: Object-Store Index Layout

- [x] Write one durable sibling index object for each Vortex segment before
  metadata commit; persist URI/bytes/schema in `trace_segments`.
- [x] Use a SmithDB-style sibling layout:
  - byte-budgeted row groups
  - `term_key` FST dictionaries for JSON paths
  - `term_value` FST dictionaries keyed as `token\0path`, plus exact scalar
    value terms for payload `eq`, `neq`, and `in`
  - `term_info` offsets into postings and positions blobs
  - block-delta postings with VInt tails
  - fixed header and directory containing absolute byte ranges
  - separate positions bytes fetched and decoded only for phrase checks
- [x] Decode tests cover FST round trips, block-delta postings/positions, and
  bounded leaf-key cases.
- [x] A selective term lookup has bounded object-store fanout: header,
  directory, selected FST/term-info chunks, and exact term postings/positions
  subranges are read before core Vortex row masks are applied.

### [x] Epic 6.4: Query Integration

- [x] Split scalar prefilters to Postgres and Phase 6 predicates to sibling
  index evaluation.
- [x] Route per segment by using the index, short-circuiting no-match row masks,
  and rejecting missing/unsupported indexes instead of scanning payload JSON.
- [x] Compose core Vortex files and sibling indexes as one logical segment by
  returning row-index masks into the existing DataFusion scan.
- [x] Add parser, physical row-mask, object-store request/byte diagnostic, and
  mockgres integration tests.

### [x] Epic 6.5: Index Compaction And L0/L1 Freshness

- [x] Write L0 indexes inline during ingestion and promote them immediately to
  durable object storage before segment metadata commits.
- [x] Compacting a project rewrites active runs through normal ingestion, which
  rebuilds sibling indexes aligned to the compacted Vortex row order.
- [x] Query routing rejects missing indexes, covering the core-segment-written
  but index-missing crash shape without an unsafe live-tail fallback.

Phase 6 evidence: `src/search/`, `src/query/search.rs`,
`src/query/filter/phase6.rs`, `V15__add_phase6_search_indexes.sql`, ingest
metadata wiring, server structured-filter support, streaming JSON construction
tests, range-read diagnostics tests, and `src/ingest/tests/phase6.rs`.

SmithDB blog parity notes:

- The two SmithDB full-text search blog posts are included in the source
  baseline above.
- This phase matches the logical sibling-index shape: `term_key` and
  `term_value` FSTs, `token\0path` keyed values, exact scalar value terms,
  byte-budgeted row groups, term metadata, block-delta postings, separate
  positions, phrase adjacency checks, and row masks aligned to Vortex row
  indexes.
- Current query execution uses object-store byte ranges instead of fetching the
  complete sibling `.search.fst` object. It reads the header and directory,
  prunes row groups by min/max term bounds, fetches selected row-group
  FST/term-info chunks, reads exact postings/positions subranges for matched
  term ordinals, coalesces adjacent ranges, and skips positions ranges for
  non-phrase predicates.
- Current construction builds a compact JSON tape with contiguous path/value
  bytes, interns per-segment search terms, emits occurrence rows, sorts those
  occurrences with an MSD radix pass, and writes byte-budgeted row groups.
  KevinDB reads exact term postings/positions ranges and coalesces adjacent
  object-store ranges rather than adopting SmithDB's fixed ~2 MiB chunk
  abstraction; current segment sizing, max indexed value bytes, and row-group
  budgets keep construction memory bounded for this repo's ingest model.
- Current compaction/freshness follows KevinDB's stateless-ingest storage rule:
  indexes are written durably with each segment before metadata commits, and
  compaction rebuilds sibling indexes through normal ingestion. Search after
  compaction is covered by tests. SmithDB's local L0 indexes, sticky routing,
  object-store L1 promotion, and streaming index merges remain distributed
  runtime work tracked in Phase 8, not a Phase 6 correctness dependency.

## [x] Phase 7: Compaction, Retention, And Lifecycle

Goal: make immutable object storage practical over time.

### [x] Epic 7.1: Compaction Service

Tasks:

- [x] Move explicit compaction into a service loop.
- [x] Use Postgres leases for compaction work.
- [x] Compact by project/time partition.
- [x] Rewrite small segments into query-optimized segments.
- [x] Merge run events into compacted snapshots when safe.
- [x] Merge or rebuild indexes when Phase 6 exists.

Subtasks:

- [x] Add lease migration and mockgres tests.
- [x] Add compaction idempotency.
- [x] Add orphaned object reconciliation.

Exit criteria:

- [x] Compaction can be run concurrently without corrupting manifests.

### [x] Epic 7.2: Retention And Deletes

Tasks:

- [x] Enforce project retention policies.
- [x] Materialize delete vectors per segment.
- [x] Apply delete masks in query planning and DataFusion scans.
- [x] Reclaim compacted/deleted objects after a grace period.

Subtasks:

- [x] Add retention service tests.
- [x] Add delete-vector pushdown tests.
- [x] Add object cleanup dry-run mode.

Exit criteria:

- [x] Deletes and retention do not require rewriting every affected object
  immediately.

## [x] Phase 8: Distributed Query And Cluster Manager

Goal: scale compute while preserving object-storage durability and low fanout.

### [x] Epic 8.1: Service Roles

Tasks:

- [x] Define stateless service roles:
  - ingestion
  - query
  - compaction
  - coordination/cluster manager
- [x] Add process-level config for each role.
- [x] Add health/readiness checks per role.

Subtasks:

- [x] Split heavy crates further to keep compile times controlled.
- [x] Add local multi-process integration tests.

Exit criteria:

- [x] Local development can run services separately.

### [x] Epic 8.2: Sticky Routing And L0

Tasks:

- [x] Route project/tenant queries to the node that recently ingested that
  scope through durable project route metadata.
- [x] Expose local L0 segments/indexes to queries on that node through the
  write-through object-store cache.
- [x] Fall back to object-store L1 segments for older or non-local data.
- [x] Keep query semantics identical across tiers by using the same
  `ObjectStore` interface for L0 cache hits and L1 reads.

Subtasks:

- [x] Add routing metadata to Postgres or a simple coordinator.
- [x] Add cache/query-path tests mixing L0 and L1 reads.
- [x] Add failure tests when the writer node disappears.

Exit criteria:

- [x] Recent data is queryable quickly without waiting for durable index
  promotion.

Progress note: `CachedObjectStore` is now write-through for single-object puts.
The writer node can serve just-written Vortex segments and `.search.fst`
sibling indexes from local L0 cache, including range reads sliced from the
cached full object. Ingest stamps `project_routes` with the most recent node
id and segment URI so a coordinator can send fresh project queries to the warm
node; a node without that local cache falls back to durable object-store L1.

### [x] Epic 8.3: Distributed Fanout

Tasks:

- [x] Add distributed query execution for high-cardinality scans.
- [x] Split work by project/time/segment partitions.
- [x] Merge pages and aggregates deterministically.
- [x] Bound per-node and total object-store requests.

Subtasks:

- [x] Add coordinator-side cancellation.
- [x] Add per-query budget enforcement.
- [x] Add load-shedding behavior.

Exit criteria:

- [x] Large project queries scale by adding query workers, not by broadening a
  single worker's object-store fanout.

## [x] Phase 9: API Parity And Compatibility

Goal: make KevinDB usable by existing LangSmith SDK clients and compatible
frontends where public contracts are available.

### [x] Epic 9.1: Runs And Traces API

Tasks:

- [x] Expand `/runs/query` compatibility for IDs, project/session selectors,
  trace/time/root/error/run-type/tag/metadata/feedback filters, and cursors.
- [x] Add public run response fields: inputs, outputs, extra, events, tags,
  and attachments if supported later.

Subtasks:

- [x] Add Python SDK compatibility tests for each query family.
- [x] Add OpenAPI snapshots.

Exit criteria:

- [x] Common `Client.list_runs(...)` usage works without custom client code.

### [x] Epic 9.2: Feedback API

Tasks:

- [x] Complete feedback data format parity.
- [x] Support filtering feedback by project/session, run, trace, key, score,
  value, source, and time.
- [x] Support feedback aggregations in Phase 5.

Subtasks:

- [x] Add SDK tests for create/list/read/update feedback if public SDK supports
  it.

Exit criteria:

- [x] Feedback filters are usable in run queries and aggregate queries.

### [x] Epic 9.3: Threads API

Tasks:

- [x] Add thread list/query endpoints.
- [x] Add thread trace endpoint.
- [x] Add cursor pagination.
- [x] Include preview and cost/token fields.

Subtasks:

- [x] Add tests using documented `/v2/threads/{thread_id}/traces` shape.

Exit criteria:

- [x] Thread reconstruction can support a LangSmith-like thread UI.

## [x] Phase 10: Operations, Reliability, And Release Readiness

Goal: make the system credible as a deployable database, not only a local demo.

Tasks:

- [x] Add crash/restart tests for ingest, compaction, delete vectors, and ack
  failure boundaries.
- [x] Add orphan object reconciliation plus backup/restore notes for the
  Postgres metastore and object storage.
- [x] Emit production metrics for ingest, compaction, query planning/execution,
  object-store I/O, fanout, and cache behavior.
- [x] Add structured query-plan tracing and slow query logging.
- [x] Add production config, Docker/local compose, S3 support, and
  self-hosted deployment docs.

Exit criteria:

- [x] Known failure modes are documented and tested, users can explain slow
  queries, and KevinDB can run locally and in a basic self-hosted environment.

## Explicit Non-Goals Until Phase 6
Until the Phase 6 index exists, do not add payload scan fallbacks for
full-text search, Postgres JSONB storage for large `inputs` or `outputs`,
production token tables, phrase search without positions, JSON path/value
search without path-aware postings, or index formats that cannot be compacted
with Vortex row-order changes.
