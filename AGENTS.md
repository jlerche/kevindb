# Kevindb Agent Guide

## Mission

- Build a Rust database for agent observability, inspired by SmithDB.
- Treat object storage as the durable payload layer and Postgres as the metadata/index layer.
- Keep ingest workers stateless: in-memory buffers are batching only, never durable state.
- Prefer OTLP-compatible ingest at the boundary, with internal normalized span/run records.
- Use Vortex for segment storage from the beginning so layout and query paths grow around it.
- Treat performance as a product feature. A feature is not complete until its query fanout, object-store request count, bytes read, and latency behavior are understood.

## Required Checks

Run the full local gate before handing off code:

```bash
just check
```

The gate enforces:

- `cargo fmt --all -- --check`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `cargo test --workspace`
- `cargo llvm-cov --workspace --lib --fail-under-lines 80`
- no human-maintained source file over 1000 lines

The `check` recipe delegates to `scripts/check.sh`; keep the script as the single source of truth for the gate.

`cargo-llvm-cov` is the standard coverage tool for this repo. Tarpaulin may be installed locally, but do not use it as the default project gate unless the repo policy changes.

## Coverage Policy

- Maintain at least 80% line coverage for library unit tests.
- Integration tests are still valuable for Postgres/object-store wiring, but they do not count toward the required coverage threshold.
- Put focused unit tests near the code they cover, especially around encoding, normalization, metadata decisions, and retry/idempotency behavior.
- When a bug is fixed, add a unit test that fails without the fix unless the behavior is only observable through integration wiring.

## File Size

- Keep human-maintained source, scripts, config, and docs at or below 1000 lines per file.
- Exclude generated/build artifacts and lockfiles from the size rule.
- Start splitting around 700 lines if a file is accumulating multiple responsibilities.

## Crate Boundaries

Be aggressive about subcrates as the project grows. Heavy dependencies should live behind narrow crates so simple changes do not rebuild the world.

Prefer this shape over one large crate:

- `kevindb-core`: dependency-light domain types and pure logic.
- `kevindb-otlp`: OTLP decoding and normalization.
- `kevindb-vortex`: Vortex segment encoding/decoding.
- `kevindb-metastore-postgres`: migrations and Postgres metadata operations.
- `kevindb-ingest`: buffering, flush coordination, and durable ack orchestration.
- `kevindb-server`: HTTP/gRPC surfaces and process wiring.

Do not add Vortex, Postgres, Tokio server, or object-store dependencies to a core crate unless there is a clear boundary reason.

## Storage Rules

- Payload bytes belong in object storage.
- Postgres stores metadata, indexes, materialized run heads, segment manifests, leases, and coordination state.
- Write object data before committing metadata that points at it. Orphaned objects are acceptable for now and can be reconciled later; metadata pointing to missing objects is worse.
- Use `mockgres` for Postgres-dependent tests by default.
- Use `object_store::memory::InMemory` for tests unless the behavior specifically requires another backend.

## Performance Rules

- Plan queries before reading object storage. Prefer narrowing by project, time bucket, trace/run locator, segment manifest, and cheap scalar metadata before opening Vortex files.
- Avoid broad scans as a hidden fallback. If a query would need to scan large payload columns or too many segments, reject it clearly or require an explicit bounded mode.
- Instrument performance-sensitive paths with enough detail to explain latency: candidate segments, object-store requests, bytes read, DataFusion planning/execution time, and Postgres time.
- Add benchmarks or fanout assertions for new query capabilities, especially random access, filtering, tree queries, thread reconstruction, aggregations, compaction, and index work.
- Keep DataFusion as the default execution path. Add special fast paths only after measurement shows the general path is insufficient.

## Deferred Search And JSON Filtering

- Do not implement production full-text search with a simple token table, Postgres JSONB over large payloads, or payload scans.
- Do not implement arbitrary JSON filtering by scanning `inputs`, `outputs`, `extra`, or large `attributes_json` payloads at query time.
- Full-text and JSON filtering should wait for an object-store-aware index design aligned with the SmithDB blog posts: postings, positions where needed, path-aware JSON keys, row masks, bounded byte ranges, compaction-aware merge, and Vortex/DataFusion pushdown.
- It is acceptable to parse, reject, or mark full-text/JSON predicates as unsupported before that index exists. Silent slow fallback is not acceptable.

## Style

- Keep parsing and normalization pure where possible.
- Keep side effects behind explicit adapters.
- Prefer small modules with clear ownership over broad utility modules.
- Add abstractions only when they reduce real coupling or compile cost.
