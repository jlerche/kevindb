# Kevindb Agent Guide

## Mission

- Build a Rust database for agent observability, inspired by SmithDB.
- Treat object storage as the durable payload layer and Postgres as the metadata/index layer.
- Keep ingest workers stateless: in-memory buffers are batching only, never durable state.
- Prefer OTLP-compatible ingest at the boundary, with internal normalized span/run records.
- Use Vortex for segment storage from the beginning so layout and query paths grow around it.

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

## Style

- Keep parsing and normalization pure where possible.
- Keep side effects behind explicit adapters.
- Prefer small modules with clear ownership over broad utility modules.
- Add abstractions only when they reduce real coupling or compile cost.
