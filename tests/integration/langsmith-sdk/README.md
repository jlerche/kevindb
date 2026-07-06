# LangSmith SDK Integration Test

This is a Python integration test for KevinDB's LangSmith-compatible
API. It starts `mockgres`, starts `kevindb-server`, ingests one OTLP trace,
and verifies that `langsmith.Client` can create, update, list, and read stored
runs, while raw HTTP requests cover newer compatibility endpoints not exposed
by the pinned SDK.

It is intentionally separate from `just check` because it launches external
processes and exercises the server as a black box.

## Prerequisites

- `uv`
- `mockgres` on `PATH`
- Rust/Cargo toolchain for this repo

## Run

From this directory:

```bash
uv sync
uv run pytest -q
```

The test chooses free local ports for `mockgres` and the HTTP server. It also
sets `KEVINDB_POSTGRES_URL` and `KEVINDB_BIND_ADDR` for the server process.

If the Rust server has not been built recently, the first run can take longer
because `cargo run -p kevindb-server` may compile dependencies before the test
can reach `/readyz`.

## What It Covers

- KevinDB server startup against `mockgres`
- startup migrations
- OTLP protobuf ingest through `POST /v1/projects/{project_name}/traces`
- OTLP ingest idempotency for retried payloads
- generated LangSmith-compatible run IDs for OTLP spans
- LangSmith run creation through `POST /runs`
- LangSmith run updates through `PATCH /runs/{run_id}`, including preserved
  inputs, hydrated outputs, events, and tags
- LangSmith project lookup through `GET /sessions`
- LangSmith run query through `POST /runs/query`, including the minimal cursor
  envelope and cursor pagination
- explicit rejection of unsupported natural-language `query` run filters
- explicit rejection of unsupported run attachments
- LangSmith run lookup through `GET /runs/{run_id}`, including not-found
  responses and generated OTLP run IDs
- trace lookup through `GET /v1/projects/{project_name}/traces/{trace_id}`
- tree-filtered run query through `tree_filter`
- parent and time-window run query filters
- SDK run query filters for indexed metadata and feedback predicates
- structured Phase 6 payload filters for scoped search, exact value equality,
  `in`, `json_key`, and scoped `json_key_search`
- LangSmith aggregate queries through `/runs/aggregate` and `/v1/runs/aggregate`,
  including Phase 6 filtered aggregates, requested feedback score metrics, and
  generated-run-ID feedback aggregation
- thread listing and thread trace lookup through `/v2/threads/query` and
  `/v2/threads/{thread_id}/traces`
- LangSmith error filtering
- LangSmith feedback creation, listing, lookup, update, and run-scoped lookup
- indexed feedback listing by project, trace, score, and value
- `/v1` run create, update, read, and query aliases
- LangSmith SDK model parsing of KevinDB's project and run responses

This test does not cover attachments, datasets/examples, batch or multipart run
ingest, public sharing, feedback tokens/formulas, bulk export, or the LangSmith
`/runs/stats` response shape.
