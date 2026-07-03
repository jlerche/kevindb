# LangSmith SDK Integration Test

This is a Python integration test for KevinDB's initial LangSmith-compatible
read API. It starts `mockgres`, starts `kevindb-server`, ingests one OTLP trace,
and verifies that `langsmith.Client.list_runs(...)` can read the stored runs.

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
- LangSmith project lookup through `GET /sessions`
- LangSmith run query through `POST /runs/query`
- LangSmith SDK model parsing of KevinDB's project and run responses

This test does not cover LangSmith write APIs, feedback APIs, cursor pagination,
or frontend-specific query APIs.
