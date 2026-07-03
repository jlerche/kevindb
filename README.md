# KevinDB

KevinDB is a Rust database for agent observability. Object storage holds durable
trace payloads, Postgres stores metadata and indexes, and DataFusion queries
Vortex segments.

## Local Server

Start Postgres-compatible test storage with `mockgres`:

```bash
mockgres --host 127.0.0.1 --port 55432
```

Run the HTTP server:

```bash
KEVINDB_POSTGRES_URL=postgresql://127.0.0.1:55432/postgres \
cargo run -p kevindb-server
```

Optional environment variables:

- `KEVINDB_BIND_ADDR`: socket address to bind, default `127.0.0.1:3000`.
- `KEVINDB_OBJECT_STORE`: object store backend, currently only `memory`.

The server always runs migrations on startup.

Current endpoints:

- `GET /healthz`
- `GET /readyz`
- `GET /sessions`
- `POST /runs`
- `PATCH /runs/{run_id}`
- `POST /runs/query`
- `POST /v1/runs/query`
- `POST /v1/projects/{project_name}/traces`
- `GET /v1/projects/{project_name}/traces/{trace_id}/runs`

`GET /sessions` and `POST /runs/query` are the initial LangSmith SDK
compatibility surface for project lookup and `Client.list_runs(...)`.
