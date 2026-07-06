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
- `KEVINDB_OBJECT_STORE`: object store backend, `memory` or `s3`, default
  `memory`.
- `KEVINDB_S3_BUCKET`: S3 bucket, required when `KEVINDB_OBJECT_STORE=s3`.
- `KEVINDB_S3_REGION`: optional S3 region override.
- `KEVINDB_S3_ENDPOINT`: optional S3-compatible endpoint such as MinIO.
- `KEVINDB_S3_ALLOW_HTTP`: allow non-TLS S3 endpoints, default `false`.
- `KEVINDB_S3_PREFIX`: optional bucket prefix for this deployment.
- `KEVINDB_CACHE_MODE`: object-store cache mode, `memory` or `hybrid`,
  default `memory`.
- `KEVINDB_CACHE_MEMORY_CAPACITY_BYTES`: in-memory cache capacity, default
  `67108864`.
- `KEVINDB_CACHE_HYBRID_DIR`: directory for hybrid cache files, required when
  `KEVINDB_CACHE_MODE=hybrid`.
- `KEVINDB_CACHE_DISK_CAPACITY_BYTES`: hybrid cache disk capacity, default
  `1073741824`.
- `KEVINDB_CACHE_DISK_BLOCK_BYTES`: hybrid cache block size, default
  `16777216`.
- `KEVINDB_INGEST_MAX_SPANS_PER_SEGMENT`: max spans per Vortex segment,
  default `1024`.
- `KEVINDB_INGEST_MAX_FLUSH_DELAY_MS`: max time an underfilled ingest batch
  waits before a durable flush, default `500`.

The server always runs migrations on startup.

For a local Postgres + MinIO deployment, see [docs/operations.md](docs/operations.md).
The LangSmith-compatible OpenAPI snapshot is checked in at
[docs/openapi/langsmith-compat.openapi.json](docs/openapi/langsmith-compat.openapi.json).

Current endpoints:

- `GET /healthz`
- `GET /metrics`
- `GET /readyz`
- `GET /sessions`
- `GET /v1/sessions`
- `POST /runs`
- `POST /v1/runs`
- `GET /runs/{run_id}`
- `PATCH /runs/{run_id}`
- `GET /v1/runs/{run_id}`
- `PATCH /v1/runs/{run_id}`
- `GET /runs/{run_id}/feedback`
- `GET /v1/runs/{run_id}/feedback`
- `POST /runs/query`
- `POST /v1/runs/query`
- `POST /runs/aggregate`
- `POST /v1/runs/aggregate`
- `GET /feedback`
- `POST /feedback`
- `GET /v1/feedback`
- `POST /v1/feedback`
- `GET /feedback/{feedback_id}`
- `PATCH /feedback/{feedback_id}`
- `GET /v1/feedback/{feedback_id}`
- `PATCH /v1/feedback/{feedback_id}`
- `POST /v1/projects/{project_name}/traces`
- `GET /v1/projects/{project_name}/traces/{trace_id}`
- `GET /v1/projects/{project_name}/traces/{trace_id}/runs`
- `GET /v1/projects/{project_name}/route`
- `POST /v2/threads/query`
- `GET /v2/threads/{thread_id}/traces`

The compatibility surface currently targets LangSmith project lookup, run
create/update/read/query, feedback, aggregate summaries, OTLP trace ingest,
trace lookup, route lookup, and thread summaries/traces. Unsupported LangSmith
API areas are rejected or left unimplemented instead of silently scanning or
falling back: datasets/examples, multipart attachments, batch run ingest,
public sharing, delete/share/rule endpoints, feedback tokens/formulas, bulk
export, natural-language `query` run filters, and the SDK `/runs/stats` shape.
