# KevinDB Operations

## Local Compose

Run a basic self-hosted stack with Postgres, MinIO, and the KevinDB HTTP server:

```bash
docker compose up --build
```

The compose stack uses Postgres for metadata and MinIO through the S3 object
store for durable Vortex segments and search indexes. The server listens on
`http://127.0.0.1:3000`.

For multi-process deployments, run migrations from one process or release job.
Secondary roles can start with `KEVINDB_RUN_MIGRATIONS=false` after the schema
has been initialized.

KevinDB currently ships one canonical development schema. Schema rewrites
require rebuilding Postgres; upgrade backfills are intentionally not provided.

## Object Storage

Use `KEVINDB_OBJECT_STORE=s3` for S3-compatible storage.

Required:

- `KEVINDB_S3_BUCKET`
- `KEVINDB_S3_PREFIX` to isolate one KevinDB deployment inside a bucket
- Standard AWS credentials such as `AWS_ACCESS_KEY_ID` and
  `AWS_SECRET_ACCESS_KEY`, or another credential source supported by
  `object_store::aws::AmazonS3Builder::from_env`

Optional:

- `KEVINDB_S3_REGION`
- `KEVINDB_S3_ENDPOINT` for MinIO or another S3-compatible endpoint
- `KEVINDB_S3_ALLOW_HTTP=true` for local non-TLS endpoints only

## Backup And Restore

Back up Postgres and object storage together. Metadata can point at objects
immediately after ingest, so restoring only one side can produce missing-object
or orphan-object states.

Recommended backup order:

1. Pause ingest traffic or route it to a new deployment.
2. Snapshot the Postgres database.
3. Snapshot or version the S3 bucket/prefix.
4. Resume ingest traffic.

Recommended restore order:

1. Restore the S3 bucket/prefix.
2. Restore Postgres from the matching snapshot.
3. Start KevinDB query nodes first and verify `/readyz`.
4. Start ingest and compaction roles.
5. Run orphan object reconciliation in dry-run mode before deleting anything.

## Failure Modes

- Metadata pointing at missing objects is unsafe. KevinDB writes object data
  before metadata, so orphan objects are preferred over missing referenced
  objects.
- Retried ingest uses idempotency keys and current run locators to avoid
  duplicate visible runs.
- Compaction uses project leases and writes new compacted objects before
  marking old segments compacted.
- Delete vectors and retention masks are metadata-first logical deletes; object
  cleanup can run later.
- Slow queries should be debugged from returned diagnostics: candidate segments,
  candidate runs, estimated and actual object-store requests, bytes read,
  Postgres time, and DataFusion planning/execution time.
