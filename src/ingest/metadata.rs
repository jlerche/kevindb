use anyhow::{Context, Result, anyhow};

use crate::otlp::SpanRecord;
use crate::query::generated_run_id;
use crate::segment::SPAN_SEGMENT_SCHEMA_VERSION;

use super::indexes::{ScalarIndexes, replace_run_scalar_indexes, root_locator_for_record};
use super::tree::refresh_trace_tree_metadata;
use super::{PartitionKey, event_time_unix_nano, run_event_idempotency_key, status_from_record};

pub(super) async fn persist_metadata(
    tx: &tokio_postgres::Transaction<'_>,
    partition: &PartitionKey,
    segment_uri: &str,
    etag: &str,
    total_bytes: usize,
    records: &[SpanRecord],
) -> Result<bool> {
    let first = records
        .first()
        .ok_or_else(|| anyhow!("cannot persist empty segment"))?;
    let min_start = records
        .iter()
        .map(|record| record.start_time_unix_nano)
        .min()
        .unwrap_or(0);
    let max_end = records
        .iter()
        .map(|record| record.end_time_unix_nano)
        .max()
        .unwrap_or(0);

    tx.execute(
        "INSERT INTO projects(name) VALUES ($1) ON CONFLICT (name) DO NOTHING",
        &[&first.project_name],
    )
    .await
    .context("upsert project")?;

    let row = tx
        .query_one(
            "INSERT INTO trace_segments(
                project_name, uri, etag, total_bytes, span_count,
                min_start_time_unix_nano, max_end_time_unix_nano,
                time_bucket_start_unix_nano, schema_version
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            RETURNING id",
            &[
                &first.project_name,
                &segment_uri,
                &etag,
                &(total_bytes as i64),
                &(records.len() as i64),
                &min_start,
                &max_end,
                &partition.time_bucket_start_unix_nano,
                &SPAN_SEGMENT_SCHEMA_VERSION,
            ],
        )
        .await
        .context("insert trace segment")?;
    let segment_id: i64 = row.get(0);

    for (row_index, record) in records.iter().enumerate() {
        let event_row = tx
            .query_opt(
                "INSERT INTO run_events(
                trace_segment_id, project_name, run_id, trace_id, span_id,
                event_type, event_time_unix_nano, row_index, idempotency_key
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (project_name, idempotency_key) DO NOTHING
            RETURNING id",
                &[
                    &segment_id,
                    &record.project_name,
                    &record.run_id,
                    &record.trace_id,
                    &record.span_id,
                    &record.event_kind.as_str(),
                    &event_time_unix_nano(record),
                    &(row_index as i64),
                    &run_event_idempotency_key(record),
                ],
            )
            .await
            .context("insert run event")?;
        let Some(event_row) = event_row else {
            return Ok(false);
        };
        let run_event_id: i64 = event_row.get(0);
        let generated_id =
            generated_run_id(&record.project_name, &record.trace_id, &record.span_id);
        let root = root_locator_for_record(tx, record, &generated_id).await?;
        let scalar_indexes = ScalarIndexes::from_record(record, root);

        tx.execute(
            "INSERT INTO trace_segment_spans(
                trace_segment_id, project_name, run_id, trace_id, span_id,
                parent_run_id, parent_span_id,
                name, run_type, start_time_unix_nano, end_time_unix_nano,
                status_code, status, is_root, row_index, event_type, event_time_unix_nano
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)",
            &[
                &segment_id,
                &record.project_name,
                &record.run_id,
                &record.trace_id,
                &record.span_id,
                &record.parent_run_id,
                &record.parent_span_id,
                &record.name,
                &record.run_type,
                &record.start_time_unix_nano,
                &record.end_time_unix_nano,
                &record.status_code,
                &status_from_record(record),
                &record.parent_span_id.is_none(),
                &(row_index as i64),
                &record.event_kind.as_str(),
                &event_time_unix_nano(record),
            ],
        )
        .await
        .context("insert trace segment span")?;

        let run_head_updated = tx
            .execute(
                "INSERT INTO run_heads(
                project_name, run_id, generated_run_id,
                trace_id, span_id, parent_run_id, parent_span_id,
                name, run_type,
                start_time_unix_nano, end_time_unix_nano, status_code, status, is_root,
                root_run_id, root_span_id, latency_nanos,
                prompt_tokens, completion_tokens, total_tokens, total_cost,
                model_name, provider_name,
                last_trace_segment_id, last_row_index,
                last_event_type, last_event_time_unix_nano, last_run_event_id, updated_at
            )
            VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13,
                $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25,
                $26, $27, $28, CURRENT_TIMESTAMP
            )
            ON CONFLICT (project_name, trace_id, span_id)
            DO UPDATE SET
                run_id = EXCLUDED.run_id,
                generated_run_id = EXCLUDED.generated_run_id,
                parent_run_id = EXCLUDED.parent_run_id,
                parent_span_id = EXCLUDED.parent_span_id,
                name = EXCLUDED.name,
                run_type = EXCLUDED.run_type,
                start_time_unix_nano = EXCLUDED.start_time_unix_nano,
                end_time_unix_nano = EXCLUDED.end_time_unix_nano,
                status_code = EXCLUDED.status_code,
                status = EXCLUDED.status,
                is_root = EXCLUDED.is_root,
                root_run_id = EXCLUDED.root_run_id,
                root_span_id = EXCLUDED.root_span_id,
                latency_nanos = EXCLUDED.latency_nanos,
                prompt_tokens = EXCLUDED.prompt_tokens,
                completion_tokens = EXCLUDED.completion_tokens,
                total_tokens = EXCLUDED.total_tokens,
                total_cost = EXCLUDED.total_cost,
                model_name = EXCLUDED.model_name,
                provider_name = EXCLUDED.provider_name,
                last_trace_segment_id = EXCLUDED.last_trace_segment_id,
                last_row_index = EXCLUDED.last_row_index,
                last_event_type = EXCLUDED.last_event_type,
                last_event_time_unix_nano = EXCLUDED.last_event_time_unix_nano,
                last_run_event_id = EXCLUDED.last_run_event_id,
                deleted_at_unix_nano = NULL,
                deletion_reason = NULL,
                updated_at = CURRENT_TIMESTAMP
            WHERE run_heads.last_event_time_unix_nano < EXCLUDED.last_event_time_unix_nano
                OR (
                    run_heads.last_event_time_unix_nano = EXCLUDED.last_event_time_unix_nano
                    AND COALESCE(run_heads.last_run_event_id, 0) <= EXCLUDED.last_run_event_id
                )",
                &[
                    &record.project_name,
                    &record.run_id,
                    &generated_id,
                    &record.trace_id,
                    &record.span_id,
                    &record.parent_run_id,
                    &record.parent_span_id,
                    &record.name,
                    &record.run_type,
                    &record.start_time_unix_nano,
                    &record.end_time_unix_nano,
                    &record.status_code,
                    &status_from_record(record),
                    &record.parent_span_id.is_none(),
                    &scalar_indexes.root_run_id,
                    &scalar_indexes.root_span_id,
                    &scalar_indexes.latency_nanos,
                    &scalar_indexes.prompt_tokens,
                    &scalar_indexes.completion_tokens,
                    &scalar_indexes.total_tokens,
                    &scalar_indexes.total_cost,
                    &scalar_indexes.model_name,
                    &scalar_indexes.provider_name,
                    &segment_id,
                    &(row_index as i64),
                    &record.event_kind.as_str(),
                    &event_time_unix_nano(record),
                    &run_event_id,
                ],
            )
            .await
            .context("upsert run head")?
            > 0;

        if run_head_updated {
            replace_run_scalar_indexes(tx, record, &scalar_indexes).await?;
            refresh_trace_tree_metadata(tx, &record.project_name, &record.trace_id).await?;
        }

        tx.execute(
            "INSERT INTO run_locators(
                project_name, run_id, generated_run_id, trace_id, span_id,
                trace_segment_id, row_index, event_type, event_time_unix_nano, run_event_id
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            ON CONFLICT (project_name, trace_id, span_id)
            DO UPDATE SET
                run_id = EXCLUDED.run_id,
                generated_run_id = EXCLUDED.generated_run_id,
                trace_segment_id = EXCLUDED.trace_segment_id,
                row_index = EXCLUDED.row_index,
                event_type = EXCLUDED.event_type,
                event_time_unix_nano = EXCLUDED.event_time_unix_nano,
                run_event_id = EXCLUDED.run_event_id,
                updated_at = CURRENT_TIMESTAMP
            WHERE run_locators.event_time_unix_nano < EXCLUDED.event_time_unix_nano
                OR (
                    run_locators.event_time_unix_nano = EXCLUDED.event_time_unix_nano
                    AND COALESCE(run_locators.run_event_id, 0) <= EXCLUDED.run_event_id
                )",
            &[
                &record.project_name,
                &record.run_id,
                &generated_id,
                &record.trace_id,
                &record.span_id,
                &segment_id,
                &(row_index as i64),
                &record.event_kind.as_str(),
                &event_time_unix_nano(record),
                &run_event_id,
            ],
        )
        .await
        .context("upsert run locator")?;

        tx.execute(
            "INSERT INTO trace_locators(
                project_name, trace_id, span_id, trace_segment_id, row_index,
                event_type, event_time_unix_nano, run_event_id
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (project_name, trace_id, span_id)
            DO UPDATE SET
                trace_segment_id = EXCLUDED.trace_segment_id,
                row_index = EXCLUDED.row_index,
                event_type = EXCLUDED.event_type,
                event_time_unix_nano = EXCLUDED.event_time_unix_nano,
                run_event_id = EXCLUDED.run_event_id,
                updated_at = CURRENT_TIMESTAMP
            WHERE trace_locators.event_time_unix_nano < EXCLUDED.event_time_unix_nano
                OR (
                    trace_locators.event_time_unix_nano = EXCLUDED.event_time_unix_nano
                    AND COALESCE(trace_locators.run_event_id, 0) <= EXCLUDED.run_event_id
                )",
            &[
                &record.project_name,
                &record.trace_id,
                &record.span_id,
                &segment_id,
                &(row_index as i64),
                &record.event_kind.as_str(),
                &event_time_unix_nano(record),
                &run_event_id,
            ],
        )
        .await
        .context("upsert trace locator")?;
    }

    Ok(true)
}
