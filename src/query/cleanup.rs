use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use object_store::path::Path;
use object_store::{Error as ObjectStoreError, ObjectStore, ObjectStoreExt};
use tokio_postgres::NoTls;

use super::{QueryEngine, current_time_unix_nano};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectCleanupCandidate {
    pub trace_segment_id: i64,
    pub segment_uri: String,
    pub segment_bytes: i64,
    pub search_index_uri: Option<String>,
    pub search_index_bytes: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectCleanupReceipt {
    pub dry_run: bool,
    pub candidates: Vec<ObjectCleanupCandidate>,
    pub reclaimable_bytes: i64,
    pub deleted_objects: usize,
}

impl QueryEngine {
    pub async fn cleanup_compacted_objects(
        &self,
        grace_period: Duration,
        dry_run: bool,
    ) -> Result<ObjectCleanupReceipt> {
        self.cleanup_compacted_objects_at(current_time_unix_nano()?, grace_period, dry_run)
            .await
    }

    pub async fn cleanup_compacted_objects_at(
        &self,
        now_unix_nano: i64,
        grace_period: Duration,
        dry_run: bool,
    ) -> Result<ObjectCleanupReceipt> {
        let grace_nanos = duration_nanos(grace_period)?;
        let cutoff_unix_nano = now_unix_nano
            .checked_sub(grace_nanos)
            .ok_or_else(|| anyhow!("cleanup cutoff underflow"))?;
        let candidates = self
            .load_compacted_cleanup_candidates(cutoff_unix_nano)
            .await?;
        let reclaimable_bytes = candidates.iter().map(candidate_bytes).sum::<i64>();
        if dry_run || candidates.is_empty() {
            return Ok(ObjectCleanupReceipt {
                dry_run,
                candidates,
                reclaimable_bytes,
                deleted_objects: 0,
            });
        }

        let mut deleted_objects = 0;
        for candidate in &candidates {
            delete_if_exists(self.object_store.as_ref(), &candidate.segment_uri).await?;
            deleted_objects += 1;
            if let Some(search_index_uri) = &candidate.search_index_uri {
                delete_if_exists(self.object_store.as_ref(), search_index_uri).await?;
                deleted_objects += 1;
            }
        }
        self.mark_compacted_objects_cleaned(&candidates, now_unix_nano)
            .await?;

        Ok(ObjectCleanupReceipt {
            dry_run,
            candidates,
            reclaimable_bytes,
            deleted_objects,
        })
    }

    async fn load_compacted_cleanup_candidates(
        &self,
        cutoff_unix_nano: i64,
    ) -> Result<Vec<ObjectCleanupCandidate>> {
        let (client, connection) = tokio_postgres::connect(&self.postgres_url, NoTls)
            .await
            .context("connect postgres for compacted object cleanup")?;
        tokio::spawn(async move {
            if let Err(err) = connection.await {
                tracing::warn!(error = %err, "postgres compacted cleanup connection failed");
            }
        });

        let rows = client
            .query(
                "SELECT id, uri, total_bytes, search_index_uri, search_index_bytes
                FROM trace_segments
                WHERE compacted_at IS NOT NULL
                    AND object_deleted_at_unix_nano IS NULL
                    AND COALESCE(compacted_at_unix_nano, 0) <= $1
                ORDER BY id",
                &[&cutoff_unix_nano],
            )
            .await
            .context("load compacted object cleanup candidates")?;

        Ok(rows
            .into_iter()
            .map(|row| ObjectCleanupCandidate {
                trace_segment_id: row.get(0),
                segment_uri: row.get(1),
                segment_bytes: row.get(2),
                search_index_uri: row.get(3),
                search_index_bytes: row.get(4),
            })
            .collect())
    }

    async fn mark_compacted_objects_cleaned(
        &self,
        candidates: &[ObjectCleanupCandidate],
        deleted_at_unix_nano: i64,
    ) -> Result<()> {
        let ids = candidates
            .iter()
            .map(|candidate| candidate.trace_segment_id.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        let (client, connection) = tokio_postgres::connect(&self.postgres_url, NoTls)
            .await
            .context("connect postgres for compacted cleanup marker")?;
        tokio::spawn(async move {
            if let Err(err) = connection.await {
                tracing::warn!(error = %err, "postgres compacted cleanup marker failed");
            }
        });

        client
            .execute(
                format!(
                    "UPDATE trace_segments
                    SET object_deleted_at_unix_nano = $1
                    WHERE id IN ({ids})"
                )
                .as_str(),
                &[&deleted_at_unix_nano],
            )
            .await
            .context("mark compacted objects cleaned")?;
        Ok(())
    }
}

fn candidate_bytes(candidate: &ObjectCleanupCandidate) -> i64 {
    candidate
        .segment_bytes
        .saturating_add(candidate.search_index_bytes)
}

async fn delete_if_exists(object_store: &dyn ObjectStore, uri: &str) -> Result<()> {
    match object_store.delete(&Path::from(uri)).await {
        Ok(()) | Err(ObjectStoreError::NotFound { .. }) => Ok(()),
        Err(error) => Err(error).with_context(|| format!("delete compacted object {uri}")),
    }
}

fn duration_nanos(duration: Duration) -> Result<i64> {
    i64::try_from(duration.as_nanos()).context("cleanup grace period does not fit in i64 nanos")
}
