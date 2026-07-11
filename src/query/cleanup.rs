use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use futures_util::TryStreamExt;
use object_store::path::Path;
use object_store::{Error as ObjectStoreError, ObjectStore, ObjectStoreExt};
use tokio_postgres::NoTls;

use super::{QueryEngine, current_time_unix_nano};

const MAX_CLEANUP_CANDIDATES: i64 = 1000;
const MAX_ORPHAN_OBJECTS_SCANNED: usize = 100_000;
const MAX_ORPHAN_REFERENCES: i64 = 100_000;

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrphanObjectCandidate {
    pub uri: String,
    pub bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrphanObjectCleanupReceipt {
    pub dry_run: bool,
    pub candidates: Vec<OrphanObjectCandidate>,
    pub reclaimable_bytes: u64,
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
            deleted_objects += usize::from(
                delete_if_exists(self.object_store.as_ref(), &candidate.segment_uri).await?,
            );
            if let Some(search_index_uri) = &candidate.search_index_uri {
                deleted_objects += usize::from(
                    delete_if_exists(self.object_store.as_ref(), search_index_uri).await?,
                );
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

    pub async fn cleanup_orphaned_objects(
        &self,
        dry_run: bool,
    ) -> Result<OrphanObjectCleanupReceipt> {
        let referenced = self.load_referenced_object_uris().await?;
        let mut objects = self.object_store.list(None);
        let mut candidates = Vec::new();
        let mut objects_scanned = 0;
        while let Some(meta) = objects
            .try_next()
            .await
            .context("list object store for orphan cleanup")?
        {
            objects_scanned += 1;
            if objects_scanned > MAX_ORPHAN_OBJECTS_SCANNED {
                anyhow::bail!(
                    "orphan cleanup rejected: object count exceeds {MAX_ORPHAN_OBJECTS_SCANNED}"
                );
            }
            let uri = meta.location.to_string();
            if !referenced.contains(&uri) {
                candidates.push(OrphanObjectCandidate {
                    uri,
                    bytes: meta.size,
                });
            }
        }
        candidates.sort_by(|left, right| left.uri.cmp(&right.uri));
        let reclaimable_bytes = candidates
            .iter()
            .map(|candidate| candidate.bytes)
            .sum::<u64>();
        if dry_run {
            return Ok(OrphanObjectCleanupReceipt {
                dry_run,
                candidates,
                reclaimable_bytes,
                deleted_objects: 0,
            });
        }

        let mut deleted_objects = 0;
        for candidate in &candidates {
            deleted_objects +=
                usize::from(delete_if_exists(self.object_store.as_ref(), &candidate.uri).await?);
        }
        Ok(OrphanObjectCleanupReceipt {
            dry_run,
            candidates,
            reclaimable_bytes,
            deleted_objects,
        })
    }

    async fn load_referenced_object_uris(&self) -> Result<std::collections::HashSet<String>> {
        let (client, connection) = tokio_postgres::connect(&self.postgres_url, NoTls)
            .await
            .context("connect postgres for orphan object reconciliation")?;
        tokio::spawn(async move {
            if let Err(err) = connection.await {
                tracing::warn!(error = %err, "postgres orphan reconciliation failed");
            }
        });

        let segment_rows = client
            .query(
                "SELECT uri
                FROM trace_segments
                WHERE object_deleted_at_unix_nano IS NULL
                LIMIT $1",
                &[&(MAX_ORPHAN_REFERENCES + 1)],
            )
            .await
            .context("load referenced segment uris")?;
        let search_index_rows = client
            .query(
                "SELECT search_index_uri
                FROM trace_segments
                WHERE search_index_uri IS NOT NULL
                    AND object_deleted_at_unix_nano IS NULL
                LIMIT $1",
                &[&(MAX_ORPHAN_REFERENCES + 1)],
            )
            .await
            .context("load referenced search index uris")?;

        if segment_rows.len() > MAX_ORPHAN_REFERENCES as usize
            || search_index_rows.len() > MAX_ORPHAN_REFERENCES as usize
        {
            anyhow::bail!(
                "orphan cleanup rejected: metadata reference count exceeds {MAX_ORPHAN_REFERENCES}"
            );
        }
        let mut referenced = segment_rows
            .into_iter()
            .map(|row| row.get(0))
            .collect::<std::collections::HashSet<_>>();
        referenced.extend(search_index_rows.into_iter().map(|row| row.get(0)));
        Ok(referenced)
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
                ORDER BY id
                LIMIT $2",
                &[&cutoff_unix_nano, &MAX_CLEANUP_CANDIDATES],
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

async fn delete_if_exists(object_store: &dyn ObjectStore, uri: &str) -> Result<bool> {
    match object_store.delete(&Path::from(uri)).await {
        Ok(()) => Ok(true),
        Err(ObjectStoreError::NotFound { .. }) => Ok(false),
        Err(error) => Err(error).with_context(|| format!("delete compacted object {uri}")),
    }
}

fn duration_nanos(duration: Duration) -> Result<i64> {
    i64::try_from(duration.as_nanos()).context("cleanup grace period does not fit in i64 nanos")
}
