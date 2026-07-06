use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use tokio::sync::watch;
use tokio::time::sleep;
use tokio_postgres::NoTls;

use super::{CompactReceipt, Ingestor, current_time_unix_nano};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactionServiceConfig {
    pub holder_id: String,
    pub lease_duration: Duration,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CompactionServiceReceipt {
    pub projects_scanned: usize,
    pub leases_acquired: usize,
    pub compacted_runs: usize,
    pub compacted_segments: usize,
    pub written_segments: usize,
}

impl Ingestor {
    pub async fn run_compaction_service_loop(
        &self,
        config: CompactionServiceConfig,
        interval: Duration,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<()> {
        loop {
            if *shutdown.borrow() {
                return Ok(());
            }
            self.run_compaction_service_once(config.clone()).await?;
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_ok() && *shutdown.borrow() {
                        return Ok(());
                    }
                }
                () = sleep(interval) => {}
            }
        }
    }

    pub async fn run_compaction_service_once(
        &self,
        config: CompactionServiceConfig,
    ) -> Result<CompactionServiceReceipt> {
        let holder_id = normalize_holder_id(config.holder_id)?;
        let lease_duration_nanos = duration_nanos(config.lease_duration)?;
        let projects = self.list_compaction_projects().await?;
        let mut receipt = CompactionServiceReceipt {
            projects_scanned: projects.len(),
            ..CompactionServiceReceipt::default()
        };

        for project_name in projects {
            let now = current_time_unix_nano()?;
            let lease_expires_at = now
                .checked_add(lease_duration_nanos)
                .ok_or_else(|| anyhow!("compaction lease expiry overflow"))?;
            if !self
                .try_acquire_compaction_lease(&project_name, &holder_id, lease_expires_at, now)
                .await?
            {
                continue;
            }
            receipt.leases_acquired += 1;
            let compacted = self.compact_project(&project_name).await?;
            add_compaction_receipt(&mut receipt, compacted);
            self.release_compaction_lease(&project_name, &holder_id)
                .await?;
        }

        Ok(receipt)
    }

    async fn list_compaction_projects(&self) -> Result<Vec<String>> {
        let (client, connection) = tokio_postgres::connect(&self.postgres_url, NoTls)
            .await
            .context("connect postgres for compaction project scan")?;
        tokio::spawn(async move {
            if let Err(err) = connection.await {
                tracing::warn!(error = %err, "postgres compaction project scan failed");
            }
        });

        let rows = client
            .query(
                "SELECT DISTINCT project_name
                FROM (
                    SELECT project_name
                    FROM trace_segments
                    WHERE compacted_at IS NULL
                    GROUP BY project_name, time_bucket_start_unix_nano
                    HAVING count(*) > 1
                ) AS compactable_projects
                ORDER BY project_name",
                &[],
            )
            .await
            .context("list compaction projects")?;
        Ok(rows.into_iter().map(|row| row.get(0)).collect())
    }

    async fn try_acquire_compaction_lease(
        &self,
        project_name: &str,
        holder_id: &str,
        lease_expires_at_unix_nano: i64,
        now_unix_nano: i64,
    ) -> Result<bool> {
        let (mut client, connection) = tokio_postgres::connect(&self.postgres_url, NoTls)
            .await
            .context("connect postgres for compaction lease acquire")?;
        tokio::spawn(async move {
            if let Err(err) = connection.await {
                tracing::warn!(error = %err, "postgres compaction lease acquire failed");
            }
        });

        let tx = client
            .transaction()
            .await
            .context("begin compaction lease acquire")?;
        tx.execute(
            "DELETE FROM compaction_leases
            WHERE project_name = $1
                AND (
                    lease_expires_at_unix_nano <= $2
                    OR holder_id = $3
                )",
            &[&project_name, &now_unix_nano, &holder_id],
        )
        .await
        .context("clear expired compaction lease")?;
        let row = tx
            .query_opt(
                "INSERT INTO compaction_leases(
                    project_name, holder_id, lease_expires_at_unix_nano, updated_at
                )
                VALUES ($1, $2, $3, CURRENT_TIMESTAMP)
                ON CONFLICT (project_name) DO NOTHING
                RETURNING holder_id",
                &[&project_name, &holder_id, &lease_expires_at_unix_nano],
            )
            .await
            .context("acquire compaction lease")?;
        tx.commit()
            .await
            .context("commit compaction lease acquire")?;
        Ok(row.is_some())
    }

    async fn release_compaction_lease(&self, project_name: &str, holder_id: &str) -> Result<()> {
        let (client, connection) = tokio_postgres::connect(&self.postgres_url, NoTls)
            .await
            .context("connect postgres for compaction lease release")?;
        tokio::spawn(async move {
            if let Err(err) = connection.await {
                tracing::warn!(error = %err, "postgres compaction lease release failed");
            }
        });

        client
            .execute(
                "DELETE FROM compaction_leases
                WHERE project_name = $1 AND holder_id = $2",
                &[&project_name, &holder_id],
            )
            .await
            .context("release compaction lease")?;
        Ok(())
    }
}

fn add_compaction_receipt(total: &mut CompactionServiceReceipt, next: CompactReceipt) {
    total.compacted_runs += next.compacted_runs;
    total.compacted_segments += next.compacted_segments;
    total.written_segments += next.written_segments;
}

fn normalize_holder_id(holder_id: String) -> Result<String> {
    let holder_id = holder_id.trim();
    if holder_id.is_empty() {
        bail!("compaction holder_id must not be empty");
    }
    Ok(holder_id.to_owned())
}

fn duration_nanos(duration: Duration) -> Result<i64> {
    if duration.is_zero() {
        bail!("compaction lease duration must be greater than zero");
    }
    i64::try_from(duration.as_nanos())
        .context("compaction lease duration does not fit in i64 nanos")
}
