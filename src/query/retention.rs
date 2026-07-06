use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use tokio_postgres::NoTls;

use super::{QueryEngine, current_time_unix_nano};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectRetentionPolicy {
    pub project_name: String,
    pub retention_period_nanos: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetentionEnforcementReceipt {
    pub projects_checked: usize,
    pub expired_runs: usize,
}

impl QueryEngine {
    pub async fn set_project_retention_policy(
        &self,
        project_name: &str,
        retention_period: Duration,
    ) -> Result<()> {
        let retention_period_nanos = duration_nanos(retention_period)?;
        let (client, connection) = tokio_postgres::connect(&self.postgres_url, NoTls)
            .await
            .context("connect postgres for retention policy upsert")?;
        tokio::spawn(async move {
            if let Err(err) = connection.await {
                tracing::warn!(error = %err, "postgres retention policy connection failed");
            }
        });

        client
            .execute(
                "INSERT INTO projects(name) VALUES ($1) ON CONFLICT (name) DO NOTHING",
                &[&project_name],
            )
            .await
            .context("ensure retention policy project")?;
        client
            .execute(
                "INSERT INTO project_retention_policies(
                    project_name, ttl_unix_nanos, updated_at
                )
                VALUES ($1, $2, CURRENT_TIMESTAMP)
                ON CONFLICT (project_name)
                DO UPDATE SET
                    ttl_unix_nanos = EXCLUDED.ttl_unix_nanos,
                    updated_at = CURRENT_TIMESTAMP",
                &[&project_name, &retention_period_nanos],
            )
            .await
            .context("upsert project retention policy")?;
        Ok(())
    }

    pub async fn load_project_retention_policy(
        &self,
        project_name: &str,
    ) -> Result<Option<ProjectRetentionPolicy>> {
        let (client, connection) = tokio_postgres::connect(&self.postgres_url, NoTls)
            .await
            .context("connect postgres for retention policy lookup")?;
        tokio::spawn(async move {
            if let Err(err) = connection.await {
                tracing::warn!(error = %err, "postgres retention policy lookup failed");
            }
        });

        let policy = client
            .query_opt(
                "SELECT project_name, ttl_unix_nanos
                FROM project_retention_policies
                WHERE project_name = $1",
                &[&project_name],
            )
            .await
            .context("load project retention policy")?
            .map(|row| ProjectRetentionPolicy {
                project_name: row.get(0),
                retention_period_nanos: row.get(1),
            });
        Ok(policy)
    }

    pub async fn enforce_project_retention_policies(&self) -> Result<RetentionEnforcementReceipt> {
        self.enforce_project_retention_policies_at(current_time_unix_nano()?)
            .await
    }

    pub async fn enforce_project_retention_policies_at(
        &self,
        now_unix_nano: i64,
    ) -> Result<RetentionEnforcementReceipt> {
        let policies = self.list_project_retention_policies().await?;
        let mut expired_runs = 0;
        for policy in &policies {
            let cutoff = now_unix_nano
                .checked_sub(policy.retention_period_nanos)
                .ok_or_else(|| anyhow!("retention cutoff underflow for {}", policy.project_name))?;
            expired_runs += self
                .expire_project_runs_before(&policy.project_name, cutoff)
                .await?;
        }

        Ok(RetentionEnforcementReceipt {
            projects_checked: policies.len(),
            expired_runs,
        })
    }

    async fn list_project_retention_policies(&self) -> Result<Vec<ProjectRetentionPolicy>> {
        let (client, connection) = tokio_postgres::connect(&self.postgres_url, NoTls)
            .await
            .context("connect postgres for retention policy scan")?;
        tokio::spawn(async move {
            if let Err(err) = connection.await {
                tracing::warn!(error = %err, "postgres retention policy scan failed");
            }
        });

        let rows = client
            .query(
                "SELECT project_name, ttl_unix_nanos
                FROM project_retention_policies
                ORDER BY project_name",
                &[],
            )
            .await
            .context("list project retention policies")?;

        Ok(rows
            .into_iter()
            .map(|row| ProjectRetentionPolicy {
                project_name: row.get(0),
                retention_period_nanos: row.get(1),
            })
            .collect())
    }
}

fn duration_nanos(duration: Duration) -> Result<i64> {
    if duration.is_zero() {
        bail!("retention period must be greater than zero");
    }
    i64::try_from(duration.as_nanos()).context("retention period does not fit in i64 nanoseconds")
}
