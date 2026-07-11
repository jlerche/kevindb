use std::collections::BTreeSet;

use anyhow::Result;

use super::{indexes, thread, tree};

pub(crate) async fn refresh_deleted_run_metadata(
    tx: &tokio_postgres::Transaction<'_>,
    project_name: &str,
    trace_ids: &BTreeSet<String>,
    affected_start_times_unix_nano: &BTreeSet<i64>,
) -> Result<()> {
    for trace_id in trace_ids {
        tree::refresh_trace_tree_metadata(tx, project_name, trace_id).await?;
        thread::refresh_trace_thread_metadata(tx, project_name, trace_id).await?;
    }
    let buckets = affected_start_times_unix_nano
        .iter()
        .map(|start_time| indexes::rollup_time_bucket(*start_time))
        .collect::<BTreeSet<_>>();
    for bucket in buckets {
        indexes::refresh_project_aggregate_rollups(tx, project_name, bucket).await?;
    }
    Ok(())
}
