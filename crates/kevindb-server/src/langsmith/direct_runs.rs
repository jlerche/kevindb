use anyhow::Context;
use kevindb::query::{RunProjection, RunQueryDiagnostics, RunSummary};

use super::{ESTIMATED_OBJECT_STORE_REQUESTS_PER_VORTEX_FILE, MAX_RUN_QUERY_CANDIDATE_SEGMENTS};
use crate::{ApiError, ServerState};

const MAX_DIRECT_RUN_BYTES: u64 = 128 * 1024 * 1024;

#[derive(Debug)]
pub(super) struct DirectRunLoad {
    pub(super) runs: Vec<RunSummary>,
    pub(super) diagnostics: RunQueryDiagnostics,
}

pub(super) async fn load_direct_runs(
    state: &ServerState,
    run_ids: Vec<String>,
    projection: RunProjection,
) -> Result<DirectRunLoad, ApiError> {
    validate_direct_run_count(run_ids.len())?;

    let query_engine = state.query_engine();
    let mut runs = Vec::new();
    let mut diagnostics = RunQueryDiagnostics::default();
    for run_id in run_ids {
        let result = query_engine
            .load_run_by_id_with_projection(&run_id, projection)
            .await
            .context("load requested run")?;
        add_diagnostics(&mut diagnostics, result.diagnostics);
        enforce_direct_runtime_limits(&diagnostics)?;
        if let Some(run) = result.run {
            runs.push(run);
        }
    }

    Ok(DirectRunLoad { runs, diagnostics })
}

fn validate_direct_run_count(count: usize) -> Result<(), ApiError> {
    if count > MAX_RUN_QUERY_CANDIDATE_SEGMENTS {
        return Err(ApiError::bad_request(format!(
            "query rejected: direct run_ids {count} exceed limit {MAX_RUN_QUERY_CANDIDATE_SEGMENTS}"
        )));
    }
    Ok(())
}

fn enforce_direct_runtime_limits(diagnostics: &RunQueryDiagnostics) -> Result<(), ApiError> {
    let request_limit =
        (MAX_RUN_QUERY_CANDIDATE_SEGMENTS * ESTIMATED_OBJECT_STORE_REQUESTS_PER_VORTEX_FILE) as u64;
    if diagnostics.actual_object_store_requests > request_limit {
        return Err(ApiError::bad_request(format!(
            "query rejected: actual object-store requests {} exceed limit {}",
            diagnostics.actual_object_store_requests, request_limit
        )));
    }
    if diagnostics.actual_object_store_bytes_read > MAX_DIRECT_RUN_BYTES {
        return Err(ApiError::bad_request(format!(
            "query rejected: actual bytes read {} exceed limit {}",
            diagnostics.actual_object_store_bytes_read, MAX_DIRECT_RUN_BYTES
        )));
    }
    Ok(())
}

fn add_diagnostics(total: &mut RunQueryDiagnostics, next: RunQueryDiagnostics) {
    total.candidate_segments = total
        .candidate_segments
        .saturating_add(next.candidate_segments);
    total.candidate_runs = total.candidate_runs.saturating_add(next.candidate_runs);
    total.candidate_bytes = total.candidate_bytes.saturating_add(next.candidate_bytes);
    total.estimated_object_store_requests = total
        .estimated_object_store_requests
        .saturating_add(next.estimated_object_store_requests);
    total.actual_object_store_requests = total
        .actual_object_store_requests
        .saturating_add(next.actual_object_store_requests);
    total.actual_object_store_bytes_read = total
        .actual_object_store_bytes_read
        .saturating_add(next.actual_object_store_bytes_read);
    total.vortex_files_opened = total
        .vortex_files_opened
        .saturating_add(next.vortex_files_opened);
    total.rows_returned = total.rows_returned.saturating_add(next.rows_returned);
    total.postgres_query_time += next.postgres_query_time;
    total.datafusion_planning_time += next.datafusion_planning_time;
    total.datafusion_execution_time += next.datafusion_execution_time;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn direct_run_count_uses_segment_budget() {
        assert!(validate_direct_run_count(MAX_RUN_QUERY_CANDIDATE_SEGMENTS).is_ok());
        assert!(matches!(
            validate_direct_run_count(MAX_RUN_QUERY_CANDIDATE_SEGMENTS + 1),
            Err(ApiError::BadRequest(message)) if message.contains("direct run_ids")
        ));
    }

    #[test]
    fn adds_direct_run_diagnostics() {
        let mut total = RunQueryDiagnostics::default();
        add_diagnostics(
            &mut total,
            RunQueryDiagnostics {
                candidate_segments: 1,
                candidate_runs: 1,
                candidate_bytes: 10,
                estimated_object_store_requests: 48,
                actual_object_store_requests: 2,
                actual_object_store_bytes_read: 20,
                vortex_files_opened: 1,
                rows_returned: 1,
                ..RunQueryDiagnostics::default()
            },
        );
        add_diagnostics(
            &mut total,
            RunQueryDiagnostics {
                candidate_segments: 2,
                candidate_runs: 2,
                candidate_bytes: 30,
                estimated_object_store_requests: 96,
                actual_object_store_requests: 3,
                actual_object_store_bytes_read: 40,
                vortex_files_opened: 2,
                rows_returned: 2,
                ..RunQueryDiagnostics::default()
            },
        );

        assert_eq!(total.candidate_segments, 3);
        assert_eq!(total.candidate_runs, 3);
        assert_eq!(total.candidate_bytes, 40);
        assert_eq!(total.estimated_object_store_requests, 144);
        assert_eq!(total.actual_object_store_requests, 5);
        assert_eq!(total.actual_object_store_bytes_read, 60);
        assert_eq!(total.vortex_files_opened, 3);
        assert_eq!(total.rows_returned, 3);
    }
}
