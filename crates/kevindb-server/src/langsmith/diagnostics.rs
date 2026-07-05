use std::time::Duration;

use kevindb::query::RunQueryDiagnostics;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunQueryDiagnosticsResponse {
    pub candidate_segments: usize,
    pub candidate_runs: usize,
    pub candidate_bytes: i64,
    pub estimated_object_store_requests: usize,
    pub actual_object_store_requests: u64,
    pub actual_object_store_bytes_read: u64,
    pub vortex_files_opened: usize,
    pub rows_returned: usize,
    pub postgres_query_time_nanos: u64,
    pub datafusion_planning_time_nanos: u64,
    pub datafusion_execution_time_nanos: u64,
}

impl From<RunQueryDiagnostics> for RunQueryDiagnosticsResponse {
    fn from(diagnostics: RunQueryDiagnostics) -> Self {
        Self {
            candidate_segments: diagnostics.candidate_segments,
            candidate_runs: diagnostics.candidate_runs,
            candidate_bytes: diagnostics.candidate_bytes,
            estimated_object_store_requests: diagnostics.estimated_object_store_requests,
            actual_object_store_requests: diagnostics.actual_object_store_requests,
            actual_object_store_bytes_read: diagnostics.actual_object_store_bytes_read,
            vortex_files_opened: diagnostics.vortex_files_opened,
            rows_returned: diagnostics.rows_returned,
            postgres_query_time_nanos: duration_nanos(diagnostics.postgres_query_time),
            datafusion_planning_time_nanos: duration_nanos(diagnostics.datafusion_planning_time),
            datafusion_execution_time_nanos: duration_nanos(diagnostics.datafusion_execution_time),
        }
    }
}

fn duration_nanos(duration: Duration) -> u64 {
    duration.as_nanos().min(u128::from(u64::MAX)) as u64
}
