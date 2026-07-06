use axum::Json;
use axum::extract::{Path, State};
use kevindb::query::ProjectRoute;
use serde::{Deserialize, Serialize};

use crate::{ApiError, ServerState};

pub(crate) async fn read_project_route(
    State(state): State<ServerState>,
    Path(project_name): Path<String>,
) -> Result<Json<ProjectRouteResponse>, ApiError> {
    let route = state
        .query_engine()
        .load_project_route(&project_name)
        .await?
        .ok_or_else(|| ApiError::not_found("project route not found".to_owned()))?;
    Ok(Json(ProjectRouteResponse::from(route)))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectRouteResponse {
    pub project_name: String,
    pub node_id: String,
    pub last_segment_uri: String,
}

impl From<ProjectRoute> for ProjectRouteResponse {
    fn from(route: ProjectRoute) -> Self {
        Self {
            project_name: route.project_name,
            node_id: route.node_id,
            last_segment_uri: route.last_segment_uri,
        }
    }
}
