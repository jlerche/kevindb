use std::collections::{HashMap, HashSet};

use anyhow::Context;
use axum::Json;
use axum::extract::{Query, State};
use chrono::{DateTime, SecondsFormat, Utc};
use kevindb::query::{RunQuery, RunSummary};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio_postgres::NoTls;
use uuid::Uuid;

use crate::{ApiError, ServerState};

impl ServerState {
    async fn list_project_names(
        &self,
        name: Option<&str>,
        limit: Option<usize>,
    ) -> anyhow::Result<Vec<String>> {
        let (client, connection) = tokio_postgres::connect(&self.postgres_url, NoTls)
            .await
            .context("connect postgres for project list")?;
        tokio::spawn(async move {
            if let Err(err) = connection.await {
                tracing::warn!(error = %err, "postgres project list connection failed");
            }
        });

        let limit = i64::try_from(limit.unwrap_or(100).min(1000)).unwrap_or(1000);
        let rows = if let Some(name) = name {
            client
                .query(
                    "SELECT name FROM projects WHERE name = $1 ORDER BY name LIMIT $2",
                    &[&name, &limit],
                )
                .await
        } else {
            client
                .query(
                    "SELECT name FROM projects ORDER BY name LIMIT $1",
                    &[&limit],
                )
                .await
        }
        .context("list projects")?;

        Ok(rows.into_iter().map(|row| row.get(0)).collect())
    }

    async fn resolve_project_selectors(
        &self,
        selectors: Vec<String>,
    ) -> anyhow::Result<Vec<String>> {
        if selectors.is_empty() {
            return Ok(Vec::new());
        }

        let project_names = self.list_project_names(None, None).await?;
        let project_names_by_id = project_names
            .iter()
            .map(|name| (project_uuid(name).to_string(), name.clone()))
            .collect::<HashMap<_, _>>();
        let known_names = project_names.into_iter().collect::<HashSet<_>>();

        let mut resolved = Vec::new();
        for selector in selectors {
            if selector.trim().is_empty() {
                continue;
            }

            let uuid_key = Uuid::parse_str(&selector).map(|id| id.to_string()).ok();
            if let Some(name) = uuid_key
                .as_ref()
                .and_then(|key| project_names_by_id.get(key))
                .cloned()
            {
                resolved.push(name);
            } else if known_names.contains(&selector) || uuid_key.is_none() {
                resolved.push(selector);
            }
        }

        Ok(dedupe(resolved))
    }
}

pub(super) async fn list_sessions(
    State(state): State<ServerState>,
    Query(query): Query<ListSessionsQuery>,
) -> Result<Json<Vec<ProjectResponse>>, ApiError> {
    let project_names = state
        .list_project_names(query.name.as_deref(), query.limit)
        .await?;
    let sessions = project_names
        .into_iter()
        .map(ProjectResponse::from_project_name)
        .collect();

    Ok(Json(sessions))
}

pub(super) async fn query_runs(
    State(state): State<ServerState>,
    Json(request): Json<RunsQueryRequest>,
) -> Result<Json<RunsResponse>, ApiError> {
    let mut project_names = request
        .project_name
        .clone()
        .map(StringList::into_vec)
        .unwrap_or_default();

    if let Some(session) = request.session.clone() {
        project_names.extend(state.resolve_project_selectors(session.into_vec()).await?);
    }

    let project_names = dedupe(project_names);
    if project_names.is_empty() {
        return Err(ApiError::bad_request(
            "project_name or session is required".to_owned(),
        ));
    }

    let runs = state
        .query_engine()
        .list_runs(RunQuery {
            project_names,
            trace_id: normalize_trace_filter(request.trace_id),
            run_type: request.run_type,
            is_root: request.is_root,
            error: request.error,
            limit: request.limit,
        })
        .await
        .context("query runs")?;

    Ok(Json(RunsResponse {
        runs: runs.into_iter().map(RunResponse::from).collect(),
    }))
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub(super) struct ListSessionsQuery {
    name: Option<String>,
    limit: Option<usize>,
    #[allow(dead_code)]
    include_stats: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectResponse {
    pub id: String,
    pub name: String,
    pub tenant_id: String,
    pub reference_dataset_id: Option<String>,
    pub start_time: String,
    pub end_time: Option<String>,
}

impl ProjectResponse {
    fn from_project_name(name: String) -> Self {
        Self {
            id: project_uuid(&name).to_string(),
            name,
            tenant_id: tenant_uuid().to_string(),
            reference_dataset_id: None,
            start_time: unix_nano_to_rfc3339(0),
            end_time: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct RunsQueryRequest {
    #[serde(default, alias = "project_names", alias = "session_name")]
    pub project_name: Option<StringList>,
    #[serde(default)]
    pub session: Option<StringList>,
    #[serde(default, alias = "trace")]
    pub trace_id: Option<String>,
    #[serde(default)]
    pub run_type: Option<String>,
    #[serde(default)]
    pub is_root: Option<bool>,
    #[serde(default)]
    pub error: Option<bool>,
    #[serde(default)]
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(untagged)]
pub enum StringList {
    One(String),
    Many(Vec<String>),
}

impl StringList {
    fn into_vec(self) -> Vec<String> {
        match self {
            Self::One(value) => vec![value],
            Self::Many(values) => values,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunsResponse {
    pub runs: Vec<RunResponse>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunResponse {
    pub id: String,
    pub project_name: String,
    pub session_id: String,
    pub trace_id: String,
    pub span_id: String,
    pub parent_span_id: Option<String>,
    pub parent_run_id: Option<String>,
    pub name: String,
    pub run_type: String,
    pub status: String,
    pub start_time: String,
    pub end_time: Option<String>,
    pub start_time_unix_nano: i64,
    pub end_time_unix_nano: i64,
    pub is_root: bool,
    pub inputs: Value,
    pub outputs: Option<Value>,
    pub extra: Value,
    pub error: Option<String>,
    pub dotted_order: String,
    pub child_run_ids: Vec<String>,
}

impl From<RunSummary> for RunResponse {
    fn from(run: RunSummary) -> Self {
        let id = run_uuid(&run.project_name, &run.trace_id, &run.span_id).to_string();
        let session_id = project_uuid(&run.project_name).to_string();
        let parent_run_id = run.parent_span_id.as_ref().map(|parent_span_id| {
            run_uuid(&run.project_name, &run.trace_id, parent_span_id).to_string()
        });
        let start_time = unix_nano_to_rfc3339(run.start_time_unix_nano);
        let end_time =
            (run.end_time_unix_nano > 0).then(|| unix_nano_to_rfc3339(run.end_time_unix_nano));
        let error = (run.status == "error").then(|| "error".to_owned());
        let dotted_order = format!("{:020}.{}", run.start_time_unix_nano.max(0), run.span_id);

        Self {
            id,
            project_name: run.project_name,
            session_id,
            trace_id: run.trace_id,
            span_id: run.span_id,
            parent_span_id: run.parent_span_id,
            parent_run_id,
            name: run.name,
            run_type: run.run_type,
            status: run.status,
            start_time,
            end_time,
            start_time_unix_nano: run.start_time_unix_nano,
            end_time_unix_nano: run.end_time_unix_nano,
            is_root: run.is_root,
            inputs: json!({}),
            outputs: None,
            extra: json!({}),
            error,
            dotted_order,
            child_run_ids: Vec::new(),
        }
    }
}

fn tenant_uuid() -> Uuid {
    Uuid::from_u128(0x4b4556494e4440008000000000000001)
}

fn project_uuid(project_name: &str) -> Uuid {
    Uuid::new_v5(
        &Uuid::NAMESPACE_URL,
        format!("kevindb:project:{project_name}").as_bytes(),
    )
}

fn run_uuid(project_name: &str, trace_id: &str, span_id: &str) -> Uuid {
    Uuid::new_v5(
        &Uuid::NAMESPACE_URL,
        format!("kevindb:run:{project_name}:{trace_id}:{span_id}").as_bytes(),
    )
}

fn normalize_trace_filter(trace_id: Option<String>) -> Option<String> {
    trace_id.map(|trace_id| {
        Uuid::parse_str(&trace_id)
            .map(|uuid| uuid.simple().to_string())
            .unwrap_or(trace_id)
    })
}

fn unix_nano_to_rfc3339(nanos: i64) -> String {
    let seconds = nanos.div_euclid(1_000_000_000);
    let subsecond_nanos = nanos.rem_euclid(1_000_000_000) as u32;

    DateTime::<Utc>::from_timestamp(seconds, subsecond_nanos)
        .unwrap_or(DateTime::<Utc>::UNIX_EPOCH)
        .to_rfc3339_opts(SecondsFormat::Nanos, true)
}

fn dedupe(values: Vec<String>) -> Vec<String> {
    let mut seen = HashSet::new();
    values
        .into_iter()
        .filter(|value| !value.trim().is_empty())
        .filter(|value| seen.insert(value.clone()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_langsmith_trace_filter_to_otel_hex() {
        assert_eq!(
            normalize_trace_filter(Some("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa".to_owned())),
            Some("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned())
        );
        assert_eq!(
            normalize_trace_filter(Some("not-a-uuid".to_owned())),
            Some("not-a-uuid".to_owned())
        );
    }

    #[test]
    fn builds_langsmith_run_response_fields() {
        let response = RunResponse::from(RunSummary {
            project_name: "demo".to_owned(),
            trace_id: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
            span_id: "1111111111111111".to_owned(),
            parent_span_id: Some("2222222222222222".to_owned()),
            name: "llm.call".to_owned(),
            run_type: "llm".to_owned(),
            status: "error".to_owned(),
            start_time_unix_nano: 1,
            end_time_unix_nano: 2,
            is_root: false,
        });

        assert!(Uuid::parse_str(&response.id).is_ok());
        assert!(Uuid::parse_str(&response.session_id).is_ok());
        assert!(Uuid::parse_str(response.parent_run_id.as_deref().unwrap()).is_ok());
        assert_eq!(response.start_time, "1970-01-01T00:00:00.000000001Z");
        assert_eq!(
            response.end_time.as_deref(),
            Some("1970-01-01T00:00:00.000000002Z")
        );
        assert_eq!(response.error.as_deref(), Some("error"));
        assert_eq!(response.inputs, json!({}));
    }
}
