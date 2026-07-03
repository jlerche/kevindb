use std::collections::{HashMap, HashSet};

use anyhow::Context;
use axum::Json;
use axum::extract::Path;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use chrono::{DateTime, SecondsFormat, Utc};
use kevindb::otlp::{RunEventKind, SpanRecord};
use kevindb::query::{RunQuery, RunSummary, generated_run_id};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio_postgres::NoTls;
use uuid::Uuid;

use crate::{ApiError, ServerState};

mod feedback;
pub use feedback::FeedbackResponse;
pub(crate) use feedback::{create_feedback, list_feedback, list_run_feedback, read_feedback};

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

pub(super) async fn create_run(
    State(state): State<ServerState>,
    Json(request): Json<RunWriteRequest>,
) -> Result<StatusCode, ApiError> {
    let record = request.into_span_record(&state, None).await?;
    state.ingestor.ingest_records(vec![record]).await?;
    Ok(StatusCode::ACCEPTED)
}

pub(super) async fn update_run(
    State(state): State<ServerState>,
    Path(run_id): Path<String>,
    Json(request): Json<RunWriteRequest>,
) -> Result<StatusCode, ApiError> {
    let record = request.into_span_record(&state, Some(run_id)).await?;
    state.ingestor.ingest_records(vec![record]).await?;
    Ok(StatusCode::OK)
}

pub(super) async fn read_run(
    State(state): State<ServerState>,
    Path(run_id): Path<String>,
) -> Result<Json<RunResponse>, ApiError> {
    let run_id = canonical_uuid(&run_id, "run_id")?;
    let run = state
        .load_run_summary_by_id(&run_id)
        .await?
        .ok_or_else(|| ApiError::not_found("run not found".to_owned()))?;
    Ok(Json(RunResponse::from(run)))
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

    let parent_run_id = request
        .parent_run_id
        .as_deref()
        .map(|parent_run_id| canonical_uuid(parent_run_id, "parent_run_id"))
        .transpose()?;
    let start_time_min_unix_nano = parse_time_nanos(
        request
            .start_time_min
            .as_deref()
            .or(request.start_time_gte.as_deref())
            .or(request.start_time.as_deref()),
    )?;
    let start_time_max_unix_nano = parse_time_nanos(
        request
            .start_time_max
            .as_deref()
            .or(request.start_time_lte.as_deref())
            .or(request.end_time.as_deref()),
    )?;
    let offset = request.cursor_offset().or(request.offset);
    let limit = request.limit;
    let runs = state
        .query_engine()
        .list_runs(RunQuery {
            project_names,
            trace_id: normalize_trace_filter(request.trace_id),
            parent_run_id,
            parent_span_id: request.parent_span_id,
            run_type: request.run_type,
            is_root: request.is_root,
            error: request.error,
            start_time_min_unix_nano,
            start_time_max_unix_nano,
            limit: limit.map(|limit| limit.saturating_add(1)),
            offset,
            retention_cutoff_unix_nano: None,
            include_deleted: false,
        })
        .await
        .context("query runs")?;

    Ok(Json(RunsResponse::from_runs_with_limit(
        runs, limit, offset,
    )))
}

pub(super) async fn read_project_trace(
    State(state): State<ServerState>,
    Path((project_name, trace_id)): Path<(String, String)>,
) -> Result<Json<TraceResponse>, ApiError> {
    let trace_id = normalize_trace_filter(Some(trace_id)).unwrap_or_default();
    let runs = state
        .query_engine()
        .list_runs_in_trace(&project_name, &trace_id)
        .await
        .with_context(|| format!("read trace {trace_id}"))?;

    if runs.is_empty() {
        return Err(ApiError::not_found("trace not found".to_owned()));
    }

    Ok(Json(TraceResponse::from_runs(project_name, trace_id, runs)))
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub(super) struct ListSessionsQuery {
    name: Option<String>,
    limit: Option<usize>,
    #[allow(dead_code)]
    include_stats: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct RunWriteRequest {
    id: Option<String>,
    name: Option<String>,
    run_type: Option<String>,
    #[serde(default, alias = "project_name")]
    session_name: Option<String>,
    session_id: Option<String>,
    trace_id: Option<String>,
    parent_run_id: Option<String>,
    start_time: Option<String>,
    end_time: Option<String>,
    error: Option<String>,
    inputs: Option<Value>,
    outputs: Option<Value>,
    extra: Option<Value>,
}

impl RunWriteRequest {
    async fn into_span_record(
        self,
        state: &ServerState,
        path_run_id: Option<String>,
    ) -> Result<SpanRecord, ApiError> {
        let run_id = canonical_uuid(
            self.id
                .as_deref()
                .or(path_run_id.as_deref())
                .ok_or_else(|| ApiError::bad_request("run id is required".to_owned()))?,
            "run id",
        )?;
        let existing = state.load_run_head(&run_id).await?;
        let existing_payload = if existing.is_some() {
            state.load_run_payload(&run_id).await?
        } else {
            LangSmithPayload::default()
        };
        let project_name = state
            .resolve_write_project_name(
                self.session_name.as_deref(),
                self.session_id.as_deref(),
                existing.as_ref(),
            )
            .await?;
        let trace_id = self
            .trace_id
            .as_deref()
            .map(|trace_id| uuid_to_otel_trace_id(trace_id, "trace_id"))
            .transpose()?
            .or_else(|| existing.as_ref().map(|run| run.trace_id.clone()))
            .unwrap_or_else(|| uuid_simple(&run_id));
        let parent_run_id = self
            .parent_run_id
            .as_deref()
            .map(|parent_run_id| canonical_uuid(parent_run_id, "parent_run_id"))
            .transpose()?
            .or_else(|| existing.as_ref().and_then(|run| run.parent_run_id.clone()));
        let parent_span_id = parent_run_id
            .as_deref()
            .map(uuid_to_span_id)
            .or_else(|| existing.as_ref().and_then(|run| run.parent_span_id.clone()));
        let name = self
            .name
            .or_else(|| existing.as_ref().map(|run| run.name.clone()))
            .ok_or_else(|| ApiError::bad_request("run name is required".to_owned()))?;
        let run_type = self
            .run_type
            .or_else(|| existing.as_ref().map(|run| run.run_type.clone()))
            .ok_or_else(|| ApiError::bad_request("run_type is required".to_owned()))?;
        let start_time_unix_nano = parse_time_nanos(self.start_time.as_deref())?
            .or_else(|| existing.as_ref().map(|run| run.start_time_unix_nano))
            .unwrap_or_else(current_time_nanos);
        let event_kind = match (
            existing.is_some(),
            self.end_time.is_some() || self.error.is_some(),
        ) {
            (_, true) => RunEventKind::End,
            (true, false) => RunEventKind::Update,
            (false, false) => RunEventKind::Start,
        };
        let end_time_unix_nano = parse_time_nanos(self.end_time.as_deref())?
            .or_else(|| existing.as_ref().map(|run| run.end_time_unix_nano))
            .unwrap_or(0);
        let payload = existing_payload.merge(self.inputs, self.outputs, self.extra, self.error);
        let status_code = if payload.error.is_some() {
            2
        } else if end_time_unix_nano == 0 {
            0
        } else {
            1
        };
        let span_id = uuid_to_span_id(&run_id);

        Ok(SpanRecord {
            project_name,
            run_id,
            trace_id,
            span_id,
            parent_run_id,
            parent_span_id,
            name,
            run_type,
            start_time_unix_nano,
            end_time_unix_nano,
            status_code,
            event_kind,
            attributes_json: payload.to_attributes_json(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StoredRunHead {
    project_name: String,
    trace_id: String,
    parent_run_id: Option<String>,
    parent_span_id: Option<String>,
    name: String,
    run_type: String,
    start_time_unix_nano: i64,
    end_time_unix_nano: i64,
}

impl ServerState {
    async fn resolve_write_project_name(
        &self,
        session_name: Option<&str>,
        session_id: Option<&str>,
        existing: Option<&StoredRunHead>,
    ) -> Result<String, ApiError> {
        if let Some(session_name) = session_name.filter(|name| !name.trim().is_empty()) {
            return Ok(session_name.to_owned());
        }
        if let Some(existing) = existing {
            return Ok(existing.project_name.clone());
        }
        if let Some(session_id) = session_id {
            let mut projects = self
                .resolve_project_selectors(vec![session_id.to_owned()])
                .await?;
            if let Some(project_name) = projects.pop() {
                return Ok(project_name);
            }
        }

        Ok("default".to_owned())
    }

    async fn load_run_head(&self, run_id: &str) -> Result<Option<StoredRunHead>, ApiError> {
        let (client, connection) = tokio_postgres::connect(&self.postgres_url, NoTls)
            .await
            .context("connect postgres for run head lookup")?;
        tokio::spawn(async move {
            if let Err(err) = connection.await {
                tracing::warn!(error = %err, "postgres run head lookup connection failed");
            }
        });

        let row = client
            .query_opt(
                "SELECT project_name, trace_id, parent_run_id, parent_span_id, name,
                    run_type, start_time_unix_nano, end_time_unix_nano
                FROM run_heads
                WHERE run_id = $1
                LIMIT 1",
                &[&run_id],
            )
            .await
            .context("load run head by run id")?;

        Ok(row.map(|row| StoredRunHead {
            project_name: row.get(0),
            trace_id: row.get(1),
            parent_run_id: row.get(2),
            parent_span_id: row.get(3),
            name: row.get(4),
            run_type: row.get(5),
            start_time_unix_nano: row.get(6),
            end_time_unix_nano: row.get(7),
        }))
    }

    async fn load_run_summary_by_id(&self, run_id: &str) -> Result<Option<RunSummary>, ApiError> {
        if let Some(run) = self
            .query_engine()
            .load_run_by_id(run_id)
            .await
            .context("load run summary by id")?
        {
            return Ok(Some(run));
        }

        self.load_generated_run_summary_by_id(run_id).await
    }

    async fn load_generated_run_summary_by_id(
        &self,
        run_id: &str,
    ) -> Result<Option<RunSummary>, ApiError> {
        let (client, connection) = tokio_postgres::connect(&self.postgres_url, NoTls)
            .await
            .context("connect postgres for generated run lookup")?;
        tokio::spawn(async move {
            if let Err(err) = connection.await {
                tracing::warn!(error = %err, "postgres generated run lookup connection failed");
            }
        });

        let scopes = client
            .query(
                "SELECT DISTINCT project_name, trace_id FROM run_heads ORDER BY project_name, trace_id",
                &[],
            )
            .await
            .context("load run lookup scopes")?;

        for scope in scopes {
            let project_name: String = scope.get(0);
            let trace_id: String = scope.get(1);
            let runs = self
                .query_engine()
                .list_runs_in_trace(&project_name, &trace_id)
                .await
                .context("load generated run lookup trace")?;

            if let Some(run) = runs.into_iter().find(|run| response_run_id(run) == run_id) {
                return Ok(Some(run));
            }
        }

        Ok(None)
    }

    async fn load_run_payload(&self, run_id: &str) -> Result<LangSmithPayload, ApiError> {
        Ok(self
            .query_engine()
            .load_run_by_id(run_id)
            .await
            .context("load run payload")?
            .map(|run| LangSmithPayload::from_attributes_json(&run.attributes_json))
            .unwrap_or_default())
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
struct LangSmithPayload {
    inputs: Option<Value>,
    outputs: Option<Value>,
    extra: Option<Value>,
    error: Option<String>,
}

impl LangSmithPayload {
    fn from_attributes_json(attributes_json: &str) -> Self {
        let Ok(Value::Object(attributes)) = serde_json::from_str(attributes_json) else {
            return Self::default();
        };

        Self {
            inputs: attributes
                .get("langsmith.inputs")
                .filter(|value| !value.is_null())
                .cloned(),
            outputs: attributes
                .get("langsmith.outputs")
                .filter(|value| !value.is_null())
                .cloned(),
            extra: attributes
                .get("langsmith.extra")
                .filter(|value| !value.is_null())
                .cloned(),
            error: attributes
                .get("langsmith.error")
                .and_then(Value::as_str)
                .map(str::to_owned),
        }
    }

    fn merge(
        self,
        inputs: Option<Value>,
        outputs: Option<Value>,
        extra: Option<Value>,
        error: Option<String>,
    ) -> Self {
        Self {
            inputs: inputs.or(self.inputs),
            outputs: outputs.or(self.outputs),
            extra: extra.or(self.extra),
            error: error.or(self.error),
        }
    }

    fn to_attributes_json(&self) -> String {
        json!({
            "langsmith.inputs": self.inputs.clone().unwrap_or_else(|| json!({})),
            "langsmith.outputs": self.outputs,
            "langsmith.extra": self.extra.clone().unwrap_or_else(|| json!({})),
            "langsmith.error": self.error,
        })
        .to_string()
    }
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
    #[serde(default, alias = "parent_run")]
    pub parent_run_id: Option<String>,
    #[serde(default)]
    pub parent_span_id: Option<String>,
    #[serde(default)]
    pub run_type: Option<String>,
    #[serde(default)]
    pub is_root: Option<bool>,
    #[serde(default)]
    pub error: Option<bool>,
    #[serde(default)]
    pub start_time: Option<String>,
    #[serde(default)]
    pub end_time: Option<String>,
    #[serde(default, alias = "start_time_after")]
    pub start_time_gte: Option<String>,
    #[serde(default, alias = "start_time_before")]
    pub start_time_lte: Option<String>,
    #[serde(default, alias = "start_time_min")]
    pub start_time_min: Option<String>,
    #[serde(default, alias = "start_time_max")]
    pub start_time_max: Option<String>,
    #[serde(default)]
    pub limit: Option<usize>,
    #[serde(default)]
    pub offset: Option<usize>,
    #[serde(default)]
    pub cursor: Option<String>,
}

impl RunsQueryRequest {
    fn cursor_offset(&self) -> Option<usize> {
        self.cursor
            .as_deref()
            .filter(|cursor| !cursor.trim().is_empty())
            .and_then(|cursor| cursor.parse().ok())
    }
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
    #[serde(default)]
    pub cursors: CursorResponse,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TraceResponse {
    pub project_name: String,
    pub trace_id: String,
    pub root_run_ids: Vec<String>,
    pub runs: Vec<RunResponse>,
}

impl TraceResponse {
    fn from_runs(project_name: String, trace_id: String, runs: Vec<RunSummary>) -> Self {
        let runs = RunsResponse::new(runs.into_iter().map(RunResponse::from).collect()).runs;
        let root_run_ids = runs
            .iter()
            .filter(|run| run.is_root)
            .map(|run| run.id.clone())
            .collect();

        Self {
            project_name,
            trace_id,
            root_run_ids,
            runs,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CursorResponse {
    pub next: Option<String>,
}

impl RunsResponse {
    pub fn new(runs: Vec<RunResponse>) -> Self {
        Self {
            runs: runs_with_children(runs),
            cursors: CursorResponse { next: None },
        }
    }

    fn from_runs_with_limit(
        runs: Vec<RunSummary>,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> Self {
        let mut runs = runs.into_iter().map(RunResponse::from).collect::<Vec<_>>();
        let next = limit.and_then(|limit| {
            if runs.len() > limit {
                runs.truncate(limit);
                Some(offset.unwrap_or(0).saturating_add(limit).to_string())
            } else {
                None
            }
        });

        Self {
            runs: runs_with_children(runs),
            cursors: CursorResponse { next },
        }
    }
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
        let id = run
            .run_id
            .clone()
            .unwrap_or_else(|| generated_run_id(&run.project_name, &run.trace_id, &run.span_id));
        let session_id = project_uuid(&run.project_name).to_string();
        let parent_run_id = run.parent_run_id.clone().or_else(|| {
            run.parent_span_id.as_ref().map(|parent_span_id| {
                generated_run_id(&run.project_name, &run.trace_id, parent_span_id)
            })
        });
        let start_time = unix_nano_to_rfc3339(run.start_time_unix_nano);
        let end_time =
            (run.end_time_unix_nano > 0).then(|| unix_nano_to_rfc3339(run.end_time_unix_nano));
        let error = (run.status == "error").then(|| "error".to_owned());
        let dotted_order = format!("{:020}.{}", run.start_time_unix_nano.max(0), run.span_id);
        let payload = LangSmithPayload::from_attributes_json(&run.attributes_json);

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
            inputs: payload.inputs.unwrap_or_else(|| json!({})),
            outputs: payload.outputs,
            extra: payload.extra.unwrap_or_else(|| json!({})),
            error: payload.error.or(error),
            dotted_order,
            child_run_ids: Vec::new(),
        }
    }
}

fn runs_with_children(mut runs: Vec<RunResponse>) -> Vec<RunResponse> {
    let mut children_by_parent = HashMap::<String, Vec<String>>::new();
    for run in &runs {
        if let Some(parent_run_id) = &run.parent_run_id {
            children_by_parent
                .entry(parent_run_id.clone())
                .or_default()
                .push(run.id.clone());
        }
    }

    for run in &mut runs {
        run.child_run_ids = children_by_parent.remove(&run.id).unwrap_or_default();
    }

    runs
}

fn response_run_id(run: &RunSummary) -> String {
    run.run_id
        .clone()
        .unwrap_or_else(|| generated_run_id(&run.project_name, &run.trace_id, &run.span_id))
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

fn canonical_uuid(value: &str, field: &str) -> Result<String, ApiError> {
    Uuid::parse_str(value)
        .map(|uuid| uuid.to_string())
        .map_err(|error| ApiError::bad_request(format!("{field} must be a UUID: {error}")))
}

fn uuid_to_otel_trace_id(value: &str, field: &str) -> Result<String, ApiError> {
    Uuid::parse_str(value)
        .map(|uuid| uuid.simple().to_string())
        .map_err(|error| ApiError::bad_request(format!("{field} must be a UUID: {error}")))
}

fn uuid_simple(value: &str) -> String {
    Uuid::parse_str(value)
        .map(|uuid| uuid.simple().to_string())
        .unwrap_or_else(|_| value.replace('-', ""))
}

fn uuid_to_span_id(value: &str) -> String {
    uuid_simple(value).chars().take(16).collect()
}

fn otel_trace_id_to_uuid(trace_id: &str) -> String {
    if let Ok(uuid) = Uuid::parse_str(trace_id) {
        return uuid.to_string();
    }
    if trace_id.len() == 32 && trace_id.chars().all(|char| char.is_ascii_hexdigit()) {
        return format!(
            "{}-{}-{}-{}-{}",
            &trace_id[0..8],
            &trace_id[8..12],
            &trace_id[12..16],
            &trace_id[16..20],
            &trace_id[20..32]
        );
    }

    trace_id.to_owned()
}

fn normalize_trace_filter(trace_id: Option<String>) -> Option<String> {
    trace_id.map(|trace_id| {
        Uuid::parse_str(&trace_id)
            .map(|uuid| uuid.simple().to_string())
            .unwrap_or(trace_id)
    })
}

fn parse_time_nanos(value: Option<&str>) -> Result<Option<i64>, ApiError> {
    value
        .map(|value| {
            DateTime::parse_from_rfc3339(value)
                .map_err(|error| {
                    ApiError::bad_request(format!("timestamp must be RFC3339: {error}"))
                })
                .and_then(|datetime| {
                    datetime
                        .timestamp()
                        .checked_mul(1_000_000_000)
                        .and_then(|seconds| {
                            seconds.checked_add(i64::from(datetime.timestamp_subsec_nanos()))
                        })
                        .ok_or_else(|| {
                            ApiError::bad_request("timestamp is out of range".to_owned())
                        })
                })
        })
        .transpose()
}

fn current_time_nanos() -> i64 {
    let now = Utc::now();
    now.timestamp()
        .saturating_mul(1_000_000_000)
        .saturating_add(i64::from(now.timestamp_subsec_nanos()))
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
mod tests;
