use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use kevindb_metastore_postgres::{FeedbackFilter, FeedbackRecord, PostgresMetastore};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use super::{
    StringList, canonical_uuid, current_time_nanos, otel_trace_id_to_uuid, parse_time_nanos,
    unix_nano_to_rfc3339,
};
use crate::{ApiError, ServerState};

pub(crate) async fn create_feedback(
    State(state): State<ServerState>,
    Json(request): Json<FeedbackWriteRequest>,
) -> Result<StatusCode, ApiError> {
    let feedback = request.into_feedback(&state).await?;
    metastore(&state)
        .insert_feedback(&feedback.to_record()?)
        .await?;
    Ok(StatusCode::ACCEPTED)
}

pub(crate) async fn list_feedback(
    State(state): State<ServerState>,
    Query(query): Query<ListFeedbackQuery>,
) -> Result<Json<Vec<FeedbackResponse>>, ApiError> {
    let records = metastore(&state)
        .list_feedback(query.into_filter(None)?)
        .await?;
    Ok(Json(
        records.into_iter().map(FeedbackResponse::from).collect(),
    ))
}

pub(crate) async fn list_run_feedback(
    State(state): State<ServerState>,
    Path(run_id): Path<String>,
    Query(query): Query<ListFeedbackQuery>,
) -> Result<Json<Vec<FeedbackResponse>>, ApiError> {
    let run_id = canonical_uuid(&run_id, "run_id")?;
    let records = metastore(&state)
        .list_feedback(query.into_filter(Some(run_id))?)
        .await?;
    Ok(Json(
        records.into_iter().map(FeedbackResponse::from).collect(),
    ))
}

pub(crate) async fn read_feedback(
    State(state): State<ServerState>,
    Path(feedback_id): Path<String>,
) -> Result<Json<FeedbackResponse>, ApiError> {
    let feedback_id = canonical_uuid(&feedback_id, "feedback_id")?;
    let feedback = metastore(&state)
        .load_feedback(&feedback_id)
        .await?
        .map(FeedbackResponse::from)
        .ok_or_else(|| ApiError::not_found("feedback not found".to_owned()))?;
    Ok(Json(feedback))
}

pub(crate) async fn update_feedback(
    State(state): State<ServerState>,
    Path(feedback_id): Path<String>,
    Json(request): Json<FeedbackUpdateRequest>,
) -> Result<StatusCode, ApiError> {
    let feedback_id = canonical_uuid(&feedback_id, "feedback_id")?;
    let store = metastore(&state);
    let mut record = store
        .load_feedback(&feedback_id)
        .await?
        .ok_or_else(|| ApiError::not_found("feedback not found".to_owned()))?;
    request.apply(&mut record);
    store.insert_feedback(&record).await?;
    Ok(StatusCode::OK)
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct FeedbackWriteRequest {
    id: Option<String>,
    run_id: Option<String>,
    trace_id: Option<String>,
    key: String,
    score: Option<Value>,
    value: Option<Value>,
    correction: Option<Value>,
    comment: Option<String>,
    feedback_source: Option<Value>,
    extra: Option<Value>,
    created_at: Option<String>,
    modified_at: Option<String>,
}

impl FeedbackWriteRequest {
    async fn into_feedback(self, state: &ServerState) -> Result<FeedbackResponse, ApiError> {
        let run_id = self
            .run_id
            .as_deref()
            .map(|run_id| canonical_uuid(run_id, "run_id"))
            .transpose()?;
        let run = match &run_id {
            Some(run_id) => Some(
                state
                    .load_run_summary_by_id(run_id)
                    .await?
                    .ok_or_else(|| ApiError::not_found("run not found".to_owned()))?,
            ),
            None => None,
        };
        let requested_trace_id = self
            .trace_id
            .as_deref()
            .map(|trace_id| canonical_uuid(trace_id, "trace_id"))
            .transpose()?;
        let trace_id = match requested_trace_id {
            Some(trace_id) => Some(trace_id),
            None => run
                .as_ref()
                .map(|run| otel_trace_id_to_uuid(&run.trace_id))
                .transpose()?,
        };
        let project_name = run.as_ref().map(|run| run.project_name.clone());
        let created_at_unix_nano =
            parse_time_nanos(self.created_at.as_deref())?.unwrap_or_else(current_time_nanos);
        let modified_at_unix_nano =
            parse_time_nanos(self.modified_at.as_deref())?.unwrap_or(created_at_unix_nano);
        let id = self
            .id
            .as_deref()
            .map(|id| canonical_uuid(id, "feedback_id"))
            .transpose()?
            .unwrap_or_else(|| feedback_uuid(run_id.as_deref(), &self.key, created_at_unix_nano));

        Ok(FeedbackResponse {
            id,
            created_at: unix_nano_to_rfc3339(created_at_unix_nano),
            modified_at: unix_nano_to_rfc3339(modified_at_unix_nano),
            run_id,
            trace_id,
            project_name,
            key: self.key,
            score: self.score,
            value: self.value,
            correction: self.correction,
            comment: self.comment,
            feedback_source: self.feedback_source,
            extra: self.extra,
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
pub struct FeedbackUpdateRequest {
    score: Option<Value>,
    value: Option<Value>,
    correction: Option<Value>,
    comment: Option<String>,
}

impl FeedbackUpdateRequest {
    fn apply(self, record: &mut FeedbackRecord) {
        if let Some(score) = self.score {
            record.score = Some(score);
        }
        if let Some(value) = self.value {
            record.value = Some(value);
        }
        if let Some(correction) = self.correction {
            record.correction = Some(correction);
        }
        if let Some(comment) = self.comment {
            record.comment = Some(comment);
        }
        record.modified_at_unix_nano = current_time_nanos();
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct ListFeedbackQuery {
    #[serde(default, alias = "run_id")]
    run: Option<StringList>,
    #[serde(default, alias = "trace_id")]
    trace: Option<StringList>,
    #[serde(default, alias = "project", alias = "session", alias = "session_name")]
    project_name: Option<StringList>,
    #[serde(default, alias = "feedback_key")]
    key: Option<StringList>,
    #[serde(default, alias = "feedback_score")]
    score: Option<f64>,
    #[serde(default, alias = "min_score", alias = "feedback_score_min")]
    score_min: Option<f64>,
    #[serde(default, alias = "max_score", alias = "feedback_score_max")]
    score_max: Option<f64>,
    #[serde(default, alias = "feedback_value", alias = "value_text")]
    value: Option<StringList>,
    #[serde(default, alias = "created_after", alias = "created_at_start")]
    created_at_min: Option<String>,
    #[serde(default, alias = "created_before", alias = "created_at_end")]
    created_at_max: Option<String>,
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default)]
    offset: Option<usize>,
}

impl ListFeedbackQuery {
    fn into_filter(self, path_run_id: Option<String>) -> Result<FeedbackFilter, ApiError> {
        let run_ids = match path_run_id {
            Some(run_id) => vec![run_id],
            None => self
                .run
                .map(StringList::into_vec)
                .unwrap_or_default()
                .into_iter()
                .map(|run_id| canonical_uuid(&run_id, "run_id"))
                .collect::<Result<Vec<_>, _>>()?,
        };
        let trace_ids = self
            .trace
            .map(StringList::into_vec)
            .unwrap_or_default()
            .into_iter()
            .map(|trace_id| canonical_uuid(&trace_id, "trace_id"))
            .collect::<Result<Vec<_>, _>>()?;

        Ok(FeedbackFilter {
            run_ids,
            trace_ids,
            project_names: self
                .project_name
                .map(StringList::into_vec)
                .unwrap_or_default(),
            keys: self.key.map(StringList::into_vec).unwrap_or_default(),
            score: self.score,
            score_min: self.score_min,
            score_max: self.score_max,
            value_texts: self.value.map(StringList::into_vec).unwrap_or_default(),
            created_time_min_unix_nano: parse_time_nanos(self.created_at_min.as_deref())?,
            created_time_max_unix_nano: parse_time_nanos(self.created_at_max.as_deref())?,
            limit: self.limit.unwrap_or(100).min(1000),
            offset: self.offset.unwrap_or(0),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FeedbackResponse {
    pub id: String,
    pub created_at: String,
    pub modified_at: String,
    pub run_id: Option<String>,
    pub trace_id: Option<String>,
    pub project_name: Option<String>,
    pub key: String,
    pub score: Option<Value>,
    pub value: Option<Value>,
    pub correction: Option<Value>,
    pub comment: Option<String>,
    pub feedback_source: Option<Value>,
    pub extra: Option<Value>,
}

impl FeedbackResponse {
    fn to_record(&self) -> Result<FeedbackRecord, ApiError> {
        Ok(FeedbackRecord {
            id: self.id.clone(),
            run_id: self.run_id.clone(),
            trace_id: self.trace_id.clone(),
            project_name: self.project_name.clone(),
            key: self.key.clone(),
            score: self.score.clone(),
            value: self.value.clone(),
            correction: self.correction.clone(),
            comment: self.comment.clone(),
            feedback_source: self.feedback_source.clone(),
            extra: self.extra.clone(),
            created_at_unix_nano: parse_time_nanos(Some(&self.created_at))?
                .ok_or_else(|| ApiError::bad_request("created_at is required".to_owned()))?,
            modified_at_unix_nano: parse_time_nanos(Some(&self.modified_at))?
                .ok_or_else(|| ApiError::bad_request("modified_at is required".to_owned()))?,
        })
    }
}

impl From<FeedbackRecord> for FeedbackResponse {
    fn from(record: FeedbackRecord) -> Self {
        Self {
            id: record.id,
            created_at: unix_nano_to_rfc3339(record.created_at_unix_nano),
            modified_at: unix_nano_to_rfc3339(record.modified_at_unix_nano),
            run_id: record.run_id,
            trace_id: record.trace_id,
            project_name: record.project_name,
            key: record.key,
            score: record.score,
            value: record.value,
            correction: record.correction,
            comment: record.comment,
            feedback_source: record.feedback_source,
            extra: record.extra,
        }
    }
}

fn feedback_uuid(run_id: Option<&str>, key: &str, created_at_unix_nano: i64) -> String {
    let owner = run_id
        .map(|run_id| format!("run:{run_id}"))
        .unwrap_or_else(|| "unscoped".to_owned());
    Uuid::new_v5(
        &Uuid::NAMESPACE_URL,
        format!("kevindb:feedback:{owner}:{key}:{created_at_unix_nano}").as_bytes(),
    )
    .to_string()
}

fn metastore(state: &ServerState) -> PostgresMetastore {
    PostgresMetastore::new(state.postgres_url.clone())
}
