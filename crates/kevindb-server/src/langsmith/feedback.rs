use anyhow::Context;
use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio_postgres::{NoTls, Row};
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
    state.insert_feedback(&feedback).await?;
    Ok(StatusCode::ACCEPTED)
}

pub(crate) async fn list_feedback(
    State(state): State<ServerState>,
    Query(query): Query<ListFeedbackQuery>,
) -> Result<Json<Vec<FeedbackResponse>>, ApiError> {
    Ok(Json(state.list_feedback(query, None).await?))
}

pub(crate) async fn list_run_feedback(
    State(state): State<ServerState>,
    Path(run_id): Path<String>,
    Query(query): Query<ListFeedbackQuery>,
) -> Result<Json<Vec<FeedbackResponse>>, ApiError> {
    let run_id = canonical_uuid(&run_id, "run_id")?;
    Ok(Json(state.list_feedback(query, Some(run_id)).await?))
}

pub(crate) async fn read_feedback(
    State(state): State<ServerState>,
    Path(feedback_id): Path<String>,
) -> Result<Json<FeedbackResponse>, ApiError> {
    let feedback_id = canonical_uuid(&feedback_id, "feedback_id")?;
    let feedback = state
        .load_feedback(&feedback_id)
        .await?
        .ok_or_else(|| ApiError::not_found("feedback not found".to_owned()))?;
    Ok(Json(feedback))
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
        let trace_id = self
            .trace_id
            .as_deref()
            .map(|trace_id| canonical_uuid(trace_id, "trace_id"))
            .transpose()?
            .or_else(|| run.as_ref().map(|run| otel_trace_id_to_uuid(&run.trace_id)));
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

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct ListFeedbackQuery {
    #[serde(default, alias = "run_id")]
    run: Option<StringList>,
    #[serde(default, alias = "feedback_key")]
    key: Option<StringList>,
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default)]
    offset: Option<usize>,
}

impl ServerState {
    async fn insert_feedback(&self, feedback: &FeedbackResponse) -> Result<(), ApiError> {
        let (client, connection) = tokio_postgres::connect(&self.postgres_url, NoTls)
            .await
            .context("connect postgres for feedback insert")?;
        tokio::spawn(async move {
            if let Err(err) = connection.await {
                tracing::warn!(error = %err, "postgres feedback insert connection failed");
            }
        });

        let created_at_unix_nano = parse_time_nanos(Some(&feedback.created_at))?.unwrap_or(0);
        let modified_at_unix_nano = parse_time_nanos(Some(&feedback.modified_at))?.unwrap_or(0);
        let score_json = json_option_to_string(&feedback.score);
        let value_json = json_option_to_string(&feedback.value);
        let correction_json = json_option_to_string(&feedback.correction);
        let feedback_source_json = json_option_to_string(&feedback.feedback_source);
        let extra_json = json_option_to_string(&feedback.extra);
        client
            .execute(
                "INSERT INTO feedback(
                    id, run_id, trace_id, project_name, key, score_json, value_json,
                    correction_json, comment, feedback_source_json, extra_json,
                    created_at_unix_nano, modified_at_unix_nano
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                ON CONFLICT (id) DO UPDATE SET
                    run_id = EXCLUDED.run_id,
                    trace_id = EXCLUDED.trace_id,
                    project_name = EXCLUDED.project_name,
                    key = EXCLUDED.key,
                    score_json = EXCLUDED.score_json,
                    value_json = EXCLUDED.value_json,
                    correction_json = EXCLUDED.correction_json,
                    comment = EXCLUDED.comment,
                    feedback_source_json = EXCLUDED.feedback_source_json,
                    extra_json = EXCLUDED.extra_json,
                    modified_at_unix_nano = EXCLUDED.modified_at_unix_nano",
                &[
                    &feedback.id,
                    &feedback.run_id,
                    &feedback.trace_id,
                    &feedback.project_name,
                    &feedback.key,
                    &score_json,
                    &value_json,
                    &correction_json,
                    &feedback.comment,
                    &feedback_source_json,
                    &extra_json,
                    &created_at_unix_nano,
                    &modified_at_unix_nano,
                ],
            )
            .await
            .context("insert feedback")?;

        Ok(())
    }

    async fn list_feedback(
        &self,
        query: ListFeedbackQuery,
        path_run_id: Option<String>,
    ) -> Result<Vec<FeedbackResponse>, ApiError> {
        let (client, connection) = tokio_postgres::connect(&self.postgres_url, NoTls)
            .await
            .context("connect postgres for feedback list")?;
        tokio::spawn(async move {
            if let Err(err) = connection.await {
                tracing::warn!(error = %err, "postgres feedback list connection failed");
            }
        });

        let rows = client
            .query(
                "SELECT id, run_id, trace_id, project_name, key, score_json, value_json,
                    correction_json, comment, feedback_source_json, extra_json,
                    created_at_unix_nano, modified_at_unix_nano
                FROM feedback
                ORDER BY created_at_unix_nano ASC, id ASC",
                &[],
            )
            .await
            .context("list feedback")?;
        let run_ids = match path_run_id {
            Some(run_id) => vec![run_id],
            None => query
                .run
                .map(StringList::into_vec)
                .unwrap_or_default()
                .into_iter()
                .map(|run_id| canonical_uuid(&run_id, "run_id"))
                .collect::<Result<Vec<_>, _>>()?,
        };
        let keys = query.key.map(StringList::into_vec).unwrap_or_default();
        let offset = query.offset.unwrap_or(0);
        let limit = query.limit.unwrap_or(100).min(1000);

        Ok(rows
            .into_iter()
            .map(feedback_from_row)
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .filter(|feedback| {
                run_ids.is_empty()
                    || feedback
                        .run_id
                        .as_ref()
                        .is_some_and(|run_id| run_ids.contains(run_id))
            })
            .filter(|feedback| keys.is_empty() || keys.contains(&feedback.key))
            .skip(offset)
            .take(limit)
            .collect())
    }

    async fn load_feedback(&self, feedback_id: &str) -> Result<Option<FeedbackResponse>, ApiError> {
        let (client, connection) = tokio_postgres::connect(&self.postgres_url, NoTls)
            .await
            .context("connect postgres for feedback lookup")?;
        tokio::spawn(async move {
            if let Err(err) = connection.await {
                tracing::warn!(error = %err, "postgres feedback lookup connection failed");
            }
        });

        client
            .query_opt(
                "SELECT id, run_id, trace_id, project_name, key, score_json, value_json,
                    correction_json, comment, feedback_source_json, extra_json,
                    created_at_unix_nano, modified_at_unix_nano
                FROM feedback
                WHERE id = $1",
                &[&feedback_id],
            )
            .await
            .context("load feedback")?
            .map(feedback_from_row)
            .transpose()
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

fn feedback_from_row(row: Row) -> Result<FeedbackResponse, ApiError> {
    let created_at_unix_nano: i64 = row.get(11);
    let modified_at_unix_nano: i64 = row.get(12);

    Ok(FeedbackResponse {
        id: row.get(0),
        run_id: row.get(1),
        trace_id: row.get(2),
        project_name: row.get(3),
        key: row.get(4),
        score: json_string_to_option(row.get(5))?,
        value: json_string_to_option(row.get(6))?,
        correction: json_string_to_option(row.get(7))?,
        comment: row.get(8),
        feedback_source: json_string_to_option(row.get(9))?,
        extra: json_string_to_option(row.get(10))?,
        created_at: unix_nano_to_rfc3339(created_at_unix_nano),
        modified_at: unix_nano_to_rfc3339(modified_at_unix_nano),
    })
}

fn json_option_to_string(value: &Option<Value>) -> Option<String> {
    value.as_ref().map(Value::to_string)
}

fn json_string_to_option(value: Option<String>) -> Result<Option<Value>, ApiError> {
    value
        .map(|value| {
            serde_json::from_str(&value)
                .with_context(|| format!("parse stored feedback JSON value: {value}"))
                .map_err(ApiError::from)
        })
        .transpose()
}

fn feedback_uuid(run_id: Option<&str>, key: &str, created_at_unix_nano: i64) -> String {
    Uuid::new_v5(
        &Uuid::NAMESPACE_URL,
        format!(
            "kevindb:feedback:{}:{key}:{created_at_unix_nano}",
            run_id.unwrap_or("")
        )
        .as_bytes(),
    )
    .to_string()
}
