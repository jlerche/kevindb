use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result, anyhow};
use kevindb_core::generated_project_id;
use refinery::embed_migrations;
use serde_json::Value;
use tokio_postgres::types::ToSql;
use tokio_postgres::{NoTls, Row};

const FEEDBACK_ROLLUP_BUCKET_NANOS: i64 = 60 * 60 * 1_000_000_000;

mod embedded {
    use super::embed_migrations;
    embed_migrations!("migrations");
}

pub async fn run_migrations(postgres_url: &str) -> Result<()> {
    let (mut client, connection) = tokio_postgres::connect(postgres_url, NoTls).await?;
    tokio::spawn(async move {
        if let Err(err) = connection.await {
            tracing::warn!(error = %err, "postgres migration connection failed");
        }
    });
    embedded::migrations::runner()
        .run_async(&mut client)
        .await?;
    Ok(())
}

#[derive(Debug, Clone, PartialEq)]
pub struct FeedbackRecord {
    pub id: String,
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
    pub created_at_unix_nano: i64,
    pub modified_at_unix_nano: i64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct FeedbackFilter {
    pub run_ids: Vec<String>,
    pub trace_ids: Vec<String>,
    pub project_names: Vec<String>,
    pub keys: Vec<String>,
    pub score: Option<f64>,
    pub score_min: Option<f64>,
    pub score_max: Option<f64>,
    pub value_texts: Vec<String>,
    pub created_time_min_unix_nano: Option<i64>,
    pub created_time_max_unix_nano: Option<i64>,
    pub limit: usize,
    pub offset: usize,
}

impl Default for FeedbackFilter {
    fn default() -> Self {
        Self {
            run_ids: Vec::new(),
            trace_ids: Vec::new(),
            project_names: Vec::new(),
            keys: Vec::new(),
            score: None,
            score_min: None,
            score_max: None,
            value_texts: Vec::new(),
            created_time_min_unix_nano: None,
            created_time_max_unix_nano: None,
            limit: 100,
            offset: 0,
        }
    }
}

#[derive(Debug, Clone)]
pub struct PostgresMetastore {
    postgres_url: String,
}

impl PostgresMetastore {
    pub fn new(postgres_url: impl Into<String>) -> Self {
        Self {
            postgres_url: postgres_url.into(),
        }
    }

    pub async fn insert_feedback(&self, feedback: &FeedbackRecord) -> Result<()> {
        let (mut client, connection) = tokio_postgres::connect(&self.postgres_url, NoTls)
            .await
            .context("connect postgres for feedback insert")?;
        tokio::spawn(async move {
            if let Err(err) = connection.await {
                tracing::warn!(error = %err, "postgres feedback insert connection failed");
            }
        });

        let tx = client
            .transaction()
            .await
            .context("begin feedback insert")?;
        let lock_id = feedback_lock_id(&feedback.id);
        tx.query_one("SELECT pg_advisory_lock($1)", &[&lock_id])
            .await
            .context("lock feedback write")?;
        let score_json = json_option_to_string(&feedback.score);
        let score_number = json_option_to_f64(&feedback.score);
        let value_json = json_option_to_string(&feedback.value);
        let value_text = json_option_to_scalar_text(&feedback.value);
        let correction_json = json_option_to_string(&feedback.correction);
        let feedback_source_json = json_option_to_string(&feedback.feedback_source);
        let extra_json = json_option_to_string(&feedback.extra);
        let old_rollup_bucket = tx
            .query_opt(
                "SELECT project_name, created_at_unix_nano
                FROM feedback
                WHERE id = $1
                FOR UPDATE",
                &[&feedback.id],
            )
            .await
            .context("load old feedback bucket for rollup refresh")?
            .and_then(|row| {
                row.get::<_, Option<String>>(0).map(|project_name| {
                    (project_name, feedback_rollup_bucket(row.get::<_, i64>(1)))
                })
            });
        if let Some(project_name) = &feedback.project_name {
            tx.execute(
                "INSERT INTO projects(name, id) VALUES ($1, $2) ON CONFLICT (name) DO NOTHING",
                &[project_name, &generated_project_id(project_name)],
            )
            .await
            .context("ensure feedback project")?;
        }
        let mut locked_projects = BTreeSet::new();
        if let Some((project_name, _)) = &old_rollup_bucket {
            locked_projects.insert(project_name.clone());
        }
        if let Some(project_name) = &feedback.project_name {
            locked_projects.insert(project_name.clone());
        }
        for project_name in locked_projects {
            tx.query_one(
                "SELECT name FROM projects WHERE name = $1 FOR UPDATE",
                &[&project_name],
            )
            .await
            .context("lock feedback rollup project")?;
        }
        let stored_feedback = tx
            .query_one(
                "INSERT INTO feedback(
                    id, run_id, trace_id, project_name, key, score_json, value_json,
                    score_number, value_text,
                    correction_json, comment, feedback_source_json, extra_json,
                    created_at_unix_nano, modified_at_unix_nano
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
                ON CONFLICT (id) DO UPDATE SET
                    run_id = EXCLUDED.run_id,
                    trace_id = EXCLUDED.trace_id,
                    project_name = EXCLUDED.project_name,
                    key = EXCLUDED.key,
                    score_json = EXCLUDED.score_json,
                    value_json = EXCLUDED.value_json,
                    score_number = EXCLUDED.score_number,
                    value_text = EXCLUDED.value_text,
                    correction_json = EXCLUDED.correction_json,
                    comment = EXCLUDED.comment,
                    feedback_source_json = EXCLUDED.feedback_source_json,
                    extra_json = EXCLUDED.extra_json,
                    modified_at_unix_nano = EXCLUDED.modified_at_unix_nano
                RETURNING project_name, created_at_unix_nano",
                &[
                    &feedback.id,
                    &feedback.run_id,
                    &feedback.trace_id,
                    &feedback.project_name,
                    &feedback.key,
                    &score_json,
                    &value_json,
                    &score_number,
                    &value_text,
                    &correction_json,
                    &feedback.comment,
                    &feedback_source_json,
                    &extra_json,
                    &feedback.created_at_unix_nano,
                    &feedback.modified_at_unix_nano,
                ],
            )
            .await
            .context("insert feedback")?;

        let mut affected_buckets = BTreeSet::new();
        if let Some(bucket) = old_rollup_bucket {
            affected_buckets.insert(bucket);
        }
        if let Some(project_name) = stored_feedback.get::<_, Option<String>>(0) {
            affected_buckets.insert((
                project_name,
                feedback_rollup_bucket(stored_feedback.get::<_, i64>(1)),
            ));
        }
        for (project_name, time_bucket_start_unix_nano) in affected_buckets {
            refresh_feedback_metric_rollups(&tx, &project_name, time_bucket_start_unix_nano)
                .await?;
        }
        tx.commit().await.context("commit feedback insert")?;
        let unlocked: bool = client
            .query_one("SELECT pg_advisory_unlock($1)", &[&lock_id])
            .await
            .context("unlock feedback write")?
            .get(0);
        if !unlocked {
            return Err(anyhow!("feedback write lock was not held at unlock"));
        }

        Ok(())
    }

    pub async fn list_feedback(&self, filter: FeedbackFilter) -> Result<Vec<FeedbackRecord>> {
        let (client, connection) = tokio_postgres::connect(&self.postgres_url, NoTls)
            .await
            .context("connect postgres for feedback list")?;
        tokio::spawn(async move {
            if let Err(err) = connection.await {
                tracing::warn!(error = %err, "postgres feedback list connection failed");
            }
        });

        let (sql, params) = list_feedback_query(&filter)?;
        let params = params
            .iter()
            .map(|param| param.as_ref() as &(dyn ToSql + Sync))
            .collect::<Vec<_>>();
        let rows = client.query(&sql, &params).await.context("list feedback")?;

        rows.into_iter().map(feedback_from_row).collect()
    }

    pub async fn load_feedback(&self, feedback_id: &str) -> Result<Option<FeedbackRecord>> {
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

type SqlParam = Box<dyn ToSql + Sync + Send>;

fn list_feedback_query(filter: &FeedbackFilter) -> Result<(String, Vec<SqlParam>)> {
    // TODO(mockgres): Use array parameters with cardinality() once mockgres supports that
    // PostgreSQL path; expanded typed parameters preserve the same semantics for now.
    let mut predicates = Vec::new();
    let mut params = Vec::<SqlParam>::new();
    push_text_list_predicate(&mut predicates, &mut params, "run_id", &filter.run_ids);
    push_text_list_predicate(&mut predicates, &mut params, "trace_id", &filter.trace_ids);
    push_text_list_predicate(
        &mut predicates,
        &mut params,
        "project_name",
        &filter.project_names,
    );
    push_text_list_predicate(&mut predicates, &mut params, "key", &filter.keys);
    push_score_predicate(
        &mut predicates,
        &mut params,
        "score_number =",
        "score",
        filter.score,
    )?;
    push_score_predicate(
        &mut predicates,
        &mut params,
        "score_number >=",
        "score_min",
        filter.score_min,
    )?;
    push_score_predicate(
        &mut predicates,
        &mut params,
        "score_number <=",
        "score_max",
        filter.score_max,
    )?;
    push_text_list_predicate(
        &mut predicates,
        &mut params,
        "value_text",
        &filter.value_texts,
    );
    push_optional_predicate(
        &mut predicates,
        &mut params,
        "created_at_unix_nano >=",
        filter.created_time_min_unix_nano,
    );
    push_optional_predicate(
        &mut predicates,
        &mut params,
        "created_at_unix_nano <=",
        filter.created_time_max_unix_nano,
    );

    let where_sql = if predicates.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", predicates.join(" AND "))
    };
    let limit = i64::try_from(filter.limit.min(1000)).context("feedback limit overflow")?;
    let offset = i64::try_from(filter.offset).context("feedback offset overflow")?;
    let limit_param = bind_param(&mut params, limit);
    let offset_param = bind_param(&mut params, offset);
    Ok((
        format!(
            "SELECT id, run_id, trace_id, project_name, key, score_json, value_json,
                correction_json, comment, feedback_source_json, extra_json,
                created_at_unix_nano, modified_at_unix_nano
            FROM feedback{where_sql}
            ORDER BY created_at_unix_nano ASC, id ASC
            LIMIT {limit_param} OFFSET {offset_param}"
        ),
        params,
    ))
}

fn push_text_list_predicate(
    predicates: &mut Vec<String>,
    params: &mut Vec<SqlParam>,
    column: &str,
    values: &[String],
) {
    if values.is_empty() {
        return;
    }
    let placeholders = values
        .iter()
        .cloned()
        .map(|value| bind_param(params, value))
        .collect::<Vec<_>>()
        .join(", ");
    predicates.push(format!("{column} IN ({placeholders})"));
}

fn push_score_predicate(
    predicates: &mut Vec<String>,
    params: &mut Vec<SqlParam>,
    expression: &str,
    name: &str,
    score: Option<f64>,
) -> Result<()> {
    if let Some(score) = score {
        if !score.is_finite() {
            return Err(anyhow!("feedback {name} must be finite"));
        }
        push_optional_predicate(predicates, params, expression, Some(score));
    }
    Ok(())
}

fn push_optional_predicate<T>(
    predicates: &mut Vec<String>,
    params: &mut Vec<SqlParam>,
    expression: &str,
    value: Option<T>,
) where
    T: ToSql + Sync + Send + 'static,
{
    if let Some(value) = value {
        let placeholder = bind_param(params, value);
        predicates.push(format!("{expression} {placeholder}"));
    }
}

fn bind_param<T>(params: &mut Vec<SqlParam>, value: T) -> String
where
    T: ToSql + Sync + Send + 'static,
{
    params.push(Box::new(value));
    format!("${}", params.len())
}

async fn refresh_feedback_metric_rollups(
    tx: &tokio_postgres::Transaction<'_>,
    project_name: &str,
    time_bucket_start_unix_nano: i64,
) -> Result<()> {
    tx.execute(
        "DELETE FROM feedback_metric_rollups
            WHERE project_name = $1 AND time_bucket_start_unix_nano = $2",
        &[&project_name, &time_bucket_start_unix_nano],
    )
    .await
    .context("delete old feedback aggregate rollups")?;

    let rows = tx
        .query(
            "SELECT key, created_at_unix_nano, score_number
            FROM feedback
            WHERE project_name = $1
                AND created_at_unix_nano >= $2
                AND created_at_unix_nano < $3",
            &[
                &project_name,
                &time_bucket_start_unix_nano,
                &time_bucket_start_unix_nano.saturating_add(FEEDBACK_ROLLUP_BUCKET_NANOS),
            ],
        )
        .await
        .context("load feedback rows for aggregate rollups")?;
    let mut groups = BTreeMap::<(i64, String), FeedbackRollupAccumulator>::new();
    for row in rows {
        let key: String = row.get(0);
        let created_at_unix_nano: i64 = row.get(1);
        groups
            .entry((feedback_rollup_bucket(created_at_unix_nano), key))
            .or_default()
            .push(row.get(2));
    }

    for ((time_bucket_start_unix_nano, key), accumulator) in groups {
        let stats = accumulator.finish();
        tx.execute(
            "INSERT INTO feedback_metric_rollups(
                    project_name, bucket_size_unix_nanos, time_bucket_start_unix_nano,
                    feedback_key, feedback_count, score_count, score_min, score_max,
                    score_avg, score_p50, score_p95, score_p99,
                    score_distribution_json
                )
                VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8,
                    $9, $10, $11, $12, $13
                )",
            &[
                &project_name,
                &FEEDBACK_ROLLUP_BUCKET_NANOS,
                &time_bucket_start_unix_nano,
                &key,
                &stats.feedback_count,
                &stats.score_count,
                &stats.score_min,
                &stats.score_max,
                &stats.score_avg,
                &stats.score_p50,
                &stats.score_p95,
                &stats.score_p99,
                &stats.score_distribution_json,
            ],
        )
        .await
        .context("insert feedback aggregate rollup")?;
    }

    Ok(())
}

fn feedback_rollup_bucket(timestamp_unix_nano: i64) -> i64 {
    timestamp_unix_nano.div_euclid(FEEDBACK_ROLLUP_BUCKET_NANOS) * FEEDBACK_ROLLUP_BUCKET_NANOS
}

fn feedback_lock_id(feedback_id: &str) -> i64 {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in feedback_id.bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash as i64
}

#[derive(Debug, Default)]
struct FeedbackRollupAccumulator {
    feedback_count: i64,
    score_numbers: Vec<f64>,
    missing_scores: u64,
}

impl FeedbackRollupAccumulator {
    fn push(&mut self, score: Option<f64>) {
        self.feedback_count += 1;
        match score.filter(|score| score.is_finite()) {
            Some(score) => self.score_numbers.push(score),
            None => self.missing_scores += 1,
        }
    }

    fn finish(mut self) -> FeedbackRollupStats {
        self.score_numbers.sort_by(f64::total_cmp);
        let score_count = self.score_numbers.len() as i64;
        FeedbackRollupStats {
            feedback_count: self.feedback_count,
            score_count,
            score_min: self.score_numbers.first().copied(),
            score_max: self.score_numbers.last().copied(),
            score_avg: avg_f64(&self.score_numbers),
            score_p50: percentile_f64(&self.score_numbers, 50),
            score_p95: percentile_f64(&self.score_numbers, 95),
            score_p99: percentile_f64(&self.score_numbers, 99),
            score_distribution_json: feedback_distribution_json(
                &self.score_numbers,
                self.missing_scores,
            ),
        }
    }
}

#[derive(Debug, Clone)]
struct FeedbackRollupStats {
    feedback_count: i64,
    score_count: i64,
    score_min: Option<f64>,
    score_max: Option<f64>,
    score_avg: Option<f64>,
    score_p50: Option<f64>,
    score_p95: Option<f64>,
    score_p99: Option<f64>,
    score_distribution_json: String,
}

fn avg_f64(values: &[f64]) -> Option<f64> {
    (!values.is_empty()).then(|| values.iter().sum::<f64>() / values.len() as f64)
}

fn percentile_f64(sorted_values: &[f64], percentile: usize) -> Option<f64> {
    if sorted_values.is_empty() {
        return None;
    }
    let index = ((sorted_values.len() - 1) * percentile).div_ceil(100);
    Some(sorted_values[index.min(sorted_values.len() - 1)])
}

fn feedback_distribution_json(values: &[f64], missing: u64) -> String {
    let mut distribution = BTreeMap::from([
        ("lt_0", 0_u64),
        ("0_to_0_25", 0),
        ("0_25_to_0_5", 0),
        ("0_5_to_0_75", 0),
        ("0_75_to_1", 0),
        ("gt_1", 0),
        ("missing", missing),
    ]);
    for value in values {
        let key = if *value < 0.0 {
            "lt_0"
        } else if *value < 0.25 {
            "0_to_0_25"
        } else if *value < 0.5 {
            "0_25_to_0_5"
        } else if *value < 0.75 {
            "0_5_to_0_75"
        } else if *value <= 1.0 {
            "0_75_to_1"
        } else {
            "gt_1"
        };
        *distribution.entry(key).or_default() += 1;
    }
    serde_json::to_string(&distribution).expect("serialize feedback distribution")
}

fn feedback_from_row(row: Row) -> Result<FeedbackRecord> {
    Ok(FeedbackRecord {
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
        created_at_unix_nano: row.get(11),
        modified_at_unix_nano: row.get(12),
    })
}

fn json_option_to_string(value: &Option<Value>) -> Option<String> {
    value.as_ref().map(Value::to_string)
}

fn json_option_to_f64(value: &Option<Value>) -> Option<f64> {
    match value.as_ref()? {
        Value::Number(number) => number.as_f64(),
        Value::String(value) => value.parse::<f64>().ok(),
        _ => None,
    }
}

fn json_option_to_scalar_text(value: &Option<Value>) -> Option<String> {
    match value.as_ref()? {
        Value::String(value) => Some(value.clone()),
        Value::Number(number) => Some(number.to_string()),
        Value::Bool(value) => Some(value.to_string()),
        _ => None,
    }
}

fn json_string_to_option(value: Option<String>) -> Result<Option<Value>> {
    value
        .map(|value| {
            serde_json::from_str(&value)
                .with_context(|| format!("parse stored feedback JSON value: {value}"))
        })
        .transpose()
}

#[cfg(test)]
mod tests {
    use std::process::Stdio;
    use std::time::{Duration, Instant};

    use super::*;
    use anyhow::anyhow;
    use serde_json::json;
    use tokio::process::{Child, Command};
    use tokio::time::sleep;

    #[tokio::test]
    async fn stores_loads_and_sql_filters_feedback() {
        let mockgres = Mockgres::start().await.expect("start mockgres");
        run_migrations(mockgres.postgres_url())
            .await
            .expect("run migrations");

        let metastore = PostgresMetastore::new(mockgres.postgres_url().to_owned());
        metastore
            .insert_feedback(&feedback("one", "run-a", "quality", 1))
            .await
            .expect("insert first feedback");
        metastore
            .insert_feedback(&feedback("two", "run-b", "cost", 2))
            .await
            .expect("insert second feedback");
        metastore
            .insert_feedback(&feedback("three", "run-a", "quality", 3))
            .await
            .expect("insert third feedback");
        metastore
            .insert_feedback(&feedback("four", "run-c", "quality's", 4))
            .await
            .expect("insert feedback with quoted key");

        let page = metastore
            .list_feedback(FeedbackFilter {
                run_ids: vec!["run-a".to_owned()],
                keys: vec!["quality".to_owned()],
                limit: 1,
                offset: 1,
                ..FeedbackFilter::default()
            })
            .await
            .expect("list filtered feedback");
        assert_eq!(page.len(), 1);
        assert_eq!(page[0].id, "three");
        assert_eq!(page[0].score, Some(json!(3)));

        let scalar_filtered = metastore
            .list_feedback(FeedbackFilter {
                trace_ids: vec!["trace-a".to_owned()],
                project_names: vec!["demo".to_owned()],
                keys: vec!["quality".to_owned()],
                score_min: Some(2.0),
                score_max: Some(3.0),
                value_texts: vec!["quality".to_owned()],
                ..FeedbackFilter::default()
            })
            .await
            .expect("list scalar filtered feedback");
        assert_eq!(
            scalar_filtered
                .iter()
                .map(|feedback| feedback.id.as_str())
                .collect::<Vec<_>>(),
            vec!["three"]
        );

        let loaded = metastore
            .load_feedback("one")
            .await
            .expect("load feedback")
            .expect("feedback exists");
        assert_eq!(loaded.run_id.as_deref(), Some("run-a"));
        assert_eq!(loaded.extra, Some(json!({"source": "test"})));

        let quoted = metastore
            .list_feedback(FeedbackFilter {
                keys: vec!["quality's".to_owned()],
                ..FeedbackFilter::default()
            })
            .await
            .expect("list feedback with quoted key");
        assert_eq!(quoted.len(), 1);
        assert_eq!(quoted[0].id, "four");

        let (client, connection) = tokio_postgres::connect(mockgres.postgres_url(), NoTls)
            .await
            .expect("connect postgres for feedback rollup check");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let rollup = client
            .query_one(
                "SELECT feedback_count, score_count, score_avg, score_distribution_json
                FROM feedback_metric_rollups
                WHERE project_name = 'demo' AND feedback_key = 'quality'",
                &[],
            )
            .await
            .expect("load feedback rollup");
        assert_eq!(rollup.get::<_, i64>(0), 2);
        assert_eq!(rollup.get::<_, i64>(1), 2);
        assert_eq!(rollup.get::<_, Option<f64>>(2), Some(2.0));
        let distribution: Value =
            serde_json::from_str(&rollup.get::<_, String>(3)).expect("parse distribution");
        assert_eq!(distribution["gt_1"], json!(1));

        mockgres.stop().await.expect("stop mockgres");
    }

    #[tokio::test]
    async fn concurrent_feedback_writes_serialize_rollup_refreshes() {
        let mockgres = Mockgres::start().await.expect("start mockgres");
        run_migrations(mockgres.postgres_url())
            .await
            .expect("run migrations");
        let metastore = PostgresMetastore::new(mockgres.postgres_url().to_owned());
        let (client, connection) = tokio_postgres::connect(mockgres.postgres_url(), NoTls)
            .await
            .expect("connect postgres");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
            .execute(
                "INSERT INTO projects(name, id) VALUES ('demo', 'demo-id')",
                &[],
            )
            .await
            .expect("seed concurrent feedback project");

        let first_feedback = feedback("one", "run-a", "quality", 1);
        let second_feedback = feedback("two", "run-b", "quality", 2);
        let (first, second) = tokio::join!(
            metastore.insert_feedback(&first_feedback),
            metastore.insert_feedback(&second_feedback),
        );
        first.expect("first concurrent feedback insert");
        second.expect("second concurrent feedback insert");

        let feedback_count: i64 = client
            .query_one(
                "SELECT feedback_count
                FROM feedback_metric_rollups
                WHERE project_name = 'demo' AND feedback_key = 'quality'",
                &[],
            )
            .await
            .expect("load serialized feedback rollup")
            .get(0);
        assert_eq!(feedback_count, 2);

        mockgres.stop().await.expect("stop mockgres");
    }

    #[tokio::test]
    async fn concurrent_feedback_id_reuse_does_not_leave_stale_project_rollups() {
        let mockgres = Mockgres::start().await.expect("start mockgres");
        run_migrations(mockgres.postgres_url())
            .await
            .expect("run migrations");
        let metastore = PostgresMetastore::new(mockgres.postgres_url().to_owned());
        let (client, connection) = tokio_postgres::connect(mockgres.postgres_url(), NoTls)
            .await
            .expect("connect postgres");
        tokio::spawn(async move {
            let _ = connection.await;
        });

        let mut first = feedback("shared", "run-a", "quality", 1);
        first.project_name = Some("first-project".to_owned());
        let mut second = feedback("shared", "run-a", "quality", 2);
        second.project_name = Some("second-project".to_owned());
        let (first_result, second_result) = tokio::join!(
            metastore.insert_feedback(&first),
            metastore.insert_feedback(&second),
        );
        first_result.expect("first concurrent feedback insert");
        second_result.expect("second concurrent feedback insert");

        let rollups = client
            .query(
                "SELECT project_name, feedback_count
                FROM feedback_metric_rollups
                WHERE feedback_key = 'quality'",
                &[],
            )
            .await
            .expect("load feedback rollups after project move");
        assert_eq!(rollups.len(), 1);
        assert_eq!(rollups[0].get::<_, i64>(1), 1);

        mockgres.stop().await.expect("stop mockgres");
    }

    fn feedback(id: &str, run_id: &str, key: &str, created_at_unix_nano: i64) -> FeedbackRecord {
        FeedbackRecord {
            id: id.to_owned(),
            run_id: Some(run_id.to_owned()),
            trace_id: Some("trace-a".to_owned()),
            project_name: Some("demo".to_owned()),
            key: key.to_owned(),
            score: Some(json!(created_at_unix_nano)),
            value: Some(json!(key)),
            correction: None,
            comment: Some("comment".to_owned()),
            feedback_source: None,
            extra: Some(json!({"source": "test"})),
            created_at_unix_nano,
            modified_at_unix_nano: created_at_unix_nano,
        }
    }

    struct Mockgres {
        child: Child,
        postgres_url: String,
    }

    impl Mockgres {
        async fn start() -> Result<Self> {
            let port = portpicker::pick_unused_port()
                .ok_or_else(|| anyhow!("could not reserve mockgres port"))?;
            let postgres_url = format!("postgresql://127.0.0.1:{port}/postgres");
            let child = Command::new("mockgres")
                .arg("--host")
                .arg("127.0.0.1")
                .arg("--port")
                .arg(port.to_string())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .context("spawn mockgres")?;
            let mockgres = Self {
                child,
                postgres_url,
            };
            mockgres.wait_until_ready().await?;
            Ok(mockgres)
        }

        fn postgres_url(&self) -> &str {
            &self.postgres_url
        }

        async fn stop(mut self) -> Result<()> {
            self.child.start_kill()?;
            let _ = self.child.wait().await?;
            Ok(())
        }

        async fn wait_until_ready(&self) -> Result<()> {
            let deadline = Instant::now() + Duration::from_secs(5);
            loop {
                match tokio_postgres::connect(&self.postgres_url, NoTls).await {
                    Ok((client, connection)) => {
                        tokio::spawn(async move {
                            let _ = connection.await;
                        });
                        if client.simple_query("SELECT 1").await.is_ok() {
                            return Ok(());
                        }
                    }
                    Err(_) if Instant::now() >= deadline => {
                        return Err(anyhow!(
                            "mockgres did not become ready on {}",
                            self.postgres_url
                        ));
                    }
                    Err(_) => {}
                }

                sleep(Duration::from_millis(50)).await;
            }
        }
    }

    impl Drop for Mockgres {
        fn drop(&mut self) {
            let _ = self.child.start_kill();
        }
    }
}
