use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result};
use refinery::embed_migrations;
use serde_json::Value;
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
        let (client, connection) = tokio_postgres::connect(&self.postgres_url, NoTls)
            .await
            .context("connect postgres for feedback insert")?;
        tokio::spawn(async move {
            if let Err(err) = connection.await {
                tracing::warn!(error = %err, "postgres feedback insert connection failed");
            }
        });

        let score_json = json_option_to_string(&feedback.score);
        let score_number = json_option_to_f64(&feedback.score);
        let value_json = json_option_to_string(&feedback.value);
        let value_text = json_option_to_scalar_text(&feedback.value);
        let correction_json = json_option_to_string(&feedback.correction);
        let feedback_source_json = json_option_to_string(&feedback.feedback_source);
        let extra_json = json_option_to_string(&feedback.extra);
        if let Some(project_name) = &feedback.project_name {
            client
                .execute(
                    "INSERT INTO projects(name) VALUES ($1) ON CONFLICT (name) DO NOTHING",
                    &[project_name],
                )
                .await
                .context("ensure feedback project")?;
        }
        let old_rollup_bucket = client
            .query_opt(
                "SELECT project_name, created_at_unix_nano FROM feedback WHERE id = $1",
                &[&feedback.id],
            )
            .await
            .context("load old feedback bucket for rollup refresh")?
            .and_then(|row| {
                row.get::<_, Option<String>>(0).map(|project_name| {
                    (project_name, feedback_rollup_bucket(row.get::<_, i64>(1)))
                })
            });
        client
            .execute(
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
                    modified_at_unix_nano = EXCLUDED.modified_at_unix_nano",
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
        if let Some(project_name) = feedback.project_name.clone() {
            affected_buckets.insert((
                project_name,
                feedback_rollup_bucket(feedback.created_at_unix_nano),
            ));
        }
        for (project_name, time_bucket_start_unix_nano) in affected_buckets {
            refresh_feedback_metric_rollups(&client, &project_name, time_bucket_start_unix_nano)
                .await?;
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

        let rows = client
            .query(list_feedback_sql(&filter).as_str(), &[])
            .await
            .context("list feedback")?;

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

fn list_feedback_sql(filter: &FeedbackFilter) -> String {
    let mut predicates = Vec::new();
    if !filter.run_ids.is_empty() {
        predicates.push(format!("run_id IN ({})", sql_string_list(&filter.run_ids)));
    }
    if !filter.trace_ids.is_empty() {
        predicates.push(format!(
            "trace_id IN ({})",
            sql_string_list(&filter.trace_ids)
        ));
    }
    if !filter.project_names.is_empty() {
        predicates.push(format!(
            "project_name IN ({})",
            sql_string_list(&filter.project_names)
        ));
    }
    if !filter.keys.is_empty() {
        predicates.push(format!("key IN ({})", sql_string_list(&filter.keys)));
    }
    if let Some(score) = filter.score {
        predicates.push(format!("score_number = {score}"));
    }
    if let Some(score_min) = filter.score_min {
        predicates.push(format!("score_number >= {score_min}"));
    }
    if let Some(score_max) = filter.score_max {
        predicates.push(format!("score_number <= {score_max}"));
    }
    if !filter.value_texts.is_empty() {
        predicates.push(format!(
            "value_text IN ({})",
            sql_string_list(&filter.value_texts)
        ));
    }
    if let Some(created_time_min_unix_nano) = filter.created_time_min_unix_nano {
        predicates.push(format!(
            "created_at_unix_nano >= {created_time_min_unix_nano}"
        ));
    }
    if let Some(created_time_max_unix_nano) = filter.created_time_max_unix_nano {
        predicates.push(format!(
            "created_at_unix_nano <= {created_time_max_unix_nano}"
        ));
    }

    let where_sql = if predicates.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", predicates.join(" AND "))
    };
    let limit = filter.limit.min(1000);

    format!(
        "SELECT id, run_id, trace_id, project_name, key, score_json, value_json,
            correction_json, comment, feedback_source_json, extra_json,
            created_at_unix_nano, modified_at_unix_nano
        FROM feedback{where_sql}
        ORDER BY created_at_unix_nano ASC, id ASC
        LIMIT {limit} OFFSET {}",
        filter.offset
    )
}

async fn refresh_feedback_metric_rollups(
    client: &tokio_postgres::Client,
    project_name: &str,
    time_bucket_start_unix_nano: i64,
) -> Result<()> {
    client
        .execute(
            "DELETE FROM feedback_metric_rollups
            WHERE project_name = $1 AND time_bucket_start_unix_nano = $2",
            &[&project_name, &time_bucket_start_unix_nano],
        )
        .await
        .context("delete old feedback aggregate rollups")?;

    let rows = client
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
        client
            .execute(
                "INSERT INTO feedback_metric_rollups(
                    project_name, bucket_size_unix_nanos, time_bucket_start_unix_nano,
                    feedback_key, feedback_count, score_count, score_min, score_max,
                    score_avg, score_p50, score_p95, score_p99,
                    score_distribution_json, updated_at
                )
                VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8,
                    $9, $10, $11, $12, $13, CURRENT_TIMESTAMP
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

fn sql_string_list(values: &[String]) -> String {
    values
        .iter()
        .map(|value| sql_string_literal(value))
        .collect::<Vec<_>>()
        .join(", ")
}

fn sql_string_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

#[cfg(test)]
mod tests {
    use std::process::Stdio;
    use std::time::{Duration, Instant};

    use super::*;
    use anyhow::anyhow;
    use refinery::Target;
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
    async fn upgrades_nonempty_v8_schema_through_idempotency_and_locators() {
        let mockgres = Mockgres::start().await.expect("start mockgres");
        let (mut client, connection) = tokio_postgres::connect(mockgres.postgres_url(), NoTls)
            .await
            .expect("connect postgres");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        embedded::migrations::runner()
            .set_target(Target::Version(8))
            .run_async(&mut client)
            .await
            .expect("migrate through v8");
        client
            .batch_execute(
                "INSERT INTO projects(name) VALUES ('demo');
                INSERT INTO trace_segments(
                    project_name, uri, etag, total_bytes, span_count,
                    min_start_time_unix_nano, max_end_time_unix_nano,
                    time_bucket_start_unix_nano
                ) VALUES ('demo', 'segment.vortex', 'etag', 1, 1, 1, 2, 0);
                INSERT INTO run_events(
                    trace_segment_id, project_name, run_id, trace_id, span_id,
                    event_type, event_time_unix_nano, row_index
                ) VALUES (1, 'demo', 'run-a', 'trace-a', 'span-a', 'end', 2, 0);
                INSERT INTO run_heads(
                    project_name, trace_id, span_id, name,
                    start_time_unix_nano, end_time_unix_nano, status_code,
                    last_trace_segment_id, run_type, status, is_root, run_id,
                    last_event_type, last_event_time_unix_nano,
                    last_row_index, last_run_event_id
                ) VALUES (
                    'demo', 'trace-a', 'span-a', 'run-a', 1, 2, 1,
                    1, 'chain', 'success', true, 'run-a', 'end', 2, 0, 1
                );",
            )
            .await
            .expect("seed v8 rows");

        client
            .batch_execute(include_str!(
                "../migrations/V9__add_run_trace_locators_and_idempotency.sql"
            ))
            .await
            .expect("apply v9 upgrade");

        let event_key: String = client
            .query_one("SELECT idempotency_key FROM run_events WHERE id = 1", &[])
            .await
            .expect("load migrated idempotency key")
            .get(0);
        assert_eq!(event_key, "migrated-run-event:1");
        let locator_count: i64 = client
            .query_one(
                "SELECT count(*) FROM run_locators WHERE run_id = 'run-a'",
                &[],
            )
            .await
            .expect("load migrated locator")
            .get(0);
        assert_eq!(locator_count, 1);

        mockgres.stop().await.expect("stop mockgres");
    }

    #[test]
    fn list_feedback_sql_escapes_filter_values() {
        let sql = list_feedback_sql(&FeedbackFilter {
            run_ids: vec!["run-a".to_owned()],
            trace_ids: vec!["trace-a".to_owned()],
            project_names: vec!["demo".to_owned()],
            keys: vec!["quality's".to_owned()],
            score_min: Some(0.5),
            value_texts: vec!["good".to_owned()],
            limit: 2000,
            offset: 3,
            ..FeedbackFilter::default()
        });

        assert!(sql.contains("run_id IN ('run-a')"));
        assert!(sql.contains("trace_id IN ('trace-a')"));
        assert!(sql.contains("project_name IN ('demo')"));
        assert!(sql.contains("key IN ('quality''s')"));
        assert!(sql.contains("score_number >= 0.5"));
        assert!(sql.contains("value_text IN ('good')"));
        assert!(sql.contains("LIMIT 1000 OFFSET 3"));
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
