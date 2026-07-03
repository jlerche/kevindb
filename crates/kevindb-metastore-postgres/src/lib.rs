use anyhow::{Context, Result};
use serde_json::Value;
use tokio_postgres::{NoTls, Row};

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeedbackFilter {
    pub run_ids: Vec<String>,
    pub keys: Vec<String>,
    pub limit: usize,
    pub offset: usize,
}

impl Default for FeedbackFilter {
    fn default() -> Self {
        Self {
            run_ids: Vec::new(),
            keys: Vec::new(),
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
                    &feedback.created_at_unix_nano,
                    &feedback.modified_at_unix_nano,
                ],
            )
            .await
            .context("insert feedback")?;

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
    if !filter.keys.is_empty() {
        predicates.push(format!("key IN ({})", sql_string_list(&filter.keys)));
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
    use kevindb::db::run_migrations;
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
            })
            .await
            .expect("list filtered feedback");
        assert_eq!(page.len(), 1);
        assert_eq!(page[0].id, "three");
        assert_eq!(page[0].score, Some(json!(3)));

        let loaded = metastore
            .load_feedback("one")
            .await
            .expect("load feedback")
            .expect("feedback exists");
        assert_eq!(loaded.run_id.as_deref(), Some("run-a"));
        assert_eq!(loaded.extra, Some(json!({"source": "test"})));

        mockgres.stop().await.expect("stop mockgres");
    }

    #[test]
    fn list_feedback_sql_escapes_filter_values() {
        let sql = list_feedback_sql(&FeedbackFilter {
            run_ids: vec!["run-a".to_owned()],
            keys: vec!["quality's".to_owned()],
            limit: 2000,
            offset: 3,
        });

        assert!(sql.contains("run_id IN ('run-a')"));
        assert!(sql.contains("key IN ('quality''s')"));
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
            value: Some(json!({"label": key})),
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
