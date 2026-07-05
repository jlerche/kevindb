use std::collections::{BTreeMap, HashSet};
use std::time::Instant;

use anyhow::{Context, Result, anyhow};
use tokio_postgres::{NoTls, Row};

use super::super::{RunQueryDiagnostics, sql_string_literal};
use super::{
    AGGREGATE_ROLLUP_BUCKET_UNIX_NANOS, FeedbackScoreStats, NumericStats, RunAggregateGroup,
    RunAggregateMetrics, RunAggregateQuery, RunAggregateResult, RunAggregateRow,
    RunAggregateSource,
};

pub(super) async fn try_rollup_aggregate(
    postgres_url: &str,
    query: &RunAggregateQuery,
) -> Result<Option<RunAggregateResult>> {
    if !rollup_eligible(query) {
        return Ok(None);
    }

    let postgres_started = Instant::now();
    let (client, connection) = tokio_postgres::connect(postgres_url, NoTls)
        .await
        .context("connect postgres for aggregate rollup")?;
    tokio::spawn(async move {
        if let Err(err) = connection.await {
            tracing::warn!(error = %err, "postgres aggregate rollup connection failed");
        }
    });

    let (sql, source) = rollup_sql(query)?;
    let rows = client
        .query(sql.as_str(), &[])
        .await
        .context("load aggregate rollups")?;
    let postgres_query_time = postgres_started.elapsed();
    let aggregate_rows = match source {
        RunAggregateSource::FeedbackRollup => rows
            .into_iter()
            .map(feedback_rollup_row)
            .collect::<Result<Vec<_>>>()?,
        _ => rows.into_iter().map(run_rollup_row).collect(),
    };
    Ok(Some(RunAggregateResult {
        diagnostics: RunQueryDiagnostics {
            rows_returned: aggregate_rows.len(),
            postgres_query_time,
            ..RunQueryDiagnostics::default()
        },
        rows: aggregate_rows,
        source,
    }))
}

fn rollup_eligible(query: &RunAggregateQuery) -> bool {
    let Some(kind) = rollup_kind(query) else {
        return false;
    };

    query.filter.is_none()
        && query.trace_filter.is_none()
        && query.tree_filter.is_none()
        && !query.include_deleted
        && query.time_bucket_nanos == Some(AGGREGATE_ROLLUP_BUCKET_UNIX_NANOS)
        && rollup_filters_are_exact(query, kind)
        && time_filters_cover_whole_rollup_buckets(query)
}

fn rollup_kind(query: &RunAggregateQuery) -> Option<&'static str> {
    let groups = query.group_by.iter().copied().collect::<HashSet<_>>();
    if groups == HashSet::from([RunAggregateGroup::Project, RunAggregateGroup::TimeBucket]) {
        Some("project")
    } else if groups
        == HashSet::from([
            RunAggregateGroup::Project,
            RunAggregateGroup::TimeBucket,
            RunAggregateGroup::RunType,
        ])
    {
        Some("run_type")
    } else if groups
        == HashSet::from([
            RunAggregateGroup::Project,
            RunAggregateGroup::TimeBucket,
            RunAggregateGroup::Model,
        ])
    {
        Some("model")
    } else if groups
        == HashSet::from([
            RunAggregateGroup::Project,
            RunAggregateGroup::TimeBucket,
            RunAggregateGroup::Error,
        ])
    {
        Some("error")
    } else if groups
        == HashSet::from([
            RunAggregateGroup::Project,
            RunAggregateGroup::TimeBucket,
            RunAggregateGroup::FeedbackKey,
        ])
    {
        Some("feedback")
    } else {
        None
    }
}

fn rollup_filters_are_exact(query: &RunAggregateQuery, kind: &str) -> bool {
    match kind {
        "run_type" => query.error.is_none(),
        "error" => query.run_type.is_none(),
        "project" | "model" | "feedback" => query.run_type.is_none() && query.error.is_none(),
        _ => false,
    }
}

fn time_filters_cover_whole_rollup_buckets(query: &RunAggregateQuery) -> bool {
    query
        .start_time_min_unix_nano
        .is_none_or(|min| min.rem_euclid(AGGREGATE_ROLLUP_BUCKET_UNIX_NANOS) == 0)
        && query.start_time_max_unix_nano.is_none_or(|max| {
            max.checked_add(1).is_some_and(|exclusive_end| {
                exclusive_end.rem_euclid(AGGREGATE_ROLLUP_BUCKET_UNIX_NANOS) == 0
            })
        })
}

fn rollup_sql(query: &RunAggregateQuery) -> Result<(String, RunAggregateSource)> {
    let Some(kind) = rollup_kind(query) else {
        return Err(anyhow!("unsupported rollup shape"));
    };
    let mut predicates = vec![
        format!(
            "project_name IN ({})",
            sql_string_list(&query.project_names)
        ),
        format!("bucket_size_unix_nanos = {AGGREGATE_ROLLUP_BUCKET_UNIX_NANOS}"),
    ];
    if let Some(min_time) = query.start_time_min_unix_nano {
        predicates.push(format!("time_bucket_start_unix_nano >= {min_time}"));
    }
    if let Some(max_time) = query.start_time_max_unix_nano {
        predicates.push(format!("time_bucket_start_unix_nano <= {max_time}"));
    }

    if kind == "feedback" {
        if !query.feedback_keys.is_empty() {
            predicates.push(format!(
                "feedback_key IN ({})",
                sql_string_list(&query.feedback_keys)
            ));
        }
        let sql = format!(
            "SELECT project_name, time_bucket_start_unix_nano, feedback_key,
                feedback_count, score_count, score_min, score_max, score_avg,
                score_p50, score_p95, score_p99, score_distribution_json
            FROM feedback_metric_rollups
            WHERE {}
            ORDER BY project_name, time_bucket_start_unix_nano, feedback_key",
            predicates.join(" AND ")
        );
        return Ok((sql, RunAggregateSource::FeedbackRollup));
    }

    predicates.push(format!("rollup_kind = {}", sql_string_literal(kind)));
    if let Some(run_type) = &query.run_type
        && kind == "run_type"
    {
        predicates.push(format!("group_value = {}", sql_string_literal(run_type)));
    }
    if let Some(error) = query.error
        && kind == "error"
    {
        predicates.push(format!("group_value = '{}'", error));
    }

    let sql = format!(
        "SELECT project_name, time_bucket_start_unix_nano, rollup_kind, group_value,
            run_count, error_count, latency_min_nanos, latency_max_nanos,
            latency_avg_nanos, latency_p50_nanos, latency_p95_nanos, latency_p99_nanos,
            prompt_tokens_sum, prompt_tokens_avg, completion_tokens_sum,
            completion_tokens_avg, total_tokens_sum, total_tokens_avg,
            prompt_cost_sum, prompt_cost_avg, completion_cost_sum, completion_cost_avg,
            total_cost_sum, total_cost_avg, evaluator_score_avg
        FROM run_metric_rollups
        WHERE {}
        ORDER BY project_name, time_bucket_start_unix_nano, rollup_kind, group_value",
        predicates.join(" AND ")
    );
    Ok((sql, RunAggregateSource::Rollup))
}

fn run_rollup_row(row: Row) -> RunAggregateRow {
    let project_name: String = row.get(0);
    let time_bucket_start_unix_nano: i64 = row.get(1);
    let rollup_kind: String = row.get(2);
    let group_value: String = row.get(3);
    let count: i64 = row.get(4);
    let error_count: i64 = row.get(5);

    let mut group = BTreeMap::from([
        ("project_name".to_owned(), project_name),
        (
            "time_bucket_start_unix_nano".to_owned(),
            time_bucket_start_unix_nano.to_string(),
        ),
    ]);
    match rollup_kind.as_str() {
        "run_type" => {
            group.insert("run_type".to_owned(), group_value);
        }
        "model" => {
            group.insert("model_name".to_owned(), group_value);
        }
        "error" => {
            group.insert("error".to_owned(), group_value);
        }
        _ => {}
    }

    let count = count.max(0) as u64;
    let error_count = error_count.max(0) as u64;
    RunAggregateRow {
        group,
        metrics: RunAggregateMetrics {
            count,
            error_count,
            error_rate: if count == 0 {
                0.0
            } else {
                error_count as f64 / count as f64
            },
            latency_nanos: Some(NumericStats {
                count,
                sum: row.get::<_, Option<f64>>(8).map(|avg| avg * count as f64),
                avg: row.get(8),
                min: row.get::<_, Option<i64>>(6).map(|value| value as f64),
                max: row.get::<_, Option<i64>>(7).map(|value| value as f64),
                p50: row.get(9),
                p95: row.get(10),
                p99: row.get(11),
            }),
            prompt_tokens: rollup_numeric(row.get::<_, Option<i64>>(12), row.get(13), count),
            completion_tokens: rollup_numeric(row.get::<_, Option<i64>>(14), row.get(15), count),
            total_tokens: rollup_numeric(row.get::<_, Option<i64>>(16), row.get(17), count),
            prompt_cost: rollup_numeric_f64(row.get(18), row.get(19), count),
            completion_cost: rollup_numeric_f64(row.get(20), row.get(21), count),
            total_cost: rollup_numeric_f64(row.get(22), row.get(23), count),
            evaluator_score: row.get::<_, Option<f64>>(24).map(|avg| NumericStats {
                count,
                sum: Some(avg * count as f64),
                avg: Some(avg),
                min: None,
                max: None,
                p50: None,
                p95: None,
                p99: None,
            }),
            ..RunAggregateMetrics::default()
        },
    }
}

fn feedback_rollup_row(row: Row) -> Result<RunAggregateRow> {
    let project_name: String = row.get(0);
    let time_bucket_start_unix_nano: i64 = row.get(1);
    let feedback_key: String = row.get(2);
    let feedback_count: i64 = row.get(3);
    let score_count: i64 = row.get(4);
    let distribution_json: String = row.get(11);
    let distribution = serde_json::from_str::<BTreeMap<String, u64>>(&distribution_json)
        .with_context(|| format!("parse feedback score distribution: {distribution_json}"))?;

    Ok(RunAggregateRow {
        group: BTreeMap::from([
            ("project_name".to_owned(), project_name),
            (
                "time_bucket_start_unix_nano".to_owned(),
                time_bucket_start_unix_nano.to_string(),
            ),
            ("feedback_key".to_owned(), feedback_key.clone()),
        ]),
        metrics: RunAggregateMetrics {
            count: feedback_count.max(0) as u64,
            feedback_scores: BTreeMap::from([(
                feedback_key,
                FeedbackScoreStats {
                    count: score_count.max(0) as u64,
                    min: row.get(5),
                    max: row.get(6),
                    avg: row.get(7),
                    p50: row.get(8),
                    p95: row.get(9),
                    p99: row.get(10),
                    distribution,
                },
            )]),
            ..RunAggregateMetrics::default()
        },
    })
}

fn rollup_numeric(sum: Option<i64>, avg: Option<f64>, count: u64) -> Option<NumericStats> {
    rollup_numeric_f64(sum.map(|value| value as f64), avg, count)
}

fn rollup_numeric_f64(sum: Option<f64>, avg: Option<f64>, count: u64) -> Option<NumericStats> {
    if sum.is_none() && avg.is_none() {
        return None;
    }
    Some(NumericStats {
        count,
        sum,
        avg,
        min: None,
        max: None,
        p50: None,
        p95: None,
        p99: None,
    })
}

fn sql_string_list(values: &[String]) -> String {
    values
        .iter()
        .map(|value| sql_string_literal(value))
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_rollup_sql_for_dashboard_shape() {
        let mut query = RunAggregateQuery::new("demo");
        query.group_by = vec![
            RunAggregateGroup::Project,
            RunAggregateGroup::TimeBucket,
            RunAggregateGroup::RunType,
        ];
        query.time_bucket_nanos = Some(AGGREGATE_ROLLUP_BUCKET_UNIX_NANOS);

        let (sql, source) = rollup_sql(&query).expect("rollup sql");

        assert_eq!(source, RunAggregateSource::Rollup);
        assert!(sql.contains("FROM run_metric_rollups"));
        assert!(sql.contains("rollup_kind = 'run_type'"));
    }

    #[test]
    fn rollups_are_ineligible_for_filters_not_encoded_by_the_rollup() {
        let mut project_rollup = RunAggregateQuery::new("demo");
        project_rollup.group_by = vec![RunAggregateGroup::Project, RunAggregateGroup::TimeBucket];
        project_rollup.time_bucket_nanos = Some(AGGREGATE_ROLLUP_BUCKET_UNIX_NANOS);
        project_rollup.run_type = Some("llm".to_owned());
        assert!(!rollup_eligible(&project_rollup));

        let mut run_type_rollup = project_rollup.clone();
        run_type_rollup.group_by.push(RunAggregateGroup::RunType);
        assert!(rollup_eligible(&run_type_rollup));

        run_type_rollup.error = Some(true);
        assert!(!rollup_eligible(&run_type_rollup));
    }

    #[test]
    fn rollups_are_ineligible_for_partial_time_buckets() {
        let mut query = RunAggregateQuery::new("demo");
        query.group_by = vec![RunAggregateGroup::Project, RunAggregateGroup::TimeBucket];
        query.time_bucket_nanos = Some(AGGREGATE_ROLLUP_BUCKET_UNIX_NANOS);
        assert!(rollup_eligible(&query));

        query.start_time_min_unix_nano = Some(1);
        assert!(!rollup_eligible(&query));

        query.start_time_min_unix_nano = Some(AGGREGATE_ROLLUP_BUCKET_UNIX_NANOS);
        query.start_time_max_unix_nano = Some(2 * AGGREGATE_ROLLUP_BUCKET_UNIX_NANOS - 1);
        assert!(rollup_eligible(&query));

        query.start_time_max_unix_nano = Some(2 * AGGREGATE_ROLLUP_BUCKET_UNIX_NANOS);
        assert!(!rollup_eligible(&query));
    }
}
