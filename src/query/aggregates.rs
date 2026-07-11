use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result, bail};
use tokio_postgres::NoTls;

use super::{
    FilterExpr, QueryEngine, RunKey, RunQuery, RunQueryDiagnostics, RunQueryLimits, SegmentSource,
    TreeFilterExpr, load_run_query_plan, sql_string_literal,
};

mod distributed;
mod rollups;
mod scan;
#[cfg(test)]
mod tests;

use scan::{AggregateRunRow, load_aggregate_rows_with_datafusion};

const AGGREGATE_ROLLUP_BUCKET_UNIX_NANOS: i64 = 60 * 60 * 1_000_000_000;
const AGGREGATE_SEGMENT_SCHEMA_VERSION: i64 = crate::segment::SPAN_SEGMENT_SCHEMA_VERSION;

#[derive(Debug, Clone, PartialEq)]
pub struct RunAggregateQuery {
    pub project_names: Vec<String>,
    pub start_time_min_unix_nano: Option<i64>,
    pub start_time_max_unix_nano: Option<i64>,
    pub run_type: Option<String>,
    pub error: Option<bool>,
    pub filter: Option<FilterExpr>,
    pub trace_filter: Option<FilterExpr>,
    pub tree_filter: Option<TreeFilterExpr>,
    pub group_by: Vec<RunAggregateGroup>,
    pub time_bucket_nanos: Option<i64>,
    pub feedback_keys: Vec<String>,
    pub include_deleted: bool,
    pub limits: RunQueryLimits,
}

impl RunAggregateQuery {
    pub fn new(project_name: impl Into<String>) -> Self {
        Self {
            project_names: vec![project_name.into()],
            start_time_min_unix_nano: None,
            start_time_max_unix_nano: None,
            run_type: None,
            error: None,
            filter: None,
            trace_filter: None,
            tree_filter: None,
            group_by: Vec::new(),
            time_bucket_nanos: None,
            feedback_keys: Vec::new(),
            include_deleted: false,
            limits: RunQueryLimits::default(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RunAggregateGroup {
    Project,
    TimeBucket,
    RunType,
    Tag,
    Model,
    Provider,
    Error,
    FeedbackKey,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RunAggregateSource {
    Rollup,
    Vortex,
    FeedbackRollup,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RunAggregateResult {
    pub rows: Vec<RunAggregateRow>,
    pub diagnostics: RunQueryDiagnostics,
    pub source: RunAggregateSource,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RunAggregateRow {
    pub group: BTreeMap<String, String>,
    pub metrics: RunAggregateMetrics,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct RunAggregateMetrics {
    pub count: u64,
    pub error_count: u64,
    pub error_rate: f64,
    pub latency_nanos: Option<NumericStats>,
    pub prompt_tokens: Option<NumericStats>,
    pub completion_tokens: Option<NumericStats>,
    pub total_tokens: Option<NumericStats>,
    pub prompt_cost: Option<NumericStats>,
    pub completion_cost: Option<NumericStats>,
    pub total_cost: Option<NumericStats>,
    pub first_token_latency_nanos: Option<NumericStats>,
    pub evaluator_score: Option<NumericStats>,
    pub feedback_scores: BTreeMap<String, FeedbackScoreStats>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct NumericStats {
    pub count: u64,
    pub sum: Option<f64>,
    pub avg: Option<f64>,
    pub min: Option<f64>,
    pub max: Option<f64>,
    pub p50: Option<f64>,
    pub p95: Option<f64>,
    pub p99: Option<f64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct FeedbackScoreStats {
    pub count: u64,
    pub avg: Option<f64>,
    pub min: Option<f64>,
    pub max: Option<f64>,
    pub p50: Option<f64>,
    pub p95: Option<f64>,
    pub p99: Option<f64>,
    pub distribution: BTreeMap<String, u64>,
}

impl QueryEngine {
    pub async fn aggregate_runs(&self, query: RunAggregateQuery) -> Result<RunAggregateResult> {
        if let Some(max_wall_time) = query.limits.max_wall_time {
            let query = aggregate_query_without_wall_time_limit(query);
            return tokio::time::timeout(max_wall_time, self.aggregate_runs_inner(query))
                .await
                .context("aggregate query exceeded max wall clock")?;
        }
        self.aggregate_runs_inner(query).await
    }

    async fn aggregate_runs_inner(&self, query: RunAggregateQuery) -> Result<RunAggregateResult> {
        validate_aggregate_query(&query)?;
        if let Some(result) = rollups::try_rollup_aggregate(&self.postgres_url, &query).await? {
            return Ok(result);
        }
        self.aggregate_runs_from_vortex(query).await
    }

    async fn aggregate_runs_from_vortex(
        &self,
        query: RunAggregateQuery,
    ) -> Result<RunAggregateResult> {
        let run_query = query.to_run_query();
        let postgres_started = Instant::now();
        let plan = load_run_query_plan(&self.postgres_url, &run_query).await?;
        let postgres_query_time = postgres_started.elapsed();
        let (plan, search_index_reads) = super::search::apply_phase6_search_indexes(
            Arc::clone(&self.object_store),
            plan,
            &run_query,
        )
        .await?;
        reject_old_metric_segments(&plan.segments)?;

        let candidate_segments = plan.segments.len();
        let candidate_runs = plan.candidate_runs;
        let candidate_bytes = plan.candidate_bytes;
        let estimated_object_store_requests = plan.estimated_object_store_requests;
        let candidate_run_keys = plan.candidate_run_keys.clone();

        let (rows, datafusion_timing, object_store_reads) = load_aggregate_rows_with_datafusion(
            Arc::clone(&self.object_store),
            plan.segments,
            &run_query,
            Some(&candidate_run_keys),
        )
        .await?;
        let rows = rows
            .into_iter()
            .filter(|row| candidate_run_keys.contains(&row.run_key()))
            .collect::<Vec<_>>();
        let tags = if query.group_by.contains(&RunAggregateGroup::Tag) {
            load_tags(&self.postgres_url, &candidate_run_keys).await?
        } else {
            HashMap::new()
        };
        let feedback_scores = if query.group_by.contains(&RunAggregateGroup::FeedbackKey)
            || !query.feedback_keys.is_empty()
        {
            load_feedback_scores(&self.postgres_url, &query, &rows).await?
        } else {
            HashMap::new()
        };
        let aggregate_rows = aggregate_rows(rows, &query, &tags, &feedback_scores);

        Ok(RunAggregateResult {
            diagnostics: RunQueryDiagnostics {
                candidate_segments,
                candidate_runs,
                candidate_bytes,
                estimated_object_store_requests,
                actual_object_store_requests: object_store_reads
                    .request_count()
                    .saturating_add(search_index_reads.request_count()),
                actual_object_store_bytes_read: object_store_reads
                    .bytes_read
                    .saturating_add(search_index_reads.bytes_read),
                vortex_files_opened: candidate_segments,
                rows_returned: aggregate_rows.len(),
                postgres_query_time,
                datafusion_planning_time: datafusion_timing.planning_time,
                datafusion_execution_time: datafusion_timing.execution_time,
            },
            rows: aggregate_rows,
            source: RunAggregateSource::Vortex,
        })
    }
}

impl RunAggregateQuery {
    fn to_run_query(&self) -> RunQuery {
        RunQuery {
            project_names: self.project_names.clone(),
            trace_id: None,
            parent_run_id: None,
            parent_span_id: None,
            run_type: self.run_type.clone(),
            is_root: None,
            error: self.error,
            start_time_min_unix_nano: self.start_time_min_unix_nano,
            start_time_max_unix_nano: self.start_time_max_unix_nano,
            limit: None,
            offset: None,
            retention_cutoff_unix_nano: None,
            include_deleted: self.include_deleted,
            filter: self.filter.clone(),
            trace_filter: self.trace_filter.clone(),
            tree_filter: self.tree_filter.clone(),
            include_payload: false,
            newest_first: false,
            limits: self.limits.clone(),
        }
    }
}

fn aggregate_query_without_wall_time_limit(mut query: RunAggregateQuery) -> RunAggregateQuery {
    query.limits.max_wall_time = None;
    query
}

fn validate_aggregate_query(query: &RunAggregateQuery) -> Result<()> {
    if query.project_names.is_empty() {
        bail!("aggregate query rejected: project_names is required");
    }
    if query.group_by.contains(&RunAggregateGroup::TimeBucket)
        && query.time_bucket_nanos.unwrap_or(0) <= 0
    {
        bail!("aggregate query rejected: time_bucket_nanos is required for time_bucket grouping");
    }
    Ok(())
}

fn reject_old_metric_segments(segments: &[SegmentSource]) -> Result<()> {
    if let Some(segment) = segments
        .iter()
        .find(|segment| segment.schema_version < AGGREGATE_SEGMENT_SCHEMA_VERSION)
    {
        bail!(
            "aggregate query rejected: segment {} has schema version {}, typed metric columns require version {}",
            segment.uri,
            segment.schema_version,
            AGGREGATE_SEGMENT_SCHEMA_VERSION
        );
    }
    Ok(())
}

fn aggregate_rows(
    rows: Vec<AggregateRunRow>,
    query: &RunAggregateQuery,
    tags: &HashMap<RunKey, Vec<String>>,
    feedback_scores: &HashMap<RunKey, Vec<FeedbackScore>>,
) -> Vec<RunAggregateRow> {
    let mut groups = BTreeMap::<BTreeMap<String, String>, AggregateAccumulator>::new();
    for row in rows {
        for variant in group_variants(&row, query, tags, feedback_scores) {
            groups
                .entry(variant.group)
                .or_default()
                .push(&row, &variant.feedback);
        }
    }

    groups
        .into_iter()
        .map(|(group, accumulator)| RunAggregateRow {
            group,
            metrics: accumulator.finish(),
        })
        .collect()
}

#[derive(Debug, Clone)]
struct GroupVariant {
    group: BTreeMap<String, String>,
    feedback: Vec<FeedbackScore>,
}

fn group_variants(
    row: &AggregateRunRow,
    query: &RunAggregateQuery,
    tags: &HashMap<RunKey, Vec<String>>,
    feedback_scores: &HashMap<RunKey, Vec<FeedbackScore>>,
) -> Vec<GroupVariant> {
    let mut variants = vec![GroupVariant {
        group: BTreeMap::new(),
        feedback: Vec::new(),
    }];
    for group in &query.group_by {
        variants = expand_group_variants(variants, *group, row, query, tags, feedback_scores);
        if variants.is_empty() {
            return variants;
        }
    }
    if query.group_by.contains(&RunAggregateGroup::FeedbackKey) {
        variants
    } else {
        attach_feedback_metrics(variants, row, query, feedback_scores)
    }
}

fn expand_group_variants(
    variants: Vec<GroupVariant>,
    group: RunAggregateGroup,
    row: &AggregateRunRow,
    query: &RunAggregateQuery,
    tags: &HashMap<RunKey, Vec<String>>,
    feedback_scores: &HashMap<RunKey, Vec<FeedbackScore>>,
) -> Vec<GroupVariant> {
    match group {
        RunAggregateGroup::Project => {
            append_group_value(variants, "project_name", &row.project_name)
        }
        RunAggregateGroup::TimeBucket => append_group_value(
            variants,
            "time_bucket_start_unix_nano",
            &time_bucket(row.start_time_unix_nano, query.time_bucket_nanos).to_string(),
        ),
        RunAggregateGroup::RunType => append_group_value(variants, "run_type", &row.run_type),
        RunAggregateGroup::Tag => {
            let values = tags
                .get(&row.run_key())
                .filter(|tags| !tags.is_empty())
                .cloned()
                .unwrap_or_else(|| vec!["untagged".to_owned()]);
            append_group_values(variants, "tag", values)
        }
        RunAggregateGroup::Model => append_group_value(
            variants,
            "model_name",
            row.model_name.as_deref().unwrap_or("unknown"),
        ),
        RunAggregateGroup::Provider => append_group_value(
            variants,
            "provider_name",
            row.provider_name.as_deref().unwrap_or("unknown"),
        ),
        RunAggregateGroup::Error => {
            append_group_value(variants, "error", &(row.status == "error").to_string())
        }
        RunAggregateGroup::FeedbackKey => {
            expand_feedback_group(variants, row, query, feedback_scores)
        }
    }
}

fn append_group_value(variants: Vec<GroupVariant>, key: &str, value: &str) -> Vec<GroupVariant> {
    append_group_values(variants, key, vec![value.to_owned()])
}

fn append_group_values(
    variants: Vec<GroupVariant>,
    key: &str,
    values: Vec<String>,
) -> Vec<GroupVariant> {
    let mut expanded = Vec::new();
    for variant in variants {
        for value in &values {
            let mut variant = variant.clone();
            variant.group.insert(key.to_owned(), value.clone());
            expanded.push(variant);
        }
    }
    expanded
}

fn expand_feedback_group(
    variants: Vec<GroupVariant>,
    row: &AggregateRunRow,
    query: &RunAggregateQuery,
    feedback_scores: &HashMap<RunKey, Vec<FeedbackScore>>,
) -> Vec<GroupVariant> {
    let run_key = row.run_key();
    let scores = feedback_scores
        .get(&run_key)
        .into_iter()
        .flatten()
        .filter(|score| query.feedback_keys.is_empty() || query.feedback_keys.contains(&score.key))
        .cloned()
        .collect::<Vec<_>>();
    if scores.is_empty() {
        return Vec::new();
    }

    let mut expanded = Vec::new();
    for variant in variants {
        for score in &scores {
            let mut variant = variant.clone();
            variant
                .group
                .insert("feedback_key".to_owned(), score.key.clone());
            variant.feedback = vec![score.clone()];
            expanded.push(variant);
        }
    }
    expanded
}

fn attach_feedback_metrics(
    mut variants: Vec<GroupVariant>,
    row: &AggregateRunRow,
    query: &RunAggregateQuery,
    feedback_scores: &HashMap<RunKey, Vec<FeedbackScore>>,
) -> Vec<GroupVariant> {
    if query.feedback_keys.is_empty() {
        return variants;
    }
    let run_key = row.run_key();
    let scores = feedback_scores
        .get(&run_key)
        .into_iter()
        .flatten()
        .filter(|score| query.feedback_keys.contains(&score.key))
        .cloned()
        .collect::<Vec<_>>();
    if !scores.is_empty() {
        for variant in &mut variants {
            variant.feedback = scores.clone();
        }
    }
    variants
}

fn time_bucket(start_time_unix_nano: i64, bucket: Option<i64>) -> i64 {
    let bucket = bucket.unwrap_or(AGGREGATE_ROLLUP_BUCKET_UNIX_NANOS).max(1);
    start_time_unix_nano.div_euclid(bucket) * bucket
}

#[derive(Debug, Clone)]
struct FeedbackScore {
    key: String,
    score: f64,
}

async fn load_tags(
    postgres_url: &str,
    keys: &HashSet<RunKey>,
) -> Result<HashMap<RunKey, Vec<String>>> {
    if keys.is_empty() {
        return Ok(HashMap::new());
    }
    let (client, connection) = tokio_postgres::connect(postgres_url, NoTls)
        .await
        .context("connect postgres for aggregate tags")?;
    tokio::spawn(async move {
        if let Err(err) = connection.await {
            tracing::warn!(error = %err, "postgres aggregate tag connection failed");
        }
    });

    let predicates = run_key_predicates("run_tags", keys);
    let rows = client
        .query(
            format!(
                "SELECT project_name, trace_id, span_id, tag
                FROM run_tags
                WHERE {predicates}
                ORDER BY project_name, trace_id, span_id, tag"
            )
            .as_str(),
            &[],
        )
        .await
        .context("load aggregate tags")?;

    let mut tags = HashMap::<RunKey, Vec<String>>::new();
    for row in rows {
        tags.entry(RunKey {
            project_name: row.get(0),
            trace_id: row.get(1),
            span_id: row.get(2),
        })
        .or_default()
        .push(row.get(3));
    }
    Ok(tags)
}

async fn load_feedback_scores(
    postgres_url: &str,
    query: &RunAggregateQuery,
    rows: &[AggregateRunRow],
) -> Result<HashMap<RunKey, Vec<FeedbackScore>>> {
    let run_keys = rows
        .iter()
        .map(AggregateRunRow::run_key)
        .collect::<HashSet<_>>();
    if run_keys.is_empty() {
        return Ok(HashMap::new());
    }

    let (client, connection) = tokio_postgres::connect(postgres_url, NoTls)
        .await
        .context("connect postgres for aggregate feedback")?;
    tokio::spawn(async move {
        if let Err(err) = connection.await {
            tracing::warn!(error = %err, "postgres aggregate feedback connection failed");
        }
    });

    let mut predicates = vec![
        run_key_predicates("heads", &run_keys),
        "feedback.score_number IS NOT NULL".to_owned(),
        "(feedback.project_name IS NULL OR feedback.project_name = heads.project_name)".to_owned(),
    ];
    if !query.project_names.is_empty() {
        predicates.push(format!(
            "heads.project_name IN ({})",
            query
                .project_names
                .iter()
                .map(|project_name| sql_string_literal(project_name))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    if !query.feedback_keys.is_empty() {
        predicates.push(format!(
            "feedback.key IN ({})",
            query
                .feedback_keys
                .iter()
                .map(|key| sql_string_literal(key))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }

    let rows = client
        .query(
            format!(
                "SELECT heads.project_name, heads.trace_id, heads.span_id,
                    feedback.key, feedback.score_number
                FROM run_heads heads
                INNER JOIN feedback feedback
                    ON feedback.run_id IS NOT NULL
                    AND feedback.run_id = heads.run_id
                WHERE {}
                ORDER BY heads.project_name, heads.trace_id, heads.span_id,
                    feedback.key, feedback.created_at_unix_nano",
                predicates.join(" AND ")
            )
            .as_str(),
            &[],
        )
        .await
        .context("load aggregate feedback scores")?;

    let mut scores = HashMap::<RunKey, Vec<FeedbackScore>>::new();
    for row in rows {
        scores
            .entry(RunKey {
                project_name: row.get(0),
                trace_id: row.get(1),
                span_id: row.get(2),
            })
            .or_default()
            .push(FeedbackScore {
                key: row.get(3),
                score: row.get(4),
            });
    }
    Ok(scores)
}

fn run_key_predicates(alias: &str, keys: &HashSet<RunKey>) -> String {
    let mut by_project_trace = BTreeMap::<(&str, &str), Vec<&str>>::new();
    for key in keys {
        by_project_trace
            .entry((key.project_name.as_str(), key.trace_id.as_str()))
            .or_default()
            .push(key.span_id.as_str());
    }
    by_project_trace
        .into_iter()
        .map(|((project_name, trace_id), mut span_ids)| {
            span_ids.sort_unstable();
            span_ids.dedup();
            format!(
                "({alias}.project_name = {} AND {alias}.trace_id = {} AND {alias}.span_id IN ({}))",
                sql_string_literal(project_name),
                sql_string_literal(trace_id),
                span_ids
                    .into_iter()
                    .map(sql_string_literal)
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        })
        .collect::<Vec<_>>()
        .join(" OR ")
}

#[derive(Debug, Default)]
struct AggregateAccumulator {
    count: u64,
    error_count: u64,
    latency_nanos: SummaryAccumulator,
    prompt_tokens: SummaryAccumulator,
    completion_tokens: SummaryAccumulator,
    total_tokens: SummaryAccumulator,
    prompt_cost: SummaryAccumulator,
    completion_cost: SummaryAccumulator,
    total_cost: SummaryAccumulator,
    first_token_latency_nanos: SummaryAccumulator,
    evaluator_score: SummaryAccumulator,
    feedback_scores: BTreeMap<String, SummaryAccumulator>,
}

impl AggregateAccumulator {
    fn push(&mut self, row: &AggregateRunRow, feedback_scores: &[FeedbackScore]) {
        self.count += 1;
        if row.status == "error" {
            self.error_count += 1;
        }
        self.latency_nanos.push(row.latency_nanos as f64);
        self.prompt_tokens.push_optional_i64(row.prompt_tokens);
        self.completion_tokens
            .push_optional_i64(row.completion_tokens);
        self.total_tokens.push_optional_i64(row.total_tokens);
        self.prompt_cost.push_optional_f64(row.prompt_cost);
        self.completion_cost.push_optional_f64(row.completion_cost);
        self.total_cost.push_optional_f64(row.total_cost);
        self.first_token_latency_nanos
            .push_optional_i64(row.first_token_latency_nanos);
        self.evaluator_score.push_optional_f64(row.evaluator_score);
        for feedback in feedback_scores {
            self.feedback_scores
                .entry(feedback.key.clone())
                .or_default()
                .push(feedback.score);
        }
    }

    fn finish(self) -> RunAggregateMetrics {
        let error_rate = if self.count == 0 {
            0.0
        } else {
            self.error_count as f64 / self.count as f64
        };
        RunAggregateMetrics {
            count: self.count,
            error_count: self.error_count,
            error_rate,
            latency_nanos: self.latency_nanos.finish(true),
            prompt_tokens: self.prompt_tokens.finish(false),
            completion_tokens: self.completion_tokens.finish(false),
            total_tokens: self.total_tokens.finish(false),
            prompt_cost: self.prompt_cost.finish(false),
            completion_cost: self.completion_cost.finish(false),
            total_cost: self.total_cost.finish(false),
            first_token_latency_nanos: self.first_token_latency_nanos.finish(true),
            evaluator_score: self.evaluator_score.finish(false),
            feedback_scores: self
                .feedback_scores
                .into_iter()
                .map(|(key, summary)| (key, FeedbackScoreStats::from_summary(summary)))
                .collect(),
        }
    }
}

#[derive(Debug, Default)]
struct SummaryAccumulator {
    values: Vec<f64>,
}

impl SummaryAccumulator {
    fn push(&mut self, value: f64) {
        if value.is_finite() {
            self.values.push(value);
        }
    }

    fn push_optional_i64(&mut self, value: Option<i64>) {
        if let Some(value) = value {
            self.push(value as f64);
        }
    }

    fn push_optional_f64(&mut self, value: Option<f64>) {
        if let Some(value) = value {
            self.push(value);
        }
    }

    fn finish(mut self, percentiles: bool) -> Option<NumericStats> {
        if self.values.is_empty() {
            return None;
        }
        self.values.sort_by(f64::total_cmp);
        let count = self.values.len() as u64;
        let sum = self.values.iter().sum::<f64>();
        Some(NumericStats {
            count,
            sum: Some(sum),
            avg: Some(sum / count as f64),
            min: self.values.first().copied(),
            max: self.values.last().copied(),
            p50: percentiles.then(|| percentile(&self.values, 50)),
            p95: percentiles.then(|| percentile(&self.values, 95)),
            p99: percentiles.then(|| percentile(&self.values, 99)),
        })
    }
}

impl FeedbackScoreStats {
    fn from_summary(mut summary: SummaryAccumulator) -> Self {
        summary.values.sort_by(f64::total_cmp);
        let count = summary.values.len() as u64;
        let sum = summary.values.iter().sum::<f64>();
        Self {
            count,
            avg: (count > 0).then_some(sum / count as f64),
            min: summary.values.first().copied(),
            max: summary.values.last().copied(),
            p50: (count > 0).then(|| percentile(&summary.values, 50)),
            p95: (count > 0).then(|| percentile(&summary.values, 95)),
            p99: (count > 0).then(|| percentile(&summary.values, 99)),
            distribution: feedback_distribution(&summary.values),
        }
    }
}

fn percentile(sorted_values: &[f64], percentile: usize) -> f64 {
    let index = ((sorted_values.len() - 1) * percentile).div_ceil(100);
    sorted_values[index.min(sorted_values.len() - 1)]
}

fn feedback_distribution(values: &[f64]) -> BTreeMap<String, u64> {
    let mut distribution = BTreeMap::from([
        ("lt_0".to_owned(), 0),
        ("0_to_0_25".to_owned(), 0),
        ("0_25_to_0_5".to_owned(), 0),
        ("0_5_to_0_75".to_owned(), 0),
        ("0_75_to_1".to_owned(), 0),
        ("gt_1".to_owned(), 0),
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
        *distribution.entry(key.to_owned()).or_default() += 1;
    }
    distribution
}
