use std::error::Error;
use std::fmt;

use chrono::DateTime;

const MAX_FILTER_DEPTH: usize = 16;
const MAX_FILTER_NODES: usize = 128;

mod parser;

#[derive(Debug, Clone, PartialEq)]
pub struct FilterExpr {
    kind: FilterKind,
}

#[derive(Debug, Clone, PartialEq)]
enum FilterKind {
    And(Vec<FilterExpr>),
    Or(Vec<FilterExpr>),
    Compare {
        op: CompareOp,
        field: FilterField,
        value: FilterValue,
    },
    Has {
        field: FilterField,
        value: FilterValue,
    },
    In {
        field: FilterField,
        values: Vec<FilterValue>,
    },
    Contains {
        field: FilterField,
        value: FilterValue,
        negated: bool,
    },
    Search(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CompareOp {
    Eq,
    Neq,
    Gt,
    Gte,
    Lt,
    Lte,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FilterField {
    Id,
    Name,
    RunType,
    Status,
    StartTime,
    EndTime,
    Latency,
    Tags,
    MetadataKey,
    MetadataValue,
    FeedbackKey,
    FeedbackScore,
    FeedbackValue,
    IsRoot,
    TraceId,
    ProjectName,
    RootRunId,
    RootSpanId,
    ModelName,
    ProviderName,
    PromptTokens,
    CompletionTokens,
    TotalTokens,
    TotalCost,
    Error,
}

#[derive(Debug, Clone, PartialEq)]
enum FilterValue {
    String(String),
    Number(f64),
    Bool(bool),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FilterError {
    Parse(String),
    Unsupported(String),
}

impl fmt::Display for FilterError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Parse(message) => write!(formatter, "invalid filter: {message}"),
            Self::Unsupported(message) => write!(formatter, "unsupported filter: {message}"),
        }
    }
}

impl Error for FilterError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompiledFilter {
    pub predicate_sql: String,
}

impl FilterExpr {
    pub fn parse(input: &str) -> Result<Self, FilterError> {
        parser::parse_filter_expr(input)
    }

    pub fn compile_run_head_filter(&self, run_alias: &str) -> Result<CompiledFilter, FilterError> {
        Ok(CompiledFilter {
            predicate_sql: compile_expr(self, run_alias, None)?,
        })
    }

    pub(crate) fn compile_run_head_filter_for_projects(
        &self,
        run_alias: &str,
        project_names: &[String],
    ) -> Result<CompiledFilter, FilterError> {
        Ok(CompiledFilter {
            predicate_sql: compile_expr(self, run_alias, Some(project_names))?,
        })
    }
}

fn compile_expr(
    expr: &FilterExpr,
    run_alias: &str,
    project_names: Option<&[String]>,
) -> Result<String, FilterError> {
    match &expr.kind {
        FilterKind::And(children) => compile_and(children, run_alias, project_names),
        FilterKind::Or(children) => {
            if children.is_empty() {
                return Err(FilterError::Parse(
                    "or() requires at least one child".to_owned(),
                ));
            }
            let predicates = children
                .iter()
                .map(|child| compile_expr(child, run_alias, project_names))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(format!("({})", predicates.join(" OR ")))
        }
        FilterKind::Compare { op, field, value } => {
            compile_compare(*op, *field, value, run_alias, project_names)
        }
        FilterKind::Has { field, value } => compile_has(*field, value, run_alias, project_names),
        FilterKind::In { field, values } => compile_in(*field, values, run_alias, project_names),
        FilterKind::Contains {
            field,
            value,
            negated,
        } => compile_contains(*field, value, *negated, run_alias, project_names),
        FilterKind::Search(query) => {
            let _query_len = query.len();
            Err(FilterError::Unsupported(
                "full-text search requires the Phase 6 object-store index".to_owned(),
            ))
        }
    }
}

fn compile_and(
    children: &[FilterExpr],
    run_alias: &str,
    project_names: Option<&[String]>,
) -> Result<String, FilterError> {
    if children.is_empty() {
        return Err(FilterError::Parse(
            "and() requires at least one child".to_owned(),
        ));
    }

    let mut predicates = Vec::new();
    let mut metadata_conditions = Vec::new();
    let mut feedback_conditions = Vec::new();
    let has_metadata_key_anchor = children.iter().any(is_positive_metadata_key_condition);
    let has_feedback_key_anchor = children.iter().any(is_positive_feedback_key_condition);

    for child in children {
        if let Some(condition) = metadata_row_condition(child, has_metadata_key_anchor)? {
            metadata_conditions.push(condition);
            continue;
        }
        if let Some(condition) = feedback_row_condition(child, has_feedback_key_anchor)? {
            feedback_conditions.push(condition);
            continue;
        }
        predicates.push(compile_expr(child, run_alias, project_names)?);
    }

    if !metadata_conditions.is_empty() {
        predicates.push(metadata_exists_sql(
            run_alias,
            &metadata_conditions,
            project_names,
        ));
    }
    if !feedback_conditions.is_empty() {
        predicates.push(feedback_exists_sql(
            run_alias,
            &feedback_conditions,
            project_names,
        ));
    }

    Ok(format!("({})", predicates.join(" AND ")))
}

fn compile_compare(
    op: CompareOp,
    field: FilterField,
    value: &FilterValue,
    run_alias: &str,
    project_names: Option<&[String]>,
) -> Result<String, FilterError> {
    match field {
        FilterField::MetadataKey | FilterField::MetadataValue => {
            compile_metadata_atom(op, field, value, run_alias, project_names)
        }
        FilterField::FeedbackKey | FilterField::FeedbackScore | FilterField::FeedbackValue => {
            compile_feedback_atom(op, field, value, run_alias, project_names)
        }
        FilterField::Tags => {
            let tag = value.as_string("tags")?;
            match op {
                CompareOp::Eq => Ok(tag_exists_sql(run_alias, &tag, project_names)),
                CompareOp::Neq => Ok(format!(
                    "NOT ({})",
                    tag_exists_sql(run_alias, &tag, project_names)
                )),
                _ => Err(FilterError::Unsupported(
                    "tags only support eq, neq, has, and in".to_owned(),
                )),
            }
        }
        FilterField::Id => {
            let id = value.as_string("id")?;
            let predicate = format!(
                "(NULLIF({run_alias}.run_id, '') = {id} OR {run_alias}.generated_run_id = {id})",
                id = sql_string_literal(&id)
            );
            match op {
                CompareOp::Eq => Ok(predicate),
                CompareOp::Neq => Ok(format!("NOT ({predicate})")),
                _ => Err(FilterError::Unsupported(
                    "id only supports eq, neq, and in".to_owned(),
                )),
            }
        }
        FilterField::Name => compile_text_column(op, &format!("{run_alias}.name"), value, "name"),
        FilterField::ProjectName => compile_text_column(
            op,
            &format!("{run_alias}.project_name"),
            value,
            "project_name",
        ),
        FilterField::RunType => {
            compile_text_column(op, &format!("{run_alias}.run_type"), value, "run_type")
        }
        FilterField::Status => {
            compile_text_column(op, &format!("{run_alias}.status"), value, "status")
        }
        FilterField::TraceId => {
            compile_text_column(op, &format!("{run_alias}.trace_id"), value, "trace_id")
        }
        FilterField::RootRunId => compile_text_column(
            op,
            &format!("{run_alias}.root_run_id"),
            value,
            "root_run_id",
        ),
        FilterField::RootSpanId => compile_text_column(
            op,
            &format!("{run_alias}.root_span_id"),
            value,
            "root_span_id",
        ),
        FilterField::ModelName => {
            compile_text_column(op, &format!("{run_alias}.model_name"), value, "model_name")
        }
        FilterField::ProviderName => compile_text_column(
            op,
            &format!("{run_alias}.provider_name"),
            value,
            "provider_name",
        ),
        FilterField::StartTime => compile_i64_column(
            op,
            &format!("{run_alias}.start_time_unix_nano"),
            value,
            "start_time",
        ),
        FilterField::EndTime => compile_i64_column(
            op,
            &format!("{run_alias}.end_time_unix_nano"),
            value,
            "end_time",
        ),
        FilterField::Latency => {
            compile_i64_column(op, &format!("{run_alias}.latency_nanos"), value, "latency")
        }
        FilterField::PromptTokens => compile_i64_column(
            op,
            &format!("{run_alias}.prompt_tokens"),
            value,
            "prompt_tokens",
        ),
        FilterField::CompletionTokens => compile_i64_column(
            op,
            &format!("{run_alias}.completion_tokens"),
            value,
            "completion_tokens",
        ),
        FilterField::TotalTokens => compile_i64_column(
            op,
            &format!("{run_alias}.total_tokens"),
            value,
            "total_tokens",
        ),
        FilterField::TotalCost => {
            compile_f64_column(op, &format!("{run_alias}.total_cost"), value, "total_cost")
        }
        FilterField::IsRoot => {
            let boolean = value.as_bool("is_root")?;
            compile_bool_column(op, &format!("{run_alias}.is_root"), boolean)
        }
        FilterField::Error => {
            let boolean = value.as_bool("error")?;
            compile_error_column(op, &format!("{run_alias}.status"), boolean)
        }
    }
}

fn compile_has(
    field: FilterField,
    value: &FilterValue,
    run_alias: &str,
    project_names: Option<&[String]>,
) -> Result<String, FilterError> {
    match field {
        FilterField::Tags => Ok(tag_exists_sql(
            run_alias,
            &value.as_string("tags")?,
            project_names,
        )),
        FilterField::MetadataKey => Ok(metadata_exists_sql(
            run_alias,
            &[format!(
                "key = {}",
                sql_string_literal(&value.as_string("metadata_key")?)
            )],
            project_names,
        )),
        _ => Err(FilterError::Unsupported(
            "has() is supported for tags and metadata_key only".to_owned(),
        )),
    }
}

fn compile_in(
    field: FilterField,
    values: &[FilterValue],
    run_alias: &str,
    project_names: Option<&[String]>,
) -> Result<String, FilterError> {
    if values.is_empty() {
        return Err(FilterError::Parse(
            "in() requires a non-empty list".to_owned(),
        ));
    }
    match field {
        FilterField::Tags => {
            let tags = values
                .iter()
                .map(|value| value.as_string("tags"))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(format!(
                "{} IN (
                    SELECT {}
                    FROM run_tags tag_filter
                    WHERE {}tag_filter.tag IN ({})
                )",
                run_key_sql(run_alias),
                table_run_key_sql("tag_filter"),
                project_scope_sql("tag_filter", project_names),
                sql_string_list(&tags)
            ))
        }
        FilterField::MetadataKey | FilterField::MetadataValue => {
            let conditions = values
                .iter()
                .map(|value| {
                    let value = value.as_string("metadata")?;
                    Ok(match field {
                        FilterField::MetadataKey => format!("key = {}", sql_string_literal(&value)),
                        FilterField::MetadataValue => {
                            format!("value = {}", sql_string_literal(&value))
                        }
                        _ => unreachable!(),
                    })
                })
                .collect::<Result<Vec<_>, FilterError>>()?;
            Ok(metadata_exists_sql(
                run_alias,
                &[format!("({})", conditions.join(" OR "))],
                project_names,
            ))
        }
        FilterField::Id => {
            let values = values
                .iter()
                .map(|value| value.as_string("id"))
                .collect::<Result<Vec<_>, _>>()?;
            let predicates = values
                .iter()
                .map(|id| {
                    let id = sql_string_literal(id);
                    format!("(NULLIF({run_alias}.run_id, '') = {id} OR {run_alias}.generated_run_id = {id})")
                })
                .collect::<Vec<_>>();
            Ok(format!("({})", predicates.join(" OR ")))
        }
        FilterField::RunType
        | FilterField::Status
        | FilterField::Name
        | FilterField::TraceId
        | FilterField::ProjectName
        | FilterField::RootRunId
        | FilterField::RootSpanId
        | FilterField::ModelName
        | FilterField::ProviderName => {
            let column = match field {
                FilterField::RunType => "run_type",
                FilterField::Status => "status",
                FilterField::Name => "name",
                FilterField::TraceId => "trace_id",
                FilterField::ProjectName => "project_name",
                FilterField::RootRunId => "root_run_id",
                FilterField::RootSpanId => "root_span_id",
                FilterField::ModelName => "model_name",
                FilterField::ProviderName => "provider_name",
                _ => unreachable!(),
            };
            let values = values
                .iter()
                .map(|value| value.as_string(column))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(format!(
                "{run_alias}.{column} IN ({})",
                sql_string_list(&values)
            ))
        }
        _ => Err(FilterError::Unsupported(
            "in() is supported for indexed scalar text fields, tags, and metadata".to_owned(),
        )),
    }
}

fn compile_contains(
    field: FilterField,
    value: &FilterValue,
    negated: bool,
    run_alias: &str,
    project_names: Option<&[String]>,
) -> Result<String, FilterError> {
    let value = value.as_string("contains")?;
    let predicate = match field {
        FilterField::Name
        | FilterField::RunType
        | FilterField::Status
        | FilterField::ProjectName
        | FilterField::TraceId
        | FilterField::RootRunId
        | FilterField::RootSpanId
        | FilterField::ModelName
        | FilterField::ProviderName => {
            let column = match field {
                FilterField::Name => "name",
                FilterField::RunType => "run_type",
                FilterField::Status => "status",
                FilterField::ProjectName => "project_name",
                FilterField::TraceId => "trace_id",
                FilterField::RootRunId => "root_run_id",
                FilterField::RootSpanId => "root_span_id",
                FilterField::ModelName => "model_name",
                FilterField::ProviderName => "provider_name",
                _ => unreachable!(),
            };
            format!(
                "LOWER({run_alias}.{column}) LIKE {} ESCAPE '\\'",
                sql_string_literal(&format!(
                    "%{}%",
                    escape_like_pattern(&value.to_ascii_lowercase())
                ))
            )
        }
        FilterField::MetadataValue => metadata_exists_sql(
            run_alias,
            &[format!(
                "LOWER(value) LIKE {} ESCAPE '\\'",
                sql_string_literal(&format!(
                    "%{}%",
                    escape_like_pattern(&value.to_ascii_lowercase())
                ))
            )],
            project_names,
        ),
        _ => {
            return Err(FilterError::Unsupported(
                "contains() is only supported for indexed scalar text fields".to_owned(),
            ));
        }
    };

    if negated {
        Ok(format!("NOT ({predicate})"))
    } else {
        Ok(predicate)
    }
}

fn compile_metadata_atom(
    op: CompareOp,
    field: FilterField,
    value: &FilterValue,
    run_alias: &str,
    project_names: Option<&[String]>,
) -> Result<String, FilterError> {
    if matches!(op, CompareOp::Neq) {
        let condition = metadata_compare_condition(CompareOp::Eq, field, value)?;
        Ok(format!(
            "NOT ({})",
            metadata_exists_sql(run_alias, &[condition], project_names)
        ))
    } else {
        let condition = metadata_compare_condition(op, field, value)?;
        Ok(metadata_exists_sql(run_alias, &[condition], project_names))
    }
}

fn compile_feedback_atom(
    op: CompareOp,
    field: FilterField,
    value: &FilterValue,
    run_alias: &str,
    project_names: Option<&[String]>,
) -> Result<String, FilterError> {
    if matches!(op, CompareOp::Neq) {
        let condition = feedback_compare_condition(CompareOp::Eq, field, value)?;
        Ok(format!(
            "NOT ({})",
            feedback_exists_sql(run_alias, &[condition], project_names)
        ))
    } else {
        let condition = feedback_compare_condition(op, field, value)?;
        Ok(feedback_exists_sql(run_alias, &[condition], project_names))
    }
}

fn metadata_row_condition(
    expr: &FilterExpr,
    has_key_anchor: bool,
) -> Result<Option<String>, FilterError> {
    match &expr.kind {
        FilterKind::Compare { op, field, value }
            if matches!(field, FilterField::MetadataKey | FilterField::MetadataValue) =>
        {
            if matches!((*op, *field), (CompareOp::Neq, FilterField::MetadataKey))
                || (matches!((*op, *field), (CompareOp::Neq, FilterField::MetadataValue))
                    && !has_key_anchor)
            {
                return Ok(None);
            }
            Ok(Some(metadata_compare_condition(*op, *field, value)?))
        }
        FilterKind::In {
            field: FilterField::MetadataKey,
            values,
        } => {
            let values = values
                .iter()
                .map(|value| value.as_string("metadata_key"))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Some(format!("key IN ({})", sql_string_list(&values))))
        }
        _ => Ok(None),
    }
}

fn feedback_row_condition(
    expr: &FilterExpr,
    has_key_anchor: bool,
) -> Result<Option<String>, FilterError> {
    match &expr.kind {
        FilterKind::Compare { op, field, value }
            if matches!(
                field,
                FilterField::FeedbackKey | FilterField::FeedbackScore | FilterField::FeedbackValue
            ) =>
        {
            if matches!((*op, *field), (CompareOp::Neq, FilterField::FeedbackKey))
                || (matches!(
                    (*op, *field),
                    (
                        CompareOp::Neq,
                        FilterField::FeedbackScore | FilterField::FeedbackValue
                    )
                ) && !has_key_anchor)
            {
                return Ok(None);
            }
            Ok(Some(feedback_compare_condition(*op, *field, value)?))
        }
        _ => Ok(None),
    }
}

fn is_positive_metadata_key_condition(expr: &FilterExpr) -> bool {
    match &expr.kind {
        FilterKind::Compare {
            op,
            field: FilterField::MetadataKey,
            ..
        } => !matches!(op, CompareOp::Neq),
        FilterKind::In {
            field: FilterField::MetadataKey,
            ..
        } => true,
        _ => false,
    }
}

fn is_positive_feedback_key_condition(expr: &FilterExpr) -> bool {
    matches!(
        &expr.kind,
        FilterKind::Compare {
            op,
            field: FilterField::FeedbackKey,
            ..
        } if !matches!(op, CompareOp::Neq)
    )
}

fn metadata_compare_condition(
    op: CompareOp,
    field: FilterField,
    value: &FilterValue,
) -> Result<String, FilterError> {
    let column = match field {
        FilterField::MetadataKey => "key",
        FilterField::MetadataValue => "value",
        _ => unreachable!(),
    };
    compile_text_column(op, column, value, column)
}

fn feedback_compare_condition(
    op: CompareOp,
    field: FilterField,
    value: &FilterValue,
) -> Result<String, FilterError> {
    match field {
        FilterField::FeedbackKey => compile_text_column(op, "key", value, "feedback_key"),
        FilterField::FeedbackValue => {
            compile_text_column(op, "value_text", value, "feedback_value")
        }
        FilterField::FeedbackScore => {
            compile_f64_column(op, "score_number", value, "feedback_score")
        }
        _ => unreachable!(),
    }
}

fn compile_text_column(
    op: CompareOp,
    column: &str,
    value: &FilterValue,
    field_name: &str,
) -> Result<String, FilterError> {
    let value = value.as_string(field_name)?;
    let rhs = sql_string_literal(&value);
    Ok(compare_sql(op, column, &rhs))
}

fn compile_i64_column(
    op: CompareOp,
    column: &str,
    value: &FilterValue,
    field_name: &str,
) -> Result<String, FilterError> {
    let value = match field_name {
        "latency" => value.as_duration_nanos()?,
        "start_time" | "end_time" => value.as_time_nanos(field_name)?,
        _ => value.as_i64(field_name)?,
    };
    Ok(compare_sql(op, column, &value.to_string()))
}

fn compile_f64_column(
    op: CompareOp,
    column: &str,
    value: &FilterValue,
    field_name: &str,
) -> Result<String, FilterError> {
    Ok(compare_sql(
        op,
        column,
        &value.as_f64(field_name)?.to_string(),
    ))
}

fn compile_bool_column(op: CompareOp, column: &str, value: bool) -> Result<String, FilterError> {
    match op {
        CompareOp::Eq => Ok(format!(
            "{column} = {}",
            if value { "true" } else { "false" }
        )),
        CompareOp::Neq => Ok(format!(
            "{column} <> {}",
            if value { "true" } else { "false" }
        )),
        _ => Err(FilterError::Unsupported(
            "boolean fields only support eq and neq".to_owned(),
        )),
    }
}

fn compile_error_column(op: CompareOp, column: &str, value: bool) -> Result<String, FilterError> {
    match (op, value) {
        (CompareOp::Eq, true) | (CompareOp::Neq, false) => Ok(format!("{column} = 'error'")),
        (CompareOp::Eq, false) | (CompareOp::Neq, true) => Ok(format!("{column} <> 'error'")),
        _ => Err(FilterError::Unsupported(
            "error only supports eq and neq boolean filters".to_owned(),
        )),
    }
}

fn compare_sql(op: CompareOp, column: &str, rhs: &str) -> String {
    let operator = match op {
        CompareOp::Eq => "=",
        CompareOp::Neq => "<>",
        CompareOp::Gt => ">",
        CompareOp::Gte => ">=",
        CompareOp::Lt => "<",
        CompareOp::Lte => "<=",
    };
    format!("{column} {operator} {rhs}")
}

fn tag_exists_sql(run_alias: &str, tag: &str, project_names: Option<&[String]>) -> String {
    format!(
        "{} IN (
            SELECT {}
            FROM run_tags tag_filter
            WHERE {}tag_filter.tag = {}
        )",
        run_key_sql(run_alias),
        table_run_key_sql("tag_filter"),
        project_scope_sql("tag_filter", project_names),
        sql_string_literal(tag)
    )
}

fn metadata_exists_sql(
    run_alias: &str,
    conditions: &[String],
    project_names: Option<&[String]>,
) -> String {
    format!(
        "{} IN (
            SELECT {}
            FROM run_metadata metadata_filter
            WHERE {}{}
        )",
        run_key_sql(run_alias),
        table_run_key_sql("metadata_filter"),
        project_scope_sql("metadata_filter", project_names),
        conditions.join(" AND ")
    )
}

fn project_scope_sql(alias: &str, project_names: Option<&[String]>) -> String {
    let Some(project_names) = project_names.filter(|names| !names.is_empty()) else {
        return String::new();
    };

    format!(
        "{alias}.project_name IN ({}) AND ",
        sql_string_list(project_names)
    )
}

fn feedback_exists_sql(
    run_alias: &str,
    conditions: &[String],
    project_names: Option<&[String]>,
) -> String {
    format!(
        "(({run_alias}.run_id <> '' AND {run_alias}.run_id IN ({subquery}))
            OR {run_alias}.generated_run_id IN ({subquery}))",
        subquery = format!(
            "SELECT feedback_filter.run_id FROM feedback feedback_filter
            WHERE feedback_filter.run_id IS NOT NULL AND {}{}",
            project_scope_sql("feedback_filter", project_names),
            conditions.join(" AND ")
        )
    )
}

fn run_key_sql(alias: &str) -> String {
    format!("{alias}.project_name || '\u{1f}' || {alias}.trace_id || '\u{1f}' || {alias}.span_id")
}

fn table_run_key_sql(alias: &str) -> String {
    run_key_sql(alias)
}

impl FilterValue {
    fn as_string(&self, _field_name: &str) -> Result<String, FilterError> {
        match self {
            Self::String(value) => Ok(value.clone()),
            Self::Number(value) => Ok(trim_float(*value)),
            Self::Bool(value) => Ok(value.to_string()),
        }
    }

    fn as_bool(&self, field_name: &str) -> Result<bool, FilterError> {
        match self {
            Self::Bool(value) => Ok(*value),
            Self::String(value) if value.eq_ignore_ascii_case("true") => Ok(true),
            Self::String(value) if value.eq_ignore_ascii_case("false") => Ok(false),
            _ => Err(FilterError::Parse(format!("{field_name} must be boolean"))),
        }
    }

    fn as_i64(&self, field_name: &str) -> Result<i64, FilterError> {
        match self {
            Self::Number(value) => Ok(*value as i64),
            Self::String(value) => value
                .parse::<i64>()
                .map_err(|_| FilterError::Parse(format!("{field_name} must be an integer"))),
            Self::Bool(_) => Err(FilterError::Parse(format!("{field_name} must be numeric"))),
        }
    }

    fn as_f64(&self, field_name: &str) -> Result<f64, FilterError> {
        match self {
            Self::Number(value) => Ok(*value),
            Self::String(value) => value
                .parse::<f64>()
                .map_err(|_| FilterError::Parse(format!("{field_name} must be numeric"))),
            Self::Bool(_) => Err(FilterError::Parse(format!("{field_name} must be numeric"))),
        }
    }

    fn as_duration_nanos(&self) -> Result<i64, FilterError> {
        let seconds = match self {
            Self::Number(value) => *value,
            Self::String(value) => value
                .strip_suffix('s')
                .unwrap_or(value)
                .parse::<f64>()
                .map_err(|_| FilterError::Parse("latency must be seconds".to_owned()))?,
            Self::Bool(_) => return Err(FilterError::Parse("latency must be numeric".to_owned())),
        };
        Ok((seconds * 1_000_000_000.0) as i64)
    }

    fn as_time_nanos(&self, field_name: &str) -> Result<i64, FilterError> {
        match self {
            Self::String(value) => DateTime::parse_from_rfc3339(value)
                .map(|time| {
                    time.timestamp()
                        .saturating_mul(1_000_000_000)
                        .saturating_add(i64::from(time.timestamp_subsec_nanos()))
                })
                .map_err(|_| FilterError::Parse(format!("{field_name} must be RFC3339"))),
            _ => self.as_i64(field_name),
        }
    }
}

fn trim_float(value: f64) -> String {
    let text = value.to_string();
    text.strip_suffix(".0").unwrap_or(&text).to_owned()
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

fn escape_like_pattern(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for ch in value.chars() {
        if matches!(ch, '\\' | '%' | '_') {
            escaped.push('\\');
        }
        escaped.push(ch);
    }
    escaped
}

#[cfg(test)]
mod tests;
