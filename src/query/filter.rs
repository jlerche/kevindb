use std::error::Error;
use std::fmt;

use chrono::DateTime;

const MAX_FILTER_DEPTH: usize = 16;
const MAX_FILTER_NODES: usize = 128;

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
        let tokens = lex(input)?;
        let mut parser = Parser {
            tokens,
            offset: 0,
            nodes: 0,
        };
        let expr = parser.parse_expr(0)?;
        if !parser.is_done() {
            return Err(FilterError::Parse("trailing tokens".to_owned()));
        }
        Ok(expr)
    }

    pub fn compile_run_head_filter(&self, run_alias: &str) -> Result<CompiledFilter, FilterError> {
        Ok(CompiledFilter {
            predicate_sql: compile_expr(self, run_alias)?,
        })
    }
}

fn compile_expr(expr: &FilterExpr, run_alias: &str) -> Result<String, FilterError> {
    match &expr.kind {
        FilterKind::And(children) => compile_and(children, run_alias),
        FilterKind::Or(children) => {
            if children.is_empty() {
                return Err(FilterError::Parse(
                    "or() requires at least one child".to_owned(),
                ));
            }
            let predicates = children
                .iter()
                .map(|child| compile_expr(child, run_alias))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(format!("({})", predicates.join(" OR ")))
        }
        FilterKind::Compare { op, field, value } => compile_compare(*op, *field, value, run_alias),
        FilterKind::Has { field, value } => compile_has(*field, value, run_alias),
        FilterKind::In { field, values } => compile_in(*field, values, run_alias),
        FilterKind::Contains {
            field,
            value,
            negated,
        } => compile_contains(*field, value, *negated, run_alias),
        FilterKind::Search(query) => {
            let _query_len = query.len();
            Err(FilterError::Unsupported(
                "full-text search requires the Phase 6 object-store index".to_owned(),
            ))
        }
    }
}

fn compile_and(children: &[FilterExpr], run_alias: &str) -> Result<String, FilterError> {
    if children.is_empty() {
        return Err(FilterError::Parse(
            "and() requires at least one child".to_owned(),
        ));
    }

    let mut predicates = Vec::new();
    let mut metadata_conditions = Vec::new();
    let mut feedback_conditions = Vec::new();

    for child in children {
        if let Some(condition) = metadata_row_condition(child)? {
            metadata_conditions.push(condition);
            continue;
        }
        if let Some(condition) = feedback_row_condition(child)? {
            feedback_conditions.push(condition);
            continue;
        }
        predicates.push(compile_expr(child, run_alias)?);
    }

    if !metadata_conditions.is_empty() {
        predicates.push(metadata_exists_sql(run_alias, &metadata_conditions));
    }
    if !feedback_conditions.is_empty() {
        predicates.push(feedback_exists_sql(run_alias, &feedback_conditions));
    }

    Ok(format!("({})", predicates.join(" AND ")))
}

fn compile_compare(
    op: CompareOp,
    field: FilterField,
    value: &FilterValue,
    run_alias: &str,
) -> Result<String, FilterError> {
    match field {
        FilterField::MetadataKey | FilterField::MetadataValue => {
            compile_metadata_atom(op, field, value, run_alias)
        }
        FilterField::FeedbackKey | FilterField::FeedbackScore | FilterField::FeedbackValue => {
            compile_feedback_atom(op, field, value, run_alias)
        }
        FilterField::Tags => {
            let tag = value.as_string("tags")?;
            match op {
                CompareOp::Eq => Ok(tag_exists_sql(run_alias, &tag)),
                CompareOp::Neq => Ok(format!("NOT {}", tag_exists_sql(run_alias, &tag))),
                _ => Err(FilterError::Unsupported(
                    "tags only support eq, neq, has, and in".to_owned(),
                )),
            }
        }
        FilterField::Id => {
            let id = value.as_string("id")?;
            Ok(compare_sql(
                op,
                &format!(
                    "(NULLIF({run_alias}.run_id, '') = {id} OR {run_alias}.generated_run_id = {id})",
                    id = sql_string_literal(&id)
                ),
                "true",
            ))
        }
        FilterField::Name => compile_text_column(op, &format!("{run_alias}.name"), value, "name"),
        FilterField::RunType => {
            compile_text_column(op, &format!("{run_alias}.run_type"), value, "run_type")
        }
        FilterField::Status => {
            compile_text_column(op, &format!("{run_alias}.status"), value, "status")
        }
        FilterField::TraceId => {
            compile_text_column(op, &format!("{run_alias}.trace_id"), value, "trace_id")
        }
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
        FilterField::IsRoot => {
            let boolean = value.as_bool("is_root")?;
            compile_bool_column(op, &format!("{run_alias}.is_root"), boolean)
        }
    }
}

fn compile_has(
    field: FilterField,
    value: &FilterValue,
    run_alias: &str,
) -> Result<String, FilterError> {
    match field {
        FilterField::Tags => Ok(tag_exists_sql(run_alias, &value.as_string("tags")?)),
        FilterField::MetadataKey => Ok(metadata_exists_sql(
            run_alias,
            &[format!(
                "key = {}",
                sql_string_literal(&value.as_string("metadata_key")?)
            )],
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
                "EXISTS (
                    SELECT 1 FROM run_tags tag_filter
                    WHERE tag_filter.project_name = {run_alias}.project_name
                        AND tag_filter.trace_id = {run_alias}.trace_id
                        AND tag_filter.span_id = {run_alias}.span_id
                        AND tag_filter.tag IN ({})
                )",
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
            ))
        }
        FilterField::RunType | FilterField::Status | FilterField::Name | FilterField::TraceId => {
            let column = match field {
                FilterField::RunType => "run_type",
                FilterField::Status => "status",
                FilterField::Name => "name",
                FilterField::TraceId => "trace_id",
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
            "in() is supported for run_type, status, name, trace_id, tags, and metadata".to_owned(),
        )),
    }
}

fn compile_contains(
    field: FilterField,
    value: &FilterValue,
    negated: bool,
    run_alias: &str,
) -> Result<String, FilterError> {
    let value = value.as_string("contains")?;
    let predicate = match field {
        FilterField::Name | FilterField::RunType | FilterField::Status => {
            let column = match field {
                FilterField::Name => "name",
                FilterField::RunType => "run_type",
                FilterField::Status => "status",
                _ => unreachable!(),
            };
            format!(
                "LOWER({run_alias}.{column}) LIKE {}",
                sql_string_literal(&format!("%{}%", value.to_ascii_lowercase()))
            )
        }
        FilterField::MetadataValue => metadata_exists_sql(
            run_alias,
            &[format!(
                "LOWER(value) LIKE {}",
                sql_string_literal(&format!("%{}%", value.to_ascii_lowercase()))
            )],
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
) -> Result<String, FilterError> {
    let condition = metadata_compare_condition(op, field, value)?;
    if matches!(op, CompareOp::Neq) {
        Ok(format!(
            "NOT {}",
            metadata_exists_sql(run_alias, &[condition])
        ))
    } else {
        Ok(metadata_exists_sql(run_alias, &[condition]))
    }
}

fn compile_feedback_atom(
    op: CompareOp,
    field: FilterField,
    value: &FilterValue,
    run_alias: &str,
) -> Result<String, FilterError> {
    let condition = feedback_compare_condition(op, field, value)?;
    if matches!(op, CompareOp::Neq) {
        Ok(format!(
            "NOT {}",
            feedback_exists_sql(run_alias, &[condition])
        ))
    } else {
        Ok(feedback_exists_sql(run_alias, &[condition]))
    }
}

fn metadata_row_condition(expr: &FilterExpr) -> Result<Option<String>, FilterError> {
    match &expr.kind {
        FilterKind::Compare { op, field, value }
            if matches!(field, FilterField::MetadataKey | FilterField::MetadataValue) =>
        {
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

fn feedback_row_condition(expr: &FilterExpr) -> Result<Option<String>, FilterError> {
    match &expr.kind {
        FilterKind::Compare { op, field, value }
            if matches!(
                field,
                FilterField::FeedbackKey | FilterField::FeedbackScore | FilterField::FeedbackValue
            ) =>
        {
            Ok(Some(feedback_compare_condition(*op, *field, value)?))
        }
        _ => Ok(None),
    }
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

fn tag_exists_sql(run_alias: &str, tag: &str) -> String {
    format!(
        "EXISTS (
            SELECT 1 FROM run_tags tag_filter
            WHERE tag_filter.project_name = {run_alias}.project_name
                AND tag_filter.trace_id = {run_alias}.trace_id
                AND tag_filter.span_id = {run_alias}.span_id
                AND tag_filter.tag = {}
        )",
        sql_string_literal(tag)
    )
}

fn metadata_exists_sql(run_alias: &str, conditions: &[String]) -> String {
    format!(
        "EXISTS (
            SELECT 1 FROM run_metadata metadata_filter
            WHERE metadata_filter.project_name = {run_alias}.project_name
                AND metadata_filter.trace_id = {run_alias}.trace_id
                AND metadata_filter.span_id = {run_alias}.span_id
                AND {}
        )",
        conditions.join(" AND ")
    )
}

fn feedback_exists_sql(run_alias: &str, conditions: &[String]) -> String {
    format!(
        "EXISTS (
            SELECT 1 FROM feedback feedback_filter
            WHERE (
                    feedback_filter.run_id = NULLIF({run_alias}.run_id, '')
                    OR feedback_filter.run_id = {run_alias}.generated_run_id
                )
                AND {}
        )",
        conditions.join(" AND ")
    )
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

#[derive(Debug, Clone, PartialEq)]
enum Token {
    Ident(String),
    String(String),
    Number(f64),
    LParen,
    RParen,
    LBracket,
    RBracket,
    Comma,
}

struct Parser {
    tokens: Vec<Token>,
    offset: usize,
    nodes: usize,
}

impl Parser {
    fn parse_expr(&mut self, depth: usize) -> Result<FilterExpr, FilterError> {
        if depth > MAX_FILTER_DEPTH {
            return Err(FilterError::Parse("filter nesting is too deep".to_owned()));
        }
        self.nodes += 1;
        if self.nodes > MAX_FILTER_NODES {
            return Err(FilterError::Parse("filter is too large".to_owned()));
        }

        let function = self.expect_ident()?.to_ascii_lowercase();
        self.expect(Token::LParen)?;
        let expr = match function.as_str() {
            "and" | "or" => {
                let mut children = Vec::new();
                if !self.peek_is(&Token::RParen) {
                    loop {
                        children.push(self.parse_expr(depth + 1)?);
                        if !self.consume_if(&Token::Comma) {
                            break;
                        }
                    }
                }
                if function == "and" {
                    FilterExpr {
                        kind: FilterKind::And(children),
                    }
                } else {
                    FilterExpr {
                        kind: FilterKind::Or(children),
                    }
                }
            }
            "eq" | "neq" | "gt" | "gte" | "lt" | "lte" => {
                let field = parse_field(&self.expect_ident()?)?;
                self.expect(Token::Comma)?;
                let value = self.parse_value()?;
                FilterExpr {
                    kind: FilterKind::Compare {
                        op: parse_compare_op(&function),
                        field,
                        value,
                    },
                }
            }
            "has" => {
                let field = parse_field(&self.expect_ident()?)?;
                self.expect(Token::Comma)?;
                let value = self.parse_value()?;
                FilterExpr {
                    kind: FilterKind::Has { field, value },
                }
            }
            "in" => {
                let field = parse_field(&self.expect_ident()?)?;
                self.expect(Token::Comma)?;
                let values = self.parse_list()?;
                FilterExpr {
                    kind: FilterKind::In { field, values },
                }
            }
            "contains" | "does_not_contain" | "not_contains" => {
                let field = parse_field(&self.expect_ident()?)?;
                self.expect(Token::Comma)?;
                let value = self.parse_value()?;
                FilterExpr {
                    kind: FilterKind::Contains {
                        field,
                        value,
                        negated: function != "contains",
                    },
                }
            }
            "search" => {
                let value = self.parse_value()?.as_string("search")?;
                FilterExpr {
                    kind: FilterKind::Search(value),
                }
            }
            _ => {
                return Err(FilterError::Unsupported(format!(
                    "operator {function} is not supported"
                )));
            }
        };
        self.expect(Token::RParen)?;
        Ok(expr)
    }

    fn parse_value(&mut self) -> Result<FilterValue, FilterError> {
        match self.next() {
            Some(Token::String(value)) => Ok(FilterValue::String(value)),
            Some(Token::Number(value)) => Ok(FilterValue::Number(value)),
            Some(Token::Ident(value)) if value.eq_ignore_ascii_case("true") => {
                Ok(FilterValue::Bool(true))
            }
            Some(Token::Ident(value)) if value.eq_ignore_ascii_case("false") => {
                Ok(FilterValue::Bool(false))
            }
            Some(other) => Err(FilterError::Parse(format!(
                "expected value, found {other:?}"
            ))),
            None => Err(FilterError::Parse("expected value".to_owned())),
        }
    }

    fn parse_list(&mut self) -> Result<Vec<FilterValue>, FilterError> {
        self.expect(Token::LBracket)?;
        let mut values = Vec::new();
        if !self.peek_is(&Token::RBracket) {
            loop {
                values.push(self.parse_value()?);
                if !self.consume_if(&Token::Comma) {
                    break;
                }
            }
        }
        self.expect(Token::RBracket)?;
        Ok(values)
    }

    fn expect_ident(&mut self) -> Result<String, FilterError> {
        match self.next() {
            Some(Token::Ident(value)) => Ok(value),
            Some(other) => Err(FilterError::Parse(format!(
                "expected identifier, found {other:?}"
            ))),
            None => Err(FilterError::Parse("expected identifier".to_owned())),
        }
    }

    fn expect(&mut self, expected: Token) -> Result<(), FilterError> {
        match self.next() {
            Some(token) if token == expected => Ok(()),
            Some(token) => Err(FilterError::Parse(format!(
                "expected {expected:?}, found {token:?}"
            ))),
            None => Err(FilterError::Parse(format!("expected {expected:?}"))),
        }
    }

    fn consume_if(&mut self, expected: &Token) -> bool {
        if self.peek_is(expected) {
            self.offset += 1;
            true
        } else {
            false
        }
    }

    fn peek_is(&self, expected: &Token) -> bool {
        self.tokens
            .get(self.offset)
            .is_some_and(|token| token == expected)
    }

    fn next(&mut self) -> Option<Token> {
        let token = self.tokens.get(self.offset).cloned();
        if token.is_some() {
            self.offset += 1;
        }
        token
    }

    fn is_done(&self) -> bool {
        self.offset >= self.tokens.len()
    }
}

fn parse_compare_op(value: &str) -> CompareOp {
    match value {
        "eq" => CompareOp::Eq,
        "neq" => CompareOp::Neq,
        "gt" => CompareOp::Gt,
        "gte" => CompareOp::Gte,
        "lt" => CompareOp::Lt,
        "lte" => CompareOp::Lte,
        _ => unreachable!(),
    }
}

fn parse_field(value: &str) -> Result<FilterField, FilterError> {
    match value {
        "id" | "run_id" => Ok(FilterField::Id),
        "name" => Ok(FilterField::Name),
        "run_type" => Ok(FilterField::RunType),
        "status" => Ok(FilterField::Status),
        "start_time" => Ok(FilterField::StartTime),
        "end_time" => Ok(FilterField::EndTime),
        "latency" => Ok(FilterField::Latency),
        "tags" => Ok(FilterField::Tags),
        "metadata_key" => Ok(FilterField::MetadataKey),
        "metadata_value" => Ok(FilterField::MetadataValue),
        "feedback_key" => Ok(FilterField::FeedbackKey),
        "feedback_score" => Ok(FilterField::FeedbackScore),
        "feedback_value" => Ok(FilterField::FeedbackValue),
        "is_root" => Ok(FilterField::IsRoot),
        "trace_id" | "trace" => Ok(FilterField::TraceId),
        "inputs" | "outputs" | "extra" | "attributes_json" => Err(FilterError::Unsupported(
            "payload JSON filters require the Phase 6 object-store index".to_owned(),
        )),
        other => Err(FilterError::Unsupported(format!(
            "field {other} is not indexed"
        ))),
    }
}

fn lex(input: &str) -> Result<Vec<Token>, FilterError> {
    let mut chars = input.char_indices().peekable();
    let mut tokens = Vec::new();
    while let Some((_, ch)) = chars.peek().copied() {
        match ch {
            c if c.is_whitespace() => {
                chars.next();
            }
            '(' => {
                chars.next();
                tokens.push(Token::LParen);
            }
            ')' => {
                chars.next();
                tokens.push(Token::RParen);
            }
            '[' => {
                chars.next();
                tokens.push(Token::LBracket);
            }
            ']' => {
                chars.next();
                tokens.push(Token::RBracket);
            }
            ',' => {
                chars.next();
                tokens.push(Token::Comma);
            }
            '"' | '\'' => tokens.push(Token::String(read_string(&mut chars, ch)?)),
            '-' | '0'..='9' => tokens.push(read_number(&mut chars)?),
            c if is_ident_start(c) => tokens.push(Token::Ident(read_ident(&mut chars))),
            _ => {
                return Err(FilterError::Parse(format!("unexpected character {ch:?}")));
            }
        }
    }
    Ok(tokens)
}

fn read_string<I>(chars: &mut std::iter::Peekable<I>, quote: char) -> Result<String, FilterError>
where
    I: Iterator<Item = (usize, char)>,
{
    chars.next();
    let mut value = String::new();
    while let Some((_, ch)) = chars.next() {
        if ch == quote {
            return Ok(value);
        }
        if ch == '\\' {
            let Some((_, escaped)) = chars.next() else {
                return Err(FilterError::Parse("unterminated escape".to_owned()));
            };
            value.push(escaped);
        } else {
            value.push(ch);
        }
    }
    Err(FilterError::Parse("unterminated string".to_owned()))
}

fn read_number<I>(chars: &mut std::iter::Peekable<I>) -> Result<Token, FilterError>
where
    I: Iterator<Item = (usize, char)>,
{
    let mut value = String::new();
    while let Some((_, ch)) = chars.peek().copied() {
        if ch.is_ascii_digit() || matches!(ch, '-' | '.') {
            value.push(ch);
            chars.next();
        } else {
            break;
        }
    }
    value
        .parse::<f64>()
        .map(Token::Number)
        .map_err(|_| FilterError::Parse(format!("invalid number {value}")))
}

fn read_ident<I>(chars: &mut std::iter::Peekable<I>) -> String
where
    I: Iterator<Item = (usize, char)>,
{
    let mut value = String::new();
    while let Some((_, ch)) = chars.peek().copied() {
        if ch.is_ascii_alphanumeric() || ch == '_' {
            value.push(ch);
            chars.next();
        } else {
            break;
        }
    }
    value
}

fn is_ident_start(ch: char) -> bool {
    ch.is_ascii_alphabetic() || ch == '_'
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
mod tests;
