use super::{
    CompareOp, FilterError, FilterExpr, FilterField, FilterKind, FilterValue, MAX_FILTER_DEPTH,
    MAX_FILTER_NODES,
};

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

pub(super) fn parse_filter_expr(input: &str) -> Result<FilterExpr, FilterError> {
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
                let field = self.parse_optional_field_prefix()?;
                let value = self.parse_value()?.as_string("search")?;
                FilterExpr {
                    kind: FilterKind::Search {
                        field,
                        query: value,
                    },
                }
            }
            "json_key" => {
                let field = self.parse_optional_field_prefix()?;
                let pattern = self.parse_value()?.as_string("json_key")?;
                FilterExpr {
                    kind: FilterKind::JsonKey { field, pattern },
                }
            }
            "json_key_search" => {
                let field = self.parse_optional_field_prefix()?;
                let path = self.parse_value()?.as_string("json_key_search")?;
                self.expect(Token::Comma)?;
                let query = self.parse_value()?.as_string("json_key_search")?;
                FilterExpr {
                    kind: FilterKind::JsonKeySearch { field, path, query },
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

    fn parse_optional_field_prefix(&mut self) -> Result<Option<FilterField>, FilterError> {
        match (
            self.tokens.get(self.offset),
            self.tokens.get(self.offset + 1),
        ) {
            (Some(Token::Ident(field)), Some(Token::Comma)) => {
                let field = parse_field(field)?;
                self.offset += 2;
                Ok(Some(field))
            }
            _ => Ok(None),
        }
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
        "project_name" | "session" | "session_name" => Ok(FilterField::ProjectName),
        "run_type" => Ok(FilterField::RunType),
        "status" => Ok(FilterField::Status),
        "error" => Ok(FilterField::Error),
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
        "root_run_id" => Ok(FilterField::RootRunId),
        "root_span_id" => Ok(FilterField::RootSpanId),
        "model" | "model_name" | "ls_model_name" => Ok(FilterField::ModelName),
        "provider" | "provider_name" | "ls_provider" => Ok(FilterField::ProviderName),
        "prompt_tokens" => Ok(FilterField::PromptTokens),
        "completion_tokens" => Ok(FilterField::CompletionTokens),
        "total_tokens" => Ok(FilterField::TotalTokens),
        "total_cost" => Ok(FilterField::TotalCost),
        "inputs" => Ok(FilterField::Inputs),
        "outputs" => Ok(FilterField::Outputs),
        "extra" => Ok(FilterField::Extra),
        "attributes_json" => Ok(FilterField::AttributesJson),
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
