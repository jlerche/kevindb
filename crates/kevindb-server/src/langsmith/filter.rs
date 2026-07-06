use kevindb::query::TreeFilterExpr;
use kevindb::query::filter::FilterExpr;
use serde_json::Value;

use crate::ApiError;

pub(super) fn parse_filter(
    value: Option<&Value>,
    field: &str,
) -> Result<Option<FilterExpr>, ApiError> {
    let Some(value) = value else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }

    let text = structured_filter_to_text(value)
        .map_err(|error| ApiError::bad_request(format!("{field}: {error}")))?;
    if text.trim().is_empty() {
        return Ok(None);
    }

    FilterExpr::parse(&text)
        .map(Some)
        .map_err(|error| ApiError::bad_request(format!("{field}: {error}")))
}

pub(super) fn parse_tree_filter(value: Option<&Value>) -> Result<Option<TreeFilterExpr>, ApiError> {
    let Some(value) = value else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }

    let text = structured_filter_to_text(value)
        .map_err(|error| ApiError::bad_request(format!("tree_filter: {error}")))?;
    let text = text.trim();
    if text.is_empty() {
        return Ok(None);
    }

    TreeFilterExpr::parse(text)
        .map(Some)
        .map_err(|error| ApiError::bad_request(format!("tree_filter: {error}")))
}

fn structured_filter_to_text(value: &Value) -> Result<String, String> {
    match value {
        Value::String(text) => Ok(text.clone()),
        Value::Array(children) => compile_structured_logical("and", children),
        Value::Object(object) => {
            if let Some(children) = object.get("and").and_then(Value::as_array) {
                return compile_structured_logical("and", children);
            }
            if let Some(children) = object.get("or").and_then(Value::as_array) {
                return compile_structured_logical("or", children);
            }
            if let Some(query) = object.get("search") {
                return if let Some(field) = structured_phase6_scope(object)? {
                    Ok(format!(
                        "search({field}, {})",
                        structured_filter_literal(query)?
                    ))
                } else {
                    Ok(format!("search({})", structured_filter_literal(query)?))
                };
            }

            let operator = object
                .get("operator")
                .or_else(|| object.get("op"))
                .or_else(|| object.get("comparator"))
                .or_else(|| object.get("operation"))
                .and_then(Value::as_str)
                .unwrap_or("eq");
            let operator = normalize_structured_operator(operator)?;

            if matches!(operator.as_str(), "and" | "or") {
                let children = object
                    .get("children")
                    .or_else(|| object.get("operands"))
                    .or_else(|| object.get("filters"))
                    .or_else(|| object.get("conditions"))
                    .or_else(|| object.get("args"))
                    .and_then(Value::as_array)
                    .ok_or_else(|| format!("{operator} filter requires children"))?;
                return compile_structured_logical(&operator, children);
            }
            if operator == "search" {
                let query = object
                    .get("query")
                    .or_else(|| object.get("value"))
                    .or_else(|| object.get("search"))
                    .ok_or_else(|| "search filter requires query or value".to_owned())?;
                return if let Some(field) = structured_phase6_scope(object)? {
                    Ok(format!(
                        "search({field}, {})",
                        structured_filter_literal(query)?
                    ))
                } else {
                    Ok(format!("search({})", structured_filter_literal(query)?))
                };
            }
            if operator == "json_key" {
                let has_path_argument = object
                    .get("path")
                    .or_else(|| object.get("key"))
                    .or_else(|| object.get("value"))
                    .is_some();
                let field = if has_path_argument {
                    structured_phase6_scope(object)?
                } else {
                    structured_explicit_phase6_scope(object)?
                };
                let path = object
                    .get("path")
                    .or_else(|| object.get("key"))
                    .or_else(|| object.get("value"))
                    .or_else(|| field.is_none().then(|| object.get("field")).flatten())
                    .ok_or_else(|| {
                        "json_key filter requires path, key, field, or value".to_owned()
                    })?;
                return if let Some(field) = field {
                    Ok(format!(
                        "json_key({field}, {})",
                        structured_filter_literal(path)?
                    ))
                } else {
                    Ok(format!("json_key({})", structured_filter_literal(path)?))
                };
            }
            if operator == "json_key_search" {
                let has_path_argument = object.get("path").or_else(|| object.get("key")).is_some();
                let field = if has_path_argument {
                    structured_phase6_scope(object)?
                } else {
                    structured_explicit_phase6_scope(object)?
                };
                let path = object
                    .get("path")
                    .or_else(|| object.get("key"))
                    .or_else(|| field.is_none().then(|| object.get("field")).flatten())
                    .ok_or_else(|| {
                        "json_key_search filter requires path, key, or field".to_owned()
                    })?;
                let query = object
                    .get("query")
                    .or_else(|| object.get("value"))
                    .ok_or_else(|| "json_key_search filter requires query or value".to_owned())?;
                return if let Some(field) = field {
                    Ok(format!(
                        "json_key_search({field}, {}, {})",
                        structured_filter_literal(path)?,
                        structured_filter_literal(query)?
                    ))
                } else {
                    Ok(format!(
                        "json_key_search({}, {})",
                        structured_filter_literal(path)?,
                        structured_filter_literal(query)?
                    ))
                };
            }

            let field = object
                .get("field")
                .or_else(|| object.get("key"))
                .or_else(|| object.get("column"))
                .and_then(Value::as_str)
                .ok_or_else(|| "structured filter requires field".to_owned())?;
            let field = structured_filter_identifier(field)?;
            let value = object
                .get("values")
                .or_else(|| object.get("value"))
                .ok_or_else(|| "structured filter requires value".to_owned())?;

            if operator == "in" {
                let values = value
                    .as_array()
                    .ok_or_else(|| "in filter requires array values".to_owned())?
                    .iter()
                    .map(structured_filter_literal)
                    .collect::<Result<Vec<_>, _>>()?;
                return Ok(format!("in({field}, [{}])", values.join(", ")));
            }

            Ok(format!(
                "{operator}({field}, {})",
                structured_filter_literal(value)?
            ))
        }
        _ => Err("filter must be a string, object, or array".to_owned()),
    }
}

fn compile_structured_logical(operator: &str, children: &[Value]) -> Result<String, String> {
    if children.is_empty() {
        return Err(format!("{operator} filter requires at least one child"));
    }
    let children = children
        .iter()
        .map(structured_filter_to_text)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(format!("{operator}({})", children.join(", ")))
}

fn normalize_structured_operator(operator: &str) -> Result<String, String> {
    let normalized = match operator.to_ascii_lowercase().as_str() {
        "=" | "==" | "eq" => "eq",
        "!=" | "<>" | "neq" | "ne" => "neq",
        ">" | "gt" => "gt",
        ">=" | "gte" | "ge" => "gte",
        "<" | "lt" => "lt",
        "<=" | "lte" | "le" => "lte",
        "in" | "is_one_of" | "is one of" => "in",
        "has" => "has",
        "contains" => "contains",
        "does_not_contain" | "not_contains" | "not contains" => "does_not_contain",
        "and" => "and",
        "or" => "or",
        "search" => "search",
        "json_key" => "json_key",
        "json_key_search" => "json_key_search",
        other => return Err(format!("operator {other} is not supported")),
    };
    Ok(normalized.to_owned())
}

fn structured_filter_identifier(field: &str) -> Result<String, String> {
    if field.is_empty()
        || !field
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
    {
        return Err(format!("field {field} is not a valid filter identifier"));
    }
    Ok(field.to_owned())
}

fn structured_phase6_scope(
    object: &serde_json::Map<String, Value>,
) -> Result<Option<String>, String> {
    if let Some(scope) = structured_explicit_phase6_scope(object)? {
        return Ok(Some(scope));
    }
    let Some(value) = object.get("field").and_then(Value::as_str) else {
        return Ok(None);
    };
    structured_filter_identifier(value).map(Some)
}

fn structured_explicit_phase6_scope(
    object: &serde_json::Map<String, Value>,
) -> Result<Option<String>, String> {
    for key in ["scope", "payload_field"] {
        let Some(value) = object.get(key).and_then(Value::as_str) else {
            continue;
        };
        if let Ok(identifier) = structured_filter_identifier(value) {
            return Ok(Some(identifier));
        }
        return Err(format!("{key} {value} is not a valid filter identifier"));
    }
    Ok(None)
}

fn structured_filter_literal(value: &Value) -> Result<String, String> {
    match value {
        Value::String(value) => serde_json::to_string(value).map_err(|error| error.to_string()),
        Value::Number(_) | Value::Bool(_) => Ok(value.to_string()),
        _ => Err("filter values must be scalar".to_owned()),
    }
}
