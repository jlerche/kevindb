use crate::search::{SearchField, SearchPredicate, SearchQuery};

use super::{
    CompareOp, CompiledFilter, FilterError, FilterExpr, FilterField, FilterKind, compile_expr,
};

impl FilterExpr {
    pub(crate) fn phase6_search_predicate(&self) -> Result<Option<SearchPredicate>, FilterError> {
        phase6_predicate_for_expr(self)
    }

    pub(crate) fn compile_run_head_prefilter_for_projects(
        &self,
        run_alias: &str,
        project_names: &[String],
    ) -> Result<Option<CompiledFilter>, FilterError> {
        let Some(expr) = scalar_prefilter_expr(self)? else {
            return Ok(None);
        };
        Ok(Some(CompiledFilter {
            predicate_sql: compile_expr(&expr, run_alias, Some(project_names))?,
        }))
    }
}

fn phase6_predicate_for_expr(expr: &FilterExpr) -> Result<Option<SearchPredicate>, FilterError> {
    match &expr.kind {
        FilterKind::And(children) => {
            let mut predicates = Vec::new();
            for child in children {
                if let Some(predicate) = phase6_predicate_for_expr(child)? {
                    predicates.push(predicate);
                }
            }
            Ok(combine_and(predicates))
        }
        FilterKind::Or(children) => {
            if children.iter().any(contains_phase6_atom) {
                if children.iter().any(contains_scalar_atom) {
                    return Err(FilterError::Unsupported(
                        "Phase 6 search predicates can only be ORed with other search predicates"
                            .to_owned(),
                    ));
                }
                let predicates = children
                    .iter()
                    .map(|child| {
                        phase6_predicate_for_expr(child)?.ok_or_else(|| {
                            FilterError::Unsupported(
                                "or() search branch did not contain a search predicate".to_owned(),
                            )
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(combine_or(predicates))
            } else {
                Ok(None)
            }
        }
        _ => phase6_atom_predicate(expr),
    }
}

fn scalar_prefilter_expr(expr: &FilterExpr) -> Result<Option<FilterExpr>, FilterError> {
    match &expr.kind {
        FilterKind::And(children) => {
            let mut scalar_children = Vec::new();
            for child in children {
                if let Some(scalar_child) = scalar_prefilter_expr(child)? {
                    scalar_children.push(scalar_child);
                }
            }
            Ok(match scalar_children.len() {
                0 => None,
                1 => scalar_children.into_iter().next(),
                _ => Some(FilterExpr {
                    kind: FilterKind::And(scalar_children),
                }),
            })
        }
        FilterKind::Or(children) if children.iter().any(contains_phase6_atom) => {
            if children.iter().any(contains_scalar_atom) {
                return Err(FilterError::Unsupported(
                    "Phase 6 search predicates can only be ORed with other search predicates"
                        .to_owned(),
                ));
            }
            Ok(None)
        }
        _ if is_phase6_atom(expr) => Ok(None),
        _ => Ok(Some(expr.clone())),
    }
}

fn phase6_atom_predicate(expr: &FilterExpr) -> Result<Option<SearchPredicate>, FilterError> {
    match &expr.kind {
        FilterKind::Search { field, query } => {
            let query = SearchQuery::parse(query);
            match field {
                Some(field) => Ok(Some(field_text_predicate(*field, query)?)),
                None => Ok(Some(text_predicate(SearchField::All, query))),
            }
        }
        FilterKind::JsonKey { field, pattern } => match field {
            Some(field) if field.is_payload() => Ok(Some(payload_path_predicate(*field, pattern))),
            Some(field) => Err(unsupported_search_field(*field)),
            None => Ok(Some(SearchPredicate::JsonKey {
                pattern: pattern.clone(),
            })),
        },
        FilterKind::JsonKeySearch { field, path, query } => {
            let query = SearchQuery::parse(query);
            match field {
                Some(field) => Ok(Some(field_exact_path_text_predicate(*field, path, query)?)),
                None => Ok(Some(text_predicate(
                    SearchField::ExactPath(path.clone()),
                    query,
                ))),
            }
        }
        FilterKind::Compare { op, field, value } if field.is_payload() => {
            let predicate = payload_exact_value_predicate(*field, &value.as_string(field.name())?);
            match op {
                CompareOp::Eq => Ok(Some(predicate)),
                CompareOp::Neq => Ok(Some(SearchPredicate::Not(Box::new(predicate)))),
                _ => Err(FilterError::Unsupported(
                    "payload JSON comparisons only support eq and neq".to_owned(),
                )),
            }
        }
        FilterKind::Has { field, value } if field.is_payload() => Ok(Some(payload_path_predicate(
            *field,
            &value.as_string(field.name())?,
        ))),
        FilterKind::In { field, values } if field.is_payload() => {
            if values.is_empty() {
                return Err(FilterError::Parse(
                    "in() requires a non-empty list".to_owned(),
                ));
            }
            let predicates = values
                .iter()
                .map(|value| {
                    Ok(payload_exact_value_predicate(
                        *field,
                        &value.as_string(field.name())?,
                    ))
                })
                .collect::<Result<Vec<_>, FilterError>>()?;
            Ok(combine_or(predicates))
        }
        FilterKind::Contains {
            field,
            value,
            negated,
        } if field.is_payload() => {
            let predicate =
                payload_text_predicate(*field, SearchQuery::parse(&value.as_string(field.name())?));
            if *negated {
                Ok(Some(SearchPredicate::Not(Box::new(predicate))))
            } else {
                Ok(Some(predicate))
            }
        }
        _ => Ok(None),
    }
}

fn text_predicate(field: SearchField, query: SearchQuery) -> SearchPredicate {
    SearchPredicate::Text { field, query }
}

fn payload_text_predicate(field: FilterField, query: SearchQuery) -> SearchPredicate {
    let scopes = payload_scopes(field);
    if scopes.len() == 1 {
        return text_predicate(scopes.into_iter().next().expect("one scope"), query);
    }
    SearchPredicate::Or(
        scopes
            .into_iter()
            .map(|scope| text_predicate(scope, query.clone()))
            .collect(),
    )
}

fn payload_exact_value_predicate(field: FilterField, value: &str) -> SearchPredicate {
    let scopes = payload_scopes(field);
    if scopes.len() == 1 {
        return SearchPredicate::ExactValue {
            field: scopes.into_iter().next().expect("one scope"),
            value: value.to_owned(),
        };
    }
    SearchPredicate::Or(
        scopes
            .into_iter()
            .map(|scope| SearchPredicate::ExactValue {
                field: scope,
                value: value.to_owned(),
            })
            .collect(),
    )
}

fn field_text_predicate(
    field: FilterField,
    query: SearchQuery,
) -> Result<SearchPredicate, FilterError> {
    if field.is_payload() {
        return Ok(payload_text_predicate(field, query));
    }
    indexed_text_path(field)
        .map(|path| text_predicate(SearchField::ExactPath(path.to_owned()), query))
        .ok_or_else(|| unsupported_search_field(field))
}

fn field_exact_path_text_predicate(
    field: FilterField,
    path: &str,
    query: SearchQuery,
) -> Result<SearchPredicate, FilterError> {
    if !field.is_payload() {
        return Err(unsupported_search_field(field));
    }
    Ok(payload_exact_path_text_predicate(field, path, query))
}

fn payload_exact_path_text_predicate(
    field: FilterField,
    path: &str,
    query: SearchQuery,
) -> SearchPredicate {
    let prefixes = payload_path_prefixes(field);
    if prefixes.is_empty() {
        return text_predicate(SearchField::ExactPath(path.to_owned()), query);
    }
    SearchPredicate::Or(
        prefixes
            .into_iter()
            .map(|prefix| {
                text_predicate(
                    SearchField::ExactPath(join_path_pattern(prefix, path)),
                    query.clone(),
                )
            })
            .collect(),
    )
}

fn payload_path_predicate(field: FilterField, suffix: &str) -> SearchPredicate {
    let prefixes = payload_path_prefixes(field);
    if prefixes.is_empty() {
        return SearchPredicate::JsonKey {
            pattern: suffix.to_owned(),
        };
    }
    SearchPredicate::Or(
        prefixes
            .into_iter()
            .map(|prefix| SearchPredicate::JsonKey {
                pattern: join_path_pattern(prefix, suffix),
            })
            .collect(),
    )
}

fn indexed_text_path(field: FilterField) -> Option<&'static str> {
    match field {
        FilterField::Name => Some("name"),
        FilterField::RunType => Some("run_type"),
        FilterField::Status => Some("status"),
        _ => None,
    }
}

fn unsupported_search_field(field: FilterField) -> FilterError {
    FilterError::Unsupported(format!(
        "field {} is not available in the Phase 6 search index",
        field.name()
    ))
}

fn payload_scopes(field: FilterField) -> Vec<SearchField> {
    match field {
        FilterField::Inputs => vec![
            SearchField::PathPrefix("langsmith.inputs".to_owned()),
            SearchField::PathPrefix("inputs".to_owned()),
        ],
        FilterField::Outputs => vec![
            SearchField::PathPrefix("langsmith.outputs".to_owned()),
            SearchField::PathPrefix("outputs".to_owned()),
        ],
        FilterField::Extra => vec![
            SearchField::PathPrefix("langsmith.extra".to_owned()),
            SearchField::PathPrefix("extra".to_owned()),
        ],
        FilterField::AttributesJson => vec![SearchField::All],
        _ => Vec::new(),
    }
}

fn payload_path_prefixes(field: FilterField) -> Vec<&'static str> {
    match field {
        FilterField::Inputs => vec!["langsmith.inputs", "inputs"],
        FilterField::Outputs => vec!["langsmith.outputs", "outputs"],
        FilterField::Extra => vec!["langsmith.extra", "extra"],
        FilterField::AttributesJson => Vec::new(),
        _ => Vec::new(),
    }
}

fn join_path_pattern(prefix: &str, suffix: &str) -> String {
    if suffix.is_empty() {
        prefix.to_owned()
    } else {
        format!("{prefix}.{suffix}")
    }
}

fn combine_and(predicates: Vec<SearchPredicate>) -> Option<SearchPredicate> {
    match predicates.len() {
        0 => None,
        1 => predicates.into_iter().next(),
        _ => Some(SearchPredicate::And(predicates)),
    }
}

fn combine_or(predicates: Vec<SearchPredicate>) -> Option<SearchPredicate> {
    match predicates.len() {
        0 => None,
        1 => predicates.into_iter().next(),
        _ => Some(SearchPredicate::Or(predicates)),
    }
}

fn contains_phase6_atom(expr: &FilterExpr) -> bool {
    match &expr.kind {
        FilterKind::And(children) | FilterKind::Or(children) => {
            children.iter().any(contains_phase6_atom)
        }
        _ => is_phase6_atom(expr),
    }
}

fn contains_scalar_atom(expr: &FilterExpr) -> bool {
    match &expr.kind {
        FilterKind::And(children) | FilterKind::Or(children) => {
            children.iter().any(contains_scalar_atom)
        }
        _ => !is_phase6_atom(expr),
    }
}

fn is_phase6_atom(expr: &FilterExpr) -> bool {
    match &expr.kind {
        FilterKind::Search { .. }
        | FilterKind::JsonKey { .. }
        | FilterKind::JsonKeySearch { .. } => true,
        FilterKind::Compare { field, .. }
        | FilterKind::Has { field, .. }
        | FilterKind::In { field, .. }
        | FilterKind::Contains { field, .. } => field.is_payload(),
        FilterKind::And(_) | FilterKind::Or(_) => false,
    }
}

impl FilterField {
    fn is_payload(self) -> bool {
        matches!(
            self,
            Self::Inputs | Self::Outputs | Self::Extra | Self::AttributesJson
        )
    }

    fn name(self) -> &'static str {
        match self {
            Self::Inputs => "inputs",
            Self::Outputs => "outputs",
            Self::Extra => "extra",
            Self::AttributesJson => "attributes_json",
            _ => "filter",
        }
    }
}
