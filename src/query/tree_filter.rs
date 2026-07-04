use super::filter::{FilterError, FilterExpr};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TreeFilterScope {
    Trace,
    Root,
    Child,
    Descendant,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TreeFilterMode {
    ShowAll,
    FilteredOnly,
    MostRelevant,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TreeFilterExpr {
    scope: TreeFilterScope,
    mode: TreeFilterMode,
    predicate: FilterExpr,
}

impl TreeFilterExpr {
    pub fn parse(input: &str) -> Result<Self, FilterError> {
        let input = input.trim();
        let (mode, input) = unwrap_mode(input);
        let (scope, input) = unwrap_scope(input);
        Ok(Self {
            scope,
            mode,
            predicate: FilterExpr::parse(input)?,
        })
    }

    pub(crate) fn scope(&self) -> TreeFilterScope {
        self.scope
    }

    pub(crate) fn mode(&self) -> TreeFilterMode {
        self.mode
    }

    pub(crate) fn predicate(&self) -> &FilterExpr {
        &self.predicate
    }
}

fn unwrap_mode(input: &str) -> (TreeFilterMode, &str) {
    if let Some(inner) = wrapper_inner(input, "filtered_only") {
        (TreeFilterMode::FilteredOnly, inner)
    } else if let Some(inner) = wrapper_inner(input, "show_all") {
        (TreeFilterMode::ShowAll, inner)
    } else if let Some(inner) = wrapper_inner(input, "most_relevant") {
        (TreeFilterMode::MostRelevant, inner)
    } else {
        (TreeFilterMode::ShowAll, input)
    }
}

fn unwrap_scope(input: &str) -> (TreeFilterScope, &str) {
    if let Some(inner) = wrapper_inner(input, "root") {
        (TreeFilterScope::Root, inner)
    } else if let Some(inner) = wrapper_inner(input, "child") {
        (TreeFilterScope::Child, inner)
    } else if let Some(inner) = wrapper_inner(input, "descendant") {
        (TreeFilterScope::Descendant, inner)
    } else if let Some(inner) = wrapper_inner(input, "trace") {
        (TreeFilterScope::Trace, inner)
    } else {
        (TreeFilterScope::Trace, input)
    }
}

fn wrapper_inner<'a>(input: &'a str, wrapper: &str) -> Option<&'a str> {
    let input = input.trim();
    let prefix_len = wrapper.len();
    if input.len() < prefix_len + 2
        || !input[..prefix_len].eq_ignore_ascii_case(wrapper)
        || !input[prefix_len..].starts_with('(')
        || !input.ends_with(')')
    {
        return None;
    }
    let inner = &input[prefix_len + 1..input.len() - 1];
    balanced_parentheses(inner).then_some(inner.trim())
}

fn balanced_parentheses(input: &str) -> bool {
    let mut depth = 0i32;
    let mut in_string = false;
    let mut escaped = false;
    for ch in input.chars() {
        if in_string {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                in_string = false;
            }
            continue;
        }

        match ch {
            '"' => in_string = true,
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth < 0 {
                    return false;
                }
            }
            _ => {}
        }
    }
    depth == 0 && !in_string
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_default_trace_tree_filter() {
        let filter =
            TreeFilterExpr::parse(r#"eq(name, "ExpandQuery")"#).expect("parse default tree filter");
        assert_eq!(filter.scope(), TreeFilterScope::Trace);
        assert_eq!(filter.mode(), TreeFilterMode::ShowAll);
    }

    #[test]
    fn parses_scope_and_result_mode_wrappers() {
        let filter = TreeFilterExpr::parse(r#"most_relevant(descendant(eq(run_type, "tool")))"#)
            .expect("parse scoped tree filter");
        assert_eq!(filter.scope(), TreeFilterScope::Descendant);
        assert_eq!(filter.mode(), TreeFilterMode::MostRelevant);

        let child = TreeFilterExpr::parse(r#"filtered_only(child(eq(name, "Foo")))"#)
            .expect("parse child tree filter");
        assert_eq!(child.scope(), TreeFilterScope::Child);
        assert_eq!(child.mode(), TreeFilterMode::FilteredOnly);
    }

    #[test]
    fn does_not_confuse_contains_filter_operator_with_scope_wrapper() {
        let filter =
            TreeFilterExpr::parse(r#"contains(name, "Foo")"#).expect("parse contains filter");
        assert_eq!(filter.scope(), TreeFilterScope::Trace);
    }
}
