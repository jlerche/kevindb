use std::collections::BTreeSet;

use super::{MAX_TOKEN_BYTES, MIN_TOKEN_BYTES};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SearchPredicate {
    And(Vec<SearchPredicate>),
    Or(Vec<SearchPredicate>),
    Not(Box<SearchPredicate>),
    Text {
        field: SearchField,
        query: SearchQuery,
    },
    JsonKey {
        pattern: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SearchField {
    All,
    ExactPath(String),
    PathPrefix(String),
}

impl SearchField {
    pub(crate) fn matches_path(&self, path: &str) -> bool {
        match self {
            Self::All => true,
            Self::ExactPath(expected) => path == expected,
            Self::PathPrefix(prefix) => {
                path == prefix
                    || path
                        .strip_prefix(prefix)
                        .is_some_and(|rest| rest.starts_with('.'))
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchQuery {
    terms: Vec<String>,
    phrases: Vec<Vec<String>>,
}

impl SearchQuery {
    pub fn parse(value: &str) -> Self {
        let mut phrases = Vec::new();
        let mut unquoted = String::new();
        let mut chars = value.chars().peekable();
        while let Some(ch) = chars.next() {
            if ch != '"' {
                unquoted.push(ch);
                continue;
            }

            let mut phrase = String::new();
            for phrase_ch in chars.by_ref() {
                if phrase_ch == '"' {
                    break;
                }
                phrase.push(phrase_ch);
            }
            let tokens = tokens_for_text(&phrase);
            if !tokens.is_empty() {
                phrases.push(tokens);
            }
            unquoted.push(' ');
        }

        let terms = tokens_for_text(&unquoted);
        Self {
            terms: dedup_terms(terms),
            phrases,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.terms.is_empty() && self.phrases.is_empty()
    }

    pub(crate) fn terms(&self) -> &[String] {
        &self.terms
    }

    pub(crate) fn phrases(&self) -> &[Vec<String>] {
        &self.phrases
    }
}

pub fn tokens_for_text(value: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() {
            if current.len() < MAX_TOKEN_BYTES {
                current.push(ch.to_ascii_lowercase());
            }
        } else {
            push_token(&mut tokens, &mut current);
        }
    }
    push_token(&mut tokens, &mut current);
    tokens
}

fn push_token(tokens: &mut Vec<String>, current: &mut String) {
    if current.len() >= MIN_TOKEN_BYTES && !is_stop_word(current) {
        tokens.push(std::mem::take(current));
    } else {
        current.clear();
    }
}

fn dedup_terms(terms: Vec<String>) -> Vec<String> {
    let mut seen = BTreeSet::new();
    terms
        .into_iter()
        .filter(|term| seen.insert(term.clone()))
        .collect()
}

fn is_stop_word(value: &str) -> bool {
    matches!(
        value,
        "a" | "an"
            | "and"
            | "are"
            | "as"
            | "at"
            | "be"
            | "by"
            | "for"
            | "from"
            | "in"
            | "is"
            | "it"
            | "of"
            | "on"
            | "or"
            | "the"
            | "to"
            | "with"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokenizes_with_limits_and_stop_words() {
        assert_eq!(
            tokens_for_text("The Quick, brown-fox 123 a"),
            vec!["quick", "brown", "fox", "123"]
        );
        assert_eq!(tokens_for_text("x ok"), vec!["ok"]);
    }

    #[test]
    fn parses_quoted_phrases_separately_from_terms() {
        let query = SearchQuery::parse(r#"alpha "bravo charlie" delta"#);
        assert_eq!(query.terms(), &["alpha".to_owned(), "delta".to_owned()]);
        assert_eq!(
            query.phrases(),
            &[vec!["bravo".to_owned(), "charlie".to_owned()]]
        );
    }
}
