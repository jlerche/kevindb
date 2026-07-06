use super::{
    MAX_INDEXED_VALUE_BYTES, MAX_JSON_KEYS_PER_RUN, MutableSearchIndex, SearchIndex,
    exact_value_token, json_tape, tokens_for_text,
};
use crate::otlp::SpanRecord;

pub(super) fn build_search_index(records: &[SpanRecord]) -> anyhow::Result<SearchIndex> {
    let mut builder = SearchIndexBuilder::new(records.len());
    for (row_index, record) in records.iter().enumerate() {
        let row = row_index.min(u32::MAX as usize) as u32;
        builder.add_text(row, "name", &record.name);
        builder.add_text(row, "run_type", &record.run_type);
        builder.add_text(row, "status", status_from_record(record));
        builder.add_json(row, &record.attributes_json);
    }
    builder.finish()
}

struct SearchIndexBuilder {
    index: MutableSearchIndex,
    next_positions: std::collections::BTreeMap<(u32, String), u32>,
}

impl SearchIndexBuilder {
    fn new(row_count: usize) -> Self {
        Self {
            index: MutableSearchIndex::new(row_count),
            next_positions: std::collections::BTreeMap::new(),
        }
    }

    fn add_json(&mut self, row: u32, attributes_json: &str) {
        let Ok(tape) = json_tape::JsonTape::parse(attributes_json) else {
            return;
        };
        for leaf in tape.leaves().take(MAX_JSON_KEYS_PER_RUN) {
            match leaf.value {
                Some(value) => self.add_json_value(row, leaf.path, value),
                None => self.index.add_path(row, leaf.path),
            }
        }
    }

    fn add_json_value(&mut self, row: u32, path: &str, value: &str) {
        if value.len() <= MAX_INDEXED_VALUE_BYTES {
            self.index
                .add_value_position(row, path, &exact_value_token(value), 0);
        }
        self.add_text(row, path, bounded_value(value));
        self.add_leaf_position_gap(row, path);
    }

    fn add_text(&mut self, row: u32, path: &str, value: &str) {
        self.index.add_path(row, path);
        for token in tokens_for_text(value) {
            let position = self.next_position(row, path);
            self.index.add_value_position(row, path, &token, position);
        }
    }

    fn next_position(&mut self, row: u32, path: &str) -> u32 {
        let position = self
            .next_positions
            .entry((row, path.to_owned()))
            .or_default();
        let current = *position;
        *position = position.saturating_add(1);
        current
    }

    fn add_leaf_position_gap(&mut self, row: u32, path: &str) {
        let position = self
            .next_positions
            .entry((row, path.to_owned()))
            .or_default();
        *position = position.saturating_add(1);
    }

    fn finish(self) -> anyhow::Result<SearchIndex> {
        self.index.finish()
    }
}

fn bounded_value(value: &str) -> &str {
    if value.len() <= MAX_INDEXED_VALUE_BYTES {
        return value;
    }

    let mut end = MAX_INDEXED_VALUE_BYTES;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    &value[..end]
}

fn status_from_record(record: &SpanRecord) -> &'static str {
    if record.status_code == 2 {
        "error"
    } else if record.end_time_unix_nano == 0 {
        "pending"
    } else {
        "success"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::otlp::RunEventKind;
    use crate::search::{SearchField, SearchPredicate, SearchQuery};

    #[test]
    fn flattens_arrays_without_row_number_paths() {
        let index = build_search_index(&[record(
            r#"{"messages":[{"content":"hello"},{"content":"world"}]}"#,
        )])
        .expect("build index");

        let predicate = SearchPredicate::Text {
            field: SearchField::ExactPath("messages.content".to_owned()),
            query: SearchQuery::parse("hello world"),
        };
        assert_eq!(index.matching_rows(&predicate), [0].into_iter().collect());
    }

    #[test]
    fn phrase_queries_do_not_cross_collapsed_array_elements() {
        let index = build_search_index(&[record(
            r#"{"messages":[{"content":"hello"},{"content":"world"}]}"#,
        )])
        .expect("build index");

        let phrase = SearchPredicate::Text {
            field: SearchField::ExactPath("messages.content".to_owned()),
            query: SearchQuery::parse("\"hello world\""),
        };
        assert!(index.matching_rows(&phrase).is_empty());
    }

    #[test]
    fn caps_leaf_key_count_per_run() {
        let object = (0..(MAX_JSON_KEYS_PER_RUN + 10))
            .map(|index| format!(r#""k{index:03}":"v{index}""#))
            .collect::<Vec<_>>()
            .join(",");
        let index = build_search_index(&[record(&format!("{{{object}}}"))]).expect("build index");

        assert!(
            index
                .matching_rows(&SearchPredicate::JsonKey {
                    pattern: "k255".to_owned()
                })
                .contains(&0)
        );
        assert!(
            index
                .matching_rows(&SearchPredicate::JsonKey {
                    pattern: "k256".to_owned()
                })
                .is_empty()
        );
    }

    #[test]
    fn streams_json_scalars_empty_containers_and_rejects_invalid_json() {
        let index = build_search_index(&[
            record(r#"{"empty_object":{},"empty_array":[],"number":42,"flag":true,"nil":null}"#),
            record(r#"{"broken":"invoice""#),
        ])
        .expect("build index");

        for path in ["empty_object", "empty_array", "nil"] {
            assert_eq!(
                index.matching_rows(&SearchPredicate::JsonKey {
                    pattern: path.to_owned()
                }),
                [0].into_iter().collect()
            );
        }
        assert_eq!(
            index.matching_rows(&SearchPredicate::Text {
                field: SearchField::ExactPath("number".to_owned()),
                query: SearchQuery::parse("42"),
            }),
            [0].into_iter().collect()
        );
        assert_eq!(
            index.matching_rows(&SearchPredicate::Text {
                field: SearchField::ExactPath("flag".to_owned()),
                query: SearchQuery::parse("true"),
            }),
            [0].into_iter().collect()
        );
        assert!(
            index
                .matching_rows(&SearchPredicate::Text {
                    field: SearchField::All,
                    query: SearchQuery::parse("invoice"),
                })
                .is_empty()
        );
    }

    #[test]
    fn indexes_exact_json_scalar_values_separately_from_tokens() {
        let index = build_search_index(&[
            record(r#"{"inputs":{"prompt":"invoice alpha"}}"#),
            record(r#"{"inputs":{"prompt":"invoice beta"}}"#),
        ])
        .expect("build index");

        assert_eq!(
            index.matching_rows(&SearchPredicate::ExactValue {
                field: SearchField::PathPrefix("inputs".to_owned()),
                value: "invoice alpha".to_owned(),
            }),
            [0].into_iter().collect()
        );
        assert!(
            index
                .matching_rows(&SearchPredicate::ExactValue {
                    field: SearchField::PathPrefix("inputs".to_owned()),
                    value: "invoice".to_owned(),
                })
                .is_empty()
        );
        assert_eq!(
            index.matching_rows(&SearchPredicate::Text {
                field: SearchField::PathPrefix("inputs".to_owned()),
                query: SearchQuery::parse("invoice"),
            }),
            [0, 1].into_iter().collect()
        );
    }

    fn record(attributes_json: &str) -> SpanRecord {
        SpanRecord {
            project_name: "demo".to_owned(),
            run_id: String::new(),
            trace_id: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
            span_id: "1111111111111111".to_owned(),
            parent_run_id: None,
            parent_span_id: None,
            name: "root".to_owned(),
            run_type: "chain".to_owned(),
            start_time_unix_nano: 1,
            end_time_unix_nano: 2,
            status_code: 1,
            event_kind: RunEventKind::End,
            attributes_json: attributes_json.to_owned(),
            idempotency_key: None,
        }
    }
}
