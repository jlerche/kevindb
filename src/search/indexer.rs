use serde_json::Value;

use super::{
    MAX_INDEXED_VALUE_BYTES, MAX_JSON_KEYS_PER_RUN, MutableSearchIndex, SearchIndex,
    tokens_for_text,
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
        let Ok(value) = serde_json::from_str::<Value>(attributes_json) else {
            return;
        };
        let mut key_count = 0;
        self.add_json_value(row, "", &value, &mut key_count);
    }

    fn add_json_value(&mut self, row: u32, path: &str, value: &Value, key_count: &mut usize) {
        if *key_count >= MAX_JSON_KEYS_PER_RUN {
            return;
        }

        match value {
            Value::Object(map) => {
                if map.is_empty() && !path.is_empty() {
                    self.index.add_path(row, path);
                    *key_count += 1;
                }
                for (key, child) in map {
                    let child_path = if path.is_empty() {
                        key.clone()
                    } else {
                        format!("{path}.{key}")
                    };
                    self.add_json_value(row, &child_path, child, key_count);
                    if *key_count >= MAX_JSON_KEYS_PER_RUN {
                        break;
                    }
                }
            }
            Value::Array(values) => {
                if values.is_empty() && !path.is_empty() {
                    self.index.add_path(row, path);
                    *key_count += 1;
                }
                for child in values {
                    self.add_json_value(row, path, child, key_count);
                    if *key_count >= MAX_JSON_KEYS_PER_RUN {
                        break;
                    }
                }
            }
            Value::String(value) if !path.is_empty() => {
                self.add_text(row, path, bounded_value(value));
                *key_count += 1;
            }
            Value::Number(value) if !path.is_empty() => {
                self.add_text(row, path, &value.to_string());
                *key_count += 1;
            }
            Value::Bool(value) if !path.is_empty() => {
                self.add_text(row, path, if *value { "true" } else { "false" });
                *key_count += 1;
            }
            Value::Null if !path.is_empty() => {
                self.index.add_path(row, path);
                *key_count += 1;
            }
            _ => {}
        }
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
