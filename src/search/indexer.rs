use std::fmt;

use serde::de::{DeserializeSeed, Deserializer, IgnoredAny, MapAccess, SeqAccess, Visitor};

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
        if serde_json::from_str::<IgnoredAny>(attributes_json).is_err() {
            return;
        }
        let mut key_count = 0;
        let mut deserializer = serde_json::Deserializer::from_str(attributes_json);
        let _ = JsonSeed {
            builder: self,
            row,
            path: String::new(),
            key_count: &mut key_count,
        }
        .deserialize(&mut deserializer);
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

    fn add_json_path_leaf(&mut self, row: u32, path: &str, key_count: &mut usize) {
        if !path.is_empty() && *key_count < MAX_JSON_KEYS_PER_RUN {
            self.index.add_path(row, path);
            *key_count += 1;
        }
    }

    fn add_json_text_leaf(&mut self, row: u32, path: &str, value: &str, key_count: &mut usize) {
        if !path.is_empty() && *key_count < MAX_JSON_KEYS_PER_RUN {
            self.add_text(row, path, bounded_value(value));
            *key_count += 1;
        }
    }
}

struct JsonSeed<'a> {
    builder: &'a mut SearchIndexBuilder,
    row: u32,
    path: String,
    key_count: &'a mut usize,
}

impl<'de> DeserializeSeed<'de> for JsonSeed<'_> {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(JsonVisitor {
            builder: self.builder,
            row: self.row,
            path: self.path,
            key_count: self.key_count,
        })
    }
}

struct JsonVisitor<'a> {
    builder: &'a mut SearchIndexBuilder,
    row: u32,
    path: String,
    key_count: &'a mut usize,
}

impl<'de> Visitor<'de> for JsonVisitor<'_> {
    type Value = ();

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("any JSON value")
    }

    fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
    where
        M: MapAccess<'de>,
    {
        let JsonVisitor {
            builder,
            row,
            path,
            key_count,
        } = self;
        let mut saw_value = false;
        while let Some(key) = map.next_key::<String>()? {
            saw_value = true;
            let child_path = join_path(&path, &key);
            map.next_value_seed(JsonSeed {
                builder: &mut *builder,
                row,
                path: child_path,
                key_count: &mut *key_count,
            })?;
            if *key_count >= MAX_JSON_KEYS_PER_RUN {
                while map.next_entry::<IgnoredAny, IgnoredAny>()?.is_some() {}
                break;
            }
        }
        if !saw_value {
            builder.add_json_path_leaf(row, &path, key_count);
        }
        Ok(())
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let JsonVisitor {
            builder,
            row,
            path,
            key_count,
        } = self;
        let mut saw_value = false;
        while seq
            .next_element_seed(JsonSeed {
                builder: &mut *builder,
                row,
                path: path.clone(),
                key_count: &mut *key_count,
            })?
            .is_some()
        {
            saw_value = true;
            if *key_count >= MAX_JSON_KEYS_PER_RUN {
                while seq.next_element::<IgnoredAny>()?.is_some() {}
                break;
            }
        }
        if !saw_value {
            builder.add_json_path_leaf(row, &path, key_count);
        }
        Ok(())
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        self.builder
            .add_json_text_leaf(self.row, &self.path, value, self.key_count);
        Ok(())
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        self.visit_str(&value)
    }

    fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        self.builder.add_json_text_leaf(
            self.row,
            &self.path,
            if value { "true" } else { "false" },
            self.key_count,
        );
        Ok(())
    }

    fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        self.visit_number(value.to_string())
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        self.visit_number(value.to_string())
    }

    fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        self.visit_number(value.to_string())
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        self.builder
            .add_json_path_leaf(self.row, &self.path, self.key_count);
        Ok(())
    }

    fn visit_none<E>(self) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        self.visit_unit()
    }
}

impl JsonVisitor<'_> {
    fn visit_number<E>(self, value: String) -> Result<(), E>
    where
        E: serde::de::Error,
    {
        self.builder
            .add_json_text_leaf(self.row, &self.path, &value, self.key_count);
        Ok(())
    }
}

fn join_path(parent: &str, key: &str) -> String {
    if parent.is_empty() {
        key.to_owned()
    } else {
        format!("{parent}.{key}")
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
