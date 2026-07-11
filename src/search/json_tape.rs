use std::fmt;
use std::ops::Range;

use serde::de::{DeserializeSeed, Deserializer, MapAccess, SeqAccess, Visitor};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct JsonTape {
    bytes: Vec<u8>,
    leaves: Vec<JsonLeaf>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct JsonLeaf {
    path: Range<usize>,
    value: Option<Range<usize>>,
}

impl JsonTape {
    pub(super) fn parse(input: &str) -> serde_json::Result<Self> {
        let mut tape = JsonTape {
            bytes: Vec::new(),
            leaves: Vec::new(),
        };
        let mut deserializer = serde_json::Deserializer::from_str(input);
        TapeSeed {
            tape: &mut tape,
            path: String::new(),
        }
        .deserialize(&mut deserializer)?;
        deserializer.end()?;
        Ok(tape)
    }

    pub(super) fn leaves(&self) -> impl Iterator<Item = JsonLeafRef<'_>> {
        self.leaves.iter().map(|leaf| JsonLeafRef {
            path: std::str::from_utf8(&self.bytes[leaf.path.clone()])
                .expect("JSON tape paths are copied from UTF-8 strings"),
            value: leaf.value.clone().map(|range| {
                std::str::from_utf8(&self.bytes[range])
                    .expect("JSON tape values are copied from UTF-8 strings")
            }),
        })
    }

    fn push_leaf(&mut self, path: &str, value: Option<&str>) {
        if path.is_empty() {
            return;
        }
        let path = self.push_bytes(path);
        let value = value.map(|value| self.push_bytes(value));
        self.leaves.push(JsonLeaf { path, value });
    }

    fn push_bytes(&mut self, value: &str) -> Range<usize> {
        let start = self.bytes.len();
        self.bytes.extend_from_slice(value.as_bytes());
        start..self.bytes.len()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct JsonLeafRef<'a> {
    pub(super) path: &'a str,
    pub(super) value: Option<&'a str>,
}

struct TapeSeed<'a> {
    tape: &'a mut JsonTape,
    path: String,
}

impl<'de> DeserializeSeed<'de> for TapeSeed<'_> {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(TapeVisitor {
            tape: self.tape,
            path: self.path,
        })
    }
}

struct TapeVisitor<'a> {
    tape: &'a mut JsonTape,
    path: String,
}

impl<'de> Visitor<'de> for TapeVisitor<'_> {
    type Value = ();

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("any JSON value")
    }

    fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
    where
        M: MapAccess<'de>,
    {
        let TapeVisitor { tape, path } = self;
        let mut saw_value = false;
        while let Some(key) = map.next_key::<String>()? {
            saw_value = true;
            map.next_value_seed(TapeSeed {
                tape: &mut *tape,
                path: join_path(&path, &key),
            })?;
        }
        if !saw_value {
            tape.push_leaf(&path, None);
        }
        Ok(())
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let TapeVisitor { tape, path } = self;
        let mut saw_value = false;
        while seq
            .next_element_seed(TapeSeed {
                tape: &mut *tape,
                path: path.clone(),
            })?
            .is_some()
        {
            saw_value = true;
        }
        if !saw_value {
            tape.push_leaf(&path, None);
        }
        Ok(())
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        self.tape.push_leaf(&self.path, Some(value));
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
        self.tape
            .push_leaf(&self.path, Some(if value { "true" } else { "false" }));
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
        self.tape.push_leaf(&self.path, None);
        Ok(())
    }

    fn visit_none<E>(self) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        self.visit_unit()
    }
}

impl TapeVisitor<'_> {
    fn visit_number<E>(self, value: String) -> Result<(), E>
    where
        E: serde::de::Error,
    {
        self.tape.push_leaf(&self.path, Some(&value));
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flattens_json_into_tape_leaves() {
        let tape = JsonTape::parse(
            r#"{"agent":"deep agents","tags":["langchain","engine"],"empty":{},"nil":null}"#,
        )
        .expect("parse tape");
        let leaves = tape
            .leaves()
            .map(|leaf| (leaf.path, leaf.value))
            .collect::<Vec<_>>();

        assert_eq!(
            leaves,
            vec![
                ("agent", Some("deep agents")),
                ("tags", Some("langchain")),
                ("tags", Some("engine")),
                ("empty", None),
                ("nil", None),
            ]
        );
    }
}
