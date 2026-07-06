use serde_json::{Value, json};

#[derive(Debug, Clone, Default, PartialEq)]
pub(super) struct LangSmithPayload {
    pub(super) inputs: Option<Value>,
    pub(super) outputs: Option<Value>,
    pub(super) extra: Option<Value>,
    pub(super) error: Option<String>,
    pub(super) events: Vec<Value>,
    pub(super) tags: Vec<String>,
}

impl LangSmithPayload {
    pub(super) fn from_attributes_json(attributes_json: &str) -> Self {
        let Ok(Value::Object(attributes)) = serde_json::from_str(attributes_json) else {
            return Self::default();
        };

        Self {
            inputs: attributes
                .get("langsmith.inputs")
                .filter(|value| !value.is_null())
                .cloned(),
            outputs: attributes
                .get("langsmith.outputs")
                .filter(|value| !value.is_null())
                .cloned(),
            extra: attributes
                .get("langsmith.extra")
                .filter(|value| !value.is_null())
                .cloned(),
            error: attributes
                .get("langsmith.error")
                .and_then(Value::as_str)
                .map(str::to_owned),
            events: array_values(&attributes, &["langsmith.events", "events"]),
            tags: string_values(&attributes, &["langsmith.tags", "tags"]),
        }
    }

    pub(super) fn merge(
        self,
        inputs: Option<Value>,
        outputs: Option<Value>,
        extra: Option<Value>,
        error: Option<String>,
        events: Option<Vec<Value>>,
        tags: Option<Vec<String>>,
    ) -> Self {
        Self {
            inputs: inputs.or(self.inputs),
            outputs: outputs.or(self.outputs),
            extra: extra.or(self.extra),
            error: error.or(self.error),
            events: events.unwrap_or(self.events),
            tags: tags.unwrap_or(self.tags),
        }
    }

    pub(super) fn to_attributes_json(&self) -> String {
        json!({
            "langsmith.inputs": self.inputs.clone().unwrap_or_else(|| json!({})),
            "langsmith.outputs": self.outputs,
            "langsmith.extra": self.extra.clone().unwrap_or_else(|| json!({})),
            "langsmith.error": self.error,
            "langsmith.events": self.events,
            "langsmith.tags": self.tags,
        })
        .to_string()
    }
}

fn array_values(attributes: &serde_json::Map<String, Value>, keys: &[&str]) -> Vec<Value> {
    keys.iter()
        .find_map(|key| attributes.get(*key).and_then(Value::as_array))
        .cloned()
        .unwrap_or_default()
}

fn string_values(attributes: &serde_json::Map<String, Value>, keys: &[&str]) -> Vec<String> {
    keys.iter()
        .find_map(|key| attributes.get(*key).and_then(Value::as_array))
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::to_owned)
        .collect()
}
