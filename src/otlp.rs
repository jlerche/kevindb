use std::collections::BTreeMap;

use anyhow::{Context, Result, bail};
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
use opentelemetry_proto::tonic::trace::v1::Span;
use serde_json::{Map, Value, json};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpanRecord {
    pub project_name: String,
    pub run_id: String,
    pub trace_id: String,
    pub span_id: String,
    pub parent_run_id: Option<String>,
    pub parent_span_id: Option<String>,
    pub name: String,
    pub run_type: String,
    pub start_time_unix_nano: i64,
    pub end_time_unix_nano: i64,
    pub status_code: i32,
    pub event_kind: RunEventKind,
    pub attributes_json: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunEventKind {
    Start,
    Update,
    End,
    Compact,
    Tombstone,
}

impl RunEventKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Start => "start",
            Self::Update => "update",
            Self::End => "end",
            Self::Compact => "compact",
            Self::Tombstone => "tombstone",
        }
    }
}

pub fn span_records_from_export(
    project_name: impl Into<String>,
    request: ExportTraceServiceRequest,
) -> Result<Vec<SpanRecord>> {
    let project_name = project_name.into();
    let mut records = Vec::new();

    for resource_spans in request.resource_spans {
        let mut resource_attributes = BTreeMap::new();
        if let Some(resource) = resource_spans.resource {
            resource_attributes.extend(attributes_to_json_map(resource.attributes));
        }

        for scope_spans in resource_spans.scope_spans {
            for span in scope_spans.spans {
                records.push(span_record_from_span(
                    project_name.clone(),
                    &resource_attributes,
                    span,
                )?);
            }
        }
    }

    Ok(records)
}

fn span_record_from_span(
    project_name: String,
    resource_attributes: &BTreeMap<String, Value>,
    span: Span,
) -> Result<SpanRecord> {
    let trace_id = hex_id(&span.trace_id, 16, "trace_id")?;
    let span_id = hex_id(&span.span_id, 8, "span_id")?;
    let parent_span_id = if span.parent_span_id.is_empty() {
        None
    } else {
        Some(hex_id(&span.parent_span_id, 8, "parent_span_id")?)
    };

    let start_time_unix_nano = i64::try_from(span.start_time_unix_nano)
        .context("span start_time_unix_nano does not fit in postgres BIGINT")?;
    let end_time_unix_nano = i64::try_from(span.end_time_unix_nano)
        .context("span end_time_unix_nano does not fit in postgres BIGINT")?;
    let status_code = span.status.as_ref().map(|status| status.code).unwrap_or(0);
    let event_kind = if end_time_unix_nano > 0 || status_code == 2 {
        RunEventKind::End
    } else {
        RunEventKind::Start
    };

    let span_attributes = attributes_to_json_map(span.attributes);
    let run_type = infer_run_type(&span.name, &span_attributes);

    let mut attributes = Map::new();
    for (key, value) in resource_attributes {
        attributes.insert(format!("resource.{key}"), value.clone());
    }
    for (key, value) in span_attributes {
        attributes.insert(key, value);
    }

    Ok(SpanRecord {
        project_name,
        run_id: String::new(),
        trace_id,
        span_id,
        parent_run_id: None,
        parent_span_id,
        name: span.name,
        run_type,
        start_time_unix_nano,
        end_time_unix_nano,
        status_code,
        event_kind,
        attributes_json: Value::Object(attributes).to_string(),
    })
}

fn infer_run_type(name: &str, attributes: &BTreeMap<String, Value>) -> String {
    for key in ["langsmith.run_type", "kevindb.run_type", "run_type"] {
        if let Some(Value::String(value)) = attributes.get(key) {
            return normalize_run_type(value);
        }
    }

    if let Some(Value::String(value)) = attributes.get("gen_ai.operation.name") {
        return match value.as_str() {
            "chat" | "completion" => "llm".to_owned(),
            "embeddings" => "embedding".to_owned(),
            _ => "span".to_owned(),
        };
    }

    let lower_name = name.to_ascii_lowercase();
    if lower_name.contains("llm") || lower_name.contains("chat") {
        "llm".to_owned()
    } else if lower_name.contains("tool") {
        "tool".to_owned()
    } else if lower_name.contains("retriev") {
        "retriever".to_owned()
    } else {
        "span".to_owned()
    }
}

fn normalize_run_type(value: &str) -> String {
    match value.to_ascii_lowercase().as_str() {
        "chain" | "llm" | "tool" | "retriever" | "embedding" | "prompt" | "parser" => {
            value.to_ascii_lowercase()
        }
        _ => "span".to_owned(),
    }
}

fn attributes_to_json_map(attributes: Vec<KeyValue>) -> BTreeMap<String, Value> {
    attributes
        .into_iter()
        .map(|attribute| (attribute.key, any_value_to_json(attribute.value)))
        .collect()
}

fn any_value_to_json(value: Option<AnyValue>) -> Value {
    match value.and_then(|value| value.value) {
        Some(any_value::Value::StringValue(value)) => Value::String(value),
        Some(any_value::Value::BoolValue(value)) => Value::Bool(value),
        Some(any_value::Value::IntValue(value)) => json!(value),
        Some(any_value::Value::DoubleValue(value)) => json!(value),
        Some(any_value::Value::BytesValue(value)) => json!(hex::encode(value)),
        Some(any_value::Value::StringValueStrindex(value)) => json!(value),
        Some(any_value::Value::ArrayValue(value)) => Value::Array(
            value
                .values
                .into_iter()
                .map(|v| any_value_to_json(Some(v)))
                .collect(),
        ),
        Some(any_value::Value::KvlistValue(value)) => {
            let object = value
                .values
                .into_iter()
                .map(|kv| (kv.key, any_value_to_json(kv.value)))
                .collect();
            Value::Object(object)
        }
        None => Value::Null,
    }
}

fn hex_id(bytes: &[u8], expected_len: usize, field: &str) -> Result<String> {
    if bytes.len() != expected_len {
        bail!("{field} must be {expected_len} bytes, got {}", bytes.len());
    }
    if bytes.iter().all(|byte| *byte == 0) {
        bail!("{field} must not be all zeroes");
    }
    Ok(hex::encode(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::resource::v1::Resource;
    use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Status, status};

    #[test]
    fn converts_otlp_export_to_span_records() {
        let request = ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: Some(Resource {
                    attributes: vec![string_attr("service.name", "agent-api")],
                    dropped_attributes_count: 0,
                    entity_refs: vec![],
                }),
                scope_spans: vec![ScopeSpans {
                    scope: None,
                    spans: vec![Span {
                        trace_id: repeated_bytes(0xAA, 16),
                        span_id: repeated_bytes(0x11, 8),
                        parent_span_id: repeated_bytes(0x22, 8),
                        name: "llm.call".to_owned(),
                        start_time_unix_nano: 1,
                        end_time_unix_nano: 2,
                        attributes: vec![
                            string_attr("gen_ai.request.model", "gpt-test"),
                            KeyValue {
                                key: "cache.hit".to_owned(),
                                key_strindex: 0,
                                value: Some(AnyValue {
                                    value: Some(any_value::Value::BoolValue(true)),
                                }),
                            },
                        ],
                        status: Some(Status {
                            message: String::new(),
                            code: status::StatusCode::Ok as i32,
                        }),
                        ..Default::default()
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };

        let records = span_records_from_export("demo", request).expect("convert export");
        assert_eq!(records.len(), 1);
        let record = &records[0];
        assert_eq!(record.project_name, "demo");
        assert_eq!(record.trace_id, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        assert_eq!(record.span_id, "1111111111111111");
        assert_eq!(record.parent_span_id.as_deref(), Some("2222222222222222"));
        assert_eq!(record.name, "llm.call");
        assert_eq!(record.run_type, "llm");
        assert_eq!(record.start_time_unix_nano, 1);
        assert_eq!(record.end_time_unix_nano, 2);
        assert_eq!(record.status_code, status::StatusCode::Ok as i32);
        assert_eq!(record.event_kind, RunEventKind::End);
        assert!(record.attributes_json.contains("resource.service.name"));
        assert!(record.attributes_json.contains("gen_ai.request.model"));
    }

    #[test]
    fn prefers_explicit_run_type_attribute() {
        let request = ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: None,
                scope_spans: vec![ScopeSpans {
                    scope: None,
                    spans: vec![Span {
                        trace_id: repeated_bytes(0xAA, 16),
                        span_id: repeated_bytes(0x11, 8),
                        name: "custom".to_owned(),
                        attributes: vec![string_attr("langsmith.run_type", "tool")],
                        ..Default::default()
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };

        let records = span_records_from_export("demo", request).expect("convert export");
        assert_eq!(records[0].run_type, "tool");
    }

    #[test]
    fn rejects_invalid_trace_ids() {
        let request = ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: None,
                scope_spans: vec![ScopeSpans {
                    scope: None,
                    spans: vec![Span {
                        trace_id: vec![0; 16],
                        span_id: repeated_bytes(0x11, 8),
                        name: "bad".to_owned(),
                        ..Default::default()
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };

        let error = span_records_from_export("demo", request).expect_err("invalid trace id");
        assert!(
            error
                .to_string()
                .contains("trace_id must not be all zeroes")
        );
    }

    fn string_attr(key: &str, value: &str) -> KeyValue {
        KeyValue {
            key: key.to_owned(),
            key_strindex: 0,
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(value.to_owned())),
            }),
        }
    }

    fn repeated_bytes(byte: u8, len: usize) -> Vec<u8> {
        vec![byte; len]
    }
}
