use uuid::Uuid;

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
    pub idempotency_key: Option<String>,
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

pub fn generated_run_id(project_name: &str, trace_id: &str, span_id: &str) -> String {
    Uuid::new_v5(
        &Uuid::NAMESPACE_URL,
        format!("kevindb:run:{project_name}:{trace_id}:{span_id}").as_bytes(),
    )
    .to_string()
}

pub fn generated_project_id(project_name: &str) -> String {
    Uuid::new_v5(
        &Uuid::NAMESPACE_URL,
        format!("kevindb:project:{project_name}").as_bytes(),
    )
    .to_string()
}

pub fn canonicalize_record_ids(record: &mut SpanRecord) {
    if record.run_id.trim().is_empty() {
        record.run_id = generated_run_id(&record.project_name, &record.trace_id, &record.span_id);
    }
    if record.parent_run_id.is_none()
        && let Some(parent_span_id) = record.parent_span_id.as_deref()
    {
        record.parent_run_id = Some(generated_run_id(
            &record.project_name,
            &record.trace_id,
            parent_span_id,
        ));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonicalizes_missing_run_and_parent_ids() {
        let mut record = SpanRecord {
            project_name: "demo".to_owned(),
            run_id: String::new(),
            trace_id: "trace".to_owned(),
            span_id: "child".to_owned(),
            parent_run_id: None,
            parent_span_id: Some("parent".to_owned()),
            name: "child".to_owned(),
            run_type: "chain".to_owned(),
            start_time_unix_nano: 1,
            end_time_unix_nano: 2,
            status_code: 1,
            event_kind: RunEventKind::End,
            attributes_json: "{}".to_owned(),
            idempotency_key: None,
        };

        canonicalize_record_ids(&mut record);

        assert_eq!(record.run_id, generated_run_id("demo", "trace", "child"));
        assert_eq!(
            record.parent_run_id.as_deref(),
            Some(generated_run_id("demo", "trace", "parent").as_str())
        );
    }

    #[test]
    fn generates_stable_project_ids() {
        assert_eq!(generated_project_id("demo"), generated_project_id("demo"));
        assert_ne!(generated_project_id("demo"), generated_project_id("other"));
    }
}
