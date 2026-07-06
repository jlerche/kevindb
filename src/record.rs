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
