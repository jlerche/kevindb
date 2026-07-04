use std::collections::HashMap;
use std::sync::Arc;

use object_store::ObjectStore;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStoreExt, PutPayload};

use super::*;
use crate::otlp::{RunEventKind, SpanRecord};
use crate::query::SegmentSource;
use crate::segment::encode_span_records;

const TRACE_ID: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

#[tokio::test]
async fn aggregates_typed_vortex_columns_without_payload_json() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let segment_uri = "projects/demo/trace-segments/aggregate.vortex";
    let records = vec![
        span_record(TestSpan {
            name: "root",
            run_type: "chain",
            start_time_unix_nano: 10,
            end_time_unix_nano: 30,
            status_code: 1,
            prompt_tokens: 10,
            completion_tokens: 2,
            total_cost: 0.01,
            model: "gpt-a",
        }),
        span_record(TestSpan {
            name: "child",
            run_type: "llm",
            start_time_unix_nano: 40,
            end_time_unix_nano: 90,
            status_code: 2,
            prompt_tokens: 20,
            completion_tokens: 3,
            total_cost: 0.02,
            model: "gpt-b",
        }),
    ];
    let payload = encode_span_records(&records).await.expect("encode segment");
    object_store
        .put(&Path::from(segment_uri), PutPayload::from_bytes(payload))
        .await
        .expect("write segment");

    let query = RunAggregateQuery {
        project_names: vec!["demo".to_owned()],
        group_by: vec![RunAggregateGroup::RunType],
        ..RunAggregateQuery::new("demo")
    };
    let rows = scan::load_aggregate_rows_with_datafusion(
        object_store,
        vec![SegmentSource {
            uri: segment_uri.to_owned(),
            total_bytes: 1,
            schema_version: crate::segment::SPAN_SEGMENT_SCHEMA_VERSION,
            candidate_rows: Vec::new(),
        }],
        &query.to_run_query(),
        None,
    )
    .await
    .expect("load aggregate rows")
    .0;
    let aggregate_rows = aggregate_rows(rows, &query, &HashMap::new(), &HashMap::new());

    let llm = aggregate_rows
        .iter()
        .find(|row| {
            row.group
                .get("run_type")
                .is_some_and(|value| value == "llm")
        })
        .expect("llm aggregate");
    assert_eq!(llm.metrics.count, 1);
    assert_eq!(llm.metrics.error_count, 1);
    assert_eq!(
        llm.metrics
            .total_tokens
            .as_ref()
            .and_then(|stats| stats.sum),
        Some(23.0)
    );
    assert_eq!(
        llm.metrics.total_cost.as_ref().and_then(|stats| stats.sum),
        Some(0.02)
    );
}

struct TestSpan<'a> {
    name: &'a str,
    run_type: &'a str,
    start_time_unix_nano: i64,
    end_time_unix_nano: i64,
    status_code: i32,
    prompt_tokens: i64,
    completion_tokens: i64,
    total_cost: f64,
    model: &'a str,
}

fn span_record(spec: TestSpan<'_>) -> SpanRecord {
    SpanRecord {
        project_name: "demo".to_owned(),
        run_id: format!("run-{}", spec.name),
        trace_id: TRACE_ID.to_owned(),
        span_id: spec.name.to_owned(),
        parent_run_id: None,
        parent_span_id: None,
        name: spec.name.to_owned(),
        run_type: spec.run_type.to_owned(),
        start_time_unix_nano: spec.start_time_unix_nano,
        end_time_unix_nano: spec.end_time_unix_nano,
        status_code: spec.status_code,
        event_kind: RunEventKind::End,
        attributes_json: serde_json::json!({
            "metrics": {
                "prompt_tokens": spec.prompt_tokens,
                "completion_tokens": spec.completion_tokens,
                "total_cost": spec.total_cost
            },
            "model": spec.model
        })
        .to_string(),
        idempotency_key: None,
    }
}
