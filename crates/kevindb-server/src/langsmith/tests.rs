use super::*;

#[test]
fn normalizes_langsmith_trace_filter_to_otel_hex() {
    assert_eq!(
        normalize_trace_filter(Some("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa".to_owned())),
        Some("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned())
    );
    assert_eq!(
        normalize_trace_filter(Some("not-a-uuid".to_owned())),
        Some("not-a-uuid".to_owned())
    );
}

#[test]
fn builds_langsmith_run_response_fields() {
    let run_id = "33333333-3333-5333-8333-333333333333";
    let parent_run_id = "22222222-2222-5222-8222-222222222222";
    let response = RunResponse::from(RunSummary {
        project_name: "demo".to_owned(),
        run_id: Some(run_id.to_owned()),
        trace_id: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
        span_id: "1111111111111111".to_owned(),
        parent_run_id: Some(parent_run_id.to_owned()),
        parent_span_id: Some("2222222222222222".to_owned()),
        name: "llm.call".to_owned(),
        run_type: "llm".to_owned(),
        status: "error".to_owned(),
        start_time_unix_nano: 1,
        end_time_unix_nano: 2,
        is_root: false,
        attributes_json: json!({
            "langsmith.inputs": {"messages": ["hello"]},
            "langsmith.outputs": {"text": "world"},
            "langsmith.extra": {"metadata": {"key": "value"}},
            "langsmith.error": "boom",
            "langsmith.events": [{"name": "token"}],
            "langsmith.tags": ["demo"],
        })
        .to_string(),
    });

    assert_eq!(response.id, run_id);
    assert!(Uuid::parse_str(&response.session_id).is_ok());
    assert_eq!(response.parent_run_id.as_deref(), Some(parent_run_id));
    assert_eq!(response.start_time, "1970-01-01T00:00:00.000000001Z");
    assert_eq!(
        response.end_time.as_deref(),
        Some("1970-01-01T00:00:00.000000002Z")
    );
    assert_eq!(response.error.as_deref(), Some("boom"));
    assert_eq!(response.inputs, json!({"messages": ["hello"]}));
    assert_eq!(response.outputs, Some(json!({"text": "world"})));
    assert_eq!(response.extra, json!({"metadata": {"key": "value"}}));
    assert_eq!(response.events, vec![json!({"name": "token"})]);
    assert_eq!(response.tags, vec!["demo"]);
}

#[test]
fn merges_partial_langsmith_payload_updates() {
    let payload = LangSmithPayload {
        inputs: Some(json!({"prompt": "hello"})),
        outputs: None,
        extra: Some(json!({"metadata": {"version": 1}})),
        error: None,
        events: vec![json!({"name": "token"})],
        tags: vec!["demo".to_owned()],
    }
    .merge(None, Some(json!({"answer": "world"})), None, None);

    assert_eq!(payload.inputs, Some(json!({"prompt": "hello"})));
    assert_eq!(payload.outputs, Some(json!({"answer": "world"})));
    assert_eq!(payload.extra, Some(json!({"metadata": {"version": 1}})));
    assert_eq!(payload.events, vec![json!({"name": "token"})]);
    assert_eq!(payload.tags, vec!["demo"]);

    let round_trip = LangSmithPayload::from_attributes_json(&payload.to_attributes_json());
    assert_eq!(round_trip, payload);
}

#[test]
fn parses_structured_filters() {
    let filter = parse_filter(
        Some(&json!({
            "operator": "and",
            "children": [
                {"field": "name", "operator": "contains", "value": "llm"},
                {"field": "run_type", "operator": "is one of", "values": ["llm", "tool"]},
                {"field": "error", "operator": "eq", "value": false}
            ]
        })),
        "filter",
    )
    .expect("parse structured filter")
    .expect("filter should exist");

    let compiled = filter
        .compile_run_head_filter("run_heads")
        .expect("compile structured filter");
    assert!(compiled.predicate_sql.contains("run_heads.run_type IN"));
    assert!(
        compiled
            .predicate_sql
            .contains("run_heads.status <> 'error'")
    );
}

#[test]
fn parses_structured_tree_filters() {
    assert!(
        parse_tree_filter(Some(&json!({
            "field": "run_type",
            "operator": "eq",
            "value": "tool"
        })))
        .expect("parse structured tree filter")
        .is_some()
    );
}

#[test]
fn structured_filters_accept_phase6_payload_fields() {
    let filter = parse_filter(
        Some(&json!({"field": "inputs", "operator": "eq", "value": "secret"})),
        "filter",
    )
    .expect("payload filter should parse")
    .expect("filter should exist");
    assert!(matches!(
        filter.compile_run_head_filter("run_heads"),
        Err(kevindb::query::filter::FilterError::Unsupported(message))
            if message.contains("payload JSON")
    ));
}

#[test]
fn structured_filters_parse_json_key_search() {
    let filter = parse_filter(
        Some(&json!({
            "operator": "json_key_search",
            "path": "langsmith.outputs.answer",
            "query": "\"hello world\""
        })),
        "filter",
    )
    .expect("json key search filter should parse")
    .expect("filter should exist");

    assert!(matches!(
        filter.compile_run_head_filter("run_heads"),
        Err(kevindb::query::filter::FilterError::Unsupported(message))
            if message.contains("payload JSON")
    ));
}
