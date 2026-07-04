use super::*;

#[test]
fn parses_and_compiles_documented_examples() {
    let examples = [
        r#"eq(name, "my_chain")"#,
        r#"neq(status, "error")"#,
        r#"gt(latency, "5s")"#,
        r#"and(gt(start_time, "2024-01-01T00:00:00Z"), lt(start_time, "2024-02-01T00:00:00Z"))"#,
        r#"has(tags, "production")"#,
        r#"and(eq(metadata_key, "thread_id"), eq(metadata_value, "abc"))"#,
        r#"and(eq(feedback_key, "correctness"), lt(feedback_score, 0.5))"#,
    ];

    for example in examples {
        let compiled = FilterExpr::parse(example)
            .and_then(|expr| expr.compile_run_head_filter("run_heads"))
            .expect(example);
        assert!(!compiled.predicate_sql.is_empty());
    }
}

#[test]
fn rejects_full_text_and_payload_filters() {
    assert!(matches!(
        FilterExpr::parse(r#"search("invoice")"#)
            .and_then(|expr| expr.compile_run_head_filter("run_heads")),
        Err(FilterError::Unsupported(message)) if message.contains("full-text search")
    ));
    assert!(matches!(
        FilterExpr::parse(r#"eq(inputs, "value")"#),
        Err(FilterError::Unsupported(message)) if message.contains("payload JSON")
    ));
}

#[test]
fn pairs_metadata_key_and_value_on_one_index_row() {
    let compiled = FilterExpr::parse(
        r#"and(in(metadata_key, ["session_id","thread_id"]), eq(metadata_value, "abc"))"#,
    )
    .and_then(|expr| expr.compile_run_head_filter("run_heads"))
    .expect("compile metadata pair");

    assert!(compiled.predicate_sql.contains("run_metadata"));
    assert!(
        compiled
            .predicate_sql
            .contains("key IN ('session_id', 'thread_id')")
    );
    assert!(compiled.predicate_sql.contains("value = 'abc'"));
}
