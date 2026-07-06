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
        r#"and(eq(model, "gpt-test"), gte(total_tokens, 100))"#,
        r#"and(eq(provider, "openai"), lt(total_cost, 0.01))"#,
        r#"eq(error, true)"#,
        r#"in(root_run_id, ["root-a","root-b"])"#,
    ];

    for example in examples {
        let compiled = FilterExpr::parse(example)
            .and_then(|expr| expr.compile_run_head_filter("run_heads"))
            .expect(example);
        assert!(!compiled.predicate_sql.is_empty());
    }
}

#[test]
fn compiles_phase2_materialized_scalar_fields() {
    let compiled = FilterExpr::parse(
        r#"and(eq(model_name, "gpt-test"), eq(provider_name, "openai"), gte(total_tokens, 42), lt(total_cost, 0.01), eq(error, false))"#,
    )
    .and_then(|expr| expr.compile_run_head_filter("run_heads"))
    .expect("compile scalar fields");

    assert!(
        compiled
            .predicate_sql
            .contains("run_heads.model_name = 'gpt-test'")
    );
    assert!(
        compiled
            .predicate_sql
            .contains("run_heads.provider_name = 'openai'")
    );
    assert!(
        compiled
            .predicate_sql
            .contains("run_heads.total_tokens >= 42")
    );
    assert!(
        compiled
            .predicate_sql
            .contains("run_heads.total_cost < 0.01")
    );
    assert!(
        compiled
            .predicate_sql
            .contains("run_heads.status <> 'error'")
    );
}

#[test]
fn rejects_full_text_and_payload_filters() {
    assert!(matches!(
        FilterExpr::parse(r#"search("invoice")"#)
            .and_then(|expr| expr.compile_run_head_filter("run_heads")),
        Err(FilterError::Unsupported(message)) if message.contains("full-text search")
    ));
    assert!(matches!(
        FilterExpr::parse(r#"eq(inputs, "value")"#)
            .and_then(|expr| expr.compile_run_head_filter("run_heads")),
        Err(FilterError::Unsupported(message)) if message.contains("payload JSON")
    ));
    assert!(matches!(
        FilterExpr::parse(r#"gt(id, "00000000-0000-0000-0000-000000000000")"#)
            .and_then(|expr| expr.compile_run_head_filter("run_heads")),
        Err(FilterError::Unsupported(message)) if message.contains("id only supports")
    ));
}

#[test]
fn extracts_phase6_search_predicates_from_payload_filters() {
    let expr = FilterExpr::parse(
        r#"and(eq(run_type, "llm"), json_key("metadata.*"), json_key_search("langsmith.outputs.answer", "\"hello world\""), contains(inputs, "invoice"))"#,
    )
    .expect("parse phase 6 filter");

    let prefilter = expr
        .compile_run_head_prefilter_for_projects("run_heads", &["demo".to_owned()])
        .expect("compile scalar prefilter")
        .expect("scalar prefilter");
    assert_eq!(prefilter.predicate_sql, "run_heads.run_type = 'llm'");

    let predicate = expr
        .phase6_search_predicate()
        .expect("extract search predicate")
        .expect("search predicate");
    assert!(matches!(predicate, crate::search::SearchPredicate::And(_)));
}

#[test]
fn extracts_blog_scoped_phase6_search_predicates() {
    let expr = FilterExpr::parse(
        r#"and(json_key(inputs, "author.%"), json_key_search(outputs, "answer", "\"hello world\""), search(inputs, "invoice"), search(status, "error"))"#,
    )
    .expect("parse scoped phase 6 filter");

    let predicate = expr
        .phase6_search_predicate()
        .expect("extract scoped search predicate")
        .expect("scoped search predicate");

    assert!(matches!(predicate, crate::search::SearchPredicate::And(_)));
}

#[test]
fn rejects_json_key_on_non_payload_fields() {
    let err = FilterExpr::parse(r#"json_key(status, "metadata.thread_id")"#)
        .and_then(|expr| expr.phase6_search_predicate().map(|_| expr))
        .expect_err("status is not a payload JSON field");

    assert!(matches!(
        err,
        FilterError::Unsupported(message) if message.contains("search index")
    ));
}

#[test]
fn rejects_mixed_scalar_and_search_or_filters() {
    let err = FilterExpr::parse(r#"or(eq(run_type, "llm"), search("invoice"))"#)
        .and_then(|expr| expr.phase6_search_predicate().map(|_| expr))
        .expect_err("mixed or should reject");

    assert!(matches!(
        err,
        FilterError::Unsupported(message) if message.contains("ORed")
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

#[test]
fn negative_key_filters_compile_as_anti_exists_predicates() {
    let compiled = FilterExpr::parse(
        r#"and(neq(metadata_key, "phone"), eq(metadata_value, "1234567890"), neq(feedback_key, "bad_score"), neq(tags, "staging"))"#,
    )
    .and_then(|expr| expr.compile_run_head_filter("run_heads"))
    .expect("compile negative indexed filters");

    assert!(
        compiled
            .predicate_sql
            .contains("NOT (run_heads.project_name")
    );
    assert!(compiled.predicate_sql.contains("key = 'phone'"));
    assert!(compiled.predicate_sql.contains("key = 'bad_score'"));
    assert!(
        compiled
            .predicate_sql
            .contains("tag_filter.tag = 'staging'")
    );
    assert!(!compiled.predicate_sql.contains("key <> 'phone'"));
    assert!(!compiled.predicate_sql.contains("key <> 'bad_score'"));
}

#[test]
fn planner_scoped_filters_bound_index_subqueries_by_project() {
    let projects = vec!["demo".to_owned(), "other".to_owned()];
    let compiled =
        FilterExpr::parse(r#"and(has(tags, "production"), eq(metadata_key, "thread_id"))"#)
            .and_then(|expr| expr.compile_run_head_filter_for_projects("run_heads", &projects))
            .expect("compile project-scoped filter");

    assert!(
        compiled
            .predicate_sql
            .contains("tag_filter.project_name IN ('demo', 'other')")
    );
    assert!(
        compiled
            .predicate_sql
            .contains("metadata_filter.project_name IN ('demo', 'other')")
    );
}

#[test]
fn anchored_negative_values_stay_on_the_same_index_row() {
    let compiled = FilterExpr::parse(
        r#"and(eq(metadata_key, "phone"), neq(metadata_value, "1234567890"), eq(feedback_key, "quality"), neq(feedback_score, 0))"#,
    )
    .and_then(|expr| expr.compile_run_head_filter("run_heads"))
    .expect("compile anchored negative values");

    assert!(compiled.predicate_sql.contains("key = 'phone'"));
    assert!(compiled.predicate_sql.contains("value <> '1234567890'"));
    assert!(compiled.predicate_sql.contains("key = 'quality'"));
    assert!(compiled.predicate_sql.contains("score_number <> 0"));
}

#[test]
fn contains_escapes_like_wildcards() {
    let compiled = FilterExpr::parse(r#"contains(name, "100%_ok\\done")"#)
        .and_then(|expr| expr.compile_run_head_filter("run_heads"))
        .expect("compile contains wildcard filter");

    assert!(compiled.predicate_sql.contains("LIKE '%100\\%\\_ok"));
    assert!(compiled.predicate_sql.contains("\\\\done%' ESCAPE '\\'"));
}
