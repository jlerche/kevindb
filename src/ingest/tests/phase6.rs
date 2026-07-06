use super::*;
use crate::query::filter::FilterExpr;
use serde_json::json;

#[tokio::test]
async fn phase6_search_and_json_filters_use_sibling_indexes() {
    let mockgres = Mockgres::start().await.expect("start mockgres");
    run_migrations(mockgres.postgres_url())
        .await
        .expect("run migrations");

    let object_store = Arc::new(InMemory::new());
    let ingestor = Ingestor::new(
        mockgres.postgres_url().to_owned(),
        object_store.clone(),
        IngestConfig {
            max_spans_per_segment: 64,
            max_flush_delay: Duration::ZERO,
        },
    );

    ingestor
        .ingest_records(vec![
            searchable_record(SearchRecordArgs {
                span_id: "1111111111111111",
                start_time_unix_nano: 10,
                prompt: "find invoice alpha",
                answer: "hello brave world",
                thread_id: "thread-a",
            }),
            searchable_record(SearchRecordArgs {
                span_id: "2222222222222222",
                start_time_unix_nano: 20,
                prompt: "plain request",
                answer: "ordinary result",
                thread_id: "thread-b",
            }),
        ])
        .await
        .expect("ingest searchable records");

    let (client, connection) = tokio_postgres::connect(mockgres.postgres_url(), NoTls)
        .await
        .expect("connect postgres");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    let search_index_uri: Option<String> = client
        .query_one("SELECT search_index_uri FROM trace_segments", &[])
        .await
        .expect("load search index uri")
        .get(0);
    let search_index_uri = search_index_uri.expect("search index uri");
    object_store
        .head(&Path::from(search_index_uri.as_str()))
        .await
        .expect("search index object exists");

    let query_engine = QueryEngine::new(mockgres.postgres_url().to_owned(), object_store);

    assert_filter_matches(&query_engine, r#"search("invoice")"#, &["1111111111111111"]).await;
    assert_filter_matches(
        &query_engine,
        r#"json_key("metadata.thread_id")"#,
        &["1111111111111111", "2222222222222222"],
    )
    .await;
    assert_filter_matches(
        &query_engine,
        r#"json_key_search("langsmith.outputs.answer", "\"brave world\"")"#,
        &["1111111111111111"],
    )
    .await;
    assert_filter_matches(
        &query_engine,
        r#"and(eq(run_type, "chain"), contains(inputs, "invoice"))"#,
        &["1111111111111111"],
    )
    .await;
    assert_filter_matches(
        &query_engine,
        r#"does_not_contain(inputs, "invoice")"#,
        &["2222222222222222"],
    )
    .await;

    let mut diagnostics_query = RunQuery::new("demo");
    diagnostics_query.filter = Some(FilterExpr::parse(r#"contains(inputs, "invoice")"#).unwrap());
    diagnostics_query.include_payload = false;
    let result = query_engine
        .list_runs_with_diagnostics(diagnostics_query)
        .await
        .expect("diagnostic search query");
    assert_eq!(result.runs.len(), 1);
    assert_eq!(result.diagnostics.candidate_runs, 1);
    assert_eq!(result.diagnostics.candidate_segments, 1);
    assert!(result.diagnostics.actual_object_store_requests >= 1);
    assert!(result.diagnostics.actual_object_store_bytes_read > 0);

    mockgres.stop().await.expect("stop mockgres");
}

#[tokio::test]
async fn phase6_search_indexes_are_rebuilt_during_compaction() {
    let mockgres = Mockgres::start().await.expect("start mockgres");
    run_migrations(mockgres.postgres_url())
        .await
        .expect("run migrations");

    let object_store = Arc::new(InMemory::new());
    let ingestor = Ingestor::new(
        mockgres.postgres_url().to_owned(),
        object_store.clone(),
        IngestConfig {
            max_spans_per_segment: 1,
            max_flush_delay: Duration::ZERO,
        },
    );

    ingestor
        .ingest_records(vec![
            searchable_record(SearchRecordArgs {
                span_id: "1111111111111111",
                start_time_unix_nano: 10,
                prompt: "find invoice alpha",
                answer: "hello brave world",
                thread_id: "thread-a",
            }),
            searchable_record(SearchRecordArgs {
                span_id: "2222222222222222",
                start_time_unix_nano: 20,
                prompt: "plain request",
                answer: "ordinary result",
                thread_id: "thread-b",
            }),
        ])
        .await
        .expect("ingest compactable search records");

    let compacted = ingestor
        .compact_project("demo")
        .await
        .expect("compact searchable project");
    assert_eq!(compacted.compacted_segments, 2);
    assert_eq!(compacted.written_segments, 2);

    let query_engine = QueryEngine::new(mockgres.postgres_url().to_owned(), object_store);
    assert_filter_matches(&query_engine, r#"search("invoice")"#, &["1111111111111111"]).await;
    assert_filter_matches(
        &query_engine,
        r#"json_key_search(outputs, "answer", "\"brave world\"")"#,
        &["1111111111111111"],
    )
    .await;
    assert_filter_matches(
        &query_engine,
        r#"json_key(inputs, "prompt")"#,
        &["1111111111111111", "2222222222222222"],
    )
    .await;

    mockgres.stop().await.expect("stop mockgres");
}

async fn assert_filter_matches(query_engine: &QueryEngine, filter: &str, span_ids: &[&str]) {
    let mut query = RunQuery::new("demo");
    query.filter = Some(FilterExpr::parse(filter).expect("parse filter"));
    query.include_payload = false;
    let runs = query_engine
        .list_runs(query)
        .await
        .unwrap_or_else(|error| panic!("query {filter} failed: {error:#}"));
    assert_eq!(
        runs.iter()
            .map(|run| run.span_id.as_str())
            .collect::<Vec<_>>(),
        span_ids
    );
}

struct SearchRecordArgs<'a> {
    span_id: &'a str,
    start_time_unix_nano: i64,
    prompt: &'a str,
    answer: &'a str,
    thread_id: &'a str,
}

fn searchable_record(args: SearchRecordArgs<'_>) -> SpanRecord {
    let mut record = sample_record(args.span_id, args.start_time_unix_nano);
    record.attributes_json = json!({
        "langsmith.inputs": {
            "prompt": args.prompt
        },
        "langsmith.outputs": {
            "answer": args.answer
        },
        "metadata": {
            "thread_id": args.thread_id
        }
    })
    .to_string();
    record
}
