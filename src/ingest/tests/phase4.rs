use super::*;
use crate::query::{ThreadListQuery, ThreadTraceQuery, filter::FilterExpr};

use serde_json::{Map, Value, json};

#[tokio::test]
async fn thread_metadata_materializes_multi_trace_threads() {
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
            thread_root(ThreadRootArgs {
                trace_id: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                span_id: "1111111111111111",
                start_time_unix_nano: 10,
                thread_key: "thread_id",
                thread_id: "thread-a",
                user_message: "hello one",
                assistant_message: "answer one",
                metrics: Some((10, 5, 15, 0.01)),
            }),
            thread_root(ThreadRootArgs {
                trace_id: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                span_id: "2222222222222222",
                start_time_unix_nano: 100,
                thread_key: "session_id",
                thread_id: "thread-a",
                user_message: "hello two",
                assistant_message: "answer two",
                metrics: Some((12, 6, 18, 0.02)),
            }),
            thread_root(ThreadRootArgs {
                trace_id: "cccccccccccccccccccccccccccccccc",
                span_id: "3333333333333333",
                start_time_unix_nano: 200,
                thread_key: "thread_id",
                thread_id: "thread-b",
                user_message: "broken",
                assistant_message: "bad",
                metrics: None,
            }),
            ordinary_trace("dddddddddddddddddddddddddddddddd", "4444444444444444", 300),
        ])
        .await
        .expect("ingest threaded traces");

    let query_engine = QueryEngine::new(mockgres.postgres_url().to_owned(), object_store);
    let first_page = query_engine
        .list_thread_traces(ThreadTraceQuery {
            project_name: "demo".to_owned(),
            thread_id: "thread-a".to_owned(),
            page_size: 1,
            cursor: None,
            filter: None,
        })
        .await
        .expect("list thread traces");
    assert_eq!(first_page.items.len(), 1);
    assert_eq!(
        first_page.items[0].trace_id,
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    );
    assert_eq!(
        first_page.items[0].inputs_preview.as_deref(),
        Some("hello one")
    );
    assert_eq!(
        first_page.items[0].outputs_preview.as_deref(),
        Some("answer one")
    );
    assert_eq!(first_page.items[0].total_tokens, Some(15));
    assert_eq!(first_page.diagnostics.candidate_segments, 0);
    assert_eq!(first_page.diagnostics.vortex_files_opened, 0);
    assert_eq!(first_page.diagnostics.actual_object_store_requests, 0);

    ingestor
        .ingest_records(vec![thread_root(ThreadRootArgs {
            trace_id: "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
            span_id: "5555555555555555",
            start_time_unix_nano: 5,
            thread_key: "thread_id",
            thread_id: "thread-a",
            user_message: "inserted before cursor",
            assistant_message: "older answer",
            metrics: Some((1, 1, 2, 0.001)),
        })])
        .await
        .expect("ingest earlier trace after first page");

    let second_page = query_engine
        .list_thread_traces(ThreadTraceQuery {
            project_name: "demo".to_owned(),
            thread_id: "thread-a".to_owned(),
            page_size: 10,
            cursor: first_page.next_cursor,
            filter: None,
        })
        .await
        .expect("list second cursor page");
    assert_eq!(
        second_page
            .items
            .iter()
            .map(|trace| trace.trace_id.as_str())
            .collect::<Vec<_>>(),
        vec!["bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"]
    );

    let thread_page = query_engine
        .list_threads(ThreadListQuery {
            project_name: "demo".to_owned(),
            page_size: 10,
            filter: None,
            min_start_time_unix_nano: None,
            max_start_time_unix_nano: None,
            cursor: None,
        })
        .await
        .expect("list threads");
    let thread_a = thread_page
        .items
        .iter()
        .find(|thread| thread.thread_id == "thread-a")
        .expect("thread-a summary");
    assert_eq!(thread_a.count, 3);
    assert_eq!(
        thread_a.first_inputs.as_deref(),
        Some("inserted before cursor")
    );
    assert_eq!(thread_a.last_outputs.as_deref(), Some("answer two"));
    assert_eq!(thread_a.total_tokens, Some(35));
    assert!((thread_a.total_cost.expect("total cost") - 0.031).abs() < 0.000001);
    assert_eq!(thread_page.diagnostics.actual_object_store_requests, 0);

    let messages = query_engine
        .list_thread_messages("demo", "thread-a", 20)
        .await
        .expect("list thread messages");
    assert_eq!(
        messages
            .iter()
            .map(|message| (message.role.as_str(), message.preview.as_str()))
            .collect::<Vec<_>>(),
        vec![
            ("user", "inserted before cursor"),
            ("assistant", "older answer"),
            ("user", "hello one"),
            ("assistant", "answer one"),
            ("user", "hello two"),
            ("assistant", "answer two"),
        ]
    );
    assert!(
        messages
            .iter()
            .all(|message| message.trace_segment_id.is_some())
    );
    assert!(messages.iter().all(|message| message.row_index.is_some()));

    let thread_b = query_engine
        .list_threads(ThreadListQuery {
            project_name: "demo".to_owned(),
            page_size: 10,
            filter: Some(FilterExpr::parse(r#"eq(status, "error")"#).expect("parse filter")),
            min_start_time_unix_nano: None,
            max_start_time_unix_nano: None,
            cursor: None,
        })
        .await
        .expect("filter errored threads");
    assert_eq!(
        thread_b
            .items
            .iter()
            .map(|thread| thread.thread_id.as_str())
            .collect::<Vec<_>>(),
        vec!["thread-b"]
    );

    let (client, connection) = tokio_postgres::connect(mockgres.postgres_url(), NoTls)
        .await
        .expect("connect postgres");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    let ordinary_count: i64 = client
        .query_one(
            "SELECT count(*)
            FROM thread_traces
            WHERE project_name = 'demo' AND trace_id = 'dddddddddddddddddddddddddddddddd'",
            &[],
        )
        .await
        .expect("count ordinary traces")
        .get(0);
    assert_eq!(ordinary_count, 0);

    mockgres.stop().await.expect("stop mockgres");
}

#[tokio::test]
async fn long_thread_overview_uses_bounded_metastore_pages() {
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
    let records = (0..24)
        .map(|index| {
            let trace_id = format!("{:032x}", index + 1);
            let span_id = format!("{:016x}", index + 1);
            let user_message = format!("turn {index} user");
            let assistant_message = format!("turn {index} assistant");
            thread_root(ThreadRootArgs {
                trace_id: &trace_id,
                span_id: &span_id,
                start_time_unix_nano: 1_000 + index,
                thread_key: "thread_id",
                thread_id: "thread-long",
                user_message: &user_message,
                assistant_message: &assistant_message,
                metrics: Some((1, 1, 2, 0.001)),
            })
        })
        .collect::<Vec<_>>();

    ingestor
        .ingest_records(records)
        .await
        .expect("ingest long thread");

    let query_engine = QueryEngine::new(mockgres.postgres_url().to_owned(), object_store);
    let first_page = query_engine
        .list_thread_traces(ThreadTraceQuery {
            project_name: "demo".to_owned(),
            thread_id: "thread-long".to_owned(),
            filter: None,
            page_size: 7,
            cursor: None,
        })
        .await
        .expect("list first long thread page");
    assert_eq!(first_page.items.len(), 7);
    assert!(first_page.next_cursor.is_some());
    assert_eq!(first_page.diagnostics.candidate_segments, 0);
    assert_eq!(first_page.diagnostics.actual_object_store_requests, 0);

    let summary = query_engine
        .list_threads(ThreadListQuery::new("demo"))
        .await
        .expect("list long thread summary")
        .items
        .into_iter()
        .find(|thread| thread.thread_id == "thread-long")
        .expect("thread summary");
    assert_eq!(summary.count, 24);
    assert_eq!(summary.total_tokens, Some(48));

    let messages = query_engine
        .list_thread_messages("demo", "thread-long", 100)
        .await
        .expect("list long thread messages");
    assert_eq!(messages.len(), 48);
    assert_eq!(messages[0].preview, "turn 0 user");
    assert_eq!(messages[47].preview, "turn 23 assistant");

    mockgres.stop().await.expect("stop mockgres");
}

struct ThreadRootArgs<'a> {
    trace_id: &'a str,
    span_id: &'a str,
    start_time_unix_nano: i64,
    thread_key: &'a str,
    thread_id: &'a str,
    user_message: &'a str,
    assistant_message: &'a str,
    metrics: Option<(i64, i64, i64, f64)>,
}

fn thread_root(args: ThreadRootArgs<'_>) -> SpanRecord {
    let mut record = sample_record(args.span_id, args.start_time_unix_nano);
    record.trace_id = args.trace_id.to_owned();
    record.status_code = if args.thread_id == "thread-b" { 2 } else { 1 };
    let mut metadata = Map::new();
    metadata.insert(args.thread_key.to_owned(), json!(args.thread_id));
    record.attributes_json = json!({
        "metadata": Value::Object(metadata),
        "langsmith.inputs": {
            "messages": [{"role": "user", "content": args.user_message}]
        },
        "langsmith.outputs": {
            "choices": [{"message": {"role": "assistant", "content": args.assistant_message}}]
        },
        "metrics": args.metrics.map(|(prompt_tokens, completion_tokens, total_tokens, total_cost)| {
            json!({
                "prompt_tokens": prompt_tokens,
                "completion_tokens": completion_tokens,
                "total_tokens": total_tokens,
                "total_cost": total_cost
            })
        }),
        "first_token_time_unix_nano": args.start_time_unix_nano + 1
    })
    .to_string();
    record
}

fn ordinary_trace(trace_id: &str, span_id: &str, start_time_unix_nano: i64) -> SpanRecord {
    let mut record = sample_record(span_id, start_time_unix_nano);
    record.trace_id = trace_id.to_owned();
    record.attributes_json = json!({
        "langsmith.inputs": {"messages": [{"role": "user", "content": "ordinary"}]},
        "langsmith.outputs": {"content": "ordinary answer"}
    })
    .to_string();
    record
}
