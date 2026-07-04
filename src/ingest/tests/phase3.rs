use super::*;
use crate::query::{TreeFilterExpr, filter::FilterExpr};

#[tokio::test]
async fn tree_metadata_repairs_late_parents_and_indexes_nested_sets() {
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

    let mut child = tree_record(
        "2222222222222222",
        "llm.call",
        "llm",
        Some("1111111111111111"),
        20,
    );
    child.parent_run_id = Some("1111111111111111".to_owned());
    ingestor
        .ingest_records(vec![child])
        .await
        .expect("ingest child before parent");
    let orphan = load_tree_node(mockgres.postgres_url(), "2222222222222222").await;
    assert_eq!(orphan.parent_span_id, None);
    assert_eq!(orphan.root_span_id, "2222222222222222");
    assert!(orphan.unresolved_parent);

    let root = tree_record("1111111111111111", "agent.root", "chain", None, 10);
    let grandchild = tree_record(
        "3333333333333333",
        "tool.call",
        "tool",
        Some("2222222222222222"),
        30,
    );
    let second_root = tree_record("4444444444444444", "agent.other", "chain", None, 40);
    ingestor
        .ingest_records(vec![root, grandchild, second_root])
        .await
        .expect("ingest parent and second root");

    let root = load_tree_node(mockgres.postgres_url(), "1111111111111111").await;
    let child = load_tree_node(mockgres.postgres_url(), "2222222222222222").await;
    let grandchild = load_tree_node(mockgres.postgres_url(), "3333333333333333").await;
    let root_count = count_tree_roots(mockgres.postgres_url()).await;

    assert_eq!(root_count, 2);
    assert_eq!(root.depth, 0);
    assert_eq!(root.descendant_count, 2);
    assert_eq!(child.parent_span_id.as_deref(), Some("1111111111111111"));
    assert_eq!(child.root_span_id, "1111111111111111");
    assert_eq!(child.depth, 1);
    assert!(!child.unresolved_parent);
    assert_eq!(grandchild.depth, 2);
    assert!(root.subtree_start < child.subtree_start);
    assert!(child.subtree_start < grandchild.subtree_start);
    assert!(root.subtree_end > grandchild.subtree_end);

    let trace_tree = QueryEngine::new(mockgres.postgres_url().to_owned(), object_store.clone())
        .load_trace_tree_with_diagnostics("demo", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
        .await
        .expect("load trace tree from metadata");
    assert_eq!(trace_tree.trace_tree.roots.len(), 2);
    assert_eq!(trace_tree.trace_tree.roots[0].run.attributes_json, "{}");
    assert_eq!(trace_tree.diagnostics.candidate_segments, 0);
    assert_eq!(trace_tree.diagnostics.vortex_files_opened, 0);
    assert_eq!(trace_tree.diagnostics.actual_object_store_requests, 0);
    assert_eq!(trace_tree.diagnostics.actual_object_store_bytes_read, 0);

    let mut query = RunQuery::new("demo");
    query.filter = Some(FilterExpr::parse(r#"eq(name, "agent.root")"#).expect("parse filter"));
    query.tree_filter =
        Some(TreeFilterExpr::parse(r#"eq(name, "tool.call")"#).expect("parse tree filter"));
    query.include_payload = false;
    let result = QueryEngine::new(mockgres.postgres_url().to_owned(), object_store.clone())
        .list_runs_with_diagnostics(query)
        .await
        .expect("query trace contains tree filter");
    assert_eq!(result.runs.len(), 1);
    assert_eq!(result.runs[0].span_id, "1111111111111111");
    assert_eq!(result.diagnostics.candidate_runs, 1);

    let mut descendant_query = RunQuery::new("demo");
    descendant_query.is_root = Some(true);
    descendant_query.tree_filter = Some(
        TreeFilterExpr::parse(r#"descendant(eq(run_type, "tool"))"#)
            .expect("parse descendant tree filter"),
    );
    descendant_query.include_payload = false;
    let descendant = QueryEngine::new(mockgres.postgres_url().to_owned(), object_store)
        .list_runs_with_diagnostics(descendant_query)
        .await
        .expect("query descendant tree filter");
    assert_eq!(
        descendant
            .runs
            .iter()
            .map(|run| run.span_id.as_str())
            .collect::<Vec<_>>(),
        vec!["1111111111111111"]
    );

    mockgres.stop().await.expect("stop mockgres");
}

#[tokio::test]
async fn batched_child_before_parent_refreshes_tree_once_after_heads_are_written() {
    let mockgres = Mockgres::start().await.expect("start mockgres");
    run_migrations(mockgres.postgres_url())
        .await
        .expect("run migrations");

    let ingestor = Ingestor::new(
        mockgres.postgres_url().to_owned(),
        Arc::new(InMemory::new()),
        IngestConfig {
            max_spans_per_segment: 64,
            max_flush_delay: Duration::ZERO,
        },
    );

    let child = tree_record(
        "2222222222222222",
        "llm.call",
        "llm",
        Some("1111111111111111"),
        20,
    );
    let root = tree_record("1111111111111111", "agent.root", "chain", None, 10);
    ingestor
        .ingest_records(vec![child, root])
        .await
        .expect("ingest child and parent in one segment");

    let root = load_tree_node(mockgres.postgres_url(), "1111111111111111").await;
    let child = load_tree_node(mockgres.postgres_url(), "2222222222222222").await;
    assert_eq!(root.descendant_count, 1);
    assert_eq!(child.parent_span_id.as_deref(), Some("1111111111111111"));
    assert_eq!(child.root_span_id, "1111111111111111");
    assert!(!child.unresolved_parent);

    mockgres.stop().await.expect("stop mockgres");
}

#[tokio::test]
async fn cyclic_parent_edges_are_guarded_in_tree_metadata() {
    let mockgres = Mockgres::start().await.expect("start mockgres");
    run_migrations(mockgres.postgres_url())
        .await
        .expect("run migrations");

    let object_store = Arc::new(InMemory::new());
    let ingestor = Ingestor::new(
        mockgres.postgres_url().to_owned(),
        object_store,
        IngestConfig {
            max_spans_per_segment: 64,
            max_flush_delay: Duration::ZERO,
        },
    );

    let a = tree_record(
        "aaaaaaaaaaaaaaaa",
        "cycle.a",
        "chain",
        Some("bbbbbbbbbbbbbbbb"),
        10,
    );
    let b = tree_record(
        "bbbbbbbbbbbbbbbb",
        "cycle.b",
        "chain",
        Some("aaaaaaaaaaaaaaaa"),
        20,
    );
    ingestor
        .ingest_records(vec![a, b])
        .await
        .expect("ingest guarded cycle");

    let a = load_tree_node(mockgres.postgres_url(), "aaaaaaaaaaaaaaaa").await;
    let b = load_tree_node(mockgres.postgres_url(), "bbbbbbbbbbbbbbbb").await;
    assert_eq!(a.parent_span_id, None);
    assert_eq!(b.parent_span_id, None);
    assert!(a.cycle_detected);
    assert!(b.cycle_detected);

    mockgres.stop().await.expect("stop mockgres");
}

#[tokio::test]
async fn deleting_parent_refreshes_current_tree_metadata() {
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

    let root = tree_record("1111111111111111", "agent.root", "chain", None, 10);
    let child = tree_record(
        "2222222222222222",
        "llm.call",
        "llm",
        Some("1111111111111111"),
        20,
    );
    ingestor
        .ingest_records(vec![root, child])
        .await
        .expect("ingest tree");

    let query_engine = QueryEngine::new(mockgres.postgres_url().to_owned(), object_store);
    assert!(
        query_engine
            .delete_run(
                "demo",
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "1111111111111111",
                Some("unit-test"),
            )
            .await
            .expect("delete root")
    );

    let child = load_tree_node(mockgres.postgres_url(), "2222222222222222").await;
    assert_eq!(child.parent_span_id, None);
    assert_eq!(child.root_span_id, "2222222222222222");
    assert_eq!(child.depth, 0);
    assert!(child.unresolved_parent);

    let trace_tree = query_engine
        .load_trace_tree("demo", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
        .await
        .expect("load current tree");
    assert_eq!(trace_tree.roots.len(), 1);
    assert_eq!(trace_tree.roots[0].run.span_id, "2222222222222222");
    assert!(trace_tree.roots[0].run.is_root);

    mockgres.stop().await.expect("stop mockgres");
}

#[derive(Debug)]
struct TreeNodeRow {
    parent_span_id: Option<String>,
    root_span_id: String,
    depth: i64,
    subtree_start: i64,
    subtree_end: i64,
    descendant_count: i64,
    unresolved_parent: bool,
    cycle_detected: bool,
}

fn tree_record(
    span_id: &str,
    name: &str,
    run_type: &str,
    parent_span_id: Option<&str>,
    start_time_unix_nano: i64,
) -> SpanRecord {
    let mut record = sample_record(span_id, start_time_unix_nano);
    record.name = name.to_owned();
    record.run_type = run_type.to_owned();
    record.parent_span_id = parent_span_id.map(str::to_owned);
    record
}

async fn load_tree_node(postgres_url: &str, span_id: &str) -> TreeNodeRow {
    let (client, connection) = tokio_postgres::connect(postgres_url, NoTls)
        .await
        .expect("connect postgres");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    let row = client
        .query_one(
            "SELECT parent_span_id, root_span_id, depth, subtree_start, subtree_end,
                descendant_count, unresolved_parent, cycle_detected
            FROM run_tree_nodes
            WHERE project_name = 'demo'
                AND trace_id = 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
                AND span_id = $1",
            &[&span_id],
        )
        .await
        .expect("load tree node");

    TreeNodeRow {
        parent_span_id: row.get(0),
        root_span_id: row.get(1),
        depth: row.get(2),
        subtree_start: row.get(3),
        subtree_end: row.get(4),
        descendant_count: row.get(5),
        unresolved_parent: row.get(6),
        cycle_detected: row.get(7),
    }
}

async fn count_tree_roots(postgres_url: &str) -> i64 {
    let (client, connection) = tokio_postgres::connect(postgres_url, NoTls)
        .await
        .expect("connect postgres");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .query_one(
            "SELECT count(*) FROM run_tree_nodes
            WHERE project_name = 'demo'
                AND trace_id = 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
                AND parent_span_id IS NULL",
            &[],
        )
        .await
        .expect("count roots")
        .get(0)
}
