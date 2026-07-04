use super::*;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};

use crate::otlp::SpanRecord;
use crate::segment::encode_span_records;

const TRACE_ID: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

#[tokio::test]
async fn datafusion_scans_vortex_segments() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let segment_uri = "projects/demo/trace-segments/test.vortex";
    let records = vec![
        span_record("ignored-project", "other-project", TRACE_ID, None, 1, 2, 1),
        span_record(
            "ignored-trace",
            "demo",
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            None,
            2,
            3,
            1,
        ),
        span_record("child", "demo", TRACE_ID, Some("root"), 20, 30, 2),
        span_record("root", "demo", TRACE_ID, None, 10, 40, 1),
    ];
    let payload = encode_span_records(&records)
        .await
        .expect("encode Vortex segment");
    object_store
        .put(&Path::from(segment_uri), PutPayload::from_bytes(payload))
        .await
        .expect("write Vortex segment");

    let result = query_trace_segments_with_datafusion(
        object_store,
        vec![segment_uri.to_owned()],
        &RunQuery {
            project_names: vec!["demo".to_owned()],
            trace_id: Some(TRACE_ID.to_owned()),
            parent_run_id: None,
            parent_span_id: None,
            run_type: None,
            is_root: None,
            error: None,
            start_time_min_unix_nano: None,
            start_time_max_unix_nano: None,
            limit: None,
            offset: None,
            retention_cutoff_unix_nano: None,
            include_deleted: false,
            filter: None,
            trace_filter: None,
            include_payload: true,
            newest_first: false,
            limits: Default::default(),
        },
    )
    .await
    .expect("query trace segments");

    assert_eq!(
        result
            .iter()
            .map(|run| run.name.as_str())
            .collect::<Vec<_>>(),
        vec!["root", "child"]
    );
    assert_eq!(result[0].run_type, "chain");
    assert_eq!(result[0].status, "success");
    assert!(result[0].is_root);
    assert_eq!(result[1].parent_span_id.as_deref(), Some("root"));
    assert_eq!(result[1].run_type, "llm");
    assert_eq!(result[1].status, "error");
}

#[tokio::test]
async fn datafusion_applies_run_query_filters() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let segment_uri = "projects/demo/trace-segments/filtered.vortex";
    let records = vec![
        span_record("root", "demo", TRACE_ID, None, 10, 40, 1),
        span_record("child", "demo", TRACE_ID, Some("root"), 20, 30, 2),
    ];
    let payload = encode_span_records(&records)
        .await
        .expect("encode Vortex segment");
    object_store
        .put(&Path::from(segment_uri), PutPayload::from_bytes(payload))
        .await
        .expect("write Vortex segment");

    let result = query_trace_segments_with_datafusion(
        object_store,
        vec![segment_uri.to_owned()],
        &RunQuery {
            project_names: vec!["demo".to_owned()],
            trace_id: None,
            parent_run_id: None,
            parent_span_id: None,
            run_type: Some("llm".to_owned()),
            is_root: Some(false),
            error: Some(true),
            start_time_min_unix_nano: None,
            start_time_max_unix_nano: None,
            limit: Some(1),
            offset: None,
            retention_cutoff_unix_nano: None,
            include_deleted: false,
            filter: None,
            trace_filter: None,
            include_payload: true,
            newest_first: false,
            limits: Default::default(),
        },
    )
    .await
    .expect("query trace segments");

    assert_eq!(result.len(), 1);
    assert_eq!(result[0].name, "child");
}

#[tokio::test]
async fn datafusion_filters_latest_run_versions() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let segment_uri = "projects/demo/trace-segments/updates.vortex";
    let records = vec![
        span_record("root", "demo", TRACE_ID, None, 10, 0, 1),
        span_record("root", "demo", TRACE_ID, None, 10, 40, 2),
    ];
    let payload = encode_span_records(&records)
        .await
        .expect("encode Vortex segment");
    object_store
        .put(&Path::from(segment_uri), PutPayload::from_bytes(payload))
        .await
        .expect("write Vortex segment");

    let successful_runs = query_trace_segments_with_datafusion(
        Arc::clone(&object_store),
        vec![segment_uri.to_owned()],
        &RunQuery {
            project_names: vec!["demo".to_owned()],
            trace_id: Some(TRACE_ID.to_owned()),
            parent_run_id: None,
            parent_span_id: None,
            run_type: None,
            is_root: None,
            error: Some(false),
            start_time_min_unix_nano: None,
            start_time_max_unix_nano: None,
            limit: None,
            offset: None,
            retention_cutoff_unix_nano: None,
            include_deleted: false,
            filter: None,
            trace_filter: None,
            include_payload: true,
            newest_first: false,
            limits: Default::default(),
        },
    )
    .await
    .expect("query successful runs");
    assert!(successful_runs.is_empty());

    let error_runs = query_trace_segments_with_datafusion(
        object_store,
        vec![segment_uri.to_owned()],
        &RunQuery {
            project_names: vec!["demo".to_owned()],
            trace_id: Some(TRACE_ID.to_owned()),
            parent_run_id: None,
            parent_span_id: None,
            run_type: None,
            is_root: None,
            error: Some(true),
            start_time_min_unix_nano: None,
            start_time_max_unix_nano: None,
            limit: None,
            offset: None,
            retention_cutoff_unix_nano: None,
            include_deleted: false,
            filter: None,
            trace_filter: None,
            include_payload: true,
            newest_first: false,
            limits: Default::default(),
        },
    )
    .await
    .expect("query error runs");
    assert_eq!(error_runs.len(), 1);
    assert_eq!(error_runs[0].status, "error");
    assert_eq!(error_runs[0].end_time_unix_nano, 40);
}

#[test]
fn builds_trace_tree_from_runs() {
    let tree = trace_tree_from_runs(
        "demo",
        TRACE_ID,
        vec![
            run("demo", TRACE_ID, "child", Some("root"), 20),
            run("demo", TRACE_ID, "root", None, 10),
            run("demo", TRACE_ID, "orphan", Some("missing"), 30),
        ],
    );

    assert_eq!(tree.project_name, "demo");
    assert_eq!(tree.trace_id, TRACE_ID);
    assert_eq!(tree.roots.len(), 2);
    assert_eq!(tree.roots[0].run.name, "root");
    assert_eq!(tree.roots[0].children.len(), 1);
    assert_eq!(tree.roots[0].children[0].run.name, "child");
    assert_eq!(tree.roots[1].run.name, "orphan");
}

#[test]
fn escapes_sql_string_literals() {
    assert_eq!(sql_string_literal("project's trace"), "'project''s trace'");
    assert_eq!(
        sql_object_store_path("projects/project's/test.vortex"),
        "'/projects/project''s/test.vortex'"
    );
    assert_eq!(
        run_query_where_sql(&RunQuery {
            project_names: vec!["demo".to_owned()],
            trace_id: Some("trace".to_owned()),
            parent_run_id: Some("parent-run".to_owned()),
            parent_span_id: Some("parent-span".to_owned()),
            run_type: Some("llm".to_owned()),
            is_root: Some(false),
            error: Some(false),
            start_time_min_unix_nano: Some(10),
            start_time_max_unix_nano: Some(20),
            limit: None,
            offset: None,
            retention_cutoff_unix_nano: None,
            include_deleted: false,
            filter: None,
            trace_filter: None,
            include_payload: true,
            newest_first: false,
            limits: Default::default(),
        }),
        "project_name IN ('demo') AND trace_id = 'trace' AND parent_run_id = 'parent-run' AND parent_span_id = 'parent-span' AND run_type = 'llm' AND is_root = false AND status <> 'error' AND start_time_unix_nano >= 10 AND start_time_unix_nano <= 20"
    );
}

#[test]
fn datafusion_sql_pushes_projection_and_source_predicates() {
    let mut query = RunQuery::new("demo");
    query.project_names.push("other's".to_owned());
    query.trace_id = Some(TRACE_ID.to_owned());
    query.run_type = Some("llm".to_owned());
    query.start_time_min_unix_nano = Some(10);
    query.start_time_max_unix_nano = Some(20);

    let source_where = run_source_pushdown_where_sql(&query, None);
    assert_eq!(
        source_where,
        "project_name IN ('demo', 'other''s') AND trace_id = 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' AND run_type = 'llm' AND start_time_unix_nano >= 10 AND start_time_unix_nano <= 20"
    );

    let sql = run_head_datafusion_sql(
        &[
            current_segment_source("projects/demo/trace-segments/a.vortex".to_owned()),
            current_segment_source("projects/demo/trace-segments/b.vortex".to_owned()),
        ],
        &query,
        false,
        None,
    );

    assert_eq!(sql.matches(&format!("WHERE {source_where}")).count(), 2);
    assert!(sql.contains("'{}' AS attributes_json"));
    assert!(sql.contains("WHERE run_version = 1 AND project_name IN ('demo', 'other''s')"));
}

#[test]
fn datafusion_sql_pushes_candidate_run_keys_to_sources() {
    let query = RunQuery::new("demo");
    let candidate_run_keys = std::collections::HashSet::from([
        RunKey {
            project_name: "demo".to_owned(),
            trace_id: TRACE_ID.to_owned(),
            span_id: "child".to_owned(),
        },
        RunKey {
            project_name: "demo".to_owned(),
            trace_id: TRACE_ID.to_owned(),
            span_id: "root".to_owned(),
        },
    ]);

    let sql = run_head_datafusion_sql(
        &[current_segment_source(
            "projects/demo/trace-segments/a.vortex".to_owned(),
        )],
        &query,
        false,
        Some(&candidate_run_keys),
    );

    assert!(sql.contains("span_id IN ('child', 'root')"));
    assert!(sql.contains("trace_id = 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'"));
}

#[test]
fn datafusion_sql_pushes_segment_candidate_rows_to_sources() {
    let query = RunQuery::new("demo");
    let sql = run_head_datafusion_sql(
        &[SegmentSource {
            uri: "projects/demo/trace-segments/a.vortex".to_owned(),
            total_bytes: 123,
            candidate_rows: vec![
                SegmentCandidateRow {
                    project_name: "demo".to_owned(),
                    trace_id: TRACE_ID.to_owned(),
                    span_id: "root".to_owned(),
                    row_index: 7,
                },
                SegmentCandidateRow {
                    project_name: "demo".to_owned(),
                    trace_id: TRACE_ID.to_owned(),
                    span_id: "child".to_owned(),
                    row_index: 11,
                },
            ],
        }],
        &query,
        false,
        None,
    );

    assert!(sql.contains("span_id = 'root' AND row_index = 7"));
    assert!(sql.contains("span_id = 'child' AND row_index = 11"));
}

#[test]
fn candidate_run_key_source_pushdown_is_not_capped_at_legacy_threshold() {
    let candidate_run_keys = (0..1025)
        .map(|index| RunKey {
            project_name: "demo".to_owned(),
            trace_id: TRACE_ID.to_owned(),
            span_id: format!("span-{index}"),
        })
        .collect::<std::collections::HashSet<_>>();

    let predicate = candidate_run_source_pushdown_sql(Some(&candidate_run_keys))
        .expect("candidate pushdown should be emitted");

    assert!(predicate.contains("span-1024"));
}

fn run(
    project_name: &str,
    trace_id: &str,
    name: &str,
    parent_span_id: Option<&str>,
    start_time_unix_nano: i64,
) -> RunSummary {
    RunSummary {
        project_name: project_name.to_owned(),
        run_id: None,
        trace_id: trace_id.to_owned(),
        span_id: name.to_owned(),
        parent_run_id: None,
        parent_span_id: parent_span_id.map(str::to_owned),
        name: name.to_owned(),
        run_type: if parent_span_id.is_some() {
            "llm".to_owned()
        } else {
            "chain".to_owned()
        },
        status: "success".to_owned(),
        start_time_unix_nano,
        end_time_unix_nano: start_time_unix_nano + 10,
        is_root: parent_span_id.is_none(),
        attributes_json: "{}".to_owned(),
    }
}

fn span_record(
    name: &str,
    project_name: &str,
    trace_id: &str,
    parent_span_id: Option<&str>,
    start_time_unix_nano: i64,
    end_time_unix_nano: i64,
    status_code: i32,
) -> SpanRecord {
    SpanRecord {
        project_name: project_name.to_owned(),
        run_id: String::new(),
        trace_id: trace_id.to_owned(),
        span_id: name.to_owned(),
        parent_run_id: None,
        parent_span_id: parent_span_id.map(str::to_owned),
        name: name.to_owned(),
        run_type: if parent_span_id.is_some() {
            "llm".to_owned()
        } else {
            "chain".to_owned()
        },
        start_time_unix_nano,
        end_time_unix_nano,
        status_code,
        event_kind: crate::otlp::RunEventKind::End,
        attributes_json: "{}".to_owned(),
    }
}
