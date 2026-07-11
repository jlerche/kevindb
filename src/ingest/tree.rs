use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
struct RunTreeInput {
    span_id: String,
    parent_span_id: Option<String>,
    run_id: String,
    start_time_unix_nano: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RunTreeIndexNode {
    span_id: String,
    run_id: String,
    parent_span_id: Option<String>,
    root_span_id: String,
    root_run_id: String,
    depth: i64,
    sibling_order: i64,
    subtree_start: i64,
    subtree_end: i64,
    descendant_count: i64,
    unresolved_parent: bool,
    cycle_detected: bool,
}

pub(super) async fn refresh_trace_tree_metadata(
    tx: &tokio_postgres::Transaction<'_>,
    project_name: &str,
    trace_id: &str,
) -> Result<()> {
    let rows = tx
        .query(
            "SELECT
                heads.span_id,
                heads.parent_span_id,
                heads.run_id,
                heads.start_time_unix_nano
            FROM run_heads heads
            WHERE heads.project_name = $1
                AND heads.trace_id = $2
                AND heads.deleted_at_unix_nano IS NULL
            ORDER BY heads.start_time_unix_nano ASC, heads.span_id ASC",
            &[&project_name, &trace_id],
        )
        .await
        .context("load run heads for tree refresh")?;
    let inputs = rows
        .into_iter()
        .map(|row| RunTreeInput {
            span_id: row.get(0),
            parent_span_id: row.get(1),
            run_id: row.get(2),
            start_time_unix_nano: row.get(3),
        })
        .collect::<Vec<_>>();
    let nodes = build_trace_tree_index(inputs);

    tx.execute(
        "DELETE FROM run_tree_edges WHERE project_name = $1 AND trace_id = $2",
        &[&project_name, &trace_id],
    )
    .await
    .context("delete stale run tree edges")?;
    tx.execute(
        "DELETE FROM run_tree_nodes WHERE project_name = $1 AND trace_id = $2",
        &[&project_name, &trace_id],
    )
    .await
    .context("delete stale run tree nodes")?;

    for node in nodes {
        tx.execute(
            "INSERT INTO run_tree_nodes(
                project_name, trace_id, span_id, run_id, parent_span_id,
                root_span_id, root_run_id, depth, sibling_order, subtree_start, subtree_end,
                descendant_count, unresolved_parent, cycle_detected
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)",
            &[
                &project_name,
                &trace_id,
                &node.span_id,
                &node.run_id,
                &node.parent_span_id,
                &node.root_span_id,
                &node.root_run_id,
                &node.depth,
                &node.sibling_order,
                &node.subtree_start,
                &node.subtree_end,
                &node.descendant_count,
                &node.unresolved_parent,
                &node.cycle_detected,
            ],
        )
        .await
        .context("insert run tree node")?;

        if let Some(parent_span_id) = &node.parent_span_id {
            tx.execute(
                "INSERT INTO run_tree_edges(
                    project_name, trace_id, parent_span_id, child_span_id,
                    sibling_order, depth
                )
                VALUES ($1, $2, $3, $4, $5, $6)",
                &[
                    &project_name,
                    &trace_id,
                    &parent_span_id,
                    &node.span_id,
                    &node.sibling_order,
                    &node.depth,
                ],
            )
            .await
            .context("insert run tree edge")?;
        }

        tx.execute(
            "UPDATE run_heads
            SET root_run_id = $4,
                root_span_id = $5,
                is_root = $6
            WHERE project_name = $1 AND trace_id = $2 AND span_id = $3",
            &[
                &project_name,
                &trace_id,
                &node.span_id,
                &node.root_run_id,
                &node.root_span_id,
                &node.parent_span_id.is_none(),
            ],
        )
        .await
        .context("refresh run head tree fields")?;
    }

    Ok(())
}

fn build_trace_tree_index(mut runs: Vec<RunTreeInput>) -> Vec<RunTreeIndexNode> {
    runs.sort_by(|left, right| {
        left.start_time_unix_nano
            .cmp(&right.start_time_unix_nano)
            .then_with(|| left.span_id.cmp(&right.span_id))
    });
    let known_ids = runs
        .iter()
        .map(|run| run.span_id.clone())
        .collect::<HashSet<_>>();
    let parent_by_span = runs
        .iter()
        .map(|run| (run.span_id.clone(), run.parent_span_id.clone()))
        .collect::<HashMap<_, _>>();
    let mut inputs_by_span = HashMap::new();
    let mut children_by_parent: HashMap<Option<String>, Vec<String>> = HashMap::new();

    for run in runs {
        let unresolved_parent = run
            .parent_span_id
            .as_ref()
            .is_some_and(|parent| !known_ids.contains(parent));
        let cycle_detected = run.parent_span_id.as_ref().is_some_and(|parent| {
            known_ids.contains(parent)
                && parent_creates_cycle(&run.span_id, parent, &parent_by_span)
        });
        let effective_parent = run
            .parent_span_id
            .as_ref()
            .filter(|parent| known_ids.contains(*parent))
            .filter(|parent| parent.as_str() != run.span_id)
            .filter(|parent| !parent_creates_cycle(&run.span_id, parent, &parent_by_span))
            .cloned();
        children_by_parent
            .entry(effective_parent.clone())
            .or_default()
            .push(run.span_id.clone());
        inputs_by_span.insert(
            run.span_id.clone(),
            IndexedRunInput {
                run,
                effective_parent,
                unresolved_parent,
                cycle_detected,
            },
        );
    }

    let mut nodes = Vec::new();
    let mut next_order = 0;
    for (sibling_order, root_span_id) in children_by_parent
        .get(&None)
        .cloned()
        .unwrap_or_default()
        .iter()
        .enumerate()
    {
        let root_run_id = inputs_by_span
            .get(root_span_id)
            .map(|input| stable_run_id(&input.run))
            .unwrap_or_else(|| root_span_id.clone());
        visit_tree_node(
            root_span_id,
            0,
            sibling_order as i64,
            root_span_id,
            &root_run_id,
            &inputs_by_span,
            &children_by_parent,
            &mut next_order,
            &mut nodes,
        );
    }
    nodes.sort_by(|left, right| left.subtree_start.cmp(&right.subtree_start));
    nodes
}

#[derive(Debug, Clone)]
struct IndexedRunInput {
    run: RunTreeInput,
    effective_parent: Option<String>,
    unresolved_parent: bool,
    cycle_detected: bool,
}

fn parent_creates_cycle(
    child_span_id: &str,
    parent_span_id: &str,
    parent_by_span: &HashMap<String, Option<String>>,
) -> bool {
    let mut seen = HashSet::new();
    let mut current = Some(parent_span_id);
    while let Some(span_id) = current {
        if span_id == child_span_id {
            return true;
        }
        if !seen.insert(span_id) {
            return false;
        }
        current = parent_by_span.get(span_id).and_then(Option::as_deref);
    }
    false
}

#[allow(clippy::too_many_arguments)]
fn visit_tree_node(
    span_id: &str,
    depth: i64,
    sibling_order: i64,
    root_span_id: &str,
    root_run_id: &str,
    inputs_by_span: &HashMap<String, IndexedRunInput>,
    children_by_parent: &HashMap<Option<String>, Vec<String>>,
    next_order: &mut i64,
    nodes: &mut Vec<RunTreeIndexNode>,
) -> i64 {
    let Some(input) = inputs_by_span.get(span_id) else {
        return 0;
    };
    let subtree_start = *next_order;
    *next_order += 1;

    let mut descendant_count = 0;
    if let Some(children) = children_by_parent.get(&Some(span_id.to_owned())) {
        for (child_order, child_span_id) in children.iter().enumerate() {
            descendant_count += 1 + visit_tree_node(
                child_span_id,
                depth + 1,
                child_order as i64,
                root_span_id,
                root_run_id,
                inputs_by_span,
                children_by_parent,
                next_order,
                nodes,
            );
        }
    }
    let subtree_end = *next_order;
    *next_order += 1;

    nodes.push(RunTreeIndexNode {
        span_id: input.run.span_id.clone(),
        run_id: input.run.run_id.clone(),
        parent_span_id: input.effective_parent.clone(),
        root_span_id: root_span_id.to_owned(),
        root_run_id: root_run_id.to_owned(),
        depth,
        sibling_order,
        subtree_start,
        subtree_end,
        descendant_count,
        unresolved_parent: input.unresolved_parent,
        cycle_detected: input.cycle_detected,
    });
    descendant_count
}

fn stable_run_id(run: &RunTreeInput) -> String {
    run.run_id.clone()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_nested_set_tree_with_multiple_roots() {
        let nodes = build_trace_tree_index(vec![
            input("child", Some("root"), 20),
            input("root", None, 10),
            input("orphan", Some("missing"), 30),
        ]);

        let root = node(&nodes, "root");
        let child = node(&nodes, "child");
        let orphan = node(&nodes, "orphan");
        assert_eq!(root.root_span_id, "root");
        assert_eq!(root.depth, 0);
        assert_eq!(root.descendant_count, 1);
        assert!(root.subtree_start < child.subtree_start);
        assert!(root.subtree_end > child.subtree_end);
        assert_eq!(child.parent_span_id.as_deref(), Some("root"));
        assert_eq!(child.depth, 1);
        assert_eq!(orphan.parent_span_id, None);
        assert!(orphan.unresolved_parent);
    }

    #[test]
    fn guards_cycle_edges_without_losing_nodes() {
        let nodes = build_trace_tree_index(vec![
            input("a", Some("b"), 10),
            input("b", Some("a"), 20),
            input("c", Some("a"), 30),
        ]);

        assert_eq!(nodes.len(), 3);
        assert!(node(&nodes, "a").cycle_detected);
        assert!(node(&nodes, "b").cycle_detected);
        assert_eq!(node(&nodes, "a").parent_span_id, None);
        assert_eq!(node(&nodes, "b").parent_span_id, None);
        assert_eq!(node(&nodes, "c").parent_span_id.as_deref(), Some("a"));
    }

    fn input(
        span_id: &str,
        parent_span_id: Option<&str>,
        start_time_unix_nano: i64,
    ) -> RunTreeInput {
        RunTreeInput {
            span_id: span_id.to_owned(),
            parent_span_id: parent_span_id.map(str::to_owned),
            run_id: format!("run-{span_id}"),
            start_time_unix_nano,
        }
    }

    fn node<'a>(nodes: &'a [RunTreeIndexNode], span_id: &str) -> &'a RunTreeIndexNode {
        nodes
            .iter()
            .find(|node| node.span_id == span_id)
            .expect("tree node")
    }
}
