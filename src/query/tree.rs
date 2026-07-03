use std::collections::{HashMap, HashSet};

use super::RunSummary;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunNode {
    pub run: RunSummary,
    pub children: Vec<RunNode>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceTree {
    pub project_name: String,
    pub trace_id: String,
    pub roots: Vec<RunNode>,
}

pub(crate) fn trace_tree_from_runs(
    project_name: &str,
    trace_id: &str,
    runs: Vec<RunSummary>,
) -> TraceTree {
    let ordered_ids = runs
        .iter()
        .map(|run| run.span_id.clone())
        .collect::<Vec<_>>();
    let known_ids = ordered_ids.iter().cloned().collect::<HashSet<_>>();
    let mut children_by_parent: HashMap<Option<String>, Vec<String>> = HashMap::new();

    for run in &runs {
        let parent = run
            .parent_span_id
            .as_ref()
            .filter(|parent| known_ids.contains(parent.as_str()))
            .cloned();
        children_by_parent
            .entry(parent)
            .or_default()
            .push(run.span_id.clone());
    }

    let runs_by_id = runs
        .into_iter()
        .map(|run| (run.span_id.clone(), run))
        .collect::<HashMap<_, _>>();
    let mut visited = HashSet::new();
    let mut active = HashSet::new();
    let mut roots = Vec::new();

    for span_id in children_by_parent.get(&None).into_iter().flatten() {
        if let Some(node) = build_run_node(
            span_id,
            &runs_by_id,
            &children_by_parent,
            &mut visited,
            &mut active,
        ) {
            roots.push(node);
        }
    }
    for span_id in ordered_ids {
        if !visited.contains(&span_id)
            && let Some(node) = build_run_node(
                &span_id,
                &runs_by_id,
                &children_by_parent,
                &mut visited,
                &mut active,
            )
        {
            roots.push(node);
        }
    }

    TraceTree {
        project_name: project_name.to_owned(),
        trace_id: trace_id.to_owned(),
        roots,
    }
}

fn build_run_node(
    span_id: &str,
    runs_by_id: &HashMap<String, RunSummary>,
    children_by_parent: &HashMap<Option<String>, Vec<String>>,
    visited: &mut HashSet<String>,
    active: &mut HashSet<String>,
) -> Option<RunNode> {
    if active.contains(span_id) || !visited.insert(span_id.to_owned()) {
        return None;
    }
    active.insert(span_id.to_owned());

    let children = children_by_parent
        .get(&Some(span_id.to_owned()))
        .into_iter()
        .flatten()
        .filter_map(|child_id| {
            build_run_node(child_id, runs_by_id, children_by_parent, visited, active)
        })
        .collect::<Vec<_>>();
    active.remove(span_id);

    runs_by_id
        .get(span_id)
        .cloned()
        .map(|run| RunNode { run, children })
}
