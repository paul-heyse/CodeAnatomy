use std::collections::HashSet;

use codeanatomy_engine::compiler::cost_model::{derive_task_costs, schedule_tasks, CostModelConfig};
use codeanatomy_engine::compiler::scheduling::TaskGraph;

#[test]
fn test_task_graph_topological_sort_and_schedule() {
    let mut graph = TaskGraph::from_inferred_deps(
        &[("v1".to_string(), vec![]), ("v2".to_string(), vec!["v1".to_string()])],
        &[("scan".to_string(), vec![])],
        &[("out".to_string(), vec!["v2".to_string()])],
    )
    .unwrap();
    graph.topological_sort().unwrap();
    let costs = derive_task_costs(&graph, None, &CostModelConfig::default());
    let schedule = schedule_tasks(&graph, &costs.costs);
    assert!(!schedule.execution_order.is_empty());
    assert!(!schedule.bottom_level_costs.is_empty());
}

#[test]
fn test_task_graph_prune() {
    let mut graph = TaskGraph::from_inferred_deps(
        &[
            ("a".to_string(), vec![]),
            ("b".to_string(), vec!["a".to_string()]),
            ("c".to_string(), vec!["b".to_string()]),
        ],
        &[],
        &[],
    )
    .unwrap();
    let mut keep = HashSet::new();
    keep.insert("c".to_string());
    let report = graph.prune(&keep);
    assert_eq!(report.removed_nodes, 0);
    assert!(graph.nodes.contains_key("a"));
    assert!(graph.nodes.contains_key("b"));
    assert!(graph.nodes.contains_key("c"));
}

