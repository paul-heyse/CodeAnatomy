//! Deterministic cost estimation and scheduling heuristics.

use std::collections::{BTreeMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::compiler::scheduling::{TaskGraph, TaskSchedule, TaskType};
use crate::executor::metrics_collector::CollectedMetrics;
use crate::executor::warnings::{RunWarning, WarningCode, WarningStage};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
pub enum StatsQuality {
    Exact,
    Estimated,
    #[default]
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CostModelConfig {
    pub default_view_cost: f64,
    pub default_scan_cost: f64,
    pub row_count_scale: f64,
}

impl Default for CostModelConfig {
    fn default() -> Self {
        Self {
            default_view_cost: 1.0,
            default_scan_cost: 2.0,
            row_count_scale: 1_000.0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CostModelOutcome {
    pub costs: BTreeMap<String, f64>,
    pub stats_quality: StatsQuality,
    pub warnings: Vec<RunWarning>,
}

pub fn derive_task_costs(
    graph: &TaskGraph,
    metrics: Option<&CollectedMetrics>,
    config: &CostModelConfig,
) -> CostModelOutcome {
    let mut costs: BTreeMap<String, f64> = BTreeMap::new();
    let mut warnings = Vec::new();

    let stats_quality = match metrics {
        Some(m) if m.output_rows > 0 => StatsQuality::Estimated,
        Some(_) => StatsQuality::Unknown,
        None => StatsQuality::Unknown,
    };

    if stats_quality == StatsQuality::Unknown {
        warnings.push(
            RunWarning::new(
                WarningCode::CostModelStatsFallback,
                WarningStage::Compilation,
                "Task cost model fell back to deterministic defaults due to low-quality statistics",
            )
            .with_context("stats_quality", "unknown"),
        );
    }

    let rows_factor = metrics
        .map(|m| (m.output_rows as f64 / config.row_count_scale).max(1.0))
        .unwrap_or(1.0);

    for (name, node) in &graph.nodes {
        let base = match node.task_type {
            TaskType::Scan => config.default_scan_cost,
            TaskType::View | TaskType::Output => config.default_view_cost,
        };
        costs.insert(name.clone(), base * rows_factor);
    }

    CostModelOutcome {
        costs,
        stats_quality,
        warnings,
    }
}

pub fn schedule_tasks(graph: &TaskGraph, costs: &BTreeMap<String, f64>) -> TaskSchedule {
    let mut schedule = TaskSchedule::default();
    let mut order = graph.topological_order.clone();
    if order.is_empty() {
        order = graph.nodes.keys().cloned().collect();
    }

    let mut bottom_levels: BTreeMap<String, f64> = BTreeMap::new();
    for node in order.iter().rev() {
        let cost = *costs.get(node).unwrap_or(&1.0);
        let dependents = graph.downstream_closure(node);
        let max_downstream = dependents
            .iter()
            .filter_map(|dep| bottom_levels.get(dep))
            .copied()
            .fold(0.0, f64::max);
        bottom_levels.insert(node.clone(), cost + max_downstream);
    }

    let mut execution_order = order.clone();
    execution_order.sort_by(|a, b| {
        let ba = bottom_levels.get(a).copied().unwrap_or(0.0);
        let bb = bottom_levels.get(b).copied().unwrap_or(0.0);
        bb.partial_cmp(&ba).unwrap_or(std::cmp::Ordering::Equal)
    });

    let mut selected = HashSet::new();
    let mut valid_order = Vec::new();
    while valid_order.len() < graph.nodes.len() {
        let mut progressed = false;
        for candidate in &execution_order {
            if selected.contains(candidate) {
                continue;
            }
            let deps = graph
                .dependencies
                .get(candidate)
                .cloned()
                .unwrap_or_default();
            if deps.iter().all(|dep| selected.contains(dep)) {
                selected.insert(candidate.clone());
                valid_order.push(candidate.clone());
                progressed = true;
            }
        }
        if !progressed {
            break;
        }
    }

    let critical_node = bottom_levels
        .iter()
        .max_by(|a, b| a.1.partial_cmp(b.1).unwrap_or(std::cmp::Ordering::Equal))
        .map(|(name, _)| name.clone());
    let critical_path = critical_node
        .map(|node| critical_path(graph, &bottom_levels, &node))
        .unwrap_or_default();
    let critical_path_length = critical_path
        .iter()
        .filter_map(|node| costs.get(node))
        .sum::<f64>();

    let mut slack_by_task = BTreeMap::new();
    let critical_set: HashSet<String> = critical_path.iter().cloned().collect();
    for node in graph.nodes.keys() {
        let slack = if critical_set.contains(node) {
            0.0
        } else {
            (critical_path_length - bottom_levels.get(node).copied().unwrap_or(0.0)).max(0.0)
        };
        slack_by_task.insert(node.clone(), slack);
    }

    schedule.execution_order = valid_order;
    schedule.critical_path = critical_path;
    schedule.critical_path_length = critical_path_length;
    schedule.bottom_level_costs = bottom_levels;
    schedule.slack_by_task = slack_by_task;
    schedule
}

/// Schedule tasks with reliability gating based on statistics quality.
///
/// When statistics quality is unknown, this uses deterministic topological
/// ordering only and avoids cost-priority reordering.
pub fn schedule_tasks_with_quality(
    graph: &TaskGraph,
    costs: &BTreeMap<String, f64>,
    stats_quality: StatsQuality,
) -> TaskSchedule {
    if stats_quality != StatsQuality::Unknown {
        return schedule_tasks(graph, costs);
    }

    let execution_order = if graph.topological_order.is_empty() {
        graph.nodes.keys().cloned().collect::<Vec<_>>()
    } else {
        graph.topological_order.clone()
    };
    let critical_path = execution_order.clone();
    let critical_path_length = critical_path
        .iter()
        .filter_map(|name| costs.get(name))
        .sum::<f64>();
    TaskSchedule {
        execution_order,
        critical_path,
        critical_path_length,
        bottom_level_costs: BTreeMap::new(),
        slack_by_task: BTreeMap::new(),
    }
}

fn critical_path(
    graph: &TaskGraph,
    bottom_levels: &BTreeMap<String, f64>,
    start: &str,
) -> Vec<String> {
    let mut path = vec![start.to_string()];
    let mut current = start.to_string();
    loop {
        let mut next: Option<(String, f64)> = None;
        for node in graph.downstream_closure(&current) {
            let score = bottom_levels.get(&node).copied().unwrap_or(0.0);
            if next.as_ref().map(|(_, best)| score > *best).unwrap_or(true) {
                next = Some((node, score));
            }
        }
        let Some((node, _)) = next else {
            break;
        };
        if path.contains(&node) {
            break;
        }
        path.push(node.clone());
        current = node;
    }
    path
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compiler::scheduling::TaskGraph;

    fn tiny_graph() -> TaskGraph {
        TaskGraph::from_inferred_deps(
            &[("view_a".to_string(), vec!["scan_a".to_string()])],
            &[("scan_a".to_string(), vec![])],
            &[("out".to_string(), vec!["view_a".to_string()])],
        )
        .expect("graph")
    }

    #[test]
    fn test_schedule_tasks_with_unknown_stats_uses_topological_order() {
        let graph = tiny_graph();
        let outcome = derive_task_costs(&graph, None, &CostModelConfig::default());
        let schedule = schedule_tasks_with_quality(&graph, &outcome.costs, outcome.stats_quality);
        assert_eq!(outcome.stats_quality, StatsQuality::Unknown);
        assert_eq!(schedule.execution_order, graph.topological_order);
    }
}
