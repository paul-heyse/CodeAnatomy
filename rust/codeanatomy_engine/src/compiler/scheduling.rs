//! Task graph and scheduling primitives for execution planning.

use std::collections::{BTreeMap, BTreeSet, HashSet, VecDeque};

use datafusion_common::{DataFusionError, Result};
use serde::{Deserialize, Serialize};

use crate::spec::execution_spec::SemanticExecutionSpec;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum TaskType {
    View,
    Scan,
    Output,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskNode {
    pub name: String,
    pub task_type: TaskType,
    pub estimated_cost: f64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TaskGraph {
    pub dependencies: BTreeMap<String, BTreeSet<String>>,
    pub nodes: BTreeMap<String, TaskNode>,
    pub topological_order: Vec<String>,
    pub is_reduced: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ReductionReport {
    pub removed_edges: usize,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PruneReport {
    pub removed_nodes: usize,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TaskSchedule {
    pub execution_order: Vec<String>,
    pub critical_path: Vec<String>,
    pub critical_path_length: f64,
    pub bottom_level_costs: BTreeMap<String, f64>,
    pub slack_by_task: BTreeMap<String, f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CachePolicyDecision {
    pub view_name: String,
    pub policy: String,
    pub confidence: f64,
    pub rationale: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CachePolicyRequest {
    pub out_degree: BTreeMap<String, usize>,
    pub outputs: BTreeSet<String>,
    pub cache_overrides: BTreeMap<String, String>,
    pub workload_class: Option<String>,
}

fn normalize_workload_class(workload_class: Option<&str>) -> Option<String> {
    workload_class
        .map(|value| value.trim().to_lowercase())
        .filter(|value| !value.is_empty())
}

fn workload_adjusted_policy(
    base_policy: &str,
    out_degree: usize,
    is_output: bool,
    workload_class: Option<&str>,
) -> String {
    if is_output {
        return base_policy.to_string();
    }
    match workload_class {
        Some("interactive_query") if base_policy == "delta_staging" && out_degree <= 1 => {
            "none".to_string()
        }
        Some("compile_replay") if base_policy == "delta_staging" => "none".to_string(),
        _ => base_policy.to_string(),
    }
}

fn normalized_override(value: Option<&str>) -> Option<String> {
    match value {
        Some("none") => Some("none".to_string()),
        Some("delta_staging") => Some("delta_staging".to_string()),
        Some("delta_output") => Some("delta_output".to_string()),
        _ => None,
    }
}

/// Derive deterministic cache policy decisions from task-graph out-degree metadata.
pub fn derive_cache_policies(request: &CachePolicyRequest) -> Vec<CachePolicyDecision> {
    let workload = normalize_workload_class(request.workload_class.as_deref());
    let mut decisions: Vec<CachePolicyDecision> = Vec::with_capacity(request.out_degree.len());
    let mut keys: Vec<&String> = request.out_degree.keys().collect();
    keys.sort();
    for task_name in keys {
        let out_degree = *request.out_degree.get(task_name).unwrap_or(&0usize);
        let is_output = request.outputs.contains(task_name);
        let base_policy = if is_output {
            "delta_output"
        } else if out_degree > 2 {
            "delta_staging"
        } else if out_degree == 0 {
            "none"
        } else {
            "delta_staging"
        };
        let mut policy =
            workload_adjusted_policy(base_policy, out_degree, is_output, workload.as_deref());
        let mut rationale =
            format!("derived_from_topology: out_degree={out_degree}, is_output={is_output}");
        let mut confidence = if is_output { 1.0 } else { 0.85 };
        if let Some(override_policy) =
            normalized_override(request.cache_overrides.get(task_name).map(String::as_str))
        {
            policy = override_policy;
            rationale = "explicit_override".to_string();
            confidence = 1.0;
        }
        decisions.push(CachePolicyDecision {
            view_name: task_name.clone(),
            policy,
            confidence,
            rationale,
        });
    }
    decisions
}

impl TaskGraph {
    pub fn from_inferred_deps(
        view_deps: &[(String, Vec<String>)],
        scan_deps: &[(String, Vec<String>)],
        output_deps: &[(String, Vec<String>)],
    ) -> Result<Self> {
        let mut graph = TaskGraph::default();
        for (name, deps) in view_deps {
            graph.upsert_node(name, TaskType::View);
            graph.set_dependencies(name, deps);
        }
        for (name, deps) in scan_deps {
            graph.upsert_node(name, TaskType::Scan);
            graph.set_dependencies(name, deps);
        }
        for (name, deps) in output_deps {
            graph.upsert_node(name, TaskType::Output);
            graph.set_dependencies(name, deps);
        }
        let mut missing: Vec<String> = Vec::new();
        for deps in graph.dependencies.values() {
            for dep in deps {
                if !graph.nodes.contains_key(dep) {
                    missing.push(dep.clone());
                }
            }
        }
        for dep in missing {
            graph.upsert_node(&dep, TaskType::View);
        }
        graph.topological_sort()?;
        Ok(graph)
    }

    pub fn reduce(&mut self) -> ReductionReport {
        let mut removed_edges = 0usize;
        let names: Vec<String> = self.dependencies.keys().cloned().collect();
        for node in names {
            let deps = self
                .dependencies
                .get(&node)
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .collect::<Vec<_>>();
            let mut keep = BTreeSet::new();
            for dep in &deps {
                if !deps
                    .iter()
                    .filter(|other| *other != dep)
                    .any(|other| self.reaches(other, dep))
                {
                    keep.insert(dep.clone());
                } else {
                    removed_edges += 1;
                }
            }
            self.dependencies.insert(node, keep);
        }
        self.is_reduced = true;
        ReductionReport { removed_edges }
    }

    pub fn prune(&mut self, active_tasks: &HashSet<String>) -> PruneReport {
        let mut keep: HashSet<String> = HashSet::new();
        for task in active_tasks {
            keep.insert(task.clone());
            for dep in self.upstream_closure(task) {
                keep.insert(dep);
            }
        }

        let before = self.nodes.len();
        self.nodes.retain(|name, _| keep.contains(name));
        self.dependencies.retain(|name, _| keep.contains(name));
        for deps in self.dependencies.values_mut() {
            deps.retain(|dep| keep.contains(dep));
        }

        self.topological_order
            .retain(|name| self.nodes.contains_key(name));

        PruneReport {
            removed_nodes: before.saturating_sub(self.nodes.len()),
        }
    }

    pub fn topological_sort(&mut self) -> Result<()> {
        let mut in_degree: BTreeMap<String, usize> = self
            .nodes
            .keys()
            .map(|name| (name.clone(), 0usize))
            .collect();
        let mut outgoing: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for (node, deps) in &self.dependencies {
            for dep in deps {
                *in_degree.entry(node.clone()).or_default() += 1;
                outgoing.entry(dep.clone()).or_default().push(node.clone());
            }
        }
        let mut queue: VecDeque<String> = in_degree
            .iter()
            .filter_map(|(name, degree)| {
                if *degree == 0 {
                    Some(name.clone())
                } else {
                    None
                }
            })
            .collect();
        sort_queue(&mut queue);
        let mut order = Vec::with_capacity(self.nodes.len());

        while let Some(node) = queue.pop_front() {
            order.push(node.clone());
            if let Some(children) = outgoing.get(&node) {
                for child in children {
                    if let Some(entry) = in_degree.get_mut(child) {
                        *entry = entry.saturating_sub(1);
                        if *entry == 0 {
                            queue.push_back(child.clone());
                            sort_queue(&mut queue);
                        }
                    }
                }
            }
        }

        if order.len() != self.nodes.len() {
            return Err(DataFusionError::Plan(
                "Cycle detected in task dependency graph".to_string(),
            ));
        }
        self.topological_order = order;
        Ok(())
    }

    pub fn upstream_closure(&self, task_name: &str) -> HashSet<String> {
        let mut out = HashSet::new();
        let mut queue = VecDeque::new();
        queue.push_back(task_name.to_string());
        while let Some(node) = queue.pop_front() {
            if let Some(deps) = self.dependencies.get(&node) {
                for dep in deps {
                    if out.insert(dep.clone()) {
                        queue.push_back(dep.clone());
                    }
                }
            }
        }
        out
    }

    pub fn downstream_closure(&self, task_name: &str) -> HashSet<String> {
        let mut reverse: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
        for (node, deps) in &self.dependencies {
            for dep in deps {
                reverse.entry(dep.as_str()).or_default().push(node.as_str());
            }
        }
        let mut out = HashSet::new();
        let mut queue = VecDeque::new();
        queue.push_back(task_name.to_string());
        while let Some(node) = queue.pop_front() {
            if let Some(children) = reverse.get(node.as_str()) {
                for child in children {
                    if out.insert((*child).to_string()) {
                        queue.push_back((*child).to_string());
                    }
                }
            }
        }
        out
    }

    fn upsert_node(&mut self, name: &str, task_type: TaskType) {
        self.nodes
            .entry(name.to_string())
            .or_insert_with(|| TaskNode {
                name: name.to_string(),
                task_type,
                estimated_cost: 1.0,
            });
        self.dependencies.entry(name.to_string()).or_default();
    }

    fn set_dependencies(&mut self, name: &str, deps: &[String]) {
        let set = self.dependencies.entry(name.to_string()).or_default();
        set.extend(deps.iter().cloned());
    }

    fn reaches(&self, start: &str, target: &str) -> bool {
        if start == target {
            return true;
        }
        let mut visited = HashSet::new();
        let mut queue = VecDeque::new();
        queue.push_back(start.to_string());
        while let Some(node) = queue.pop_front() {
            if !visited.insert(node.clone()) {
                continue;
            }
            if let Some(deps) = self.dependencies.get(&node) {
                for dep in deps {
                    if dep == target {
                        return true;
                    }
                    queue.push_back(dep.clone());
                }
            }
        }
        false
    }
}

fn sort_queue(queue: &mut VecDeque<String>) {
    let mut values: Vec<String> = queue.drain(..).collect();
    values.sort();
    queue.extend(values);
}

/// Build a task graph from a semantic execution spec.
pub fn build_task_graph_from_spec(spec: &SemanticExecutionSpec) -> Result<TaskGraph> {
    let view_deps: Vec<(String, Vec<String>)> = spec
        .view_definitions
        .iter()
        .map(|view| (view.name.clone(), view.view_dependencies.clone()))
        .collect();
    let scan_deps: Vec<(String, Vec<String>)> = spec
        .input_relations
        .iter()
        .map(|input| (input.logical_name.clone(), Vec::new()))
        .collect();
    let output_deps: Vec<(String, Vec<String>)> = spec
        .output_targets
        .iter()
        .map(|target| (target.table_name.clone(), vec![target.source_view.clone()]))
        .collect();
    TaskGraph::from_inferred_deps(&view_deps, &scan_deps, &output_deps)
}

#[cfg(test)]
mod tests {
    use super::{derive_cache_policies, CachePolicyRequest};
    use std::collections::{BTreeMap, BTreeSet};

    #[test]
    fn cache_policy_derivation_is_deterministic() {
        let request = CachePolicyRequest {
            out_degree: BTreeMap::from([
                ("leaf".to_string(), 0usize),
                ("fanout".to_string(), 4usize),
                ("output".to_string(), 1usize),
            ]),
            outputs: BTreeSet::from(["output".to_string()]),
            cache_overrides: BTreeMap::new(),
            workload_class: Some("compile_replay".to_string()),
        };
        let decisions = derive_cache_policies(&request);
        assert_eq!(decisions[0].view_name, "fanout");
        assert_eq!(decisions[1].view_name, "leaf");
        assert_eq!(decisions[2].view_name, "output");
        let fanout = decisions
            .iter()
            .find(|item| item.view_name == "fanout")
            .expect("fanout decision");
        let leaf = decisions
            .iter()
            .find(|item| item.view_name == "leaf")
            .expect("leaf decision");
        let output = decisions
            .iter()
            .find(|item| item.view_name == "output")
            .expect("output decision");
        assert_eq!(fanout.policy, "none");
        assert_eq!(leaf.policy, "none");
        assert_eq!(output.policy, "delta_output");
    }

    #[test]
    fn cache_policy_overrides_take_precedence() {
        let request = CachePolicyRequest {
            out_degree: BTreeMap::from([("view".to_string(), 5usize)]),
            outputs: BTreeSet::new(),
            cache_overrides: BTreeMap::from([("view".to_string(), "delta_output".to_string())]),
            workload_class: None,
        };
        let decisions = derive_cache_policies(&request);
        assert_eq!(decisions.len(), 1);
        assert_eq!(decisions[0].policy, "delta_output");
        assert_eq!(decisions[0].rationale, "explicit_override");
        assert!((decisions[0].confidence - 1.0).abs() < f64::EPSILON);
    }
}
