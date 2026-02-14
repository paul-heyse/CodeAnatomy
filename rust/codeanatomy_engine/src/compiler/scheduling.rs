//! Task graph and scheduling primitives for execution planning.

use std::collections::{BTreeMap, BTreeSet, HashSet, VecDeque};

use datafusion_common::{DataFusionError, Result};
use serde::{Deserialize, Serialize};

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
        let mut order = Vec::with_capacity(self.nodes.len());

        while let Some(node) = queue.pop_front() {
            order.push(node.clone());
            if let Some(children) = outgoing.get(&node) {
                for child in children {
                    if let Some(entry) = in_degree.get_mut(child) {
                        *entry = entry.saturating_sub(1);
                        if *entry == 0 {
                            queue.push_back(child.clone());
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
