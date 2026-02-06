use std::collections::HashMap;

/// A graph builder for constructing CPG representations.
pub struct GraphBuilder {
    nodes: Vec<String>,
    edges: HashMap<String, Vec<String>>,
}

impl GraphBuilder {
    pub fn new() -> Self {
        GraphBuilder {
            nodes: Vec::new(),
            edges: HashMap::new(),
        }
    }

    pub fn add_node(&mut self, name: String) {
        self.nodes.push(name);
    }

    pub fn build(&self) -> String {
        format!("Graph with {} nodes", self.nodes.len())
    }
}

mod system {
    pub fn init() {
        println!("System initialized");
    }

    pub fn shutdown() {
        println!("System shutdown");
    }
}

fn helper(x: i32) -> i32 {
    x + 1
}

macro_rules! define_metric {
    ($name:ident) => {
        pub fn $name() -> u64 {
            0
        }
    };
}

define_metric!(cpu_usage);

fn main() {
    let mut builder = GraphBuilder::new();
    builder.add_node("root".to_string());
    let result = builder.build();
    println!("{}", result);
    assert_eq!(helper(1), 2);
    system::init();
}

enum NodeKind {
    Function,
    Class,
    Import,
}

trait Visitor {
    fn visit(&self, kind: &NodeKind);
}
