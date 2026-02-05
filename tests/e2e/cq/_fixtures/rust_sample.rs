use std::fmt::Debug;

struct GraphBuilder;

impl GraphBuilder {
    fn build_graph(&self, value: i32) -> i32 {
        helper(value)
    }
}

fn helper(value: i32) -> i32 {
    value + 1
}
