use crate::traits::{Compiler, Runnable};

pub struct Engine {
    pub target: String,
}

impl Engine {
    pub fn new(target: &str) -> Self {
        Self {
            target: target.to_string(),
        }
    }

    pub fn helper(&self, input: &str) -> String {
        format!("{}:{}", self.target, input)
    }
}

impl Compiler for Engine {
    fn compile(&self, input: &str) -> String {
        self.helper(input)
    }
}

impl Runnable for Engine {
    fn run(&self) -> String {
        self.compile("run")
    }
}
