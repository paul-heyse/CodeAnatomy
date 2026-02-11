use corelib::{compile_target, Compiler, Engine, Runnable};

pub fn compile_for_app(input: &str) -> String {
    let engine = Engine::new("app");
    engine.compile(input)
}

pub fn run_pipeline() -> String {
    let engine = Engine::new("app");
    format!("{}:{}", compile_target("seed"), engine.run())
}
