pub mod engine;
pub mod traits;
mod macros;

pub use engine::Engine;
pub use traits::{Compiler, Runnable};

make_metric!(cpu_usage);

pub fn compile_target(input: &str) -> String {
    let engine = Engine::new("core");
    engine.compile(input)
}

#[cfg(test)]
mod tests {
    use super::compile_target;

    #[test]
    fn smoke_compile_target() {
        assert_eq!(compile_target("a"), "core:a");
    }
}
