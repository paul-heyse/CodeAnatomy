pub trait Compiler {
    fn compile(&self, input: &str) -> String;
}

pub struct CoreCompiler;

impl Compiler for CoreCompiler {
    fn compile(&self, input: &str) -> String {
        format!("core:{input}")
    }
}

pub(crate) fn compile_target(input: &str) -> String {
    CoreCompiler.compile(input)
}

#[macro_export]
macro_rules! forward_compile {
    ($value:expr) => {
        crate::compile_target($value)
    };
}

pub fn invoke_compile(input: &str) -> String {
    compile_target(input)
}
