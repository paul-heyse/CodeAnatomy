pub trait Compiler {
    fn compile(&self, input: &str) -> String;
}

pub trait Runnable {
    fn run(&self) -> String;
}
