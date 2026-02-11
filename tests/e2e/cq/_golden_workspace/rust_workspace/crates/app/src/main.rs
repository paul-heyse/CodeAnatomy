use app::{compile_for_app, run_pipeline};

fn main() {
    let compiled = compile_for_app("entry");
    println!("{}", compiled);
    println!("{}", run_pipeline());
}
