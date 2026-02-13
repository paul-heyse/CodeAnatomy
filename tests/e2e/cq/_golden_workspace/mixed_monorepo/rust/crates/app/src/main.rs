use corelib::invoke_compile;

fn main() {
    let rendered = invoke_compile("fixture");
    println!("{rendered}");
}
