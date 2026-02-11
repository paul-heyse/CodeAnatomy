// Intentional diagnostics fixture for rust-analyzer style checks.

pub fn borrow_diagnostics() -> usize {
    let data = String::from("owned");
    let first = &data;
    let second = data;
    first.len() + second.len()
}
