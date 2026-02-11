pub fn mixed_symbol() -> &'static str {
    "rust-mixed"
}

pub fn resolve_name(name: &str) -> String {
    format!("rs:{}", name)
}
