pub struct ResolverProbe;

impl ResolverProbe {
    pub fn cq_rust_target_symbol(&self) -> usize {
        1
    }
}

pub fn cq_rust_target_symbol() -> usize {
    2
}
