//! Function metadata contracts for registry projection.

use crate::function_types::FunctionKind;

#[derive(Debug, Clone)]
pub struct FunctionMetadata {
    pub name: &'static str,
    pub kind: FunctionKind,
    pub rewrite_tags: &'static [&'static str],
    pub has_simplify: bool,
    pub has_coerce_types: bool,
    pub has_short_circuits: bool,
    pub has_groups_accumulator: bool,
    pub has_retract_batch: bool,
    pub has_reverse_expr: bool,
    pub has_sort_options: bool,
}

pub trait MetadataProvider {
    fn metadata(&self) -> FunctionMetadata;
}
