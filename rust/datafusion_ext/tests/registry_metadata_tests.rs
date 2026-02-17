use datafusion_ext::function_types::FunctionKind;
use datafusion_ext::registry::metadata::{FunctionMetadata, MetadataProvider};

struct DummyProvider;

impl MetadataProvider for DummyProvider {
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            name: "dummy_scalar",
            kind: FunctionKind::Scalar,
            rewrite_tags: &["dummy"],
            has_simplify: true,
            has_coerce_types: true,
            has_short_circuits: false,
            has_groups_accumulator: false,
            has_retract_batch: false,
            has_reverse_expr: false,
            has_sort_options: false,
        }
    }
}

#[test]
fn metadata_provider_contract_exposes_expected_fields() {
    let provider = DummyProvider;
    let metadata = provider.metadata();
    assert_eq!(metadata.name, "dummy_scalar");
    assert_eq!(metadata.kind, FunctionKind::Scalar);
    assert_eq!(metadata.rewrite_tags, &["dummy"]);
    assert!(metadata.has_simplify);
    assert!(metadata.has_coerce_types);
}
