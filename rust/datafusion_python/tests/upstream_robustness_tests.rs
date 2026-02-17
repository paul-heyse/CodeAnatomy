#[test]
fn expr_module_no_longer_exposes_signature_submodule() {
    let source = include_str!("../src/expr.rs");
    let pre_test_source = source.split("#[cfg(test)]").next().unwrap_or(source);
    assert!(
        !pre_test_source.contains("pub mod signature;"),
        "dead signature module should be removed"
    );
}

#[test]
fn dataset_schema_uses_fallback_instead_of_unwrap() {
    let source = include_str!("../src/dataset.rs");
    assert!(
        source.contains(
            "unwrap_or_else(|_| Arc::new(datafusion::arrow::datatypes::Schema::empty()))"
        ),
        "dataset schema should use a non-panicking fallback"
    );
}
