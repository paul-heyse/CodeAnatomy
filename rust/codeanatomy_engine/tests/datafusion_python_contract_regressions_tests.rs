#[test]
fn logical_wrapper_modules_use_shared_macro() {
    for source in [
        include_str!("../../datafusion_python/src/expr/filter.rs"),
        include_str!("../../datafusion_python/src/expr/projection.rs"),
        include_str!("../../datafusion_python/src/expr/limit.rs"),
        include_str!("../../datafusion_python/src/expr/create_view.rs"),
        include_str!("../../datafusion_python/src/expr/drop_view.rs"),
        include_str!("../../datafusion_python/src/expr/join.rs"),
        include_str!("../../datafusion_python/src/expr/union.rs"),
        include_str!("../../datafusion_python/src/expr/sort.rs"),
    ] {
        assert!(
            source.contains("logical_plan_wrapper!("),
            "expected logical_plan_wrapper! cutover for logical wrappers"
        );
    }
}

#[test]
fn expr_to_variant_includes_scalar_and_window_function_paths() {
    let source = include_str!("../../datafusion_python/src/expr.rs");
    let compact: String = source.chars().filter(|ch| !ch.is_whitespace()).collect();

    assert!(
        compact.contains("Expr::ScalarFunction(value)=>Ok(PyExpr::from(Expr::ScalarFunction(value.clone())).into_bound_py_any(py)?)"),
        "to_variant should convert ScalarFunction via PyExpr wrapper"
    );
    assert!(
        compact.contains("Expr::WindowFunction(value)=>Ok(PyExpr::from(Expr::WindowFunction(value.clone())).into_bound_py_any(py)?)"),
        "to_variant should convert WindowFunction via PyExpr wrapper"
    );
}

#[test]
fn expr_module_no_longer_exposes_signature_submodule() {
    let source = include_str!("../../datafusion_python/src/expr.rs");
    let pre_test_source = source.split("#[cfg(test)]").next().unwrap_or(source);
    assert!(
        !pre_test_source.contains("pub mod signature;"),
        "dead signature module should remain removed"
    );
}

#[test]
fn data_type_map_contracts_are_preserved() {
    let source = include_str!("../../datafusion_python/src/common/data_type.rs");
    let compact: String = source.chars().filter(|ch| !ch.is_whitespace()).collect();

    assert!(
        compact.contains("ScalarValue::Utf8(_)=>Ok(DataType::Utf8)"),
        "Utf8 scalar mapping should remain DataType::Utf8"
    );
    assert!(
        compact.contains("ScalarValue::Union(_,_,_)=>Err(PyNotImplementedError::new_err(\"ScalarValue::Union\".to_string(),))"),
        "Union branch should emit ScalarValue::Union label"
    );
    assert!(
        !compact.contains("ScalarValue::Union(_,_,_)=>Err(PyNotImplementedError::new_err(\"ScalarValue::LargeList\".to_string(),))"),
        "Union branch must not be mislabeled as ScalarValue::LargeList"
    );
}

#[test]
fn dataset_schema_uses_non_panicking_fallback() {
    let source = include_str!("../../datafusion_python/src/dataset.rs");
    assert!(
        source.contains(
            "unwrap_or_else(|_| Arc::new(datafusion::arrow::datatypes::Schema::empty()))"
        ),
        "dataset schema should use a non-panicking fallback"
    );
}
