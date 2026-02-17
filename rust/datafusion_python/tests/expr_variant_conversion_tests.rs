#[test]
fn to_variant_includes_scalar_function_conversion() {
    let source = include_str!("../src/expr.rs");
    let compact: String = source.chars().filter(|ch| !ch.is_whitespace()).collect();

    assert!(
        compact.contains("Expr::ScalarFunction(value)=>Ok(PyExpr::from(Expr::ScalarFunction(value.clone())).into_bound_py_any(py)?)"),
        "to_variant should convert ScalarFunction via PyExpr wrapper"
    );
}

#[test]
fn to_variant_includes_window_function_conversion() {
    let source = include_str!("../src/expr.rs");
    let compact: String = source.chars().filter(|ch| !ch.is_whitespace()).collect();

    assert!(
        compact.contains("Expr::WindowFunction(value)=>Ok(PyExpr::from(Expr::WindowFunction(value.clone())).into_bound_py_any(py)?)"),
        "to_variant should convert WindowFunction via PyExpr wrapper"
    );
}
