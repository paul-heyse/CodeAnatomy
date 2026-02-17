use datafusion_python::datafusion::logical_expr::lit;
use datafusion_python::expr::bool_expr::{
    PyIsFalse, PyIsNotFalse, PyIsNotNull, PyIsNotTrue, PyIsNotUnknown, PyIsNull, PyIsTrue,
    PyIsUnknown, PyNegative, PyNot,
};

#[test]
fn unary_wrapper_macro_builders_construct_types() {
    let expr = lit(1);
    let _ = PyNot::new(expr.clone());
    let _ = PyIsNotNull::new(expr.clone());
    let _ = PyIsNull::new(expr.clone());
    let _ = PyIsTrue::new(expr.clone());
    let _ = PyIsFalse::new(expr.clone());
    let _ = PyIsUnknown::new(expr.clone());
    let _ = PyIsNotTrue::new(expr.clone());
    let _ = PyIsNotFalse::new(expr.clone());
    let _ = PyIsNotUnknown::new(expr.clone());
    let _ = PyNegative::new(expr);
}

#[test]
fn logical_wrapper_macro_is_used_in_target_modules() {
    for source in [
        include_str!("../src/expr/filter.rs"),
        include_str!("../src/expr/projection.rs"),
        include_str!("../src/expr/limit.rs"),
        include_str!("../src/expr/create_view.rs"),
        include_str!("../src/expr/drop_view.rs"),
        include_str!("../src/expr/join.rs"),
        include_str!("../src/expr/union.rs"),
        include_str!("../src/expr/sort.rs"),
    ] {
        assert!(
            source.contains("logical_plan_wrapper!("),
            "expected logical_plan_wrapper! macro usage"
        );
    }
}
