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
