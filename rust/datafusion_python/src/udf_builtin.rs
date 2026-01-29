use datafusion_expr::lit;
use datafusion_functions::core::expr_fn as core_expr_fn;
use datafusion_functions_aggregate::expr_fn as agg_expr_fn;
use datafusion_functions_aggregate::string_agg::string_agg as string_agg_fn;
use datafusion_functions_nested::expr_fn as nested_expr_fn;
use datafusion_functions_window::expr_fn as window_expr_fn;
use crate::expr::PyExpr;
use pyo3::prelude::*;

#[pyfunction]
pub fn map_entries(expr: PyExpr) -> PyExpr {
    nested_expr_fn::map_entries(expr.into()).into()
}

#[pyfunction]
pub fn map_keys(expr: PyExpr) -> PyExpr {
    nested_expr_fn::map_keys(expr.into()).into()
}

#[pyfunction]
pub fn map_values(expr: PyExpr) -> PyExpr {
    nested_expr_fn::map_values(expr.into()).into()
}

#[pyfunction]
pub fn map_extract(expr: PyExpr, key: &str) -> PyExpr {
    nested_expr_fn::map_extract(expr.into(), lit(key)).into()
}

#[pyfunction]
pub fn list_extract(expr: PyExpr, index: i64) -> PyExpr {
    nested_expr_fn::array_element(expr.into(), lit(index)).into()
}

#[pyfunction]
pub fn list_unique(expr: PyExpr) -> PyExpr {
    let agg = agg_expr_fn::array_agg(expr.into());
    nested_expr_fn::array_distinct(agg).into()
}

#[pyfunction]
pub fn first_value_agg(expr: PyExpr) -> PyExpr {
    agg_expr_fn::first_value(expr.into(), Vec::new()).into()
}

#[pyfunction]
pub fn last_value_agg(expr: PyExpr) -> PyExpr {
    agg_expr_fn::last_value(expr.into(), Vec::new()).into()
}

#[pyfunction]
pub fn count_distinct_agg(expr: PyExpr) -> PyExpr {
    agg_expr_fn::count_distinct(expr.into()).into()
}

#[pyfunction]
pub fn string_agg(value: PyExpr, delimiter: PyExpr) -> PyExpr {
    string_agg_fn(value.into(), delimiter.into()).into()
}

#[pyfunction]
pub fn row_number_window(_value: PyExpr) -> PyExpr {
    window_expr_fn::row_number().into()
}

#[pyfunction]
pub fn lag_window(expr: PyExpr) -> PyExpr {
    window_expr_fn::lag(expr.into(), None, None).into()
}

#[pyfunction]
pub fn lead_window(expr: PyExpr) -> PyExpr {
    window_expr_fn::lead(expr.into(), None, None).into()
}

#[pyfunction]
pub fn union_tag(expr: PyExpr) -> PyExpr {
    core_expr_fn::union_tag(expr.into()).into()
}

#[pyfunction]
pub fn union_extract(expr: PyExpr, tag: &str) -> PyExpr {
    core_expr_fn::union_extract(expr.into(), tag).into()
}
