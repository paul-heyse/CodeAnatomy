use datafusion_ext::udf_builtin as core;
use crate::expr::PyExpr;
use pyo3::prelude::*;

#[pyfunction]
pub fn map_entries(expr: PyExpr) -> PyExpr {
    core::map_entries(expr.into()).into()
}

#[pyfunction]
pub fn map_keys(expr: PyExpr) -> PyExpr {
    core::map_keys(expr.into()).into()
}

#[pyfunction]
pub fn map_values(expr: PyExpr) -> PyExpr {
    core::map_values(expr.into()).into()
}

#[pyfunction]
pub fn map_extract(expr: PyExpr, key: &str) -> PyExpr {
    core::map_extract(expr.into(), key).into()
}

#[pyfunction]
pub fn list_extract(expr: PyExpr, index: i64) -> PyExpr {
    core::list_extract(expr.into(), index).into()
}

#[pyfunction]
pub fn list_unique(expr: PyExpr) -> PyExpr {
    core::list_unique(expr.into()).into()
}

#[pyfunction]
pub fn first_value_agg(expr: PyExpr) -> PyExpr {
    core::first_value_agg(expr.into()).into()
}

#[pyfunction]
pub fn last_value_agg(expr: PyExpr) -> PyExpr {
    core::last_value_agg(expr.into()).into()
}

#[pyfunction]
pub fn count_distinct_agg(expr: PyExpr) -> PyExpr {
    core::count_distinct_agg(expr.into()).into()
}

#[pyfunction]
pub fn string_agg(value: PyExpr, delimiter: PyExpr) -> PyExpr {
    core::string_agg(value.into(), delimiter.into()).into()
}

#[pyfunction]
pub fn row_number_window(value: PyExpr) -> PyExpr {
    core::row_number_window(value.into()).into()
}

#[pyfunction]
pub fn lag_window(expr: PyExpr) -> PyExpr {
    core::lag_window(expr.into()).into()
}

#[pyfunction]
pub fn lead_window(expr: PyExpr) -> PyExpr {
    core::lead_window(expr.into()).into()
}

#[pyfunction]
pub fn union_tag(expr: PyExpr) -> PyExpr {
    core::union_tag(expr.into()).into()
}

#[pyfunction]
pub fn union_extract(expr: PyExpr, tag: &str) -> PyExpr {
    core::union_extract(expr.into(), tag).into()
}
