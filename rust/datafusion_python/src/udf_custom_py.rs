use datafusion_expr::{lit, Expr};
use datafusion_ext::udf_custom as core;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyBytesMethods, PyTuple};

use crate::context::PySessionContext;
use crate::expr::PyExpr;

#[pyfunction]
pub fn install_function_factory(
    ctx: PyRef<PySessionContext>,
    policy_ipc: &Bound<'_, PyBytes>,
) -> PyResult<()> {
    core::install_function_factory_native(&ctx.ctx, policy_ipc.as_bytes())
        .map_err(|err| PyRuntimeError::new_err(format!("FunctionFactory install failed: {err}")))
}

#[pyfunction]
#[pyo3(signature = (expr, key=None))]
pub fn arrow_metadata(expr: PyExpr, key: Option<&str>) -> PyExpr {
    let udf = core::arrow_metadata_udf();
    let mut args = vec![expr.into()];
    if let Some(key) = key {
        args.push(lit(key));
    }
    udf.call(args).into()
}

#[pyfunction]
pub fn stable_hash64(value: PyExpr) -> PyExpr {
    core::stable_hash64_udf().call(vec![value.into()]).into()
}

#[pyfunction]
pub fn stable_hash128(value: PyExpr) -> PyExpr {
    core::stable_hash128_udf().call(vec![value.into()]).into()
}

#[pyfunction]
pub fn prefixed_hash64(prefix: &str, value: PyExpr) -> PyExpr {
    core::prefixed_hash64_udf()
        .call(vec![lit(prefix), value.into()])
        .into()
}

#[pyfunction]
pub fn stable_id(prefix: &str, value: PyExpr) -> PyExpr {
    core::stable_id_udf().call(vec![lit(prefix), value.into()]).into()
}

#[pyfunction]
pub fn semantic_tag(semantic_type: &str, value: PyExpr) -> PyExpr {
    core::semantic_tag_udf()
        .call(vec![lit(semantic_type), value.into()])
        .into()
}

fn push_optional_expr(args: &mut Vec<Expr>, value: Option<PyExpr>) {
    if let Some(expr) = value {
        args.push(expr.into());
    }
}

fn extend_expr_args_from_tuple(args: &mut Vec<Expr>, parts: &Bound<'_, PyTuple>) -> PyResult<()> {
    for item in parts.iter() {
        let expr: PyExpr = item.extract()?;
        args.push(expr.into());
    }
    Ok(())
}

#[pyfunction]
#[pyo3(signature = (prefix, part1, *parts))]
pub fn stable_id_parts(
    prefix: &str,
    part1: PyExpr,
    parts: &Bound<'_, PyTuple>,
) -> PyResult<PyExpr> {
    let mut args: Vec<Expr> = vec![lit(prefix), part1.into()];
    extend_expr_args_from_tuple(&mut args, parts)?;
    Ok(core::stable_id_parts_udf().call(args).into())
}

#[pyfunction]
#[pyo3(signature = (prefix, part1, *parts))]
pub fn prefixed_hash_parts64(
    prefix: &str,
    part1: PyExpr,
    parts: &Bound<'_, PyTuple>,
) -> PyResult<PyExpr> {
    let mut args: Vec<Expr> = vec![lit(prefix), part1.into()];
    extend_expr_args_from_tuple(&mut args, parts)?;
    Ok(core::prefixed_hash_parts64_udf().call(args).into())
}

#[pyfunction]
#[pyo3(signature = (value, canonical=None, null_sentinel=None))]
pub fn stable_hash_any(
    value: PyExpr,
    canonical: Option<bool>,
    null_sentinel: Option<&str>,
) -> PyExpr {
    let mut args: Vec<Expr> = vec![value.into()];
    if let Some(flag) = canonical {
        args.push(lit(flag));
    }
    if let Some(sentinel) = null_sentinel {
        args.push(lit(sentinel));
    }
    core::stable_hash_any_udf().call(args).into()
}

#[pyfunction]
#[pyo3(signature = (bstart, bend, line_base=None, col_unit=None, end_exclusive=None))]
pub fn span_make(
    bstart: PyExpr,
    bend: PyExpr,
    line_base: Option<PyExpr>,
    col_unit: Option<PyExpr>,
    end_exclusive: Option<PyExpr>,
) -> PyExpr {
    let mut args: Vec<Expr> = vec![bstart.into(), bend.into()];
    push_optional_expr(&mut args, line_base);
    push_optional_expr(&mut args, col_unit);
    push_optional_expr(&mut args, end_exclusive);
    core::span_make_udf().call(args).into()
}

#[pyfunction]
pub fn span_len(span: PyExpr) -> PyExpr {
    core::span_len_udf().call(vec![span.into()]).into()
}

#[pyfunction]
pub fn span_overlaps(span_a: PyExpr, span_b: PyExpr) -> PyExpr {
    core::span_overlaps_udf()
        .call(vec![span_a.into(), span_b.into()])
        .into()
}

#[pyfunction]
pub fn span_contains(span_a: PyExpr, span_b: PyExpr) -> PyExpr {
    core::span_contains_udf()
        .call(vec![span_a.into(), span_b.into()])
        .into()
}

#[pyfunction]
pub fn interval_align_score(
    left_start: PyExpr,
    left_end: PyExpr,
    right_start: PyExpr,
    right_end: PyExpr,
) -> PyExpr {
    core::interval_align_score_udf()
        .call(vec![
            left_start.into(),
            left_end.into(),
            right_start.into(),
            right_end.into(),
        ])
        .into()
}

#[pyfunction]
#[pyo3(signature = (prefix, path, bstart, bend, kind=None))]
pub fn span_id(
    prefix: &str,
    path: PyExpr,
    bstart: PyExpr,
    bend: PyExpr,
    kind: Option<PyExpr>,
) -> PyExpr {
    let mut args: Vec<Expr> = vec![lit(prefix), path.into(), bstart.into(), bend.into()];
    push_optional_expr(&mut args, kind);
    core::span_id_udf().call(args).into()
}

#[pyfunction]
#[pyo3(signature = (value, form=None, casefold=None, collapse_ws=None))]
pub fn utf8_normalize(
    value: PyExpr,
    form: Option<&str>,
    casefold: Option<bool>,
    collapse_ws: Option<bool>,
) -> PyExpr {
    let mut args: Vec<Expr> = vec![value.into()];
    if let Some(form) = form {
        args.push(lit(form));
    }
    if let Some(flag) = casefold {
        args.push(lit(flag));
    }
    if let Some(flag) = collapse_ws {
        args.push(lit(flag));
    }
    core::utf8_normalize_udf().call(args).into()
}

#[pyfunction]
pub fn utf8_null_if_blank(value: PyExpr) -> PyExpr {
    core::utf8_null_if_blank_udf().call(vec![value.into()]).into()
}

#[pyfunction]
#[pyo3(signature = (symbol, module=None, lang=None))]
pub fn qname_normalize(symbol: PyExpr, module: Option<PyExpr>, lang: Option<PyExpr>) -> PyExpr {
    let mut args: Vec<Expr> = vec![symbol.into()];
    push_optional_expr(&mut args, module);
    push_optional_expr(&mut args, lang);
    core::qname_normalize_udf().call(args).into()
}

#[pyfunction]
pub fn map_get_default(map_expr: PyExpr, key: &str, default_value: PyExpr) -> PyExpr {
    core::map_get_default_udf()
        .call(vec![map_expr.into(), lit(key), default_value.into()])
        .into()
}

#[pyfunction]
#[pyo3(signature = (map_expr, key_case=None, sort_keys=None))]
pub fn map_normalize(map_expr: PyExpr, key_case: Option<&str>, sort_keys: Option<bool>) -> PyExpr {
    let mut args: Vec<Expr> = vec![map_expr.into()];
    if let Some(key_case) = key_case {
        args.push(lit(key_case));
    }
    if let Some(sort_keys) = sort_keys {
        args.push(lit(sort_keys));
    }
    core::map_normalize_udf().call(args).into()
}

#[pyfunction]
pub fn list_compact(list_expr: PyExpr) -> PyExpr {
    core::list_compact_udf().call(vec![list_expr.into()]).into()
}

#[pyfunction]
pub fn list_unique_sorted(list_expr: PyExpr) -> PyExpr {
    core::list_unique_sorted_udf().call(vec![list_expr.into()]).into()
}

#[pyfunction]
#[pyo3(signature = (
    struct_expr,
    field1,
    field2=None,
    field3=None,
    field4=None,
    field5=None,
    field6=None
))]
pub fn struct_pick(
    struct_expr: PyExpr,
    field1: &str,
    field2: Option<&str>,
    field3: Option<&str>,
    field4: Option<&str>,
    field5: Option<&str>,
    field6: Option<&str>,
) -> PyExpr {
    let mut args: Vec<Expr> = vec![struct_expr.into(), lit(field1)];
    if let Some(field) = field2 {
        args.push(lit(field));
    }
    if let Some(field) = field3 {
        args.push(lit(field));
    }
    if let Some(field) = field4 {
        args.push(lit(field));
    }
    if let Some(field) = field5 {
        args.push(lit(field));
    }
    if let Some(field) = field6 {
        args.push(lit(field));
    }
    core::struct_pick_udf().call(args).into()
}

#[pyfunction]
pub fn cdf_change_rank(change_type: PyExpr) -> PyExpr {
    core::cdf_change_rank_udf().call(vec![change_type.into()]).into()
}

#[pyfunction]
pub fn cdf_is_upsert(change_type: PyExpr) -> PyExpr {
    core::cdf_is_upsert_udf().call(vec![change_type.into()]).into()
}

#[pyfunction]
pub fn cdf_is_delete(change_type: PyExpr) -> PyExpr {
    core::cdf_is_delete_udf().call(vec![change_type.into()]).into()
}

#[pyfunction]
pub fn col_to_byte(line_text: PyExpr, col_index: PyExpr, col_unit: PyExpr) -> PyExpr {
    core::col_to_byte_udf()
        .call(vec![line_text.into(), col_index.into(), col_unit.into()])
        .into()
}
