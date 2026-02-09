use datafusion::config::ConfigOptions;
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr::registry::FunctionRegistry;
use datafusion_expr::{lit, Expr};
use datafusion_functions::core::expr_fn as core_expr_fn;
use datafusion_functions_aggregate::expr_fn as agg_expr_fn;
use datafusion_functions_aggregate::string_agg::string_agg as string_agg_fn;
use datafusion_functions_nested::expr_fn as nested_expr_fn;
use datafusion_functions_window::expr_fn as window_expr_fn;

use crate::udf_registry;

fn require_arg(args: &[Expr], idx: usize, name: &str) -> Result<Expr> {
    args.get(idx).cloned().ok_or_else(|| {
        DataFusionError::Plan(format!("{name} expects at least {} arguments", idx + 1))
    })
}

fn ensure_exact_args(args: &[Expr], expected: usize, name: &str) -> Result<()> {
    if args.len() != expected {
        return Err(DataFusionError::Plan(format!(
            "{name} expects {expected} arguments"
        )));
    }
    Ok(())
}

fn extract_literal_string(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Literal(ScalarValue::Utf8(Some(value)), _)
        | Expr::Literal(ScalarValue::LargeUtf8(Some(value)), _)
        | Expr::Literal(ScalarValue::Utf8View(Some(value)), _) => Some(value.clone()),
        _ => None,
    }
}

fn extract_literal_i64(expr: &Expr) -> Option<i64> {
    match expr {
        Expr::Literal(ScalarValue::Int64(Some(value)), _) => Some(*value),
        Expr::Literal(ScalarValue::Int32(Some(value)), _) => Some(*value as i64),
        Expr::Literal(ScalarValue::Int16(Some(value)), _) => Some(*value as i64),
        Expr::Literal(ScalarValue::Int8(Some(value)), _) => Some(*value as i64),
        Expr::Literal(ScalarValue::UInt64(Some(value)), _) => Some(*value as i64),
        Expr::Literal(ScalarValue::UInt32(Some(value)), _) => Some(*value as i64),
        Expr::Literal(ScalarValue::UInt16(Some(value)), _) => Some(*value as i64),
        Expr::Literal(ScalarValue::UInt8(Some(value)), _) => Some(*value as i64),
        _ => None,
    }
}

fn literal_string_arg(args: &[Expr], idx: usize, name: &str) -> Result<String> {
    let expr = require_arg(args, idx, name)?;
    extract_literal_string(&expr).ok_or_else(|| {
        DataFusionError::Plan(format!(
            "{name} expects argument {} to be a string literal",
            idx + 1
        ))
    })
}

fn literal_i64_arg(args: &[Expr], idx: usize, name: &str) -> Result<i64> {
    let expr = require_arg(args, idx, name)?;
    extract_literal_i64(&expr).ok_or_else(|| {
        DataFusionError::Plan(format!(
            "{name} expects argument {} to be an integer literal",
            idx + 1
        ))
    })
}

fn resolve_scalar_udf(name: &str) -> Option<udf_registry::ScalarUdfSpec> {
    udf_registry::scalar_udf_specs()
        .into_iter()
        .find(|spec| spec.name == name || spec.aliases.contains(&name))
}

pub fn expr_from_name(name: &str, args: Vec<Expr>, config: Option<&ConfigOptions>) -> Result<Expr> {
    match name {
        "map_entries" => {
            ensure_exact_args(&args, 1, name)?;
            Ok(nested_expr_fn::map_entries(args[0].clone()))
        }
        "map_keys" => {
            ensure_exact_args(&args, 1, name)?;
            Ok(nested_expr_fn::map_keys(args[0].clone()))
        }
        "map_values" => {
            ensure_exact_args(&args, 1, name)?;
            Ok(nested_expr_fn::map_values(args[0].clone()))
        }
        "map_extract" => {
            ensure_exact_args(&args, 2, name)?;
            let key = literal_string_arg(&args, 1, name)?;
            Ok(nested_expr_fn::map_extract(args[0].clone(), lit(key)))
        }
        "list_extract" => {
            ensure_exact_args(&args, 2, name)?;
            let index = literal_i64_arg(&args, 1, name)?;
            Ok(nested_expr_fn::array_element(args[0].clone(), lit(index)))
        }
        "list_unique" => {
            ensure_exact_args(&args, 1, name)?;
            let agg = agg_expr_fn::array_agg(args[0].clone());
            Ok(nested_expr_fn::array_distinct(agg))
        }
        "first_value_agg" => {
            ensure_exact_args(&args, 1, name)?;
            Ok(agg_expr_fn::first_value(args[0].clone(), Vec::new()))
        }
        "last_value_agg" => {
            ensure_exact_args(&args, 1, name)?;
            Ok(agg_expr_fn::last_value(args[0].clone(), Vec::new()))
        }
        "count_distinct_agg" => {
            ensure_exact_args(&args, 1, name)?;
            Ok(agg_expr_fn::count_distinct(args[0].clone()))
        }
        "string_agg" => {
            ensure_exact_args(&args, 2, name)?;
            Ok(string_agg_fn(args[0].clone(), args[1].clone()))
        }
        "row_number_window" => {
            if args.is_empty() {
                return Err(DataFusionError::Plan(
                    "row_number_window expects at least 1 argument".to_string(),
                ));
            }
            Ok(window_expr_fn::row_number())
        }
        "lag_window" => {
            ensure_exact_args(&args, 1, name)?;
            Ok(window_expr_fn::lag(args[0].clone(), None, None))
        }
        "lead_window" => {
            ensure_exact_args(&args, 1, name)?;
            Ok(window_expr_fn::lead(args[0].clone(), None, None))
        }
        "union_tag" => {
            ensure_exact_args(&args, 1, name)?;
            Ok(core_expr_fn::union_tag(args[0].clone()))
        }
        "union_extract" => {
            ensure_exact_args(&args, 2, name)?;
            let tag = literal_string_arg(&args, 1, name)?;
            Ok(core_expr_fn::union_extract(args[0].clone(), tag.as_str()))
        }
        _ => {
            let Some(spec) = resolve_scalar_udf(name) else {
                return Err(DataFusionError::Plan(format!("Unknown scalar UDF: {name}")));
            };
            let mut udf = (spec.builder)();
            if let Some(config) = config {
                if let Some(updated) = udf.inner().with_updated_config(config) {
                    udf = updated;
                }
            }
            if !spec.aliases.is_empty() {
                udf = udf.with_aliases(spec.aliases.iter().copied());
            }
            Ok(udf.call(args))
        }
    }
}

pub fn expr_from_registry_or_specs(
    registry: &dyn FunctionRegistry,
    name: &str,
    args: Vec<Expr>,
    config: Option<&ConfigOptions>,
) -> Result<Expr> {
    if let Ok(udf) = registry.udf(name) {
        return Ok(udf.call(args));
    }
    expr_from_name(name, args, config)
}
